// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Variable-length (STRING / BYTES) GROUP BY keys — arena-backed (Task #53;
//! design doc §23 foundation step 4b).
//!
//! String keys aren't `Copy` and don't fit the fixed-width `GroupByBackend<K>`
//! / `CombineDriver<K>` path, and naïvely keying a hash map on `Vec<u8>` would
//! heap-allocate **per distinct key** — the exact GC-pressure / allocation
//! churn this project exists to remove (§8.7). Instead, the SOTA approach
//! (DuckDB string heap, ClickHouse `Arena` + `StringRef`, DataFusion
//! `GroupValuesBytes`):
//!
//! * **One arena** — a single growable `Vec<u8>` holding every distinct key's
//!   bytes concatenated. Each key is interned **once**.
//! * **`(offset, len)` references** — a parallel `Vec<(u32, u32)>` indexed by
//!   `group_id` points into the arena. No per-key `Vec`/`String` allocation.
//! * **Hash table over the arena** — a `hashbrown::hash_table::HashTable<(group_id,
//!   hash)>` keyed by `hash_bytes`, confirming collisions by comparing the
//!   arena bytes. Same raw-API + cached-hash pattern as [`crate::HashbrownTable`].
//!
//! Group ids are dense + insertion-ordered, so the per-group [`AggState`]
//! accumulators stay compact, exactly like the fixed-width path.
//!
//! A future optimization (not yet done): inline small strings (≤ ~15 bytes)
//! directly in the slot to skip the arena indirection on the hot path
//! (DuckDB / Umbra "German strings").

use hashbrown::hash_table::HashTable;
use rayon::prelude::*;

use crate::agg::{AggKind, AggState};
use crate::hash::hash_bytes;
use crate::topk::{compute_selection, OrderTerm};

/// Interning table: distinct byte key → dense `group_id`, backed by one arena.
pub struct StringTable {
    /// `(group_id, cached_hash)` entries; probe by hash, confirm by arena bytes.
    map: HashTable<(u32, u64)>,
    /// All interned key bytes, concatenated.
    arena: Vec<u8>,
    /// `offsets[group_id] = (arena_start, len)`.
    offsets: Vec<(u32, u32)>,
}

impl Default for StringTable {
    fn default() -> Self {
        Self::new()
    }
}

impl StringTable {
    pub fn new() -> Self {
        Self {
            map: HashTable::new(),
            arena: Vec::new(),
            offsets: Vec::new(),
        }
    }

    /// Pre-size for `expected_groups` distinct keys averaging `avg_key_len`
    /// bytes — reserves both the table and the arena to avoid resizes.
    pub fn with_capacity(expected_groups: usize, avg_key_len: usize) -> Self {
        Self {
            map: HashTable::with_capacity(expected_groups),
            arena: Vec::with_capacity(expected_groups * avg_key_len),
            offsets: Vec::with_capacity(expected_groups),
        }
    }

    /// Number of distinct keys interned.
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Total bytes held in the arena.
    #[inline]
    pub fn arena_len(&self) -> usize {
        self.arena.len()
    }

    /// Bytes of the key that allocated `group_id`.
    #[inline]
    pub fn bytes_at(&self, group_id: u32) -> &[u8] {
        let (start, len) = self.offsets[group_id as usize];
        &self.arena[start as usize..(start + len) as usize]
    }

    /// `key`'s `group_id` without inserting, or `None`.
    pub fn get(&self, key: &[u8]) -> Option<u32> {
        let hash = hash_bytes(key);
        self.map
            .find(hash, |&(g, h)| h == hash && self.bytes_at(g) == key)
            .map(|&(g, _)| g)
    }

    /// `key`'s `group_id`, interning it (appending to the arena, assigning the
    /// next dense id) if unseen.
    pub fn probe_or_insert(&mut self, key: &[u8]) -> u32 {
        let hash = hash_bytes(key);
        // Find phase (immutable borrows of map + arena/offsets — both shared).
        if let Some(&(g, _)) = self
            .map
            .find(hash, |&(g, h)| h == hash && self.bytes_at(g) == key)
        {
            return g;
        }
        // Insert phase: intern the key bytes once, assign a dense id.
        let g = self.offsets.len() as u32;
        let start = self.arena.len() as u32;
        self.arena.extend_from_slice(key);
        self.offsets.push((start, key.len() as u32));
        self.map.insert_unique(hash, (g, hash), |&(_, h)| h);
        g
    }
}

/// Cross-segment combine for STRING/BYTES grouping keys. Mirrors
/// [`crate::CombineDriver`] but keyed by interned byte strings (a [`StringTable`])
/// rather than a `Copy` fixed-width key.
pub struct StringCombineDriver {
    table: StringTable,
    aggs: Vec<AggState>,
}

impl StringCombineDriver {
    pub fn new(agg_kinds: &[AggKind]) -> Self {
        Self {
            table: StringTable::new(),
            aggs: agg_kinds.iter().map(|&k| AggState::new_for(k, 0)).collect(),
        }
    }

    pub fn with_capacity(agg_kinds: &[AggKind], expected_groups: usize, avg_key_len: usize) -> Self {
        Self {
            table: StringTable::with_capacity(expected_groups, avg_key_len),
            aggs: agg_kinds
                .iter()
                .map(|&k| AggState::new_for(k, expected_groups))
                .collect(),
        }
    }

    #[inline]
    pub fn num_groups(&self) -> usize {
        self.table.len()
    }

    #[inline]
    pub fn num_aggs(&self) -> usize {
        self.aggs.len()
    }

    /// The key bytes for combined group `g` (for result materialization).
    #[inline]
    pub fn key_bytes(&self, g: u32) -> &[u8] {
        self.table.bytes_at(g)
    }

    #[inline]
    pub fn agg_state(&self, idx: usize) -> &AggState {
        &self.aggs[idx]
    }

    /// Merge a single source group `(key, src_aggs[*][src_idx])`.
    pub fn merge_one(&mut self, key: &[u8], src_aggs: &[AggState], src_idx: usize) {
        let prev_len = self.table.len();
        let cg = self.table.probe_or_insert(key) as usize;
        if cg == prev_len {
            for a in self.aggs.iter_mut() {
                a.push_new_group();
            }
        }
        for (i, a) in self.aggs.iter_mut().enumerate() {
            a.merge_slot(cg, &src_aggs[i], src_idx);
        }
    }

    /// Merge one segment's string partial (`keys[g]` is group `g`'s key bytes).
    pub fn merge_partials(&mut self, keys: &[&[u8]], src_aggs: &[AggState]) {
        debug_assert_eq!(src_aggs.len(), self.aggs.len());
        debug_assert!(src_aggs.iter().all(|a| a.len() == keys.len()));
        for (g, &key) in keys.iter().enumerate() {
            self.merge_one(key, src_aggs, g);
        }
    }

    /// Consume and return the agg accumulators (group `g`'s key is
    /// [`Self::key_bytes`]`(g)` — read keys out before dropping if needed).
    pub fn into_aggs(self) -> Vec<AggState> {
        self.aggs
    }
}

/// One segment's STRING partial: keys flattened into a byte buffer + cumulative
/// offsets (`offsets[g]..offsets[g+1]` is group `g`'s key — Arrow-style, no
/// per-key allocation), plus the typed agg accumulators.
struct StringPartial {
    buffer: Vec<u8>,
    offsets: Vec<i32>,
    aggs: Vec<AggState>,
}

/// JNI-facing orchestration for the STRING combine — the begin/set-agg/commit/
/// finish/extract state machine, mirroring [`crate::CombineSession`] but for
/// variable-length keys. Wraps the existing [`StringCombineDriver`].
///
/// NOTE: `finish` is currently **single-threaded** (the radix-partitioned
/// parallel merge covers `Copy` fixed-width keys only; parallel string combine
/// is a planned fast-follow — design doc §23 step 5.2). Correct, not yet
/// parallel.
pub struct StringCombineSession {
    agg_kinds: Vec<AggKind>,
    partials: Vec<StringPartial>,
    staging_buffer: Option<Vec<u8>>,
    staging_offsets: Option<Vec<i32>>,
    staging_aggs: Vec<Option<AggState>>,
    // Flattened combined result after finish: keys concatenated into one buffer
    // with cumulative offsets (Arrow-style), plus per-agg accumulators.
    result: Option<(Vec<u8>, Vec<i32>, Vec<AggState>)>,
}

impl StringCombineSession {
    pub fn new(agg_kinds: &[AggKind]) -> Self {
        Self {
            agg_kinds: agg_kinds.to_vec(),
            partials: Vec::new(),
            staging_buffer: None,
            staging_offsets: None,
            staging_aggs: (0..agg_kinds.len()).map(|_| None).collect(),
            result: None,
        }
    }

    #[inline]
    pub fn num_aggs(&self) -> usize {
        self.agg_kinds.len()
    }

    /// Begin a segment partial: `buffer` holds all keys concatenated, `offsets`
    /// is cumulative (length `num_groups + 1`).
    pub fn begin_partial(&mut self, buffer: Vec<u8>, offsets: Vec<i32>) {
        self.staging_buffer = Some(buffer);
        self.staging_offsets = Some(offsets);
        for slot in self.staging_aggs.iter_mut() {
            *slot = None;
        }
    }

    pub fn set_agg_long(&mut self, agg_idx: usize, v: Vec<i64>) {
        self.staging_aggs[agg_idx] = Some(AggState::from_long_vec(self.agg_kinds[agg_idx], v));
    }

    pub fn set_agg_int(&mut self, agg_idx: usize, v: Vec<i32>) {
        self.staging_aggs[agg_idx] = Some(AggState::from_int_vec(self.agg_kinds[agg_idx], v));
    }

    pub fn set_agg_double(&mut self, agg_idx: usize, v: Vec<f64>) {
        self.staging_aggs[agg_idx] = Some(AggState::from_double_vec(self.agg_kinds[agg_idx], v));
    }

    pub fn set_agg_float(&mut self, agg_idx: usize, v: Vec<f32>) {
        self.staging_aggs[agg_idx] = Some(AggState::from_float_vec(self.agg_kinds[agg_idx], v));
    }

    pub fn commit_partial(&mut self) {
        let buffer = self.staging_buffer.take().expect("commit_partial without begin_partial");
        let offsets = self.staging_offsets.take().expect("commit_partial without begin_partial");
        let mut aggs = Vec::with_capacity(self.staging_aggs.len());
        for (i, slot) in self.staging_aggs.iter_mut().enumerate() {
            aggs.push(slot.take().unwrap_or_else(|| panic!("aggregation {i} not set before commit_partial")));
        }
        self.partials.push(StringPartial { buffer, offsets, aggs });
    }

    /// Merge all committed partials using a radix-partitioned, work-stealing
    /// parallel merge: partition every `(partial, group)` by the top
    /// `radix_bits` of its key hash, then merge each partition in its own
    /// `StringCombineDriver` (own arena) on a Rayon worker — disjoint by hash,
    /// lock-free. The disjoint partitions are concatenated into one flat result.
    pub fn finish(&mut self, radix_bits: u32) {
        let partials = &self.partials;
        let agg_kinds = &self.agg_kinds;
        let num_partitions = 1usize << radix_bits;

        // Phase 1: partition (partial, group) indices by key-hash top bits.
        let mut buckets: Vec<Vec<(u32, u32)>> = vec![Vec::new(); num_partitions];
        for (pi, p) in partials.iter().enumerate() {
            let num_groups = p.offsets.len().saturating_sub(1);
            for g in 0..num_groups {
                let key = &p.buffer[p.offsets[g] as usize..p.offsets[g + 1] as usize];
                let part = string_partition(hash_bytes(key), radix_bits);
                buckets[part].push((pi as u32, g as u32));
            }
        }

        // Phase 2: merge each partition independently (work-stealing, lock-free).
        let drivers: Vec<StringCombineDriver> = buckets
            .into_par_iter()
            .map(|bucket| {
                let mut driver = StringCombineDriver::new(agg_kinds);
                for (pi, g) in bucket {
                    let p = &partials[pi as usize];
                    let key = &p.buffer[p.offsets[g as usize] as usize..p.offsets[g as usize + 1] as usize];
                    driver.merge_one(key, &p.aggs, g as usize);
                }
                driver
            })
            .collect();

        // Concatenate the disjoint partitions into one flat result.
        let mut buffer: Vec<u8> = Vec::new();
        let mut offsets: Vec<i32> = vec![0];
        let mut aggs: Vec<AggState> = agg_kinds.iter().map(|&k| AggState::new_for(k, 0)).collect();
        for driver in drivers {
            let num_groups = driver.num_groups();
            for g in 0..num_groups as u32 {
                buffer.extend_from_slice(driver.key_bytes(g));
                offsets.push(buffer.len() as i32);
            }
            let mut part_aggs = driver.into_aggs();
            for (i, a) in part_aggs.iter_mut().enumerate() {
                aggs[i].append(a);
            }
        }
        self.result = Some((buffer, offsets, aggs));
    }

    pub fn result_num_groups(&self) -> usize {
        self.result.as_ref().map_or(0, |(_, offsets, _)| offsets.len().saturating_sub(1))
    }

    pub fn result_key_bytes(&self, g: u32) -> &[u8] {
        let (buffer, offsets, _) = self.result.as_ref().expect("finish not called");
        &buffer[offsets[g as usize] as usize..offsets[g as usize + 1] as usize]
    }

    /// Combined key bytes, concatenated (for JNI extraction). Pair with
    /// [`Self::result_string_offsets`].
    pub fn result_string_buffer(&self) -> &[u8] {
        &self.result.as_ref().expect("finish not called").0
    }

    /// Cumulative offsets into [`Self::result_string_buffer`] (length
    /// `num_groups + 1`).
    pub fn result_string_offsets(&self) -> &[i32] {
        &self.result.as_ref().expect("finish not called").1
    }

    pub fn result_agg(&self, agg_idx: usize) -> &AggState {
        &self.result.as_ref().expect("finish not called").2[agg_idx]
    }

    /// Apply ORDER BY top-K / no-ORDER-BY cap to the combined STRING result in
    /// place (after `finish`): reduce to `result_size` groups per `order`
    /// (§26.3). Group keys order lexicographically by their UTF-8 bytes (the
    /// raw STRING ordering); aggregation order terms use [`AggState::cmp_slots`].
    /// Rebuilds the flattened key buffer + offsets in selected order. See
    /// [`crate::topk`].
    pub fn select(&mut self, order: &[OrderTerm], result_size: usize) {
        if let Some((buffer, offsets, aggs)) = self.result.as_mut() {
            let n = offsets.len().saturating_sub(1);
            let key_at = |g: usize| -> &[u8] { &buffer[offsets[g] as usize..offsets[g + 1] as usize] };
            let perm = compute_selection(n, 1, |_c, a, b| key_at(a).cmp(key_at(b)), aggs, order, result_size);

            // Rebuild the flattened buffer + cumulative offsets in selected order.
            let mut new_buffer: Vec<u8> = Vec::with_capacity(buffer.len());
            let mut new_offsets: Vec<i32> = Vec::with_capacity(perm.len() + 1);
            new_offsets.push(0);
            for &i in &perm {
                new_buffer.extend_from_slice(key_at(i));
                new_offsets.push(new_buffer.len() as i32);
            }
            *buffer = new_buffer;
            *offsets = new_offsets;
            for a in aggs.iter_mut() {
                *a = a.gather(&perm);
            }
        }
    }
}

/// Radix partition of a string key hash: top `radix_bits` bits (0 → single
/// partition).
#[inline]
fn string_partition(hash: u64, radix_bits: u32) -> usize {
    if radix_bits == 0 {
        0
    } else {
        (hash >> (64 - radix_bits)) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn interns_each_distinct_key_once() {
        let mut t = StringTable::new();
        assert_eq!(t.probe_or_insert(b"us"), 0);
        assert_eq!(t.probe_or_insert(b"uk"), 1);
        assert_eq!(t.probe_or_insert(b"us"), 0); // repeat -> same id, no new arena bytes
        assert_eq!(t.probe_or_insert(b""), 2); // empty string is a valid distinct key
        assert_eq!(t.probe_or_insert(b"canada"), 3);
        assert_eq!(t.len(), 4);
        assert_eq!(t.arena_len(), (2 + 2) + 6); // each interned once
        assert_eq!(t.bytes_at(0), b"us");
        assert_eq!(t.bytes_at(3), b"canada");
        assert_eq!(t.get(b"uk"), Some(1));
        assert_eq!(t.get(b"nope"), None);
    }

    #[test]
    fn handles_hash_collisions_by_byte_compare() {
        // Even if two keys hashed equal, the byte compare keeps them distinct.
        // (We can't force a hash collision easily; this just exercises many
        // keys to stress the probe + compare path.)
        let mut t = StringTable::new();
        for i in 0..5000 {
            let k = format!("key-{i}");
            assert_eq!(t.probe_or_insert(k.as_bytes()), i as u32);
        }
        assert_eq!(t.len(), 5000);
        for i in 0..5000 {
            let k = format!("key-{i}");
            assert_eq!(t.get(k.as_bytes()), Some(i as u32));
        }
    }

    /// Cross-segment STRING combine matches a HashMap<Vec<u8>, ...> reference.
    #[test]
    fn string_combine_matches_reference() {
        let words: [&[u8]; 6] = [b"alpha", b"beta", b"gamma", b"delta", b"", b"epsilon"];
        let kinds = [AggKind::SumDouble, AggKind::Count];
        let mut driver = StringCombineDriver::new(&kinds);
        let mut reference: HashMap<Vec<u8>, (f64, i64)> = HashMap::new();

        let mut state: u64 = 0xc0de_1234_5678_9abc;
        let num_segments = 30;
        for _ in 0..num_segments {
            // Build a per-segment partial over a random subset of words.
            let mut seg: HashMap<Vec<u8>, (f64, i64)> = HashMap::new();
            for _ in 0..40 {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let w = words[(state >> 60) as usize % words.len()];
                let v = ((state >> 20) as i64 % 1000) as f64;
                let e = seg.entry(w.to_vec()).or_insert((0.0, 0));
                e.0 += v;
                e.1 += 1;
                let r = reference.entry(w.to_vec()).or_insert((0.0, 0));
                r.0 += v;
                r.1 += 1;
            }
            let keys: Vec<&[u8]> = seg.keys().map(|k| k.as_slice()).collect();
            let sums: Vec<f64> = seg.values().map(|v| v.0).collect();
            let cnts: Vec<i64> = seg.values().map(|v| v.1).collect();
            driver.merge_partials(&keys, &[AggState::SumDouble(sums), AggState::Count(cnts)]);
        }

        assert_eq!(driver.num_groups(), reference.len());
        let g = driver.num_groups();
        let sums = driver.agg_state(0).as_double_slice().unwrap().to_vec();
        let cnts = driver.agg_state(1).as_long_slice().unwrap().to_vec();
        for gid in 0..g as u32 {
            let key = driver.key_bytes(gid).to_vec();
            let r = reference[&key];
            assert_eq!(sums[gid as usize], r.0, "sum for {:?}", String::from_utf8_lossy(&key));
            assert_eq!(cnts[gid as usize], r.1, "cnt for {:?}", String::from_utf8_lossy(&key));
        }
    }

    /// The JNI-facing StringCombineSession (begin/set-agg/commit/finish/extract),
    /// driven the way the FFI will, matches a HashMap reference.
    #[test]
    fn string_session_matches_reference() {
        let kinds = [AggKind::SumDouble, AggKind::Count];
        let mut session = StringCombineSession::new(&kinds);
        let mut reference: HashMap<Vec<u8>, (f64, i64)> = HashMap::new();

        // Helper: flatten a segment's (key,sum,cnt) rows into buffer+cumulative offsets.
        let mut feed = |session: &mut StringCombineSession, rows: &[(&[u8], f64, i64)]| {
            let mut buffer = Vec::new();
            let mut offsets = vec![0i32];
            let (mut sums, mut cnts) = (Vec::new(), Vec::new());
            for &(k, s, c) in rows {
                buffer.extend_from_slice(k);
                offsets.push(buffer.len() as i32);
                sums.push(s);
                cnts.push(c);
                let r = reference.entry(k.to_vec()).or_insert((0.0, 0));
                r.0 += s;
                r.1 += c;
            }
            session.begin_partial(buffer, offsets);
            session.set_agg_double(0, sums);
            session.set_agg_long(1, cnts);
            session.commit_partial();
        };

        feed(&mut session, &[(b"alpha", 1.5, 3), (b"beta", 2.5, 1), (b"", 9.0, 2)]);
        feed(&mut session, &[(b"beta", 4.0, 2), (b"gamma", 7.0, 5), (b"alpha", 0.5, 1)]);

        session.finish(0);

        assert_eq!(session.result_num_groups(), 4);
        for gid in 0..session.result_num_groups() as u32 {
            let key = session.result_key_bytes(gid).to_vec();
            let r = reference[&key];
            assert_eq!(session.result_agg(0).as_double_slice().unwrap()[gid as usize], r.0,
                "sum for {:?}", String::from_utf8_lossy(&key));
            assert_eq!(session.result_agg(1).as_long_slice().unwrap()[gid as usize], r.1,
                "cnt for {:?}", String::from_utf8_lossy(&key));
        }
    }

    /// ORDER BY top-K over STRING keys: lexicographic key order and agg order,
    /// with the flattened buffer/offsets rebuilt in selected order.
    #[test]
    fn string_session_select_topk() {
        use crate::topk::{OrderRef, OrderTerm};
        let kinds = [AggKind::SumDouble];
        let mut s = StringCombineSession::new(&kinds);
        let feed = |s: &mut StringCombineSession, rows: &[(&[u8], f64)]| {
            let mut buffer = Vec::new();
            let mut offsets = vec![0i32];
            let mut sums = Vec::new();
            for &(k, v) in rows {
                buffer.extend_from_slice(k);
                offsets.push(buffer.len() as i32);
                sums.push(v);
            }
            s.begin_partial(buffer, offsets);
            s.set_agg_double(0, sums);
            s.commit_partial();
        };
        feed(&mut s, &[(b"pear", 3.0), (b"apple", 5.0), (b"fig", 1.0)]);
        feed(&mut s, &[(b"apple", 2.0), (b"date", 9.0)]);
        s.finish(0);
        // merged sums: apple 7.0, pear 3.0, fig 1.0, date 9.0 (4 groups)
        assert_eq!(s.result_num_groups(), 4);

        // ORDER BY key (lexicographic) asc, all groups.
        s.select(&[OrderTerm::new(OrderRef::Key(0), true)], 10);
        let keys: Vec<Vec<u8>> = (0..s.result_num_groups() as u32).map(|g| s.result_key_bytes(g).to_vec()).collect();
        assert_eq!(keys, vec![b"apple".to_vec(), b"date".to_vec(), b"fig".to_vec(), b"pear".to_vec()]);

        // ORDER BY sum desc, top 2 -> date (9.0), apple (7.0).
        s.select(&[OrderTerm::new(OrderRef::Agg(0), false)], 2);
        assert_eq!(s.result_num_groups(), 2);
        assert_eq!(s.result_key_bytes(0), b"date");
        assert_eq!(s.result_key_bytes(1), b"apple");
        assert_eq!(s.result_agg(0).as_double_slice().unwrap(), &[9.0, 7.0]);
    }
}
