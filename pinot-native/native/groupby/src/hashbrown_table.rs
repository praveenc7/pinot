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

//! `HashbrownTable<K>` — Path C alternative backend that wraps
//! [`hashbrown::hash_table::HashTable`] following DataFusion's pattern.
//!
//! ## Architecture
//!
//! Mirrors `datafusion/physical-plan/src/aggregates/group_values/single_group_by/primitive.rs`
//! exactly:
//!
//! * The hash table stores `(group_id: u32, hash: u64)` tuples in slots — NOT
//!   the key itself.
//! * The actual key lives in [`HashbrownTable::keys`], a `Vec<K>` indexed by
//!   group_id.
//! * The hash is duplicated in the slot so the equality closure can
//!   short-circuit on hash mismatch before doing the full key comparison.
//! * Hash function is [`crate::HashKey::hash`] — the same type-specialized
//!   foldhash we use for our custom SwissTable, plugged in via the
//!   `hashbrown::hash_table::Entry` API which takes precomputed hashes
//!   directly.
//!
//! ## Why this exists
//!
//! Per the 2026-06-03 Phase 1.D-core Path C decision (design doc §18), we
//! prototype BOTH our custom SwissTable AND a hashbrown wrapper, then
//! measure end-to-end at Task #47 via the JMH harness with a `_backend`
//! axis (Task #59) before committing to one backend.
//!
//! This file is the hashbrown-wrapper backend candidate. It deliberately
//! mirrors the API of [`crate::Table`] so the segment driver (Task #47) can
//! be generic over the backend via the `GroupByBackend` trait (Task #57).
//!
//! ## Why DataFusion's slot type (`(u32, u64)`) and not hashbrown's `(K, u32)`?
//!
//! Two reasons:
//!
//! 1. **Short-circuit equality**: storing the hash in the slot lets the
//!    equality closure reject collisions via integer compare before doing
//!    the full key compare. For wide keys (composite tuples, future
//!    variable-length) this is a meaningful win.
//! 2. **Driver flexibility**: keeping the key in `Vec<K>` indexed by
//!    `group_id` means the segment driver can read keys in dense group_id
//!    order during the segment-end materialization step (Phase 1.D-core-I,
//!    Task #52) without needing a hashbrown iterator.

use hashbrown::hash_table::HashTable;

use crate::backend::GroupByBackend;
use crate::HashKey;

/// Path C alternative backend: SwissTable via hashbrown, key storage via
/// `Vec<K>` indexed by group_id. Mirrors DataFusion's
/// `GroupValuesPrimitive<T>` design.
///
/// Public API mirrors [`crate::Table`] for backend interchangeability.
pub struct HashbrownTable<K: HashKey + Eq + Copy> {
    /// Slot type `(group_id, hash)`. The hash is duplicated here for
    /// short-circuit equality check.
    map: HashTable<(u32, u64)>,
    /// Keys indexed by `group_id`. Densely packed in insertion order.
    keys: Vec<K>,
}

impl<K: HashKey + Eq + Copy> Default for HashbrownTable<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: HashKey + Eq + Copy> GroupByBackend<K> for HashbrownTable<K> {
    const NAME: &'static str = "hashbrown";

    #[inline]
    fn new() -> Self {
        Self::new()
    }
    #[inline]
    fn with_capacity(capacity_hint: usize) -> Self {
        Self::with_capacity(capacity_hint)
    }
    #[inline]
    fn len(&self) -> usize {
        self.len()
    }
    #[inline]
    fn is_empty(&self) -> bool {
        self.is_empty()
    }
    #[inline]
    fn capacity(&self) -> usize {
        self.capacity()
    }
    #[inline]
    fn get(&self, key: &K) -> Option<u32> {
        self.get(key)
    }
    #[inline]
    fn probe_or_insert(&mut self, key: K) -> u32 {
        self.probe_or_insert(key)
    }
    #[inline]
    fn probe_or_insert_batch(&mut self, keys: &[K], out: &mut [u32]) {
        self.probe_or_insert_batch(keys, out)
    }
}

impl<K: HashKey + Eq + Copy> HashbrownTable<K> {
    /// Construct an empty table with hashbrown's default capacity (0).
    pub fn new() -> Self {
        Self {
            map: HashTable::new(),
            keys: Vec::new(),
        }
    }

    /// Construct an empty table sized for at least `capacity_hint` keys.
    /// hashbrown's `with_capacity` rounds up to the next power-of-two group
    /// count satisfying its load factor cap.
    pub fn with_capacity(capacity_hint: usize) -> Self {
        Self {
            map: HashTable::with_capacity(capacity_hint),
            keys: Vec::with_capacity(capacity_hint),
        }
    }

    /// Number of distinct keys in the table.
    #[inline]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether the table contains zero distinct keys.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Capacity reported by hashbrown. Comparable but not identical to our
    /// custom hashbrown: hashbrown's capacity is the number of
    /// keys insertable before a resize, accounting for its load factor.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.map.capacity()
    }

    /// Look up `key`'s `group_id` without inserting. Returns `None` if
    /// absent.
    pub fn get(&self, key: &K) -> Option<u32> {
        let hash = key.hash();
        self.map
            .find(hash, |&(g, h)| {
                // Short-circuit on hash mismatch before full key compare.
                // SAFETY: any (g, h) in the table was inserted by
                // probe_or_insert which guarantees g < self.keys.len() at
                // insert time, and we never remove keys.
                hash == h && unsafe { *self.keys.get_unchecked(g as usize) == *key }
            })
            .map(|&(g, _)| g)
    }

    /// Look up `key`'s `group_id`, inserting a new sequentially-assigned id
    /// if the key was not previously present.
    pub fn probe_or_insert(&mut self, key: K) -> u32 {
        let hash = key.hash();
        let entry = self.map.entry(
            hash,
            |&(g, h)| {
                // SAFETY: see comment in `get`.
                hash == h && unsafe { *self.keys.get_unchecked(g as usize) == key }
            },
            |&(_, h)| h,
        );
        match entry {
            hashbrown::hash_table::Entry::Occupied(o) => o.get().0,
            hashbrown::hash_table::Entry::Vacant(v) => {
                let g = self.keys.len() as u32;
                v.insert((g, hash));
                self.keys.push(key);
                g
            }
        }
    }

    /// Batch entry: process `keys.len()` probes, write group_ids into `out`.
    ///
    /// Unlike [`HashbrownTable::probe_or_insert_batch`], this implementation
    /// has NO batch optimization — it loops calling [`probe_or_insert`]
    /// per key. This mirrors DataFusion's `intern` hot loop exactly and is
    /// the apples-to-apples comparison point against our custom batch
    /// (pre-hashed scratch) at Task #58.
    ///
    /// If end-to-end measurement at Task #59 shows this wrapper wins,
    /// the absence of batch optimization is a feature, not a bug: DataFusion
    /// proves that per-key hashbrown is competitive at low/mid cardinality.
    pub fn probe_or_insert_batch(&mut self, keys: &[K], out: &mut [u32]) {
        assert!(
            out.len() >= keys.len(),
            "output buffer too small: keys={} out={}",
            keys.len(),
            out.len()
        );
        for (i, &k) in keys.iter().enumerate() {
            // SAFETY: i < keys.len() <= out.len() (asserted above).
            unsafe { *out.get_unchecked_mut(i) = self.probe_or_insert(k) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn empty_table_has_no_entries() {
        let t: HashbrownTable<i64> = HashbrownTable::new();
        assert_eq!(t.len(), 0);
        assert!(t.is_empty());
        assert_eq!(t.get(&42), None);
    }

    #[test]
    fn with_capacity_does_not_panic_on_zero() {
        let t: HashbrownTable<i64> = HashbrownTable::with_capacity(0);
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn insert_and_lookup_single_key() {
        let mut t: HashbrownTable<i64> = HashbrownTable::new();
        let id = t.probe_or_insert(42);
        assert_eq!(id, 0);
        assert_eq!(t.get(&42), Some(0));
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn probe_or_insert_returns_same_id_for_same_key() {
        let mut t: HashbrownTable<i64> = HashbrownTable::new();
        let id1 = t.probe_or_insert(42);
        let id2 = t.probe_or_insert(42);
        let id3 = t.probe_or_insert(42);
        assert_eq!(id1, id2);
        assert_eq!(id2, id3);
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn distinct_keys_get_distinct_dense_ids() {
        let mut t: HashbrownTable<i64> = HashbrownTable::new();
        for k in 0i64..100 {
            let id = t.probe_or_insert(k);
            assert_eq!(id, k as u32, "ids must be dense and insertion-ordered");
        }
        assert_eq!(t.len(), 100);
    }

    #[test]
    fn handles_i32_keys() {
        let mut t: HashbrownTable<i32> = HashbrownTable::new();
        for k in (-100i32..100).chain([i32::MIN, i32::MAX]) {
            t.probe_or_insert(k);
        }
        assert_eq!(t.len(), 202);
        for k in (-100i32..100).chain([i32::MIN, i32::MAX]) {
            assert!(t.get(&k).is_some(), "missing key {k}");
        }
    }

    #[test]
    fn handles_extreme_i64_values() {
        let mut t: HashbrownTable<i64> = HashbrownTable::new();
        let extremes = [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX];
        for k in extremes {
            t.probe_or_insert(k);
        }
        assert_eq!(t.len(), extremes.len());
        for k in extremes {
            assert!(t.get(&k).is_some());
        }
    }

    #[test]
    fn matches_hashmap_under_random_inserts() {
        // Use the same seed and operation sequence as the reference parity test
        // (see table.rs::matches_hashmap_under_random_inserts) so the two
        // backends are tested against an identical reference workload.
        let mut state: u64 = 0xc0ffee_dead_5eed;
        let mut t: HashbrownTable<i64> = HashbrownTable::new();
        let mut reference: HashMap<i64, u32> = HashMap::new();
        for _ in 0..10_000 {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let key = (state >> 32) as i32 % 1000;
            let key = key as i64;
            let next_ref = reference.len() as u32;
            let ref_id = *reference.entry(key).or_insert(next_ref);
            let table_id = t.probe_or_insert(key);
            assert_eq!(ref_id, table_id, "diverged on key {key}");
            assert_eq!(t.get(&key), Some(ref_id));
        }
        assert_eq!(t.len(), reference.len());
    }

    // --- batch-probe API tests (mirrors the SwissTable batch tests) ----------------

    #[test]
    fn batch_empty_input_is_noop() {
        let mut t: HashbrownTable<i64> = HashbrownTable::new();
        let mut out = [u32::MAX; 0];
        t.probe_or_insert_batch(&[], &mut out);
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn batch_single_key_matches_single_probe() {
        let mut t1: HashbrownTable<i64> = HashbrownTable::new();
        let mut t2: HashbrownTable<i64> = HashbrownTable::new();
        let id1 = t1.probe_or_insert(42);
        let mut out = [u32::MAX; 1];
        t2.probe_or_insert_batch(&[42i64], &mut out);
        assert_eq!(out[0], id1);
        assert_eq!(t1.len(), t2.len());
    }

    #[test]
    fn batch_distinct_keys_produce_distinct_dense_ids() {
        let mut t: HashbrownTable<i64> = HashbrownTable::new();
        let keys: Vec<i64> = (0..100i64).collect();
        let mut out = vec![u32::MAX; 100];
        t.probe_or_insert_batch(&keys, &mut out);
        for (i, &id) in out.iter().enumerate() {
            assert_eq!(id, i as u32);
        }
        assert_eq!(t.len(), 100);
    }

    #[test]
    fn batch_duplicate_keys_return_same_id() {
        let mut t: HashbrownTable<i64> = HashbrownTable::new();
        let keys = [42i64, 17, 42, 17, 42, 99, 99, 17];
        let mut out = vec![u32::MAX; keys.len()];
        t.probe_or_insert_batch(&keys, &mut out);
        let expected: Vec<u32> = keys
            .iter()
            .scan(std::collections::HashMap::new(), |seen, &k| {
                let len = seen.len() as u32;
                Some(*seen.entry(k).or_insert(len))
            })
            .collect();
        assert_eq!(out, expected);
        assert_eq!(t.len(), 3);
    }

    #[test]
    fn batch_matches_sequential_probe_or_insert() {
        let keys: Vec<i64> = (0..500i64).chain(0..500i64).chain(250..750i64).collect();

        let mut t_batch: HashbrownTable<i64> = HashbrownTable::new();
        let mut out_batch = vec![u32::MAX; keys.len()];
        t_batch.probe_or_insert_batch(&keys, &mut out_batch);

        let mut t_serial: HashbrownTable<i64> = HashbrownTable::new();
        let out_serial: Vec<u32> = keys.iter().map(|&k| t_serial.probe_or_insert(k)).collect();

        assert_eq!(out_batch, out_serial);
        assert_eq!(t_batch.len(), t_serial.len());
    }

    #[test]
    fn batch_triggers_resize_correctly() {
        let mut t: HashbrownTable<i64> = HashbrownTable::new();
        let keys: Vec<i64> = (0..2000i64).collect();
        let mut out = vec![u32::MAX; keys.len()];
        t.probe_or_insert_batch(&keys, &mut out);
        assert_eq!(t.len(), 2000);
        for (i, &id) in out.iter().enumerate() {
            assert_eq!(id, i as u32);
        }
        for k in 0i64..2000 {
            assert_eq!(t.get(&k), Some(k as u32));
        }
    }

    #[test]
    #[should_panic(expected = "output buffer too small")]
    fn batch_panics_on_undersized_output() {
        let mut t: HashbrownTable<i64> = HashbrownTable::new();
        let mut out = [u32::MAX; 3];
        t.probe_or_insert_batch(&[1i64, 2, 3, 4], &mut out);
    }
}
