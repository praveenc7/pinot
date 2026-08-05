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

//! Dict-direct backend — the SOTA structure for **dict-encoded** segment keys
//! (design doc §22.9 lever 2 / §23 foundation step 1).
//!
//! A dict-encoded grouping column gives every row a dense `dict_id` in
//! `[0, cardinality)`. Because the ids are already dense, GROUP BY needs **no
//! hash table**: we map `dict_id → group_id` through a **direct array index**,
//! eliminating the hash + SIMD control-byte probe entirely. This is what
//! Pinot's Java `ArrayBasedHolder`, ClickHouse's `FixedHashMap`, and DuckDB's
//! perfect-hash path all do — and it's *why* a SwissTable showed little segment
//! win on dict keys (§22.5): we were hashing ids that can just be indexed.
//!
//! ## Type-uniformity
//!
//! Every dict-encoded key column — INT / LONG / FLOAT / DOUBLE / STRING — is an
//! `i32` dict_id at this layer; the column type only affects the *final decode*
//! (`Dictionary.getInternal`) at the segment→combine boundary. So this single
//! backend covers all five key types for single-column dict GROUP BY.
//!
//! ## Why a backend (not a bespoke driver)
//!
//! `DictDirectTable` implements [`GroupByBackend<i32>`] by assigning **dense,
//! insertion-ordered `group_id`s** (id 0 to the first distinct dict_id seen,
//! 1 to the second, …) via a `dict_id → group_id` slot array. That is exactly
//! the contract the existing [`crate::GroupByDriverDictInt`] expects, so the
//! whole multi-agg driver + combine path compose over it **unchanged** — only
//! the key→group_id step changes from "hash + probe" to "one array load".
//!
//! Dense group_ids keep the per-group accumulator arrays **compact** (sized to
//! the number of distinct groups, not the dict cardinality), which is more
//! cache-friendly than indexing accumulators by raw dict_id when the query
//! touches only a subset of the dictionary (e.g. after a selective filter).
//!
//! Use it when the dict cardinality is small enough that a `u32`-per-dict_id
//! slot array is affordable; above a threshold the executor falls back to the
//! hash backends (a `Table`/`HashbrownTable`), same as ClickHouse switching off
//! its fixed map past 2^16.

use crate::backend::GroupByBackend;

/// Sentinel for an unseen dict_id in the slot array.
const UNSEEN: u32 = u32::MAX;

/// Dict-direct `dict_id → group_id` table. No hashing — `slot[dict_id]` holds
/// the dense group_id, or [`UNSEEN`]. Grows on demand if a dict_id exceeds the
/// current slot length (so `new()` works without a known cardinality; prefer
/// [`with_capacity`] with `Dictionary.length()` to avoid resizes).
///
/// [`with_capacity`]: GroupByBackend::with_capacity
pub struct DictDirectTable {
    slot: Vec<u32>,
    num_groups: u32,
}

impl Default for DictDirectTable {
    fn default() -> Self {
        <Self as GroupByBackend<i32>>::new()
    }
}

impl DictDirectTable {
    #[inline]
    fn ensure(&mut self, idx: usize) {
        if idx >= self.slot.len() {
            self.slot.resize(idx + 1, UNSEEN);
        }
    }
}

impl GroupByBackend<i32> for DictDirectTable {
    const NAME: &'static str = "dict-direct";

    #[inline]
    fn new() -> Self {
        Self {
            slot: Vec::new(),
            num_groups: 0,
        }
    }

    #[inline]
    fn with_capacity(capacity_hint: usize) -> Self {
        Self {
            slot: vec![UNSEEN; capacity_hint],
            num_groups: 0,
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.num_groups as usize
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.num_groups == 0
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.slot.len()
    }

    #[inline]
    fn get(&self, key: &i32) -> Option<u32> {
        let idx = *key as usize;
        match self.slot.get(idx) {
            Some(&g) if g != UNSEEN => Some(g),
            _ => None,
        }
    }

    #[inline]
    fn probe_or_insert(&mut self, key: i32) -> u32 {
        let idx = key as usize;
        self.ensure(idx);
        // SAFETY: ensure() guarantees idx < slot.len().
        let s = unsafe { self.slot.get_unchecked_mut(idx) };
        if *s == UNSEEN {
            let g = self.num_groups;
            *s = g;
            self.num_groups += 1;
            g
        } else {
            *s
        }
    }

    fn probe_or_insert_batch(&mut self, keys: &[i32], out: &mut [u32]) {
        for (i, &k) in keys.iter().enumerate() {
            out[i] = self.probe_or_insert(k);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agg::AggKind;
    use crate::{GroupByDriverDictInt, HashbrownTable};

    #[test]
    fn assigns_dense_insertion_ordered_group_ids() {
        let mut t = <DictDirectTable as GroupByBackend<i32>>::with_capacity(16);
        assert_eq!(t.probe_or_insert(5), 0);
        assert_eq!(t.probe_or_insert(2), 1);
        assert_eq!(t.probe_or_insert(5), 0); // repeat -> same id
        assert_eq!(t.probe_or_insert(9), 2);
        assert_eq!(t.probe_or_insert(2), 1);
        assert_eq!(t.len(), 3);
        assert_eq!(t.get(&5), Some(0));
        assert_eq!(t.get(&7), None);
    }

    #[test]
    fn grows_on_demand_past_initial_capacity() {
        let mut t = <DictDirectTable as GroupByBackend<i32>>::new();
        assert_eq!(t.probe_or_insert(1000), 0);
        assert_eq!(t.probe_or_insert(3), 1);
        assert_eq!(t.probe_or_insert(1000), 0);
        assert_eq!(t.len(), 2);
        assert!(t.capacity() >= 1001);
    }

    #[test]
    fn batch_matches_per_key() {
        let mut t = <DictDirectTable as GroupByBackend<i32>>::with_capacity(8);
        let keys = [3i32, 1, 3, 0, 1, 2];
        let mut out = [0u32; 6];
        t.probe_or_insert_batch(&keys, &mut out);
        assert_eq!(out, [0, 1, 0, 2, 1, 3]);
    }

    /// The whole point: routed through the multi-agg driver, dict-direct must
    /// produce results identical to the hash backend (same dense group_ids,
    /// same accumulation), just without hashing.
    #[test]
    fn driver_over_dict_direct_matches_hash_backend() {
        let kinds = [AggKind::SumLongToDouble, AggKind::MinInt, AggKind::MaxDouble, AggKind::Count];
        let mut state: u64 = 0xfeed_face_cafe_b0ba;
        let n = 20_000;
        let card = 300i32;
        let (mut dict_ids, mut lv, mut iv, mut dv) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for _ in 0..n {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            dict_ids.push((((state >> 33) as i32) % card).abs());
            lv.push(((state >> 17) as i64) % 1_000_000);
            iv.push(((state >> 21) as i32) % 1_000_000);
            dv.push(((state >> 5) as f64 / 1e9) % 1000.0);
        }

        let mut hash = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&kinds);
        hash.process_block_keys(&dict_ids);
        hash.apply_long(0, &lv);
        hash.apply_int(1, &iv);
        hash.apply_double(2, &dv);
        hash.apply_count(3);

        let mut direct = GroupByDriverDictInt::<DictDirectTable>::with_capacity(&kinds, card as usize);
        direct.process_block_keys(&dict_ids);
        direct.apply_long(0, &lv);
        direct.apply_int(1, &iv);
        direct.apply_double(2, &dv);
        direct.apply_count(3);

        assert_eq!(hash.keys(), direct.keys(), "group keys (dict_ids) diverged");
        assert_eq!(hash.num_groups(), direct.num_groups());
        for i in 0..kinds.len() {
            match kinds[i] {
                AggKind::MinInt | AggKind::MaxInt => {
                    assert_eq!(hash.agg_state(i).as_int_slice(), direct.agg_state(i).as_int_slice(), "agg {}", i);
                }
                AggKind::MaxDouble | AggKind::MinDouble | AggKind::SumDouble => {
                    assert_eq!(hash.agg_state(i).as_double_slice(), direct.agg_state(i).as_double_slice(), "agg {}", i);
                }
                _ => {
                    assert_eq!(hash.agg_state(i).as_long_slice(), direct.agg_state(i).as_long_slice(), "agg {}", i);
                    assert_eq!(hash.agg_state(i).as_double_slice(), direct.agg_state(i).as_double_slice(), "agg {}", i);
                }
            }
        }
    }
}
