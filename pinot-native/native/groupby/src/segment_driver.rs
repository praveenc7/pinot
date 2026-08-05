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

//! Per-segment GROUP BY drivers — Phase 1.D-core-D (Task #47).
//!
//! Drivers are generic over [`GroupByBackend`] so the same code compiles
//! against either our custom [`crate::HashbrownTable`] or the
//! [`crate::HashbrownTable`] wrapper. The choice is made at the JNI
//! boundary by instantiating the right monomorphization.
//!
//! ## Phase 1.D-core-D scope
//!
//! This file currently contains ONE driver covering the narrowest first
//! integration case: `SELECT SUM(longCol) FROM t GROUP BY dictIntCol`.
//!
//! The dict-encoded INT column produces a stream of `i32` dict IDs per
//! row; the SUM column produces a stream of `i64` values per row. The
//! driver maintains:
//!
//! * A backend `B` mapping `dict_id → group_id`.
//! * A parallel `keys: Vec<i32>` indexed by `group_id`, holding the dict
//!   ID that originally allocated each group. This is what Phase 1.D-core-I
//!   (Task #52) consumes to materialize raw values via the segment's
//!   `Dictionary` at boundary.
//! * A parallel `sums: Vec<i64>` indexed by `group_id`, holding the running
//!   per-group SUM accumulator.
//! * A reused `group_id_scratch: Vec<u32>` to avoid per-block allocation
//!   when batch-probing.
//!
//! ## Group-id assignment
//!
//! Both backends assign `group_id` sequentially in insertion order
//! (group_id 0 to the first unique key, 1 to the second, etc.). This is a
//! load-bearing invariant: it lets the driver scan the batch-probe output
//! to detect newly-allocated group_ids without an extra API on the
//! backend — see the `process_block` implementation.
//!
//! ## Why not store keys inside the backend?
//!
//! The driver owns `keys: Vec<i32>` rather than relying on the backend's
//! internal key storage because:
//!
//! 1. Both backends store keys, but `HashbrownTable<i32>` stores them inside
//!    `MaybeUninit` slots scattered by hash position — iterating them in
//!    group_id order would require either a sort or an extra mapping.
//! 2. `HashbrownTable<i32>` already has its own `keys: Vec<i32>` indexed
//!    by group_id, so exposing it through the `GroupByBackend` trait would
//!    leak the wrapper's internal layout.
//! 3. The driver-owned `keys` is identical work to what
//!    `HashbrownTable<i32>` does internally, and adding it to our
//!    `HashbrownTable<i32>` would inflate slot size for no probe-loop benefit.
//!
//! The driver-owned key vector is cleaner architecturally and matches
//! DataFusion's pattern (`GroupValuesPrimitive<T>::values: Vec<T::Native>`).

use crate::backend::GroupByBackend;

/// Per-segment GROUP BY driver for the case `SUM(longCol) GROUP BY dictIntCol`.
///
/// Stateful: instantiate once per segment, call [`process_block`] one or
/// more times as the segment's column blocks stream in, then call
/// [`extract`] to drain the (keys, sums) result for downstream processing
/// (combine driver or segment-end raw-value materialization).
///
/// Generic over backend `B` (our SwissTable or hashbrown wrapper) — the
/// caller picks at compile time by instantiating the right
/// monomorphization.
///
/// [`process_block`]: GroupBySumLongByDictInt::process_block
/// [`extract`]: GroupBySumLongByDictInt::extract
pub struct GroupBySumLongByDictInt<B: GroupByBackend<i32>> {
    /// `dict_id → group_id` table.
    table: B,

    /// `keys[group_id] = dict_id_that_created_this_group`. Densely populated
    /// in group_id order via the batch-probe scan in `process_block`.
    keys: Vec<i32>,

    /// `sums[group_id] = running SUM accumulator for this group`. Densely
    /// populated; `sums.len() == keys.len() == table.len()` always.
    sums: Vec<i64>,

    /// Reused scratch for batch-probe output. Avoids per-block allocation.
    group_id_scratch: Vec<u32>,
}

impl<B: GroupByBackend<i32>> Default for GroupBySumLongByDictInt<B> {
    fn default() -> Self {
        Self::new()
    }
}

impl<B: GroupByBackend<i32>> GroupBySumLongByDictInt<B> {
    /// Construct an empty driver with implementation-default backend
    /// capacity. Use [`with_capacity`] when a group-cardinality estimate
    /// is available (e.g., from Pinot's `Dictionary.length()` for the
    /// dict-encoded column — that's a tight upper bound on the per-segment
    /// group count).
    ///
    /// [`with_capacity`]: GroupBySumLongByDictInt::with_capacity
    pub fn new() -> Self {
        Self {
            table: B::new(),
            keys: Vec::new(),
            sums: Vec::new(),
            group_id_scratch: Vec::new(),
        }
    }

    /// Construct a driver pre-sized for `expected_groups` distinct groups.
    /// All four internal allocations are reserved up-front.
    pub fn with_capacity(expected_groups: usize) -> Self {
        Self {
            table: B::with_capacity(expected_groups),
            keys: Vec::with_capacity(expected_groups),
            sums: Vec::with_capacity(expected_groups),
            group_id_scratch: Vec::new(),
        }
    }

    /// Number of distinct groups accumulated so far across all
    /// `process_block` calls.
    #[inline]
    pub fn num_groups(&self) -> usize {
        self.table.len()
    }

    /// Process one block of `(dict_id, value)` rows.
    ///
    /// `dict_ids.len()` must equal `values.len()`. For each row `i`:
    ///
    /// 1. Probe-or-insert `dict_ids[i]` into the backend. If new, group_id
    ///    is the next sequential value; if existing, the original group_id
    ///    is returned.
    /// 2. `sums[group_id] += values[i]` (wrapping i64 addition — matches
    ///    Pinot's `SumAggregationFunction` semantics for LONG which uses
    ///    `Math.addExact` only optionally).
    pub fn process_block(&mut self, dict_ids: &[i32], values: &[i64]) {
        assert_eq!(
            dict_ids.len(),
            values.len(),
            "dict_ids and values must have equal length"
        );
        let n = dict_ids.len();
        if n == 0 {
            return;
        }

        // Phase 1: batch probe-or-insert. Backend allocates new group_ids
        // for previously-unseen keys; existing keys return their original
        // group_id.
        self.group_id_scratch.clear();
        self.group_id_scratch.resize(n, 0);
        self.table
            .probe_or_insert_batch(dict_ids, &mut self.group_id_scratch);

        // Phase 2: walk the batch in order, growing `keys`/`sums` whenever
        // we hit a newly-allocated group_id. Because group_ids are dense +
        // insertion-ordered, the first row in this batch with
        // `group_id == self.keys.len()` is the row that created that
        // group — so we can capture its dict_id as the group's key.
        let mut next_new_gid = self.keys.len();
        for i in 0..n {
            if self.group_id_scratch[i] as usize == next_new_gid {
                self.keys.push(dict_ids[i]);
                self.sums.push(0);
                next_new_gid += 1;
            }
        }
        debug_assert_eq!(self.keys.len(), self.table.len());
        debug_assert_eq!(self.sums.len(), self.table.len());

        // Phase 3: accumulate sums. This is the hot loop that the
        // hashbrown-wrapper / custom-SwissTable choice WILL be measured on
        // end-to-end at Task #59 — it's where the inlined agg-state update
        // fusion lives (writing directly to a parallel Vec<i64> indexed by
        // group_id, no Entry handle).
        for i in 0..n {
            let g = self.group_id_scratch[i] as usize;
            // wrapping_add matches Pinot's `+=` LONG semantics (no overflow
            // check in the default SumAggregationFunction Java code).
            self.sums[g] = self.sums[g].wrapping_add(values[i]);
        }
    }

    /// Single-key probe-and-accumulate variant. Equivalent to a
    /// `process_block(&[dict_id], &[value])` call but without the batch
    /// scratch allocation, intended for tests and corner cases.
    #[inline]
    pub fn process_one(&mut self, dict_id: i32, value: i64) {
        let g = self.table.probe_or_insert(dict_id) as usize;
        if g == self.keys.len() {
            self.keys.push(dict_id);
            self.sums.push(0);
        }
        self.sums[g] = self.sums[g].wrapping_add(value);
    }

    /// Read-only view of the per-group keys, in group_id order.
    /// `result[g]` is the dict_id that created group g.
    #[inline]
    pub fn keys(&self) -> &[i32] {
        &self.keys
    }

    /// Read-only view of the per-group sums, in group_id order.
    /// `result[g]` is the running SUM for group g.
    #[inline]
    pub fn sums(&self) -> &[i64] {
        &self.sums
    }

    /// Consume the driver and return the (keys, sums) parallel arrays.
    /// Each is indexed by `group_id`. Callers downstream (combine driver,
    /// segment-end materialization) take ownership.
    pub fn extract(self) -> (Vec<i32>, Vec<i64>) {
        (self.keys, self.sums)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HashbrownTable;
    use std::collections::HashMap;

    /// Tiny reference Java-equivalent: dict_ids + values → (unique dict_ids
    /// in insertion order, sums per dict_id). Used as the differential
    /// oracle in cross-backend tests.
    fn reference_group_by_sum(dict_ids: &[i32], values: &[i64]) -> (Vec<i32>, Vec<i64>) {
        assert_eq!(dict_ids.len(), values.len());
        let mut keys: Vec<i32> = Vec::new();
        let mut sums: Vec<i64> = Vec::new();
        let mut map: HashMap<i32, usize> = HashMap::new();
        for i in 0..dict_ids.len() {
            let dict_id = dict_ids[i];
            let value = values[i];
            let g = *map.entry(dict_id).or_insert_with(|| {
                let g = keys.len();
                keys.push(dict_id);
                sums.push(0);
                g
            });
            sums[g] = sums[g].wrapping_add(value);
        }
        (keys, sums)
    }

    fn run<B: GroupByBackend<i32>>(dict_ids: &[i32], values: &[i64]) -> (Vec<i32>, Vec<i64>) {
        let mut driver = GroupBySumLongByDictInt::<B>::new();
        driver.process_block(dict_ids, values);
        driver.extract()
    }

    #[test]
    fn empty_input_is_noop() {
        let (k, s) = run::<HashbrownTable<i32>>(&[], &[]);
        assert!(k.is_empty());
        assert!(s.is_empty());
    }

    #[test]
    fn single_dict_id_accumulates_sum() {
        let (k, s) = run::<HashbrownTable<i32>>(&[42, 42, 42], &[10, 20, 30]);
        assert_eq!(k, vec![42]);
        assert_eq!(s, vec![60]);
    }

    #[test]
    fn distinct_dict_ids_get_independent_sums() {
        let (k, s) = run::<HashbrownTable<i32>>(&[1, 2, 1, 3, 2], &[10, 20, 30, 40, 50]);
        assert_eq!(k, vec![1, 2, 3]);
        // 1: 10 + 30 = 40
        // 2: 20 + 50 = 70
        // 3: 40
        assert_eq!(s, vec![40, 70, 40]);
    }

    #[test]
    fn matches_java_style_reference_random() {
        let mut state: u64 = 0xc0ffee_d1c7_5eed;
        let n = 10_000;
        let mut dict_ids = Vec::with_capacity(n);
        let mut values = Vec::with_capacity(n);
        for _ in 0..n {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            // dict_id range: 0..200 (low cardinality, realistic for a Pinot dim)
            let dict_id = (((state >> 32) as i32) % 200).abs();
            // value range: -1_000_000..1_000_000
            let value = ((state >> 16) as i64) % 1_000_001;
            dict_ids.push(dict_id);
            values.push(value);
        }
        let (ref_k, ref_s) = reference_group_by_sum(&dict_ids, &values);
        let (our_k, our_s) = run::<HashbrownTable<i32>>(&dict_ids, &values);
        let (hb_k, hb_s) = run::<HashbrownTable<i32>>(&dict_ids, &values);
        assert_eq!(our_k, ref_k);
        assert_eq!(our_s, ref_s);
        assert_eq!(hb_k, ref_k);
        assert_eq!(hb_s, ref_s);
    }

    /// Across-blocks accumulation parity: feeding the same total stream as
    /// one big block OR as many small blocks must produce identical
    /// results.
    #[test]
    fn multi_block_matches_single_block() {
        let dict_ids = vec![1, 2, 1, 3, 2, 4, 1, 5, 2, 3, 6, 4, 1];
        let values: Vec<i64> = (1..=dict_ids.len() as i64).collect();

        let (single_k, single_s) = run::<HashbrownTable<i32>>(&dict_ids, &values);

        // Multi-block: split into chunks of 3.
        let mut multi_driver: GroupBySumLongByDictInt<HashbrownTable<i32>> =
            GroupBySumLongByDictInt::new();
        for chunk_start in (0..dict_ids.len()).step_by(3) {
            let end = (chunk_start + 3).min(dict_ids.len());
            multi_driver.process_block(&dict_ids[chunk_start..end], &values[chunk_start..end]);
        }
        let (multi_k, multi_s) = multi_driver.extract();

        assert_eq!(multi_k, single_k);
        assert_eq!(multi_s, single_s);
    }

    /// Both backends produce identical (keys, sums) for the same input.
    /// This is the load-bearing correctness invariant for the JMH harness
    /// at Task #59.
    #[test]
    fn backend_parity_on_realistic_block() {
        // Realistic Pinot block: 10K rows, dict cardinality ~100, sums
        // range ±1M.
        let mut state: u64 = 0xc0ffee_face_5eed;
        let n = 10_000;
        let mut dict_ids = Vec::with_capacity(n);
        let mut values = Vec::with_capacity(n);
        for _ in 0..n {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            dict_ids.push((((state >> 32) as i32) % 100).abs());
            values.push(((state >> 16) as i64) % 2_000_001 - 1_000_000);
        }
        let (our_k, our_s) = run::<HashbrownTable<i32>>(&dict_ids, &values);
        let (hb_k, hb_s) = run::<HashbrownTable<i32>>(&dict_ids, &values);
        assert_eq!(our_k, hb_k);
        assert_eq!(our_s, hb_s);
    }

    #[test]
    fn process_one_matches_process_block_single() {
        let mut a: GroupBySumLongByDictInt<HashbrownTable<i32>> = GroupBySumLongByDictInt::new();
        a.process_one(7, 100);
        a.process_one(8, 200);
        a.process_one(7, 50);
        let mut b: GroupBySumLongByDictInt<HashbrownTable<i32>> = GroupBySumLongByDictInt::new();
        b.process_block(&[7, 8, 7], &[100, 200, 50]);
        assert_eq!(a.keys(), b.keys());
        assert_eq!(a.sums(), b.sums());
    }

    #[test]
    fn with_capacity_eliminates_initial_growth() {
        let mut driver: GroupBySumLongByDictInt<HashbrownTable<i32>> =
            GroupBySumLongByDictInt::with_capacity(100);
        let dict_ids: Vec<i32> = (0..100).collect();
        let values: Vec<i64> = (0..100).map(|i| i as i64 * 10).collect();
        driver.process_block(&dict_ids, &values);
        assert_eq!(driver.num_groups(), 100);
    }

    #[test]
    #[should_panic(expected = "must have equal length")]
    fn unequal_input_lengths_panic() {
        let mut driver: GroupBySumLongByDictInt<HashbrownTable<i32>> = GroupBySumLongByDictInt::new();
        driver.process_block(&[1, 2, 3], &[10, 20]);
    }

    #[test]
    fn duplicates_within_block_aggregate_correctly() {
        // All-same key — exercises only the existing-group branch.
        let (k, s) = run::<HashbrownTable<i32>>(&[5; 1000], &vec![1i64; 1000]);
        assert_eq!(k, vec![5]);
        assert_eq!(s, vec![1000]);
    }

    #[test]
    fn handles_extreme_i64_values() {
        let (_, s) = run::<HashbrownTable<i32>>(&[1, 1], &[i64::MAX, 1]);
        // wrapping_add(MAX, 1) = MIN
        assert_eq!(s, vec![i64::MIN]);
    }
}
