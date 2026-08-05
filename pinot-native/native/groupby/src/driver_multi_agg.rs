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

//! Per-segment GROUP BY driver with multi-aggregation support — plan
//! step (1a), Task #60.
//!
//! Covers `SELECT <aggs...> FROM t GROUP BY dictIntCol` where `<aggs...>`
//! is any combination of SUM / MIN / MAX (over INT, LONG, FLOAT, DOUBLE
//! value columns) plus COUNT. Sibling drivers covering other key shapes
//! (raw INT/LONG/FLOAT/DOUBLE, multi-column) land at plan steps (1c),
//! (1d), (1e), (5).
//!
//! ## Two-phase block protocol
//!
//! Per block, the Java executor calls into this driver twice:
//!
//! 1. **Once** to probe/insert the dict-id column —
//!    [`process_block_keys`]. This grows the per-group accumulator arrays
//!    by the identity element of each declared agg, and caches the
//!    `group_id` array internally.
//! 2. **N times** (one per aggregation in the query) to apply per-agg
//!    value-array updates — [`apply_long`], [`apply_int`],
//!    [`apply_double`], [`apply_float`], or [`apply_count`]. Each reuses
//!    the cached `group_id` array, so the probe cost is paid only once
//!    per block regardless of agg count.
//!
//! The JNI surface follows the same shape: one `processBlockKeys` call
//! plus N `applyAgg<Type>` calls per block. JNI overhead per call is ~85
//! ns (per §11.A); for a typical 3-agg query that's ~340 ns/block of JNI
//! fixed cost over a block of ~10K rows, i.e. ~0.034 ns/row of overhead
//! — well below the per-row probe + agg-update cost.
//!
//! ## Group-id allocation
//!
//! Backends allocate `group_id` sequentially in insertion order (0 for
//! the first unique key, 1 for the second, etc.). This is a load-bearing
//! invariant shared with [`crate::GroupBySumLongByDictInt`]; see that
//! module's docs for the rationale. The multi-agg driver uses the same
//! "scan-for-new-group-ids" trick to grow per-group arrays without an
//! extra backend API.
//!
//! [`process_block_keys`]: GroupByDriverDictInt::process_block_keys
//! [`apply_long`]: GroupByDriverDictInt::apply_long
//! [`apply_int`]: GroupByDriverDictInt::apply_int
//! [`apply_double`]: GroupByDriverDictInt::apply_double
//! [`apply_float`]: GroupByDriverDictInt::apply_float
//! [`apply_count`]: GroupByDriverDictInt::apply_count

use crate::agg::{AggKind, AggState};
use crate::backend::GroupByBackend;
use crate::hash::HashKey;

/// Multi-aggregation per-segment GROUP BY driver, generic over the grouping
/// key type `K`.
///
/// `K` is whatever encodes a row's grouping key into a single hashable value:
/// * `i32` — a single dict-encoded column's dict_id ([`GroupByDriverDictInt`]).
/// * `i64` — a packed composite of several narrow dict-id columns (multi-key
///   fast path; see [`crate::multi_key`]).
/// * (future) `Canonical{F32,F64}` raw FP keys, row-encoded byte keys.
///
/// Generic over backend `B` (our `HashbrownTable<K>`, `HashbrownTable<K>`, or — for
/// dict-encoded `i32` keys — `DictDirectTable`). Aggregations are declared at
/// construction time and fixed thereafter, matching Pinot's per-query
/// aggregation list.
pub struct GroupByDriver<K, B>
where
    K: HashKey + Eq + Copy,
    B: GroupByBackend<K>,
{
    /// `key → group_id` table.
    table: B,

    /// `keys[group_id]` = the encoded key that originally allocated this group.
    keys: Vec<K>,

    /// One [`AggState`] per declared aggregation. All states have
    /// `len() == self.keys.len() == self.table.len()` after every block
    /// (invariant maintained by [`process_block_keys`]).
    ///
    /// [`process_block_keys`]: Self::process_block_keys
    aggs: Vec<AggState>,

    /// Group IDs from the most recent [`process_block_keys`] call. Reused
    /// by the subsequent `apply_*` calls for the same block; cleared by
    /// the next [`process_block_keys`] call.
    ///
    /// [`process_block_keys`]: Self::process_block_keys
    last_group_ids: Vec<u32>,
}

/// The single dict-encoded `i32`-key driver — a [`GroupByDriver`] over `i32`
/// dict_ids. The FFI single/multi-agg surface is built on this alias.
pub type GroupByDriverDictInt<B> = GroupByDriver<i32, B>;

impl<K, B> GroupByDriver<K, B>
where
    K: HashKey + Eq + Copy,
    B: GroupByBackend<K>,
{
    /// Construct an empty driver with the given agg list, default backend
    /// capacity. Use [`with_capacity`] when a group-cardinality estimate
    /// is available — e.g., `Dictionary.length()` is a tight upper bound
    /// for the dict-encoded case.
    ///
    /// [`with_capacity`]: Self::with_capacity
    pub fn new(agg_kinds: &[AggKind]) -> Self {
        Self {
            table: B::new(),
            keys: Vec::new(),
            aggs: agg_kinds.iter().map(|&k| AggState::new_for(k, 0)).collect(),
            last_group_ids: Vec::new(),
        }
    }

    /// Pre-size for `expected_groups` distinct groups across all internal
    /// allocations.
    pub fn with_capacity(agg_kinds: &[AggKind], expected_groups: usize) -> Self {
        Self {
            table: B::with_capacity(expected_groups),
            keys: Vec::with_capacity(expected_groups),
            aggs: agg_kinds
                .iter()
                .map(|&k| AggState::new_for(k, expected_groups))
                .collect(),
            last_group_ids: Vec::new(),
        }
    }

    // --- Inspectors -------------------------------------------------------

    /// Number of distinct groups seen so far.
    #[inline]
    pub fn num_groups(&self) -> usize {
        self.table.len()
    }

    /// Number of aggregations this driver was constructed for.
    #[inline]
    pub fn num_aggs(&self) -> usize {
        self.aggs.len()
    }

    /// Per-group keys in group_id order. `keys()[g]` is the encoded key that
    /// originally allocated group `g`.
    #[inline]
    pub fn keys(&self) -> &[K] {
        &self.keys
    }

    /// Read-only access to the agg state at index `idx`. Panics if out of
    /// range.
    #[inline]
    pub fn agg_state(&self, idx: usize) -> &AggState {
        &self.aggs[idx]
    }

    /// [`AggKind`] of the agg at index `idx`.
    #[inline]
    pub fn agg_kind(&self, idx: usize) -> AggKind {
        self.aggs[idx].kind()
    }

    // --- Block protocol ---------------------------------------------------

    /// **Phase 1 of a block:** probe/insert all dict_ids, grow per-group
    /// arrays for new groups, cache the resulting `group_ids` array for
    /// the subsequent `apply_*` calls.
    ///
    /// After this call:
    /// * `self.num_groups()` reflects all groups seen so far including
    ///   any newly allocated by this block.
    /// * Each agg state's `len()` equals `self.num_groups()` and any new
    ///   slots hold the kind's identity element.
    /// * The cached `last_group_ids` array has length `keys.len()`.
    pub fn process_block_keys(&mut self, keys: &[K]) {
        let n = keys.len();
        self.last_group_ids.clear();
        if n == 0 {
            return;
        }
        self.last_group_ids.resize(n, 0);
        self.table
            .probe_or_insert_batch(keys, &mut self.last_group_ids);

        // Walk in order. Backend assigns group_ids densely + insertion-
        // ordered, so the first row in this batch with
        // `group_id == self.keys.len()` is the row that created that group.
        let mut next_new_gid = self.keys.len();
        for i in 0..n {
            if self.last_group_ids[i] as usize == next_new_gid {
                self.keys.push(keys[i]);
                for agg in self.aggs.iter_mut() {
                    agg.push_new_group();
                }
                next_new_gid += 1;
            }
        }
        debug_assert_eq!(self.keys.len(), self.table.len());
        for agg in &self.aggs {
            debug_assert_eq!(agg.len(), self.table.len());
        }
    }

    /// **Phase 2:** apply an i64-valued aggregation (SumLong, MinLong,
    /// MaxLong) over the cached group_ids from the most recent
    /// [`process_block_keys`] call.
    ///
    /// Panics if `agg_idx` doesn't refer to an i64-valued agg, or if
    /// `values.len()` doesn't match the cached block length.
    ///
    /// [`process_block_keys`]: Self::process_block_keys
    pub fn apply_long(&mut self, agg_idx: usize, values: &[i64]) {
        assert_eq!(
            self.last_group_ids.len(),
            values.len(),
            "values.len() must match the cached block length from process_block_keys"
        );
        self.aggs[agg_idx].apply_long_batch(&self.last_group_ids, values);
    }

    /// **Phase 2:** apply an i32-valued aggregation (MinInt, MaxInt).
    /// Same protocol as [`apply_long`].
    ///
    /// [`apply_long`]: Self::apply_long
    pub fn apply_int(&mut self, agg_idx: usize, values: &[i32]) {
        assert_eq!(
            self.last_group_ids.len(),
            values.len(),
            "values.len() must match the cached block length from process_block_keys"
        );
        self.aggs[agg_idx].apply_int_batch(&self.last_group_ids, values);
    }

    /// **Phase 2:** apply an f64-valued aggregation (SumDouble, MinDouble,
    /// MaxDouble). MIN/MAX use Java NaN-propagating semantics
    /// ([`crate::agg::java_min_f64`] / [`crate::agg::java_max_f64`]).
    pub fn apply_double(&mut self, agg_idx: usize, values: &[f64]) {
        assert_eq!(
            self.last_group_ids.len(),
            values.len(),
            "values.len() must match the cached block length from process_block_keys"
        );
        self.aggs[agg_idx].apply_double_batch(&self.last_group_ids, values);
    }

    /// **Phase 2:** apply an f32-valued aggregation (MinFloat, MaxFloat).
    pub fn apply_float(&mut self, agg_idx: usize, values: &[f32]) {
        assert_eq!(
            self.last_group_ids.len(),
            values.len(),
            "values.len() must match the cached block length from process_block_keys"
        );
        self.aggs[agg_idx].apply_float_batch(&self.last_group_ids, values);
    }

    /// **Phase 2:** apply COUNT — increments per-group count by 1 for
    /// each row in the cached `last_group_ids`. Takes no value array;
    /// COUNT only depends on the dict_ids stream that was just probed.
    pub fn apply_count(&mut self, agg_idx: usize) {
        self.aggs[agg_idx].apply_count_batch(&self.last_group_ids);
    }

    /// Consume the driver and return its parts (keys + agg states).
    /// `keys[g]` is the encoded key for group `g`; each `AggState`'s slice
    /// at index `g` is the final accumulator for that group.
    pub fn extract(self) -> (Vec<K>, Vec<AggState>) {
        (self.keys, self.aggs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HashbrownTable;

    // --- Test helpers ----------------------------------------------------

    fn drive_single_agg<B: GroupByBackend<i32>>(
        kind: AggKind,
        dict_ids: &[i32],
        long_values: Option<&[i64]>,
        int_values: Option<&[i32]>,
        double_values: Option<&[f64]>,
        float_values: Option<&[f32]>,
    ) -> (Vec<i32>, AggState) {
        let mut d = GroupByDriverDictInt::<B>::new(&[kind]);
        d.process_block_keys(dict_ids);
        match kind {
            AggKind::SumLong | AggKind::MinLong | AggKind::MaxLong | AggKind::SumLongToDouble => {
                d.apply_long(0, long_values.expect("long_values required"));
            }
            AggKind::MinInt | AggKind::MaxInt | AggKind::SumIntToDouble => {
                d.apply_int(0, int_values.expect("int_values required"));
            }
            AggKind::SumDouble | AggKind::MinDouble | AggKind::MaxDouble => {
                d.apply_double(0, double_values.expect("double_values required"));
            }
            AggKind::MinFloat | AggKind::MaxFloat | AggKind::SumFloatToDouble => {
                d.apply_float(0, float_values.expect("float_values required"));
            }
            AggKind::Count => {
                d.apply_count(0);
            }
        }
        let (keys, mut aggs) = d.extract();
        (keys, aggs.pop().unwrap())
    }

    // --- Empty input -----------------------------------------------------

    #[test]
    fn empty_input_with_sum_long_yields_empty_state() {
        let mut d = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&[AggKind::SumLong]);
        d.process_block_keys(&[]);
        d.apply_long(0, &[]);
        assert_eq!(d.num_groups(), 0);
        assert_eq!(d.keys().len(), 0);
        assert_eq!(d.agg_state(0).as_long_slice(), Some(&[][..]));
    }

    #[test]
    fn zero_agg_driver_still_tracks_groups() {
        // Edge case: query with only COUNT is the common "GROUP BY x" with
        // no aggregations — but you can also construct a driver with []
        // agg list for testing pure key tracking.
        let mut d = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&[]);
        d.process_block_keys(&[1, 2, 1, 3]);
        assert_eq!(d.num_groups(), 3);
        assert_eq!(d.keys(), &[1, 2, 3]);
        assert_eq!(d.num_aggs(), 0);
    }

    // --- Single agg per kind ---------------------------------------------

    #[test]
    fn sum_long_single_block() {
        let (keys, state) = drive_single_agg::<HashbrownTable<i32>>(
            AggKind::SumLong,
            &[1, 2, 1, 3, 2],
            Some(&[10, 20, 30, 40, 50]),
            None,
            None,
            None,
        );
        assert_eq!(keys, vec![1, 2, 3]);
        assert_eq!(state.as_long_slice(), Some(&[40i64, 70, 40][..]));
    }

    #[test]
    fn min_long_single_block() {
        let (keys, state) = drive_single_agg::<HashbrownTable<i32>>(
            AggKind::MinLong,
            &[1, 2, 1, 2, 3],
            Some(&[100, 50, 10, 60, 999]),
            None,
            None,
            None,
        );
        assert_eq!(keys, vec![1, 2, 3]);
        assert_eq!(state.as_long_slice(), Some(&[10i64, 50, 999][..]));
    }

    #[test]
    fn max_long_single_block() {
        let (keys, state) = drive_single_agg::<HashbrownTable<i32>>(
            AggKind::MaxLong,
            &[1, 2, 1, 2, 3],
            Some(&[100, 50, 10, 60, 999]),
            None,
            None,
            None,
        );
        assert_eq!(keys, vec![1, 2, 3]);
        assert_eq!(state.as_long_slice(), Some(&[100i64, 60, 999][..]));
    }

    #[test]
    fn min_int_single_block() {
        let (keys, state) = drive_single_agg::<HashbrownTable<i32>>(
            AggKind::MinInt,
            &[1, 2, 1, 2],
            None,
            Some(&[5, 10, -3, 100]),
            None,
            None,
        );
        assert_eq!(keys, vec![1, 2]);
        assert_eq!(state.as_int_slice(), Some(&[-3i32, 10][..]));
    }

    #[test]
    fn max_int_single_block() {
        let (keys, state) = drive_single_agg::<HashbrownTable<i32>>(
            AggKind::MaxInt,
            &[1, 2, 1, 2],
            None,
            Some(&[5, 10, -3, 100]),
            None,
            None,
        );
        assert_eq!(keys, vec![1, 2]);
        assert_eq!(state.as_int_slice(), Some(&[5i32, 100][..]));
    }

    #[test]
    fn sum_double_single_block() {
        let (keys, state) = drive_single_agg::<HashbrownTable<i32>>(
            AggKind::SumDouble,
            &[1, 1, 2],
            None,
            None,
            Some(&[1.5, 2.5, 100.0]),
            None,
        );
        assert_eq!(keys, vec![1, 2]);
        assert_eq!(state.as_double_slice(), Some(&[4.0f64, 100.0][..]));
    }

    #[test]
    fn min_double_single_block() {
        let (keys, state) = drive_single_agg::<HashbrownTable<i32>>(
            AggKind::MinDouble,
            &[1, 1, 2],
            None,
            None,
            Some(&[1.5, -2.5, 100.0]),
            None,
        );
        assert_eq!(keys, vec![1, 2]);
        assert_eq!(state.as_double_slice(), Some(&[-2.5f64, 100.0][..]));
    }

    #[test]
    fn max_double_single_block() {
        let (keys, state) = drive_single_agg::<HashbrownTable<i32>>(
            AggKind::MaxDouble,
            &[1, 1, 2],
            None,
            None,
            Some(&[1.5, -2.5, 100.0]),
            None,
        );
        assert_eq!(keys, vec![1, 2]);
        assert_eq!(state.as_double_slice(), Some(&[1.5f64, 100.0][..]));
    }

    #[test]
    fn min_max_float_single_block() {
        let (keys_min, state_min) = drive_single_agg::<HashbrownTable<i32>>(
            AggKind::MinFloat,
            &[1, 1, 2],
            None,
            None,
            None,
            Some(&[1.5, -2.5, 100.0]),
        );
        assert_eq!(keys_min, vec![1, 2]);
        assert_eq!(state_min.as_float_slice(), Some(&[-2.5f32, 100.0][..]));

        let (keys_max, state_max) = drive_single_agg::<HashbrownTable<i32>>(
            AggKind::MaxFloat,
            &[1, 1, 2],
            None,
            None,
            None,
            Some(&[1.5, -2.5, 100.0]),
        );
        assert_eq!(keys_max, vec![1, 2]);
        assert_eq!(state_max.as_float_slice(), Some(&[1.5f32, 100.0][..]));
    }

    #[test]
    fn count_single_block() {
        let mut d = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&[AggKind::Count]);
        d.process_block_keys(&[1, 2, 1, 3, 2, 1]);
        d.apply_count(0);
        let (keys, mut aggs) = d.extract();
        assert_eq!(keys, vec![1, 2, 3]);
        assert_eq!(aggs.pop().unwrap().as_long_slice(), Some(&[3i64, 2, 1][..]));
    }

    // --- FP NaN propagation (Java semantics) -----------------------------

    #[test]
    fn min_double_propagates_nan_per_group() {
        // Group 1 sees a NaN among real values → result NaN.
        // Group 2 sees only real values → result is the smallest.
        let mut d = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&[AggKind::MinDouble]);
        d.process_block_keys(&[1, 1, 1, 2, 2]);
        d.apply_double(0, &[1.0, f64::NAN, 5.0, 100.0, 50.0]);
        let s = d.agg_state(0).as_double_slice().unwrap();
        assert!(s[0].is_nan(), "group 1 should propagate NaN");
        assert_eq!(s[1], 50.0);
    }

    #[test]
    fn max_double_propagates_nan_per_group() {
        let mut d = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&[AggKind::MaxDouble]);
        d.process_block_keys(&[1, 1, 2, 2]);
        d.apply_double(0, &[100.0, f64::NAN, 50.0, 75.0]);
        let s = d.agg_state(0).as_double_slice().unwrap();
        assert!(s[0].is_nan());
        assert_eq!(s[1], 75.0);
    }

    #[test]
    fn min_float_nan_sticks_across_blocks() {
        // Once a NaN lands in a group's accumulator, every subsequent value
        // in that group remains NaN — Java semantics.
        let mut d = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&[AggKind::MinFloat]);
        d.process_block_keys(&[7]);
        d.apply_float(0, &[f32::NAN]);
        assert!(d.agg_state(0).as_float_slice().unwrap()[0].is_nan());
        d.process_block_keys(&[7]);
        d.apply_float(0, &[1.0]);
        assert!(
            d.agg_state(0).as_float_slice().unwrap()[0].is_nan(),
            "NaN must remain sticky"
        );
    }

    #[test]
    fn min_double_signed_zero() {
        // -0.0 must beat +0.0 in MIN.
        let mut d = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&[AggKind::MinDouble]);
        d.process_block_keys(&[1, 1]);
        d.apply_double(0, &[0.0, -0.0]);
        let s = d.agg_state(0).as_double_slice().unwrap();
        assert_eq!(s[0], 0.0);
        assert!(s[0].is_sign_negative(), "min must preserve -0.0 sign");
    }

    // --- Multi-agg in one query ------------------------------------------

    #[test]
    fn multi_agg_query_one_block() {
        // SELECT SUM(longCol), MIN(longCol), MAX(longCol), COUNT(*)
        //   FROM t GROUP BY dictCol
        let kinds = [AggKind::SumLong, AggKind::MinLong, AggKind::MaxLong, AggKind::Count];
        let mut d = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&kinds);
        let dict_ids = [1, 2, 1, 3, 2, 1];
        let values: [i64; 6] = [10, 20, 30, 40, 50, 100];
        d.process_block_keys(&dict_ids);
        d.apply_long(0, &values); // SUM
        d.apply_long(1, &values); // MIN
        d.apply_long(2, &values); // MAX
        d.apply_count(3);

        assert_eq!(d.keys(), &[1, 2, 3]);
        // Group 1: rows at 0, 2, 5 — values [10, 30, 100]
        // Group 2: rows at 1, 4       — values [20, 50]
        // Group 3: row  at 3           — values [40]
        assert_eq!(d.agg_state(0).as_long_slice(), Some(&[140i64, 70, 40][..])); // SUM
        assert_eq!(d.agg_state(1).as_long_slice(), Some(&[10i64, 20, 40][..])); // MIN
        assert_eq!(d.agg_state(2).as_long_slice(), Some(&[100i64, 50, 40][..])); // MAX
        assert_eq!(d.agg_state(3).as_long_slice(), Some(&[3i64, 2, 1][..])); // COUNT
    }

    #[test]
    fn multi_agg_mixed_value_types() {
        // SELECT SUM(longCol), MIN(intCol), MAX(doubleCol), COUNT(*)
        //   FROM t GROUP BY dictCol
        let kinds = [AggKind::SumLong, AggKind::MinInt, AggKind::MaxDouble, AggKind::Count];
        let mut d = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&kinds);
        let dict_ids = [1, 2, 1, 2];
        d.process_block_keys(&dict_ids);
        d.apply_long(0, &[10, 20, 30, 40]);
        d.apply_int(1, &[5, 100, 50, 200]);
        d.apply_double(2, &[1.5, 2.5, 3.5, 0.5]);
        d.apply_count(3);

        assert_eq!(d.keys(), &[1, 2]);
        assert_eq!(d.agg_state(0).as_long_slice(), Some(&[40i64, 60][..]));
        assert_eq!(d.agg_state(1).as_int_slice(), Some(&[5i32, 100][..]));
        assert_eq!(d.agg_state(2).as_double_slice(), Some(&[3.5f64, 2.5][..]));
        assert_eq!(d.agg_state(3).as_long_slice(), Some(&[2i64, 2][..]));
    }

    // --- Multi-block accumulation -----------------------------------------

    #[test]
    fn multi_block_accumulation_matches_single_block() {
        let dict_ids = [1, 2, 1, 3, 2, 4, 1, 5, 2, 3];
        let values: Vec<i64> = (1..=dict_ids.len() as i64).collect();

        // Reference: one big block.
        let mut single = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&[AggKind::SumLong]);
        single.process_block_keys(&dict_ids);
        single.apply_long(0, &values);
        let (s_keys, mut s_aggs) = single.extract();
        let s_sums = s_aggs.pop().unwrap();

        // Multi-block: chunks of 3.
        let mut multi = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&[AggKind::SumLong]);
        for chunk_start in (0..dict_ids.len()).step_by(3) {
            let end = (chunk_start + 3).min(dict_ids.len());
            multi.process_block_keys(&dict_ids[chunk_start..end]);
            multi.apply_long(0, &values[chunk_start..end]);
        }
        let (m_keys, mut m_aggs) = multi.extract();
        let m_sums = m_aggs.pop().unwrap();

        assert_eq!(s_keys, m_keys);
        assert_eq!(s_sums.as_long_slice(), m_sums.as_long_slice());
    }

    #[test]
    fn new_groups_mid_stream_initialize_with_identity() {
        let mut d = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&[
            AggKind::SumLong,
            AggKind::MinDouble,
            AggKind::MaxInt,
            AggKind::Count,
        ]);

        // Block 1: groups 1 only.
        d.process_block_keys(&[1, 1]);
        d.apply_long(0, &[100, 200]);
        d.apply_double(1, &[10.0, 5.0]);
        d.apply_int(2, &[7, 9]);
        d.apply_count(3);

        // Block 2: introduces group 2.
        d.process_block_keys(&[2]);
        d.apply_long(0, &[50]);
        d.apply_double(1, &[3.5]);
        d.apply_int(2, &[-1]);
        d.apply_count(3);

        // Group 2 must have correctly initialized identities before its
        // values were applied — i.e. SUM 50 (not garbage), MIN 3.5
        // (not still INFINITY because the apply ran AFTER push_new_group),
        // MAX -1 (not still i32::MIN by accident), COUNT 1.
        assert_eq!(d.agg_state(0).as_long_slice(), Some(&[300i64, 50][..]));
        assert_eq!(d.agg_state(1).as_double_slice(), Some(&[5.0f64, 3.5][..]));
        assert_eq!(d.agg_state(2).as_int_slice(), Some(&[9i32, -1][..]));
        assert_eq!(d.agg_state(3).as_long_slice(), Some(&[2i64, 1][..]));
    }

    // --- Backend parity --------------------------------------------------

    #[test]
    fn both_backends_produce_identical_results() {
        let mut state: u64 = 0xc0ffee_d1c7_5eed;
        let n = 10_000;
        let mut dict_ids = Vec::with_capacity(n);
        let mut long_vals = Vec::with_capacity(n);
        let mut int_vals = Vec::with_capacity(n);
        let mut double_vals = Vec::with_capacity(n);
        for _ in 0..n {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            dict_ids.push((((state >> 32) as i32) % 100).abs());
            long_vals.push(((state >> 16) as i64) % 1_000_001);
            int_vals.push(((state >> 24) as i32) % 1_000_001);
            double_vals.push(((state >> 8) as f64 / 1e10) % 1000.0);
        }
        let kinds = [
            AggKind::SumLong,
            AggKind::MinLong,
            AggKind::MaxLong,
            AggKind::MinInt,
            AggKind::MaxInt,
            AggKind::SumDouble,
            AggKind::MinDouble,
            AggKind::MaxDouble,
            AggKind::Count,
        ];

        let mut a = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&kinds);
        a.process_block_keys(&dict_ids);
        a.apply_long(0, &long_vals);
        a.apply_long(1, &long_vals);
        a.apply_long(2, &long_vals);
        a.apply_int(3, &int_vals);
        a.apply_int(4, &int_vals);
        a.apply_double(5, &double_vals);
        a.apply_double(6, &double_vals);
        a.apply_double(7, &double_vals);
        a.apply_count(8);

        let mut b = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&kinds);
        b.process_block_keys(&dict_ids);
        b.apply_long(0, &long_vals);
        b.apply_long(1, &long_vals);
        b.apply_long(2, &long_vals);
        b.apply_int(3, &int_vals);
        b.apply_int(4, &int_vals);
        b.apply_double(5, &double_vals);
        b.apply_double(6, &double_vals);
        b.apply_double(7, &double_vals);
        b.apply_count(8);

        assert_eq!(a.keys(), b.keys());
        for i in 0..kinds.len() {
            let kind = kinds[i];
            match kind {
                AggKind::SumLong | AggKind::MinLong | AggKind::MaxLong | AggKind::Count => {
                    assert_eq!(
                        a.agg_state(i).as_long_slice(),
                        b.agg_state(i).as_long_slice(),
                        "long-valued kind {:?} diverged across backends",
                        kind
                    );
                }
                AggKind::MinInt | AggKind::MaxInt => {
                    assert_eq!(
                        a.agg_state(i).as_int_slice(),
                        b.agg_state(i).as_int_slice(),
                        "int-valued kind {:?} diverged",
                        kind
                    );
                }
                AggKind::SumDouble | AggKind::MinDouble | AggKind::MaxDouble => {
                    assert_eq!(
                        a.agg_state(i).as_double_slice(),
                        b.agg_state(i).as_double_slice(),
                        "double-valued kind {:?} diverged",
                        kind
                    );
                }
                AggKind::MinFloat
                | AggKind::MaxFloat
                | AggKind::SumIntToDouble
                | AggKind::SumLongToDouble
                | AggKind::SumFloatToDouble => unreachable!(),
            }
        }
    }

    // --- SUM accumulated in f64 (Pinot getDoubleValuesSV parity) ----------

    #[test]
    fn sum_long_to_double_widens_not_wraps() {
        // Contrast with extreme_i64_values_wrap_for_sum (SumLong, i64 wrap):
        // the ToDouble path must match Pinot's i64→f64 widen + double
        // accumulate, i.e. it rounds — it does NOT wrap to i64::MIN.
        let mut d = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&[AggKind::SumLongToDouble]);
        d.process_block_keys(&[1, 1]);
        d.apply_long(0, &[i64::MAX, 1]);
        let expected = (i64::MAX as f64) + 1.0;
        assert_eq!(d.agg_state(0).as_double_slice(), Some(&[expected][..]));
    }

    #[test]
    fn sum_int_to_double_single_block() {
        let mut d = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&[AggKind::SumIntToDouble]);
        d.process_block_keys(&[1, 2, 1]);
        d.apply_int(0, &[10, 20, 30]);
        assert_eq!(d.agg_state(0).as_double_slice(), Some(&[40.0f64, 20.0][..]));
    }

    #[test]
    fn sum_float_to_double_widens_each_element() {
        let mut d = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&[AggKind::SumFloatToDouble]);
        d.process_block_keys(&[1, 1]);
        d.apply_float(0, &[1.5f32, 2.5f32]);
        assert_eq!(d.agg_state(0).as_double_slice(), Some(&[4.0f64][..]));
    }

    #[test]
    fn sum_to_double_both_backends_agree() {
        let kinds = [
            AggKind::SumIntToDouble,
            AggKind::SumLongToDouble,
            AggKind::SumFloatToDouble,
            AggKind::SumDouble,
        ];
        let dict_ids = [1, 2, 1, 3, 2, 1, 4, 2];
        let iv: [i32; 8] = [1, 2, 3, 4, 5, 6, 7, 8];
        let lv: [i64; 8] = [10, 20, 30, 40, 50, 60, 70, 80];
        let fv: [f32; 8] = [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5];
        let dv: [f64; 8] = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8];

        let mut a = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&kinds);
        a.process_block_keys(&dict_ids);
        a.apply_int(0, &iv);
        a.apply_long(1, &lv);
        a.apply_float(2, &fv);
        a.apply_double(3, &dv);

        let mut b = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&kinds);
        b.process_block_keys(&dict_ids);
        b.apply_int(0, &iv);
        b.apply_long(1, &lv);
        b.apply_float(2, &fv);
        b.apply_double(3, &dv);

        assert_eq!(a.keys(), b.keys());
        for i in 0..kinds.len() {
            assert_eq!(
                a.agg_state(i).as_double_slice(),
                b.agg_state(i).as_double_slice(),
                "f64-sum kind {:?} diverged across backends",
                kinds[i]
            );
        }
    }

    // --- Edge cases / panics ---------------------------------------------

    #[test]
    fn with_capacity_eliminates_initial_growth() {
        let mut d = GroupByDriverDictInt::<HashbrownTable<i32>>::with_capacity(&[AggKind::SumLong], 100);
        let dict_ids: Vec<i32> = (0..100).collect();
        let values: Vec<i64> = (0..100).map(|i| i as i64 * 10).collect();
        d.process_block_keys(&dict_ids);
        d.apply_long(0, &values);
        assert_eq!(d.num_groups(), 100);
    }

    #[test]
    fn duplicates_within_block_aggregate_correctly() {
        let mut d = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&[AggKind::SumLong, AggKind::Count]);
        d.process_block_keys(&[5; 1000]);
        d.apply_long(0, &[1i64; 1000]);
        d.apply_count(1);
        assert_eq!(d.keys(), &[5]);
        assert_eq!(d.agg_state(0).as_long_slice(), Some(&[1000i64][..]));
        assert_eq!(d.agg_state(1).as_long_slice(), Some(&[1000i64][..]));
    }

    #[test]
    fn extreme_i64_values_wrap_for_sum() {
        let mut d = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&[AggKind::SumLong]);
        d.process_block_keys(&[1, 1]);
        d.apply_long(0, &[i64::MAX, 1]);
        // wrapping_add(MAX, 1) = MIN
        assert_eq!(d.agg_state(0).as_long_slice(), Some(&[i64::MIN][..]));
    }

    #[test]
    #[should_panic(expected = "apply_long_batch on non-long agg")]
    fn apply_long_panics_for_int_agg() {
        let mut d = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&[AggKind::MinInt]);
        d.process_block_keys(&[1]);
        d.apply_long(0, &[10]);
    }

    #[test]
    #[should_panic(expected = "apply_int_batch on non-int agg")]
    fn apply_int_panics_for_long_agg() {
        let mut d = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&[AggKind::SumLong]);
        d.process_block_keys(&[1]);
        d.apply_int(0, &[10]);
    }

    #[test]
    #[should_panic(expected = "apply_double_batch on non-double agg")]
    fn apply_double_panics_for_float_agg() {
        let mut d = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&[AggKind::MinFloat]);
        d.process_block_keys(&[1]);
        d.apply_double(0, &[1.0]);
    }

    #[test]
    #[should_panic(expected = "apply_count_batch on non-count agg")]
    fn apply_count_panics_for_sum_agg() {
        let mut d = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&[AggKind::SumLong]);
        d.process_block_keys(&[1]);
        d.apply_count(0);
    }

    #[test]
    #[should_panic(expected = "values.len() must match the cached block length")]
    fn apply_long_panics_on_length_mismatch() {
        let mut d = GroupByDriverDictInt::<HashbrownTable<i32>>::new(&[AggKind::SumLong]);
        d.process_block_keys(&[1, 2, 3]);
        d.apply_long(0, &[10, 20]);
    }
}
