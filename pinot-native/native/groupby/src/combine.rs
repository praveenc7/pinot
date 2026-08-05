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

//! Server-level cross-segment combine — single-threaded merge core (Task #54).
//!
//! This is the correctness-first core of the SOTA combine design (design doc
//! §22.8.1 / §23). The parallel, radix-partitioned, work-stealing layer is
//! built on top of this in a sibling module — this module owns the merge
//! *semantics*; the parallel layer owns the *scheduling*.
//!
//! ## What combine does
//!
//! Each segment's native GROUP BY produces a **partial**: a set of
//! `(raw_key, per-agg partial accumulator)` tuples (one per segment-local
//! group). Combine merges the partials of *all* segments that the query
//! touched into one cross-segment result.
//!
//! ## Why raw-value keyed (not dict_id)
//!
//! Dict ids are **segment-local** — segment A's `dict_id=5` is unrelated to
//! segment B's. There is no global dictionary. So the segment boundary
//! materializes `dict_id → raw value` (Task #52, bounded by per-segment group
//! count) and combine hashes **raw values** throughout. This driver is
//! therefore generic over the raw key type `K` (`i32` / `i64` /
//! `Canonical{F32,F64}` / … any [`HashKey`]) rather than fixed to `i32` dict
//! ids like the segment driver.
//!
//! ## Merge semantics
//!
//! Merging a partial accumulator into the running accumulator is the same op
//! the segment used, lifted to accumulator-vs-accumulator (see
//! [`AggState::merge_slot`]): SUM/COUNT → add, MIN → min, MAX → max (Java
//! NaN-propagating for FP). All are associative + commutative, which is what
//! makes the radix-partitioned parallel merge correct: each final group is
//! folded by exactly one worker, in any order.
//!
//! [`HashKey`]: crate::hash::HashKey
//! [`AggState::merge_slot`]: crate::agg::AggState::merge_slot

use crate::agg::{AggKind, AggState};
use crate::backend::GroupByBackend;
use crate::combine_parallel::combine_parallel;
use crate::hash::HashKey;
use crate::topk::{compute_selection, OrderKey, OrderTerm};

/// One segment's partial GROUP BY output, as consumed by combine.
///
/// `keys[g]` is the raw key for segment-local group `g`; `aggs[a]` holds agg
/// `a`'s partial accumulator, indexed by `g`. All `aggs[a].len() == keys.len()`.
/// The agg kinds + order match the query's aggregation list (and the combine
/// driver's).
pub struct SegmentPartial<K> {
    pub keys: Vec<K>,
    pub aggs: Vec<AggState>,
}

impl<K> SegmentPartial<K> {
    pub fn new(keys: Vec<K>, aggs: Vec<AggState>) -> Self {
        debug_assert!(aggs.iter().all(|a| a.len() == keys.len()));
        Self { keys, aggs }
    }

    /// Number of segment-local groups in this partial.
    #[inline]
    pub fn num_groups(&self) -> usize {
        self.keys.len()
    }
}

/// Cross-segment combine table, generic over raw key type `K` and Path C
/// backend `B`. Holds one running [`AggState`] per aggregation, keyed by raw
/// value. Built per query (or per radix partition, in the parallel layer).
pub struct CombineDriver<K, B> {
    /// `raw_key → combine group_id`.
    table: B,
    /// `keys[combine_group_id]` = the raw key for that combined group.
    keys: Vec<K>,
    /// One running partial per aggregation; same kinds/order as the query.
    aggs: Vec<AggState>,
}

impl<K, B> CombineDriver<K, B>
where
    K: HashKey + Eq + Copy,
    B: GroupByBackend<K>,
{
    /// Empty combine driver for the given aggregation list (must match the
    /// kinds the segments produced).
    pub fn new(agg_kinds: &[AggKind]) -> Self {
        Self {
            table: B::new(),
            keys: Vec::new(),
            aggs: agg_kinds.iter().map(|&k| AggState::new_for(k, 0)).collect(),
        }
    }

    /// Pre-size for `expected_groups` distinct combined groups (e.g. an
    /// estimate of the union cardinality across segments).
    pub fn with_capacity(agg_kinds: &[AggKind], expected_groups: usize) -> Self {
        Self {
            table: B::with_capacity(expected_groups),
            keys: Vec::with_capacity(expected_groups),
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

    #[inline]
    pub fn keys(&self) -> &[K] {
        &self.keys
    }

    #[inline]
    pub fn agg_state(&self, idx: usize) -> &AggState {
        &self.aggs[idx]
    }

    /// Merge one segment's partial (keys + per-agg accumulators) into this
    /// table. For each segment-local group, probe-or-insert its raw key to find
    /// (or allocate) the combined group, then fold each agg's partial into the
    /// combined accumulator via [`AggState::merge_slot`].
    ///
    /// Panics in debug if `seg_aggs.len()` doesn't match this driver's agg
    /// count, or if any `seg_aggs[a].len()` differs from `seg_keys.len()`.
    pub fn merge_partials(&mut self, seg_keys: &[K], seg_aggs: &[AggState]) {
        debug_assert_eq!(seg_aggs.len(), self.aggs.len());
        debug_assert!(seg_aggs.iter().all(|a| a.len() == seg_keys.len()));
        for g in 0..seg_keys.len() {
            self.merge_one(seg_keys[g], seg_aggs, g);
        }
    }

    /// Merge a single source group `(key, src_aggs[*][src_idx])` into this
    /// table. Probe-or-insert the raw key (allocating + identity-initializing a
    /// new combined group if unseen), then fold each agg's partial at `src_idx`
    /// into the combined accumulator. This is the inner step of both
    /// [`Self::merge_partials`] and the parallel partition-merge.
    #[inline]
    pub fn merge_one(&mut self, key: K, src_aggs: &[AggState], src_idx: usize) {
        let cg = self.table.probe_or_insert(key) as usize;
        if cg == self.keys.len() {
            self.keys.push(key);
            for a in self.aggs.iter_mut() {
                a.push_new_group();
            }
        }
        for (a_idx, a) in self.aggs.iter_mut().enumerate() {
            a.merge_slot(cg, &src_aggs[a_idx], src_idx);
        }
    }

    /// Convenience wrapper over [`Self::merge_partials`] for a [`SegmentPartial`].
    #[inline]
    pub fn merge(&mut self, partial: &SegmentPartial<K>) {
        self.merge_partials(&partial.keys, &partial.aggs);
    }

    /// Fold another combine driver's contents into this one. Used by the
    /// parallel partition-merge (Phase 2): the worker owning radix partition
    /// `p` folds every other worker's thread-local partition-`p` table into the
    /// final partition-`p` table. Since `other` is itself a combine partial,
    /// this is just [`Self::merge_partials`] over its keys + accumulators.
    #[inline]
    pub fn merge_driver(&mut self, other: &CombineDriver<K, B>) {
        self.merge_partials(&other.keys, &other.aggs);
    }

    /// Consume the driver and return its combined keys + accumulators.
    pub fn extract(self) -> (Vec<K>, Vec<AggState>) {
        (self.keys, self.aggs)
    }
}

/// Stateful driver of the combine over the JNI boundary: accumulate each
/// segment's partial (keys + one typed agg array per aggregation), then run the
/// parallel merge and expose the combined result. This is the orchestration the
/// combine JNI surface wraps — kept here (pure Rust) so it is testable without a
/// JVM.
///
/// Lifecycle: `begin_partial` → `set_agg_*` (once per aggregation) →
/// `commit_partial`, repeated per segment; then `finish` once; then read
/// `result_keys` / `result_agg`.
pub struct CombineSession<K, B> {
    agg_kinds: Vec<AggKind>,
    partials: Vec<SegmentPartial<K>>,
    staging_keys: Option<Vec<K>>,
    staging_aggs: Vec<Option<AggState>>,
    result: Option<SegmentPartial<K>>,
    _backend: std::marker::PhantomData<fn() -> B>,
}

impl<K, B> CombineSession<K, B>
where
    K: HashKey + Eq + Copy + Send + Sync,
    B: GroupByBackend<K> + Send,
{
    pub fn new(agg_kinds: &[AggKind]) -> Self {
        Self {
            agg_kinds: agg_kinds.to_vec(),
            partials: Vec::new(),
            staging_keys: None,
            staging_aggs: (0..agg_kinds.len()).map(|_| None).collect(),
            result: None,
            _backend: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn num_aggs(&self) -> usize {
        self.agg_kinds.len()
    }

    /// Begin a new segment partial with its per-group keys. Subsequent
    /// `set_agg_*` calls fill in each aggregation before `commit_partial`.
    pub fn begin_partial(&mut self, keys: Vec<K>) {
        self.staging_keys = Some(keys);
        self.staging_aggs.fill_with(|| None);
    }

    /// Set aggregation `agg_idx`'s partial from a segment-extracted i64 vector.
    pub fn set_agg_long(&mut self, agg_idx: usize, v: Vec<i64>) {
        self.staging_aggs[agg_idx] = Some(AggState::from_long_vec(self.agg_kinds[agg_idx], v));
    }

    /// Set aggregation `agg_idx`'s partial from a segment-extracted i32 vector.
    pub fn set_agg_int(&mut self, agg_idx: usize, v: Vec<i32>) {
        self.staging_aggs[agg_idx] = Some(AggState::from_int_vec(self.agg_kinds[agg_idx], v));
    }

    /// Set aggregation `agg_idx`'s partial from a segment-extracted f64 vector.
    pub fn set_agg_double(&mut self, agg_idx: usize, v: Vec<f64>) {
        self.staging_aggs[agg_idx] = Some(AggState::from_double_vec(self.agg_kinds[agg_idx], v));
    }

    /// Set aggregation `agg_idx`'s partial from a segment-extracted f32 vector.
    pub fn set_agg_float(&mut self, agg_idx: usize, v: Vec<f32>) {
        self.staging_aggs[agg_idx] = Some(AggState::from_float_vec(self.agg_kinds[agg_idx], v));
    }

    /// Finalize the staged partial and add it to the set to be combined.
    /// Panics if `begin_partial` wasn't called or some aggregation is unset.
    pub fn commit_partial(&mut self) {
        let keys = self.staging_keys.take().expect("commit_partial without begin_partial");
        let mut aggs = Vec::with_capacity(self.staging_aggs.len());
        for (i, slot) in self.staging_aggs.iter_mut().enumerate() {
            aggs.push(slot.take().unwrap_or_else(|| panic!("aggregation {i} not set before commit_partial")));
        }
        self.partials.push(SegmentPartial::new(keys, aggs));
    }

    /// Run the parallel radix-partitioned merge over all committed partials.
    pub fn finish(&mut self, radix_bits: u32) {
        let (keys, aggs) = combine_parallel::<K, B>(&self.partials, &self.agg_kinds, radix_bits);
        self.result = Some(SegmentPartial::new(keys, aggs));
    }

    /// Combined group count (after `finish`).
    pub fn result_num_groups(&self) -> usize {
        self.result.as_ref().map_or(0, SegmentPartial::num_groups)
    }

    /// Combined keys (after `finish`).
    pub fn result_keys(&self) -> &[K] {
        self.result.as_ref().map_or(&[], |p| p.keys.as_slice())
    }

    /// Combined accumulator for aggregation `agg_idx` (after `finish`).
    pub fn result_agg(&self, agg_idx: usize) -> &AggState {
        &self.result.as_ref().expect("finish not called").aggs[agg_idx]
    }

    /// Apply ORDER BY top-K / no-ORDER-BY cap to the combined result in place
    /// (after `finish`): reduce to `result_size` groups per `order` (§26.3).
    /// Empty `order` caps to the first `result_size` groups; non-empty does an
    /// exact top-`result_size` by the terms, sorted. No-op if `finish` hasn't
    /// run. See [`crate::topk`].
    pub fn select(&mut self, order: &[OrderTerm], result_size: usize)
    where
        K: OrderKey,
    {
        if let Some(partial) = self.result.as_mut() {
            let keys_ref: &[K] = &partial.keys;
            let perm = compute_selection(
                keys_ref.len(),
                1,
                |_c, a, b| keys_ref[a].order_cmp(&keys_ref[b]),
                &partial.aggs,
                order,
                result_size,
            );
            let new_keys: Vec<K> = perm.iter().map(|&i| partial.keys[i]).collect();
            partial.keys = new_keys;
            for a in partial.aggs.iter_mut() {
                *a = a.gather(&perm);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HashbrownTable;
    use std::collections::HashMap;

    // --- Basic two-segment merge ----------------------------------------

    #[test]
    fn merge_two_segments_sum_and_count() {
        // SELECT key, SUM(v)::f64, COUNT(*) GROUP BY key, over two segments.
        let kinds = [AggKind::SumDouble, AggKind::Count];
        let mut c = CombineDriver::<i64, HashbrownTable<i64>>::new(&kinds);
        // Segment A: key 10 -> (sum 1.5, cnt 3), key 20 -> (sum 2.5, cnt 1)
        c.merge_partials(
            &[10i64, 20],
            &[AggState::SumDouble(vec![1.5, 2.5]), AggState::Count(vec![3, 1])],
        );
        // Segment B: key 20 -> (sum 4.0, cnt 2), key 30 -> (sum 9.0, cnt 5)
        c.merge_partials(
            &[20i64, 30],
            &[AggState::SumDouble(vec![4.0, 9.0]), AggState::Count(vec![2, 5])],
        );

        assert_eq!(c.keys(), &[10, 20, 30]);
        assert_eq!(c.agg_state(0).as_double_slice(), Some(&[1.5, 6.5, 9.0][..]));
        assert_eq!(c.agg_state(1).as_long_slice(), Some(&[3i64, 3, 5][..]));
    }

    #[test]
    fn merge_min_max_across_segments() {
        let kinds = [AggKind::MinLong, AggKind::MaxLong];
        let mut c = CombineDriver::<i64, HashbrownTable<i64>>::new(&kinds);
        c.merge_partials(
            &[1i64, 2],
            &[AggState::MinLong(vec![50, 10]), AggState::MaxLong(vec![50, 10])],
        );
        c.merge_partials(
            &[2i64, 1],
            &[AggState::MinLong(vec![7, 999]), AggState::MaxLong(vec![7, 999])],
        );
        assert_eq!(c.keys(), &[1, 2]);
        // key1: min(50,999)=50 max(50,999)=999 ; key2: min(10,7)=7 max(10,7)=10
        assert_eq!(c.agg_state(0).as_long_slice(), Some(&[50i64, 7][..]));
        assert_eq!(c.agg_state(1).as_long_slice(), Some(&[999i64, 10][..]));
    }

    #[test]
    fn merge_propagates_nan_for_fp_min() {
        let kinds = [AggKind::MinDouble];
        let mut c = CombineDriver::<i64, HashbrownTable<i64>>::new(&kinds);
        c.merge_partials(&[1i64, 2], &[AggState::MinDouble(vec![5.0, 3.0])]);
        // Segment 2 contributes a NaN partial for key 1 -> result NaN (Java sem).
        c.merge_partials(&[1i64], &[AggState::MinDouble(vec![f64::NAN])]);
        let s = c.agg_state(0).as_double_slice().unwrap();
        assert!(s[0].is_nan(), "NaN partial must propagate through combine");
        assert_eq!(s[1], 3.0);
    }

    #[test]
    fn sum_to_double_kinds_merge_by_add() {
        // The segment uses Sum{Int,Long,Float}ToDouble (f64 accumulator); at
        // combine they all merge by add over f64.
        let mut c = CombineDriver::<i32, HashbrownTable<i32>>::new(&[AggKind::SumIntToDouble]);
        c.merge_partials(&[7i32], &[AggState::SumIntToDouble(vec![1.5])]);
        c.merge_partials(&[7i32], &[AggState::SumIntToDouble(vec![2.25])]);
        assert_eq!(c.agg_state(0).as_double_slice(), Some(&[3.75f64][..]));

        let mut c = CombineDriver::<i32, HashbrownTable<i32>>::new(&[AggKind::SumLongToDouble]);
        c.merge_partials(&[7i32], &[AggState::SumLongToDouble(vec![1.5])]);
        c.merge_partials(&[7i32], &[AggState::SumLongToDouble(vec![2.25])]);
        assert_eq!(c.agg_state(0).as_double_slice(), Some(&[3.75f64][..]));

        let mut c = CombineDriver::<i32, HashbrownTable<i32>>::new(&[AggKind::SumFloatToDouble]);
        c.merge_partials(&[7i32], &[AggState::SumFloatToDouble(vec![1.5])]);
        c.merge_partials(&[7i32], &[AggState::SumFloatToDouble(vec![2.25])]);
        assert_eq!(c.agg_state(0).as_double_slice(), Some(&[3.75f64][..]));
    }

    // --- merge_driver (partition-merge primitive) ------------------------

    #[test]
    fn merge_driver_folds_two_drivers() {
        let kinds = [AggKind::SumDouble, AggKind::Count];
        let mut a = CombineDriver::<i64, HashbrownTable<i64>>::new(&kinds);
        a.merge_partials(&[1i64, 2], &[AggState::SumDouble(vec![1.0, 2.0]), AggState::Count(vec![1, 1])]);
        let mut b = CombineDriver::<i64, HashbrownTable<i64>>::new(&kinds);
        b.merge_partials(&[2i64, 3], &[AggState::SumDouble(vec![5.0, 9.0]), AggState::Count(vec![1, 1])]);

        a.merge_driver(&b);
        assert_eq!(a.keys(), &[1, 2, 3]);
        assert_eq!(a.agg_state(0).as_double_slice(), Some(&[1.0, 7.0, 9.0][..]));
        assert_eq!(a.agg_state(1).as_long_slice(), Some(&[1i64, 2, 1][..]));
    }

    // --- Randomized differential vs a HashMap reference ------------------

    fn run_randomized<B: GroupByBackend<i64>>() {
        let mut state: u64 = 0x5eed_1234_abcd_ef01;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            state >> 16
        };

        let num_segments = 40;
        let card = 500i64;
        let kinds = [AggKind::SumDouble, AggKind::MinLong, AggKind::MaxLong, AggKind::Count];

        // Reference: total per key across all segments.
        let mut ref_sum: HashMap<i64, f64> = HashMap::new();
        let mut ref_min: HashMap<i64, i64> = HashMap::new();
        let mut ref_max: HashMap<i64, i64> = HashMap::new();
        let mut ref_cnt: HashMap<i64, i64> = HashMap::new();

        let mut combine = CombineDriver::<i64, B>::new(&kinds);

        for _ in 0..num_segments {
            // Build a per-segment partial over a random subset of keys.
            let mut seg: HashMap<i64, (f64, i64, i64, i64)> = HashMap::new();
            let rows = 1 + (next() % 2000) as usize;
            for _ in 0..rows {
                let k = (next() as i64) % card;
                let v = ((next() as i64) % 100_000) - 50_000;
                let e = seg.entry(k).or_insert((0.0, i64::MAX, i64::MIN, 0));
                e.0 += v as f64;
                e.1 = e.1.min(v);
                e.2 = e.2.max(v);
                e.3 += 1;
                // Reference accumulates the same per-row contribution.
                *ref_sum.entry(k).or_insert(0.0) += v as f64;
                let rm = ref_min.entry(k).or_insert(i64::MAX);
                *rm = (*rm).min(v);
                let rx = ref_max.entry(k).or_insert(i64::MIN);
                *rx = (*rx).max(v);
                *ref_cnt.entry(k).or_insert(0) += 1;
            }
            // Materialize this segment's partial in group_id order.
            let mut keys = Vec::with_capacity(seg.len());
            let mut sums = Vec::with_capacity(seg.len());
            let mut mins = Vec::with_capacity(seg.len());
            let mut maxs = Vec::with_capacity(seg.len());
            let mut cnts = Vec::with_capacity(seg.len());
            for (k, (s, mn, mx, c)) in seg {
                keys.push(k);
                sums.push(s);
                mins.push(mn);
                maxs.push(mx);
                cnts.push(c);
            }
            combine.merge_partials(
                &keys,
                &[
                    AggState::SumDouble(sums),
                    AggState::MinLong(mins),
                    AggState::MaxLong(maxs),
                    AggState::Count(cnts),
                ],
            );
        }

        assert_eq!(combine.num_groups(), ref_sum.len());
        let keys = combine.keys();
        let csum = combine.agg_state(0).as_double_slice().unwrap();
        let cmin = combine.agg_state(1).as_long_slice().unwrap();
        let cmax = combine.agg_state(2).as_long_slice().unwrap();
        let ccnt = combine.agg_state(3).as_long_slice().unwrap();
        for (i, &k) in keys.iter().enumerate() {
            assert_eq!(csum[i], ref_sum[&k], "sum key {}", k);
            assert_eq!(cmin[i], ref_min[&k], "min key {}", k);
            assert_eq!(cmax[i], ref_max[&k], "max key {}", k);
            assert_eq!(ccnt[i], ref_cnt[&k], "cnt key {}", k);
        }
    }

    #[test]
    fn randomized_differential_hashbrown() {
        run_randomized::<HashbrownTable<i64>>();
    }

    // --- CombineSession (the JNI-facing orchestration) -------------------

    #[test]
    fn session_accumulate_finish_extract() {
        // SELECT key, SUM(longCol)::f64, MIN(intCol), COUNT(*) GROUP BY key,
        // driven the way the JNI surface will: begin/set-agg/commit per segment.
        let kinds = [AggKind::SumLongToDouble, AggKind::MinInt, AggKind::Count];
        let mut s = CombineSession::<i64, HashbrownTable<i64>>::new(&kinds);

        // Segment A: keys [10, 20]
        s.begin_partial(vec![10i64, 20]);
        s.set_agg_double(0, vec![1.5, 2.5]); // SumLongToDouble extracts as f64
        s.set_agg_int(1, vec![7, 4]);
        s.set_agg_long(2, vec![3, 1]); // Count extracts as i64
        s.commit_partial();

        // Segment B: keys [20, 30]
        s.begin_partial(vec![20i64, 30]);
        s.set_agg_double(0, vec![4.0, 9.0]);
        s.set_agg_int(1, vec![2, 5]);
        s.set_agg_long(2, vec![2, 6]);
        s.commit_partial();

        s.finish(4);

        assert_eq!(s.result_num_groups(), 3);
        // Map results by key for an order-insensitive check.
        let keys = s.result_keys().to_vec();
        let sums = s.result_agg(0).as_double_slice().unwrap().to_vec();
        let mins = s.result_agg(1).as_int_slice().unwrap().to_vec();
        let cnts = s.result_agg(2).as_long_slice().unwrap().to_vec();
        let mut by_key = std::collections::HashMap::new();
        for (i, &k) in keys.iter().enumerate() {
            by_key.insert(k, (sums[i], mins[i], cnts[i]));
        }
        assert_eq!(by_key[&10], (1.5, 7, 3));
        assert_eq!(by_key[&20], (6.5, 2, 3)); // sum 2.5+4.0, min(4,2), cnt 1+2
        assert_eq!(by_key[&30], (9.0, 5, 6));
    }

    #[test]
    fn session_select_order_by_agg_desc_topk() {
        use crate::topk::{OrderRef, OrderTerm};
        let kinds = [AggKind::SumLongToDouble, AggKind::Count];
        let mut s = CombineSession::<i64, HashbrownTable<i64>>::new(&kinds);
        s.begin_partial(vec![10i64, 20]);
        s.set_agg_double(0, vec![1.5, 2.5]);
        s.set_agg_long(1, vec![3, 1]);
        s.commit_partial();
        s.begin_partial(vec![20i64, 30]);
        s.set_agg_double(0, vec![4.0, 9.0]);
        s.set_agg_long(1, vec![2, 6]);
        s.commit_partial();
        s.finish(4);
        // merged: 10->(1.5,3), 20->(6.5,3), 30->(9.0,6); ORDER BY sum desc, top 2.
        s.select(&[OrderTerm::new(OrderRef::Agg(0), false)], 2);
        assert_eq!(s.result_keys(), &[30i64, 20]);
        assert_eq!(s.result_agg(0).as_double_slice().unwrap(), &[9.0, 6.5]);
        assert_eq!(s.result_agg(1).as_long_slice().unwrap(), &[6, 3]);
    }

    #[test]
    fn session_select_order_by_key_then_no_order_cap() {
        use crate::topk::{OrderRef, OrderTerm};
        let kinds = [AggKind::Count];
        // ORDER BY key asc, all groups.
        let mut s = CombineSession::<i64, HashbrownTable<i64>>::new(&kinds);
        s.begin_partial(vec![3i64, 1, 2]);
        s.set_agg_long(0, vec![1, 1, 1]);
        s.commit_partial();
        s.finish(4);
        s.select(&[OrderTerm::new(OrderRef::Key(0), true)], 10);
        assert_eq!(s.result_keys(), &[1i64, 2, 3]);

        // No ORDER BY caps at result_size: count only (selection arbitrary).
        let mut c = CombineSession::<i64, HashbrownTable<i64>>::new(&kinds);
        c.begin_partial(vec![1i64, 2, 3, 4, 5]);
        c.set_agg_long(0, vec![1, 1, 1, 1, 1]);
        c.commit_partial();
        c.finish(4);
        c.select(&[], 3);
        assert_eq!(c.result_num_groups(), 3);
        assert!(c.result_agg(0).as_long_slice().unwrap().iter().all(|&v| v == 1));
    }
}
