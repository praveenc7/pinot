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

//! ORDER BY top-K / result selection over a combined GROUP BY result (design
//! doc §26/§27).
//!
//! The combine merge produces the *full* set of merged groups, unordered and
//! untrimmed. Before serialization the server must reduce that to `resultSize`
//! groups (§26.3):
//!
//! * **No ORDER BY** — cap at `resultSize` (= `limit`). Java's no-ORDER-BY
//!   selection is itself arbitrary/non-deterministic, so we just take the first
//!   `resultSize` groups in storage order; only the *count* and the per-group
//!   aggregates are a stable contract.
//! * **ORDER BY** — exact top-`resultSize` by the order terms, returned sorted.
//!   This is an **exact** top-K over the fully merged set (no approximate
//!   intermediate trim), so it is at least as accurate as Java and deterministic
//!   (ties break by group key, then group index).
//!
//! The order key is a single group-by key column (`OrderRef::Key`) and/or any
//! aggregation-result column (`OrderRef::Agg(i)`), in any combination, asc/desc
//! per term. Post-aggregation expressions and HAVING are out of scope (the
//! router falls back to Java) — see §26.7.
//!
//! [`compute_selection`] returns a permutation of group indices; the combine
//! sessions apply it to their keys + every [`AggState`] via [`AggState::gather`].

use crate::agg::AggState;
use crate::canonical::{CanonicalF32, CanonicalF64};
use std::cmp::Ordering;

/// The value column an ORDER BY term sorts on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderRef {
    /// Group-by key column `idx` (0 for the single-key case).
    Key(usize),
    /// Aggregation-result column `idx`.
    Agg(usize),
}

/// One ORDER BY term: a column reference plus direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderTerm {
    pub r: OrderRef,
    pub ascending: bool,
}

impl OrderTerm {
    pub fn new(r: OrderRef, ascending: bool) -> Self {
        Self { r, ascending }
    }
}

/// Total ordering of a group key under ORDER BY. FP keys order by numeric
/// *value* (not bit pattern), via `total_cmp` so the canonical NaN sorts
/// deterministically rather than yielding only a partial order.
pub trait OrderKey {
    fn order_cmp(&self, other: &Self) -> Ordering;
}

impl OrderKey for i64 {
    #[inline]
    fn order_cmp(&self, other: &Self) -> Ordering {
        self.cmp(other)
    }
}

impl OrderKey for CanonicalF64 {
    #[inline]
    fn order_cmp(&self, other: &Self) -> Ordering {
        self.to_f64().total_cmp(&other.to_f64())
    }
}

impl OrderKey for CanonicalF32 {
    #[inline]
    fn order_cmp(&self, other: &Self) -> Ordering {
        self.to_f32().total_cmp(&other.to_f32())
    }
}

/// Compute the selection permutation over `n` combined groups.
///
/// * `num_key_columns` is the number of group-by key columns (1 for single-key).
/// * `key_cmp(col, a, b)` compares groups `a` and `b` by key column `col`
///   (supplied by the caller so fixed-width and STRING results share this logic).
/// * `order` empty → no-ORDER-BY cap: first `min(result_size, n)` in storage
///   order.
/// * `order` non-empty → exact top-`min(result_size, n)` by the terms, sorted;
///   deterministic tie-break on the full key (columns `0..num_key_columns` in
///   order) then group index.
///
/// Returns indices into the original group arrays; apply with
/// [`AggState::gather`] (and the caller's key reorder).
pub fn compute_selection<F: Fn(usize, usize, usize) -> Ordering>(
    n: usize,
    num_key_columns: usize,
    key_cmp: F,
    aggs: &[AggState],
    order: &[OrderTerm],
    result_size: usize,
) -> Vec<usize> {
    let k = result_size.min(n);
    if k == 0 {
        return Vec::new();
    }
    let mut idx: Vec<usize> = (0..n).collect();
    if order.is_empty() {
        idx.truncate(k);
        return idx;
    }
    // Full multi-column key comparison (columns in grouping order) — the
    // deterministic tie-break, and better than Java's heap-order-dependent ties.
    let full_key_cmp = |a: usize, b: usize| -> Ordering {
        for c in 0..num_key_columns {
            let o = key_cmp(c, a, b);
            if o != Ordering::Equal {
                return o;
            }
        }
        Ordering::Equal
    };
    let cmp = |&a: &usize, &b: &usize| -> Ordering {
        for t in order {
            let mut o = match t.r {
                OrderRef::Key(col) => key_cmp(col, a, b),
                OrderRef::Agg(ai) => aggs[ai].cmp_slots(a, b),
            };
            if !t.ascending {
                o = o.reverse();
            }
            if o != Ordering::Equal {
                return o;
            }
        }
        // Deterministic tie-break: full key ascending, then group index.
        full_key_cmp(a, b).then_with(|| a.cmp(&b))
    };
    if k < n {
        // Partition so the top `k` (smallest under `cmp`) land in idx[0..k].
        idx.select_nth_unstable_by(k - 1, &cmp);
        idx.truncate(k);
    }
    idx.sort_by(&cmp);
    idx
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agg::AggState;

    fn sel(keys: &[i64], aggs: &[AggState], order: &[OrderTerm], rs: usize) -> Vec<usize> {
        compute_selection(keys.len(), 1, |_c, a, b| keys[a].cmp(&keys[b]), aggs, order, rs)
    }

    const KEY_ASC: OrderTerm = OrderTerm { r: OrderRef::Key(0), ascending: true };
    const KEY_DESC: OrderTerm = OrderTerm { r: OrderRef::Key(0), ascending: false };

    #[test]
    fn no_order_by_caps_in_storage_order() {
        let keys = [3i64, 1, 2, 5, 4];
        let aggs = [AggState::Count(vec![1, 1, 1, 1, 1])];
        assert_eq!(sel(&keys, &aggs, &[], 3), vec![0, 1, 2]);
        assert_eq!(sel(&keys, &aggs, &[], 10), vec![0, 1, 2, 3, 4]);
        assert_eq!(sel(&keys, &aggs, &[], 0), Vec::<usize>::new());
    }

    #[test]
    fn order_by_key_ascending_and_descending() {
        let keys = [3i64, 1, 2];
        let aggs = [AggState::Count(vec![9, 9, 9])];
        assert_eq!(sel(&keys, &aggs, &[KEY_ASC], 3), vec![1, 2, 0]); // 1,2,3
        assert_eq!(sel(&keys, &aggs, &[KEY_DESC], 3), vec![0, 2, 1]); // 3,2,1
    }

    #[test]
    fn order_by_agg_descending_topk() {
        let keys = [10i64, 11, 12, 13];
        let aggs = [AggState::Count(vec![5, 1, 9, 3])];
        let order = [OrderTerm::new(OrderRef::Agg(0), false)]; // desc by count
        // top-2 counts are 9 (idx 2) then 5 (idx 0)
        assert_eq!(sel(&keys, &aggs, &order, 2), vec![2, 0]);
        // full sort desc: 9,5,3,1 -> idx 2,0,3,1
        assert_eq!(sel(&keys, &aggs, &order, 4), vec![2, 0, 3, 1]);
    }

    #[test]
    fn multi_term_key_asc_then_agg_desc() {
        // keys tie (two 1s, two 2s); break ties by agg desc.
        let keys = [1i64, 2, 1, 2];
        let aggs = [AggState::SumDouble(vec![10.0, 7.0, 40.0, 8.0])];
        let order = [KEY_ASC, OrderTerm::new(OrderRef::Agg(0), false)];
        // key 1: idx2(40) before idx0(10); key 2: idx3(8) before idx1(7)
        assert_eq!(sel(&keys, &aggs, &order, 4), vec![2, 0, 3, 1]);
    }

    #[test]
    fn tie_break_is_deterministic_on_key_then_index() {
        // All order values equal -> tie-break: key asc, then index.
        let keys = [5i64, 5, 5];
        let aggs = [AggState::Count(vec![1, 1, 1])];
        let order = [OrderTerm::new(OrderRef::Agg(0), true)];
        assert_eq!(sel(&keys, &aggs, &order, 3), vec![0, 1, 2]);
    }

    #[test]
    fn topk_matches_full_sort_prefix() {
        let keys: Vec<i64> = (0..200).map(|i| (i * 7 + 3) % 50).collect();
        let aggs = [AggState::Count((0..200).map(|i| (i * 13 % 97) as i64).collect())];
        let order = [OrderTerm::new(OrderRef::Agg(0), false), KEY_ASC];
        let full = sel(&keys, &aggs, &order, 200);
        for k in [1usize, 5, 37, 200] {
            let topk = sel(&keys, &aggs, &order, k);
            assert_eq!(topk, full[..k.min(full.len())].to_vec(), "k={k}");
        }
    }
}
