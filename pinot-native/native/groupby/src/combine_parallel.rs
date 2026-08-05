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

//! Server-level combine — radix-partitioned, work-stealing **parallel** merge
//! (Task #54). This is the SOTA layer on top of the single-threaded
//! [`CombineDriver`] core in [`crate::combine`].
//!
//! Design (DuckDB / ClickHouse / HyPer consensus; design doc §22.8.1 / §23):
//!
//! **Phase 1 — partition.** Every segment-local group is routed to a radix
//! partition by the top `radix_bits` of its key hash. A key always hashes to
//! the same partition, so all of a key's contributions across all segments land
//! in exactly one partition.
//!
//! **Phase 2 — merge (parallel, lock-free).** Each partition is merged into its
//! own [`CombineDriver`] by one worker. Partitions are disjoint by hash, so
//! workers never touch the same table — no locks, no atomics. Rayon's
//! work-stealing pool grabs partition tasks dynamically (dynamic DOP), and
//! over-partitioning (`num_partitions > num_threads`) lets it balance skew.
//!
//! The final result is the concatenation of the disjoint partition tables.
//!
//! ## Threading
//!
//! Uses Rayon's pool (work-stealing, sized to the machine). For Pinot the pool
//! must ultimately be bounded + cancellable + shared across queries with
//! admission control (design doc §22.8.1) — that wrapper lands when this is
//! wired into the server combine operator; the algorithm here is pool-agnostic.

use rayon::prelude::*;

use crate::agg::{AggKind, AggState};
use crate::backend::GroupByBackend;
use crate::combine::{CombineDriver, SegmentPartial};
use crate::hash::HashKey;

/// Radix partition of a key hash: the top `radix_bits` bits. `radix_bits == 0`
/// means a single partition (degenerate — equivalent to the single-threaded
/// core).
#[inline]
fn partition_of(hash: u64, radix_bits: u32) -> usize {
    if radix_bits == 0 {
        0
    } else {
        (hash >> (64 - radix_bits)) as usize
    }
}

/// Pick a radix-bit count: enough partitions to over-subscribe the worker pool
/// (so work-stealing can balance skew) while keeping each partition's table
/// cache-resident. Heuristic: ~4 partitions per worker, clamped to [1, 12]
/// (i.e. 2..4096 partitions). Tuned later against prod data.
pub fn default_radix_bits(num_threads: usize) -> u32 {
    let target_partitions = (num_threads.max(1) * 4).next_power_of_two();
    let bits = target_partitions.trailing_zeros();
    bits.clamp(1, 12)
}

/// Merge all segment partials into one cross-segment result using a
/// radix-partitioned, work-stealing parallel two-phase merge.
///
/// Returns the combined `(keys, per-agg accumulators)` — the concatenation of
/// the disjoint radix partitions (group order is partition-order, which is not
/// the segments' insertion order; combine output is unordered by contract, the
/// broker applies any ORDER BY).
///
/// `agg_kinds` must match the kinds every segment produced.
pub fn combine_parallel<K, B>(
    partials: &[SegmentPartial<K>],
    agg_kinds: &[AggKind],
    radix_bits: u32,
) -> (Vec<K>, Vec<AggState>)
where
    K: HashKey + Eq + Copy + Send + Sync,
    B: GroupByBackend<K> + Send,
{
    let num_partitions = 1usize << radix_bits;

    // --- Phase 1: partition (segment, group) indices by key-hash top bits. ---
    // Cheap index-only pass (hash + shift + push); the expensive probe+fold
    // happens in Phase 2. Hash is recomputed in the probe — caching it beside
    // the key is a planned optimization (design doc §22.8.1).
    let mut buckets: Vec<Vec<(u32, u32)>> = vec![Vec::new(); num_partitions];
    for (s, partial) in partials.iter().enumerate() {
        for (g, key) in partial.keys.iter().enumerate() {
            let p = partition_of(key.hash(), radix_bits);
            buckets[p].push((s as u32, g as u32));
        }
    }

    // --- Phase 2: merge each partition independently (work-stealing). ---
    let drivers: Vec<CombineDriver<K, B>> = buckets
        .into_par_iter()
        .map(|bucket| {
            let mut driver = CombineDriver::<K, B>::with_capacity(agg_kinds, bucket.len());
            for (s, g) in bucket {
                let partial = &partials[s as usize];
                driver.merge_one(partial.keys[g as usize], &partial.aggs, g as usize);
            }
            driver
        })
        .collect();

    // --- Concatenate the disjoint partitions into one result. ---
    let mut keys: Vec<K> = Vec::new();
    let mut aggs: Vec<AggState> = agg_kinds.iter().map(|&k| AggState::new_for(k, 0)).collect();
    for driver in drivers {
        let (mut part_keys, part_aggs) = driver.extract();
        keys.append(&mut part_keys);
        for (i, mut a) in part_aggs.into_iter().enumerate() {
            aggs[i].append(&mut a);
        }
    }
    (keys, aggs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HashbrownTable;
    use std::collections::HashMap;

    /// Reference per-key accumulator built alongside the segment partials:
    /// `(sum, min, max, count)`, keyed by raw key, for cross-checking the merge.
    type ReferenceAggs = HashMap<i64, (f64, i64, i64, i64)>;

    fn build_partials(seed: u64, num_segments: usize, card: i64)
        -> (Vec<SegmentPartial<i64>>, ReferenceAggs) {
        let mut state = seed;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            state >> 16
        };
        let mut reference: HashMap<i64, (f64, i64, i64, i64)> = HashMap::new();
        let mut partials = Vec::with_capacity(num_segments);
        for _ in 0..num_segments {
            let mut seg: HashMap<i64, (f64, i64, i64, i64)> = HashMap::new();
            let rows = 1 + (next() % 1500) as usize;
            for _ in 0..rows {
                let k = (next() as i64) % card;
                let v = ((next() as i64) % 100_000) - 50_000;
                let e = seg.entry(k).or_insert((0.0, i64::MAX, i64::MIN, 0));
                e.0 += v as f64;
                e.1 = e.1.min(v);
                e.2 = e.2.max(v);
                e.3 += 1;
                let r = reference.entry(k).or_insert((0.0, i64::MAX, i64::MIN, 0));
                r.0 += v as f64;
                r.1 = r.1.min(v);
                r.2 = r.2.max(v);
                r.3 += 1;
            }
            let mut keys = Vec::new();
            let (mut sums, mut mins, mut maxs, mut cnts) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
            for (k, (s, mn, mx, c)) in seg {
                keys.push(k);
                sums.push(s);
                mins.push(mn);
                maxs.push(mx);
                cnts.push(c);
            }
            partials.push(SegmentPartial::new(
                keys,
                vec![
                    AggState::SumDouble(sums),
                    AggState::MinLong(mins),
                    AggState::MaxLong(maxs),
                    AggState::Count(cnts),
                ],
            ));
        }
        (partials, reference)
    }

    fn assert_matches_reference(
        keys: &[i64],
        aggs: &[AggState],
        reference: &HashMap<i64, (f64, i64, i64, i64)>,
    ) {
        assert_eq!(keys.len(), reference.len(), "group count mismatch");
        let sums = aggs[0].as_double_slice().unwrap();
        let mins = aggs[1].as_long_slice().unwrap();
        let maxs = aggs[2].as_long_slice().unwrap();
        let cnts = aggs[3].as_long_slice().unwrap();
        let mut seen = std::collections::HashSet::new();
        for (i, &k) in keys.iter().enumerate() {
            assert!(seen.insert(k), "duplicate key {} in output", k);
            let r = reference[&k];
            assert_eq!(sums[i], r.0, "sum key {}", k);
            assert_eq!(mins[i], r.1, "min key {}", k);
            assert_eq!(maxs[i], r.2, "max key {}", k);
            assert_eq!(cnts[i], r.3, "cnt key {}", k);
        }
    }

    const KINDS: [AggKind; 4] =
        [AggKind::SumDouble, AggKind::MinLong, AggKind::MaxLong, AggKind::Count];

    #[test]
    fn parallel_matches_reference_across_radix_bits() {
        let (partials, reference) = build_partials(0xabcd_1234, 50, 1000);
        for radix_bits in [0u32, 1, 4, 8, 10] {
            let (keys, aggs) = combine_parallel::<i64, HashbrownTable<i64>>(&partials, &KINDS, radix_bits);
            assert_matches_reference(&keys, &aggs, &reference);
        }
    }

    #[test]
    fn parallel_hashbrown_backend_matches_reference() {
        let (partials, reference) = build_partials(0x9999_7777, 60, 2000);
        let (keys, aggs) = combine_parallel::<i64, HashbrownTable<i64>>(&partials, &KINDS, 6);
        assert_matches_reference(&keys, &aggs, &reference);
    }

    #[test]
    fn parallel_matches_single_threaded_core() {
        let (partials, _reference) = build_partials(0x1357_2468, 40, 800);
        // Single-threaded reference via the core CombineDriver.
        let mut single = CombineDriver::<i64, HashbrownTable<i64>>::new(&KINDS);
        for p in &partials {
            single.merge(p);
        }
        let (sk, sa) = single.extract();
        let mut single_map: HashMap<i64, (f64, i64, i64, i64)> = HashMap::new();
        let ss = sa[0].as_double_slice().unwrap();
        let smin = sa[1].as_long_slice().unwrap();
        let smax = sa[2].as_long_slice().unwrap();
        let sc = sa[3].as_long_slice().unwrap();
        for (i, &k) in sk.iter().enumerate() {
            single_map.insert(k, (ss[i], smin[i], smax[i], sc[i]));
        }

        let (keys, aggs) = combine_parallel::<i64, HashbrownTable<i64>>(&partials, &KINDS, 8);
        assert_matches_reference(&keys, &aggs, &single_map);
    }

    #[test]
    fn default_radix_bits_is_sane() {
        assert!(default_radix_bits(1) >= 1);
        assert!(default_radix_bits(16) <= 12);
        // More threads -> at least as many partitions.
        assert!(default_radix_bits(64) >= default_radix_bits(8));
    }
}
