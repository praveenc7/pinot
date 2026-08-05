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

//! `GroupByBackend` — abstraction over group-by hash-table backends.
//!
//! Implementations:
//! * [`crate::HashbrownTable`] — our custom SwissTable (NEON / SSE2 / AVX-512BW SIMD
//!   ctrl-byte probe, no-tombstone state machine, pre-sized layout, batch
//!   API with hash pre-computation).
//! * [`crate::HashbrownTable`] — `hashbrown::hash_table::HashTable` wrapped
//!   following DataFusion's pattern: slot stores `(group_id, hash)`, keys
//!   in `Vec<K>` indexed by group_id.
//!
//! ## Why a trait?
//!
//! The segment driver (Phase 1.D-core-D, Task #47) needs to integrate
//! ONE backend with Pinot's `DefaultGroupByExecutor`. Per the 2026-06-03
//! Path C decision (design doc §18), we don't yet know which backend wins
//! end-to-end. This trait lets the driver be generic over the backend type
//! so we can:
//!
//! 1. Compile the driver against EITHER backend without code duplication.
//! 2. Run the JMH harness at Task #59 with a `_backend ∈ {swisstable, hashbrown}`
//!    axis — picking the variant is a type parameter, not a runtime branch.
//! 3. Ship whichever backend wins on end-to-end Pinot GROUP BY vs Java.
//! 4. Preserve both backends for the combine path (Phase 1.D.2) where the
//!    segment-winning backend may not be the combine-winning backend.
//!
//! ## No runtime dispatch
//!
//! This is a generic trait, not a dyn-compatible trait object. The driver
//! is monomorphized per backend at compile time — no virtual dispatch, no
//! vtable, no branch in the hot path. LLVM inlines the probe loop into the
//! driver loop the same way it would inline either backend directly.

use crate::HashKey;

/// Abstraction over a Pinot-shaped group-by hash table.
///
/// Implementations map `K → group_id : u32` where `group_id` is densely
/// assigned in insertion order (0, 1, 2, …). Drivers (segment-level,
/// combine, MSE intermediate) maintain their own parallel `Vec<AggState>`
/// arrays indexed by `group_id` — the table itself never holds aggregate
/// state.
///
/// See [`crate::HashbrownTable::probe_or_insert`] / [`crate::HashbrownTable::probe_or_insert`]
/// for the concrete semantics each implementation provides.
pub trait GroupByBackend<K>: Sized
where
    K: HashKey + Eq + Copy,
{
    /// Human-readable backend identifier. Used by the JMH harness (Task #59)
    /// as the `_backend` axis label and in diagnostic output.
    const NAME: &'static str;

    /// Construct an empty backend with implementation-default capacity.
    fn new() -> Self;

    /// Construct an empty backend sized for at least `capacity_hint` keys
    /// without triggering a resize. Each implementation may round up
    /// differently (our SwissTable rounds to next power-of-2 group count;
    /// hashbrown rounds to satisfy its load factor cap).
    fn with_capacity(capacity_hint: usize) -> Self;

    /// Number of distinct keys currently in the backend.
    fn len(&self) -> usize;

    /// Whether the backend contains zero keys.
    fn is_empty(&self) -> bool;

    /// Implementation-defined capacity. Comparable across implementations
    /// only as "how many more keys can be inserted before resize", not as
    /// raw slot count.
    fn capacity(&self) -> usize;

    /// Look up `key`'s `group_id` without inserting. Returns `None` if the
    /// key is not present.
    fn get(&self, key: &K) -> Option<u32>;

    /// Look up `key`, inserting a new sequentially-assigned `group_id` if
    /// the key was not previously present. The returned `group_id` is
    /// stable across resizes for the lifetime of this backend.
    fn probe_or_insert(&mut self, key: K) -> u32;

    /// Batch entry: probe-or-insert all `keys` and write the resulting
    /// `group_id`s into `out`. `out.len()` must be `>= keys.len()`.
    ///
    /// Implementations may optimize the batch path (e.g., hash
    /// pre-computation, prefetching) or may fall back to a per-key loop;
    /// the result must be identical regardless.
    fn probe_or_insert_batch(&mut self, keys: &[K], out: &mut [u32]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HashbrownTable;

    /// Generic driver-like helper. If the trait is correctly defined, this
    /// function compiles cleanly against ANY `GroupByBackend<i64>` —
    /// proving the abstraction is usable from a generic driver.
    fn drive_backend<B: GroupByBackend<i64>>(keys: &[i64]) -> (Vec<u32>, usize, &'static str) {
        let mut backend = B::with_capacity(keys.len());
        let mut out = vec![u32::MAX; keys.len()];
        backend.probe_or_insert_batch(keys, &mut out);
        (out, backend.len(), B::NAME)
    }

    #[test]
    fn trait_works_against_hashbrown() {
        let keys = vec![1i64, 2, 3, 1, 2, 4];
        let (out, count, name) = drive_backend::<HashbrownTable<i64>>(&keys);
        assert_eq!(name, HashbrownTable::<i64>::NAME);
        assert_eq!(name, "hashbrown");
        assert_eq!(count, 4);
        assert_eq!(out, vec![0, 1, 2, 0, 1, 3]);
    }

    /// Cross-backend output parity under the generic interface — same key
    /// stream produces identical group_id sequences regardless of backend.
    /// This is the correctness invariant the JMH `_backend` axis at #59
    /// relies on.
    #[test]
    fn backends_produce_identical_outputs() {
        let keys: Vec<i64> = (0..300i64).chain(0..300i64).chain(150..450i64).collect();
        let (ours, our_n, _) = drive_backend::<HashbrownTable<i64>>(&keys);
        let (hb, hb_n, _) = drive_backend::<HashbrownTable<i64>>(&keys);
        assert_eq!(ours, hb, "backend outputs diverged");
        assert_eq!(our_n, hb_n);
    }

    /// Probe-only path (no batch) parity, exercised through the trait.
    #[test]
    fn backends_agree_on_probe_or_insert() {
        fn probe_seq<B: GroupByBackend<i64>>(keys: &[i64]) -> Vec<u32> {
            let mut b = B::new();
            keys.iter().map(|&k| b.probe_or_insert(k)).collect()
        }
        let keys: Vec<i64> = vec![42, 17, 42, 8, 17, 0, 8, 42];
        let our_seq = probe_seq::<HashbrownTable<i64>>(&keys);
        let hb_seq = probe_seq::<HashbrownTable<i64>>(&keys);
        assert_eq!(our_seq, hb_seq);
    }
}
