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

//! Hash functions for SwissTable keys — backed by the [`foldhash`] crate.
//!
//! Type-specialized: each key type has its own [`HashKey::hash`] implementation
//! that LLVM monomorphizes into the probe loop. Fixed-width keys funnel through
//! [`hash_u64`]; variable-length keys (`&[u8]`) through [`hash_bytes`]; 128-bit
//! composites and multi-column rows through [`hash_combine`]. All are foldhash.
//!
//! ## Why foldhash (Phase 1.D scope decision, settled post-PR-review)
//!
//! Originally this module vendored Wang Yi's wyhash final3 inline — including a
//! ~180-line hand-written `unsafe` variable-length byte path that a reviewer found
//! an out-of-bounds read in. We replaced the whole thing with `foldhash`, a small,
//! maintained, dependency-light hash (the one DataFusion uses):
//! * it deletes all of that vendored `unsafe` byte-tail code — and the entire class
//!   of bug the OOB came from — in favor of a crate that owns those reads;
//! * no AES-NI / hardware-crypto dependency (unlike ahash), so it behaves the same
//!   across the whole fleet;
//! * one hash family for every key type means one set of distribution tests.
//!
//! **Perf note (why we did NOT keep a hand-rolled integer finalizer).** The integer
//! probe is the hottest call in the engine, so we microbenchmarked the swap before
//! committing. On this hardware, `FixedState::default().hash_one(u64)` runs at
//! ~0.245 ns/key — *identical* to the old vendored wyhash finalizer (~0.247 ns/key),
//! under both throughput- and latency-bound loops, because both compile down to a
//! single 64×64→128 folded multiply. A SplitMix64/`fmix64` alternative (two dependent
//! multiplies) measured ~1.7× slower, so there was nothing to gain by keeping our own
//! integer path — foldhash is both faster-or-equal and less code.
//!
//! ## Seed / determinism
//!
//! All hashes use [`FixedState::default()`], which seeds foldhash from a
//! **compile-time constant** — not a per-process random seed. This is mandatory: the
//! radix-partitioned combine routes a key to a worker via `hash >> (64 - bits)`, so a
//! key must hash identically across every thread and process for cross-segment merge
//! to be correct. The fixed seed also lets the call site stay parameter-free. (Pinot
//! group-by keys come from segment column values, not adversarial HTTP input, so a
//! random anti-DoS seed is unnecessary.)

use std::hash::{BuildHasher, Hasher};

use foldhash::fast::FixedState;

/// Fixed-seed foldhash state. `FixedState::default()` uses a compile-time fixed
/// seed, so hashing is **deterministic across runs and threads** — required by the
/// radix-partitioned combine, where a key must hash to the same value (hence the
/// same partition) in every worker. `FixedState` is a tiny value; constructing it
/// per call is free (const-folded) and lets the call sites stay parameter-free.
#[inline(always)]
fn state() -> FixedState {
    FixedState::default()
}

/// Hash a 64-bit key — the hot path for integer / packed-integer GROUP BY. Every
/// fixed-width primitive [`HashKey`] impl funnels through this. Microbenchmarked at
/// ~0.245 ns/key, matching the old vendored wyhash finalizer it replaced.
#[inline]
pub fn hash_u64(x: u64) -> u64 {
    state().hash_one(x)
}

/// Hash a variable-length byte slice. Reads only `bytes[0..bytes.len()]`
/// (foldhash bounds its own reads), so trailing bytes past the slice length can
/// never influence the result — the property the STRING key path relies on.
#[inline]
pub fn hash_bytes(bytes: &[u8]) -> u64 {
    state().hash_one(bytes)
}

/// Combine several already-computed column hashes into one composite hash for a
/// multi-column key. Feeding the per-column hashes through a single foldhash
/// `Hasher` keeps the fold order-sensitive and avalanching, replacing the
/// hand-rolled `wymix` chain the multi-column combine used to run.
#[inline]
pub fn hash_combine(hashes: impl IntoIterator<Item = u64>) -> u64 {
    let mut h = state().build_hasher();
    for x in hashes {
        h.write_u64(x);
    }
    h.finish()
}

/// Hash function for SwissTable keys. Type-specialized — each impl is
/// monomorphized into the probe loop and calls the appropriate foldhash entry
/// ([`hash_u64`] for fixed-width keys, [`hash_bytes`] for variable-length keys).
pub trait HashKey {
    fn hash(&self) -> u64;
}

impl HashKey for u64 {
    #[inline]
    fn hash(&self) -> u64 {
        hash_u64(*self)
    }
}

impl HashKey for i64 {
    #[inline]
    fn hash(&self) -> u64 {
        hash_u64(*self as u64)
    }
}

impl HashKey for i128 {
    #[inline]
    fn hash(&self) -> u64 {
        // Used by the PackedKeys (design C) keys128 path for composites whose
        // total type width is 65..=128 bits. foldhash hashes i128 natively.
        state().hash_one(*self)
    }
}

impl HashKey for u32 {
    #[inline]
    fn hash(&self) -> u64 {
        hash_u64(*self as u64)
    }
}

impl HashKey for i32 {
    #[inline]
    fn hash(&self) -> u64 {
        // Sign-extend so that small positive and small negative i32 with
        // distinct bit patterns at the high end don't collapse to the same
        // hash. Mirrors the SUM kernel's i32→i64 promotion and keeps an i32 and
        // the i64 of the same value hashing identically.
        hash_u64(*self as i64 as u64)
    }
}

impl HashKey for f64 {
    #[inline]
    fn hash(&self) -> u64 {
        // Canonicalize NaN bit patterns and -0.0 / +0.0 so they hash to the
        // same slot as their canonical f64 representation. Java's
        // `Double.hashCode()` does the equivalent of `to_bits()` on doubles;
        // for Pinot GROUP BY parity we follow that and accept that all NaNs
        // bit-equal go to one bucket and NaNs bit-unequal scatter.
        let bits = if self.is_nan() {
            // Collapse all NaN bit patterns to one canonical value so
            // GROUP BY treats all NaNs as the same group, matching the
            // Java path which uses `Double.valueOf(d).hashCode()`.
            f64::NAN.to_bits()
        } else if *self == 0.0 {
            0u64
        } else {
            self.to_bits()
        };
        hash_u64(bits)
    }
}

impl HashKey for f32 {
    #[inline]
    fn hash(&self) -> u64 {
        let bits = if self.is_nan() {
            f32::NAN.to_bits()
        } else if *self == 0.0 {
            0u32
        } else {
            self.to_bits()
        };
        hash_u64(bits as u64)
    }
}

impl HashKey for &[u8] {
    #[inline]
    fn hash(&self) -> u64 {
        hash_bytes(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn deterministic_for_primitives() {
        for k in [-7i64, 0, 1, 100, i64::MIN, i64::MAX] {
            assert_eq!(k.hash(), k.hash(), "k={k}");
        }
        for k in [-7i32, 0, 1, 100, i32::MIN, i32::MAX] {
            assert_eq!(k.hash(), k.hash(), "k={k}");
        }
        for k in [0.0f64, 1.0, -1.0, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(k.hash(), k.hash(), "k={k}");
        }
    }

    #[test]
    fn distinct_small_integers_produce_distinct_hashes() {
        let mut seen = HashSet::new();
        for k in 0i64..10_000 {
            assert!(seen.insert(k.hash()), "collision at k={k}");
        }
    }

    #[test]
    fn distinct_negative_integers_produce_distinct_hashes() {
        let mut seen = HashSet::new();
        for k in (-10_000i64..0).chain(0i64..10_000) {
            assert!(seen.insert(k.hash()), "collision at k={k}");
        }
    }

    #[test]
    fn i32_and_i64_same_value_hash_the_same() {
        for k in (-100i32..100).chain([i32::MIN, i32::MAX]) {
            assert_eq!(k.hash(), (k as i64).hash(), "k={k}");
        }
    }

    #[test]
    fn f64_zero_and_neg_zero_hash_equal() {
        assert_eq!(0.0f64.hash(), (-0.0f64).hash());
    }

    #[test]
    fn f64_all_nans_hash_equal() {
        // f64 has a 52-bit NaN payload — any bit pattern with exponent=0xFF
        // and non-zero mantissa is a NaN. We collapse all to one canonical
        // hash so GROUP BY puts NaN rows in one bucket (Java parity).
        let nan1 = f64::NAN;
        let nan2 = f64::from_bits(0x7FF8_0000_0000_0001);
        let nan3 = f64::from_bits(0xFFF8_0000_DEAD_BEEF);
        assert!(nan1.is_nan() && nan2.is_nan() && nan3.is_nan());
        assert_eq!(nan1.hash(), nan2.hash());
        assert_eq!(nan2.hash(), nan3.hash());
    }

    #[test]
    fn sequential_inputs_avalanche() {
        // Property: a 1-bit change in input flips at least 15 bits of output
        // (avalanche ≥ 23% of 64 bits) for SwissTable's purposes (we don't
        // need cryptographic avalanche; the SwissTable h2/h1 split needs the
        // top bits to be well-distributed).
        for k in 0u64..1000 {
            let h0 = hash_u64(k);
            for bit in 0..64 {
                let h1 = hash_u64(k ^ (1u64 << bit));
                let flipped = (h0 ^ h1).count_ones();
                assert!(
                    flipped >= 15,
                    "weak avalanche: k={k} bit={bit} flipped={flipped}/64"
                );
            }
        }
    }

    #[test]
    fn bytes_empty_is_stable() {
        let empty: &[u8] = &[];
        assert_eq!(empty.hash(), empty.hash());
    }

    #[test]
    fn bytes_short_inputs_distinct() {
        // Hash some short strings typical of Pinot dimensions; verify no
        // collisions in a small sample.
        let inputs: Vec<&[u8]> = vec![
            b"US", b"IN", b"DE", b"GB", b"FR",
            b"premium", b"basic", b"trial",
            b"login", b"logout", b"click", b"view",
        ];
        let mut seen = HashSet::new();
        for s in &inputs {
            assert!(seen.insert(s.hash()), "collision on {:?}", s);
        }
    }

    #[test]
    fn bytes_long_inputs_distinct() {
        // Cross the 48-byte threshold so we exercise foldhash's long-input path.
        let s1 = b"the quick brown fox jumps over the lazy dog and then some";
        let s2 = b"the quick brown fox jumps over the lazy cat and then some";
        let s3 = b"the slow brown fox jumps over the lazy dog and then some!";
        assert!(s1.len() > 48 && s2.len() > 48 && s3.len() > 48);
        let h1 = (s1 as &[u8]).hash();
        let h2 = (s2 as &[u8]).hash();
        let h3 = (s3 as &[u8]).hash();
        assert_ne!(h1, h2);
        assert_ne!(h1, h3);
        assert_ne!(h2, h3);
    }

    #[test]
    fn bytes_hash_reads_only_within_len() {
        // Regression for the historical vendored-wyhash tail OOB read (lengths
        // whose tail fell in 1..=7 read past the slice end). foldhash reads only
        // bytes[0..len], so the same logical key placed in two buffers with
        // DIFFERENT trailing bytes must still hash identically.
        for len in 1..=64usize {
            let key: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(31).wrapping_add(7)).collect();
            let mut buf_zeros = key.clone();
            buf_zeros.extend(std::iter::repeat_n(0x00, 16));
            let mut buf_ones = key.clone();
            buf_ones.extend(std::iter::repeat_n(0xFF, 16));
            let h_zeros = hash_bytes(&buf_zeros[..len]);
            let h_ones = hash_bytes(&buf_ones[..len]);
            assert_eq!(h_zeros, h_ones, "len {len}: hash depends on out-of-bounds trailing bytes");
            // ...and is consistent with the bare key.
            assert_eq!(h_zeros, hash_bytes(&key), "len {len}: hash not consistent");
        }
    }

    #[test]
    fn bytes_all_lengths_consistent_and_mostly_distinct() {
        // Every length 0..=80 hashes without panic, is deterministic, and the
        // set of hashes across lengths has no collisions.
        let mut seen = HashSet::new();
        for len in 0..=80usize {
            let key: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(97).wrapping_add(3)).collect();
            let h = hash_bytes(&key);
            assert_eq!(h, hash_bytes(&key), "len {len}: non-deterministic");
            assert!(seen.insert(h), "len {len}: unexpected collision");
        }
    }

    #[test]
    fn bytes_different_lengths_of_same_prefix_hash_differently() {
        // A hash that mixes length in (as foldhash does) keeps different-length
        // prefixes from colliding.
        let s1 = b"abc";
        let s2 = b"abcd";
        let s3 = b"abcdefghij";
        let h1 = (s1 as &[u8]).hash();
        let h2 = (s2 as &[u8]).hash();
        let h3 = (s3 as &[u8]).hash();
        assert_ne!(h1, h2);
        assert_ne!(h2, h3);
        assert_ne!(h1, h3);
    }

    /// Sanity test against the table integration: distinct keys should still
    /// land in distinct slots once the SwissTable applies the h2/h1 split.
    /// This is here in `hash.rs` because if the hash were ever swapped out the
    /// failure would surface here first.
    #[test]
    fn h2_h1_split_distributes_well() {
        // For SwissTable's purposes, the top 57 bits of the hash (h1) pick
        // the group. We want sequential inputs to NOT pile up in one group.
        let mut group_counts = std::collections::HashMap::new();
        for k in 0u64..1024 {
            let h = hash_u64(k);
            // Simulate a 16-group table: 4 bits of group selection from h1.
            let group = (h >> 7) & 0xF;
            *group_counts.entry(group).or_insert(0) += 1;
        }
        // Expected ~64 keys per group (1024 / 16). Allow 2× spread.
        for (g, cnt) in &group_counts {
            assert!(
                *cnt < 128,
                "group {g} got {cnt} keys — bad distribution"
            );
        }
        // All 16 groups should see at least one key.
        assert_eq!(group_counts.len(), 16);
    }
}
