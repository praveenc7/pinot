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

//! MIN(DOUBLE) kernel — computes the minimum `f64` across a slice.
//!
//! Empty input returns `f64::INFINITY` to match Pinot's
//! `MinAggregationFunction.DEFAULT_VALUE = Double.POSITIVE_INFINITY`.
//!
//! NaN semantics match Java's `Math.min(double, double)`: **NaN propagates**.
//! If any input element is NaN, the result is NaN.
//!
//! ISA notes:
//! * NEON `vminq_f64` is IEEE 754-2019 NaN-propagating (FMIN instruction) —
//!   matches Java for free.
//! * AVX2 `_mm256_min_pd` and AVX-512 `_mm512_min_pd` are asymmetric:
//!   `a < b ? a : b`. NaN in operand `b` propagates; NaN in operand `a` does
//!   not. To match Java we run a per-chunk `_mm*_cmp_pd(load, load, UNORD)`
//!   to detect any NaN in incoming data and OR it into a sticky "saw NaN"
//!   accumulator; at end, if set, we return NaN.
//!
//! Lane widths per loop:
//! * NEON      — `vminq_f64`: 2-lane f64; 4 accumulators → 8-wide ILP
//! * AVX2      — `_mm256_min_pd`: 4-lane f64; 4 accumulators → 16-wide ILP
//! * AVX-512F  — `_mm512_min_pd`: 8-lane f64; 4 accumulators → 32-wide ILP

/// Computes the minimum of a slice of `f64`, dispatching to the fastest
/// available implementation. NaN-propagating to match Java semantics.
#[inline]
pub fn min_f64_to_f64(values: &[f64]) -> f64 {
    if values.is_empty() {
        return f64::INFINITY;
    }
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512f") {
            return unsafe { min_f64_to_f64_avx512(values) };
        }
        if std::is_x86_feature_detected!("avx2") {
            return unsafe { min_f64_to_f64_avx2(values) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            return unsafe { min_f64_to_f64_neon(values) };
        }
    }

    min_f64_to_f64_scalar(values)
}

/// 4-way unrolled NaN-propagating scalar min.
#[inline]
pub fn min_f64_to_f64_scalar(values: &[f64]) -> f64 {
    if values.is_empty() {
        return f64::INFINITY;
    }
    let mut m0 = f64::INFINITY;
    let mut m1 = f64::INFINITY;
    let mut m2 = f64::INFINITY;
    let mut m3 = f64::INFINITY;
    let mut saw_nan = false;

    let chunks = values.chunks_exact(4);
    let remainder = chunks.remainder();
    for c in chunks {
        if c[0].is_nan() || c[1].is_nan() || c[2].is_nan() || c[3].is_nan() {
            saw_nan = true;
        }
        if c[0] < m0 {
            m0 = c[0];
        }
        if c[1] < m1 {
            m1 = c[1];
        }
        if c[2] < m2 {
            m2 = c[2];
        }
        if c[3] < m3 {
            m3 = c[3];
        }
    }
    let mut tail = f64::INFINITY;
    for &v in remainder {
        if v.is_nan() {
            saw_nan = true;
        } else if v < tail {
            tail = v;
        }
    }
    if saw_nan {
        return f64::NAN;
    }
    let m01 = if m0 < m1 { m0 } else { m1 };
    let m23 = if m2 < m3 { m2 } else { m3 };
    let m = if m01 < m23 { m01 } else { m23 };
    if m < tail {
        m
    } else {
        tail
    }
}

// --- NEON (aarch64) ---------------------------------------------------------

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn min_f64_to_f64_neon(values: &[f64]) -> f64 {
    use core::arch::aarch64::*;

    let mut acc0 = vdupq_n_f64(f64::INFINITY);
    let mut acc1 = vdupq_n_f64(f64::INFINITY);
    let mut acc2 = vdupq_n_f64(f64::INFINITY);
    let mut acc3 = vdupq_n_f64(f64::INFINITY);

    // Each chunk = 8 f64 spread across 4 accumulators (2 lanes each).
    let chunks = values.chunks_exact(8);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let p = chunk.as_ptr();
        let v0 = vld1q_f64(p);
        let v1 = vld1q_f64(p.add(2));
        let v2 = vld1q_f64(p.add(4));
        let v3 = vld1q_f64(p.add(6));

        // vminq_f64 is IEEE 754-2019 FMIN — NaN-propagating, matches Java.
        acc0 = vminq_f64(acc0, v0);
        acc1 = vminq_f64(acc1, v1);
        acc2 = vminq_f64(acc2, v2);
        acc3 = vminq_f64(acc3, v3);
    }

    let acc01 = vminq_f64(acc0, acc1);
    let acc23 = vminq_f64(acc2, acc3);
    let acc = vminq_f64(acc01, acc23);
    let mut m = vminvq_f64(acc);

    for &v in remainder {
        // Scalar f64 min: Rust's f64::min is NOT NaN-propagating, so handle it
        // explicitly to match the vector path's IEEE semantics.
        if v.is_nan() || m.is_nan() {
            m = f64::NAN;
        } else if v < m {
            m = v;
        }
    }
    m
}

// --- AVX2 (x86_64) ----------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn min_f64_to_f64_avx2(values: &[f64]) -> f64 {
    use core::arch::x86_64::*;

    let inf = _mm256_set1_pd(f64::INFINITY);
    let mut acc0 = inf;
    let mut acc1 = inf;
    let mut acc2 = inf;
    let mut acc3 = inf;
    // Sticky NaN-detect accumulator. _CMP_UNORD_Q returns all-1 lanes where
    // either operand is NaN, all-0 otherwise. By comparing a load against
    // itself we get a true NaN mask for that vector.
    let mut nan_acc = _mm256_setzero_pd();

    let chunks = values.chunks_exact(16);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let p = chunk.as_ptr();
        let v0 = _mm256_loadu_pd(p);
        let v1 = _mm256_loadu_pd(p.add(4));
        let v2 = _mm256_loadu_pd(p.add(8));
        let v3 = _mm256_loadu_pd(p.add(12));

        // OR all NaN-detect masks into the sticky accumulator.
        let nan0 = _mm256_cmp_pd::<_CMP_UNORD_Q>(v0, v0);
        let nan1 = _mm256_cmp_pd::<_CMP_UNORD_Q>(v1, v1);
        let nan2 = _mm256_cmp_pd::<_CMP_UNORD_Q>(v2, v2);
        let nan3 = _mm256_cmp_pd::<_CMP_UNORD_Q>(v3, v3);
        nan_acc = _mm256_or_pd(nan_acc, _mm256_or_pd(_mm256_or_pd(nan0, nan1), _mm256_or_pd(nan2, nan3)));

        // _mm256_min_pd is asymmetric on NaN; that's fine for the min itself
        // because non-NaN data dominates. The NaN propagation is handled by
        // nan_acc above.
        acc0 = _mm256_min_pd(acc0, v0);
        acc1 = _mm256_min_pd(acc1, v1);
        acc2 = _mm256_min_pd(acc2, v2);
        acc3 = _mm256_min_pd(acc3, v3);
    }

    // If ANY NaN was seen in the data, the result is NaN regardless of min.
    let any_nan = _mm256_movemask_pd(nan_acc) != 0;
    if any_nan {
        // Still consume remainder for NaN detection, but we already know answer.
        for &v in remainder {
            if v.is_nan() {
                return f64::NAN;
            }
        }
        return f64::NAN;
    }

    let acc01 = _mm256_min_pd(acc0, acc1);
    let acc23 = _mm256_min_pd(acc2, acc3);
    let acc = _mm256_min_pd(acc01, acc23);

    let lo = _mm256_castpd256_pd128(acc);
    let hi = _mm256_extractf128_pd(acc, 1);
    let s128 = _mm_min_pd(lo, hi);
    let high = _mm_unpackhi_pd(s128, s128);
    let s = _mm_min_sd(s128, high);
    let mut m = _mm_cvtsd_f64(s);

    for &v in remainder {
        if v.is_nan() {
            return f64::NAN;
        }
        if v < m {
            m = v;
        }
    }
    m
}

// --- AVX-512F (x86_64) ------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn min_f64_to_f64_avx512(values: &[f64]) -> f64 {
    use core::arch::x86_64::*;

    let inf = _mm512_set1_pd(f64::INFINITY);
    let mut acc0 = inf;
    let mut acc1 = inf;
    let mut acc2 = inf;
    let mut acc3 = inf;
    let mut nan_mask: __mmask8 = 0;

    let chunks = values.chunks_exact(32);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let p = chunk.as_ptr();
        let v0 = _mm512_loadu_pd(p);
        let v1 = _mm512_loadu_pd(p.add(8));
        let v2 = _mm512_loadu_pd(p.add(16));
        let v3 = _mm512_loadu_pd(p.add(24));

        // AVX-512 returns __mmask8 from cmp_pd. _CMP_UNORD_Q masks NaN lanes.
        let nan0 = _mm512_cmp_pd_mask::<_CMP_UNORD_Q>(v0, v0);
        let nan1 = _mm512_cmp_pd_mask::<_CMP_UNORD_Q>(v1, v1);
        let nan2 = _mm512_cmp_pd_mask::<_CMP_UNORD_Q>(v2, v2);
        let nan3 = _mm512_cmp_pd_mask::<_CMP_UNORD_Q>(v3, v3);
        nan_mask |= nan0 | nan1 | nan2 | nan3;

        acc0 = _mm512_min_pd(acc0, v0);
        acc1 = _mm512_min_pd(acc1, v1);
        acc2 = _mm512_min_pd(acc2, v2);
        acc3 = _mm512_min_pd(acc3, v3);
    }

    if nan_mask != 0 {
        return f64::NAN;
    }

    let acc01 = _mm512_min_pd(acc0, acc1);
    let acc23 = _mm512_min_pd(acc2, acc3);
    let acc = _mm512_min_pd(acc01, acc23);
    let mut m = _mm512_reduce_min_pd(acc);

    for &v in remainder {
        if v.is_nan() {
            return f64::NAN;
        }
        if v < m {
            m = v;
        }
    }
    m
}

// --- tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_empty_returns_pos_infinity() {
        assert_eq!(min_f64_to_f64(&[]), f64::INFINITY);
        assert_eq!(min_f64_to_f64_scalar(&[]), f64::INFINITY);
    }

    #[test]
    fn dispatch_small_range() {
        let values: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        assert_eq!(min_f64_to_f64(&values), 1.0);
    }

    #[test]
    fn dispatch_matches_scalar_random() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut state: u64 = 0xc0ffee_bad_5eed;
        let mut values = Vec::with_capacity(10_007);
        for _ in 0..values.capacity() {
            let mut h = DefaultHasher::new();
            state.hash(&mut h);
            state = h.finish();
            values.push(((state as i64 % 2_000_001) as f64) - 1_000_000.0);
        }
        let scalar = min_f64_to_f64_scalar(&values);
        let dispatched = min_f64_to_f64(&values);
        assert_eq!(scalar, dispatched);
    }

    #[cfg(target_arch = "aarch64")]
    #[test]
    fn neon_matches_scalar() {
        if !std::arch::is_aarch64_feature_detected!("neon") {
            return;
        }
        let values: Vec<f64> = (-50_000..50_000).map(|i| i as f64 * 0.5).collect();
        let scalar = min_f64_to_f64_scalar(&values);
        let neon = unsafe { min_f64_to_f64_neon(&values) };
        assert_eq!(scalar, neon);
    }

    #[test]
    fn handles_negative_values() {
        let values = vec![-1.0, -2.0, -3.0, 0.0, 1.0, 2.0, 3.0];
        assert_eq!(min_f64_to_f64(&values), -3.0);
    }

    #[test]
    fn tail_handling_short_input() {
        for len in 0..40 {
            let values: Vec<f64> = (0..len).map(|i| (i * 7) as f64 + 0.25).collect();
            let expected = if len == 0 { f64::INFINITY } else { 0.25 };
            assert_eq!(min_f64_to_f64(&values), expected, "len={}", len);
        }
    }

    #[test]
    fn nan_propagates_in_scalar() {
        let values = vec![1.0, 2.0, f64::NAN, 3.0];
        assert!(min_f64_to_f64_scalar(&values).is_nan());
    }

    #[test]
    fn nan_propagates_in_dispatch() {
        let values = vec![1.0, 2.0, f64::NAN, 3.0];
        assert!(min_f64_to_f64(&values).is_nan());
    }

    #[test]
    fn nan_propagates_at_various_positions() {
        // Test NaN at start, middle, end, and in the tail (non-chunk-aligned).
        for nan_idx in [0, 1, 7, 8, 15, 16, 100, 999] {
            let mut values: Vec<f64> = (0..1000).map(|i| (i as f64) - 500.0).collect();
            values[nan_idx] = f64::NAN;
            assert!(min_f64_to_f64(&values).is_nan(), "NaN at idx {}", nan_idx);
        }
    }

    #[test]
    fn handles_neg_infinity() {
        let values = vec![1.0, 2.0, f64::NEG_INFINITY, 3.0];
        assert_eq!(min_f64_to_f64(&values), f64::NEG_INFINITY);
    }

    #[test]
    fn handles_all_pos_infinity() {
        let values = vec![f64::INFINITY; 64];
        assert_eq!(min_f64_to_f64(&values), f64::INFINITY);
    }
}
