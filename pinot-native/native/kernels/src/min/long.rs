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

//! MIN(LONG) kernel — computes the minimum `i64` across a slice, returned as `f64`.
//!
//! Empty input returns `f64::INFINITY` to match Pinot's
//! `MinAggregationFunction.DEFAULT_VALUE`.
//!
//! The min is computed in **native i64** space (precision-preserving), then
//! converted to f64 at the JNI boundary. This matches Pinot's per-block
//! behavior: `MinAggregationFunction.aggregate(LONG)` computes `Math.min(long, long)`
//! per element and only calls `.doubleValue()` at the holder write. For
//! |min| > 2^53 the f64 conversion is lossy, same as Java's path.
//!
//! Native vector min for `i64` is missing on NEON and AVX2; we synthesize it
//! from compare-and-select. AVX-512F has it directly.
//!
//! * NEON      — `vcgtq_s64` + `vbslq_s64`: 2-lane i64 min via predicate select;
//!                4 accumulators → 8-wide ILP
//! * AVX2      — `_mm256_cmpgt_epi64` + `_mm256_blendv_epi8`: 4-lane;
//!                4 accumulators → 16-wide ILP
//! * AVX-512F  — `_mm512_min_epi64`: 8-lane native; 4 accumulators → 32-wide ILP

/// Computes the minimum of a slice of `i64`, returned as `f64`. Dispatches to
/// the fastest available implementation for the current host CPU.
#[inline]
pub fn min_i64_to_f64(values: &[i64]) -> f64 {
    if values.is_empty() {
        return f64::INFINITY;
    }
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512f") {
            return unsafe { min_i64_to_f64_avx512(values) };
        }
        if std::is_x86_feature_detected!("avx2") {
            return unsafe { min_i64_to_f64_avx2(values) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            return unsafe { min_i64_to_f64_neon(values) };
        }
    }

    min_i64_to_f64_scalar(values)
}

/// 4-way unrolled scalar min on i64.
#[inline]
pub fn min_i64_to_f64_scalar(values: &[i64]) -> f64 {
    if values.is_empty() {
        return f64::INFINITY;
    }
    let mut m0 = i64::MAX;
    let mut m1 = i64::MAX;
    let mut m2 = i64::MAX;
    let mut m3 = i64::MAX;

    let chunks = values.chunks_exact(4);
    let remainder = chunks.remainder();
    for c in chunks {
        m0 = m0.min(c[0]);
        m1 = m1.min(c[1]);
        m2 = m2.min(c[2]);
        m3 = m3.min(c[3]);
    }
    let mut tail = i64::MAX;
    for &v in remainder {
        tail = tail.min(v);
    }
    m0.min(m1).min(m2.min(m3)).min(tail) as f64
}

// --- NEON (aarch64) ---------------------------------------------------------

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn min_i64_to_f64_neon(values: &[i64]) -> f64 {
    use core::arch::aarch64::*;

    /// 2-lane i64 min via compare-and-select. NEON lacks a direct vminq_s64.
    /// `vcgtq_s64(a, b)` returns lanes where `a > b` (all-1s mask). Then
    /// `vbslq_s64(mask, b, a)` picks `b` where the mask is set, `a` otherwise.
    #[inline(always)]
    unsafe fn vmin_s64(a: int64x2_t, b: int64x2_t) -> int64x2_t {
        let mask = vcgtq_s64(a, b);
        vbslq_s64(mask, b, a)
    }

    let mut acc0 = vdupq_n_s64(i64::MAX);
    let mut acc1 = vdupq_n_s64(i64::MAX);
    let mut acc2 = vdupq_n_s64(i64::MAX);
    let mut acc3 = vdupq_n_s64(i64::MAX);

    // Each chunk = 8 i64 spread across 4 accumulators (2 lanes each).
    let chunks = values.chunks_exact(8);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let p = chunk.as_ptr();
        let v0 = vld1q_s64(p);
        let v1 = vld1q_s64(p.add(2));
        let v2 = vld1q_s64(p.add(4));
        let v3 = vld1q_s64(p.add(6));

        acc0 = vmin_s64(acc0, v0);
        acc1 = vmin_s64(acc1, v1);
        acc2 = vmin_s64(acc2, v2);
        acc3 = vmin_s64(acc3, v3);
    }

    let acc01 = vmin_s64(acc0, acc1);
    let acc23 = vmin_s64(acc2, acc3);
    let acc = vmin_s64(acc01, acc23);

    // Horizontal reduce 2 i64 lanes.
    let lo = vgetq_lane_s64(acc, 0);
    let hi = vgetq_lane_s64(acc, 1);
    let mut m = lo.min(hi);

    for &v in remainder {
        m = m.min(v);
    }
    m as f64
}

// --- AVX2 (x86_64) ----------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn min_i64_to_f64_avx2(values: &[i64]) -> f64 {
    use core::arch::x86_64::*;

    /// 4-lane i64 min via compare-and-select. AVX2 lacks a direct
    /// `_mm256_min_epi64`; we synthesize it from `_mm256_cmpgt_epi64` +
    /// `_mm256_blendv_epi8`. `cmpgt` produces all-1s lanes where `a > b`;
    /// `blendv_epi8` selects per-byte based on sign bit, which is uniform
    /// across all 8 bytes of each i64 lane when the mask is all-1s or all-0s.
    #[inline(always)]
    unsafe fn vmin_i64(a: __m256i, b: __m256i) -> __m256i {
        let mask = _mm256_cmpgt_epi64(a, b);
        _mm256_blendv_epi8(a, b, mask)
    }

    let init = _mm256_set1_epi64x(i64::MAX);
    let mut acc0 = init;
    let mut acc1 = init;
    let mut acc2 = init;
    let mut acc3 = init;

    // Each chunk = 16 i64 spread across 4 accumulators (4 lanes each).
    let chunks = values.chunks_exact(16);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let p = chunk.as_ptr() as *const __m256i;
        let v0 = _mm256_loadu_si256(p);
        let v1 = _mm256_loadu_si256(p.add(1));
        let v2 = _mm256_loadu_si256(p.add(2));
        let v3 = _mm256_loadu_si256(p.add(3));

        acc0 = vmin_i64(acc0, v0);
        acc1 = vmin_i64(acc1, v1);
        acc2 = vmin_i64(acc2, v2);
        acc3 = vmin_i64(acc3, v3);
    }

    let acc01 = vmin_i64(acc0, acc1);
    let acc23 = vmin_i64(acc2, acc3);
    let acc = vmin_i64(acc01, acc23);

    // Horizontal reduce 4 i64 lanes.
    let lo128 = _mm256_castsi256_si128(acc);
    let hi128 = _mm256_extracti128_si256(acc, 1);
    // 2 lanes each in lo/hi. Compare-and-select between them.
    let cmp = _mm_cmpgt_epi64(lo128, hi128);
    let s128 = _mm_blendv_epi8(lo128, hi128, cmp);
    let l0 = _mm_extract_epi64(s128, 0);
    let l1 = _mm_extract_epi64(s128, 1);
    let mut m = l0.min(l1);

    for &v in remainder {
        m = m.min(v);
    }
    m as f64
}

// --- AVX-512F (x86_64) ------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn min_i64_to_f64_avx512(values: &[i64]) -> f64 {
    use core::arch::x86_64::*;

    let init = _mm512_set1_epi64(i64::MAX);
    let mut acc0 = init;
    let mut acc1 = init;
    let mut acc2 = init;
    let mut acc3 = init;

    // Each chunk = 32 i64 spread across 4 accumulators (8 lanes each).
    let chunks = values.chunks_exact(32);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let p = chunk.as_ptr();
        let v0 = _mm512_loadu_si512(p as *const _);
        let v1 = _mm512_loadu_si512(p.add(8) as *const _);
        let v2 = _mm512_loadu_si512(p.add(16) as *const _);
        let v3 = _mm512_loadu_si512(p.add(24) as *const _);

        acc0 = _mm512_min_epi64(acc0, v0);
        acc1 = _mm512_min_epi64(acc1, v1);
        acc2 = _mm512_min_epi64(acc2, v2);
        acc3 = _mm512_min_epi64(acc3, v3);
    }

    let acc01 = _mm512_min_epi64(acc0, acc1);
    let acc23 = _mm512_min_epi64(acc2, acc3);
    let acc = _mm512_min_epi64(acc01, acc23);
    let mut m = _mm512_reduce_min_epi64(acc);

    for &v in remainder {
        m = m.min(v);
    }
    m as f64
}

// --- tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_empty_returns_pos_infinity() {
        assert_eq!(min_i64_to_f64(&[]), f64::INFINITY);
        assert_eq!(min_i64_to_f64_scalar(&[]), f64::INFINITY);
    }

    #[test]
    fn dispatch_small_range() {
        let values: Vec<i64> = (1..=100).collect();
        assert_eq!(min_i64_to_f64(&values), 1.0);
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
            values.push((state as i64) % 2_000_001 - 1_000_000);
        }
        let scalar = min_i64_to_f64_scalar(&values);
        let dispatched = min_i64_to_f64(&values);
        assert_eq!(scalar, dispatched);
    }

    #[cfg(target_arch = "aarch64")]
    #[test]
    fn neon_matches_scalar() {
        if !std::arch::is_aarch64_feature_detected!("neon") {
            return;
        }
        let values: Vec<i64> = (-50_000..50_000).collect();
        let scalar = min_i64_to_f64_scalar(&values);
        let neon = unsafe { min_i64_to_f64_neon(&values) };
        assert_eq!(scalar, neon);
    }

    #[test]
    fn handles_negative_extreme_values() {
        let values = vec![i64::MIN, 0, i64::MAX, 1, -1];
        // Exact when compared as i64; conversion to f64 may round but
        // i64::MIN = -2^63 is representable exactly.
        assert_eq!(min_i64_to_f64(&values), i64::MIN as f64);
    }

    #[test]
    fn tail_handling_short_input() {
        for len in 0..40 {
            let values: Vec<i64> = (0..len).map(|i| (i * 7) - 50).collect();
            let scalar = min_i64_to_f64_scalar(&values);
            let dispatched = min_i64_to_f64(&values);
            assert_eq!(scalar, dispatched, "len={}", len);
        }
    }

    #[test]
    fn handles_all_max_values() {
        let values = vec![i64::MAX; 64];
        // i64::MAX = 2^63 - 1, which rounds when converted to f64. Match that.
        assert_eq!(min_i64_to_f64(&values), i64::MAX as f64);
    }
}
