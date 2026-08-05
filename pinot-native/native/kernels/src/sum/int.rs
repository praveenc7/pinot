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

//! SUM(INT) kernel — `i32 -> f64` accumulation.
//!
//! All ISAs have a hardware i32→f64 vector convert. Lane widths per loop:
//!
//! * NEON      — `vcvt_f64_f32` + `vcvt_high_f64_f32` on `vcvtq_f32_s32` output;
//!                4 i32 in → 4 f64 out per 128-bit register; 4 accumulators → 16-wide ILP
//! * AVX2      — `_mm256_cvtepi32_pd`: 4 i32 (low half of `__m256i`) → 4 f64;
//!                4 accumulators → 16-wide ILP
//! * AVX-512F  — `_mm512_cvtepi32_pd`: 8 i32 (`__m256i`) → 8 f64;
//!                4 accumulators → 32-wide ILP

/// Sums a slice of `i32` as `f64`, dispatching to the fastest available
/// implementation for the current host CPU.
#[inline]
pub fn sum_i32_to_f64(values: &[i32]) -> f64 {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512f") {
            // SAFETY: feature was just detected at runtime.
            return unsafe { sum_i32_to_f64_avx512(values) };
        }
        if std::is_x86_feature_detected!("avx2") {
            // SAFETY: feature was just detected at runtime.
            return unsafe { sum_i32_to_f64_avx2(values) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            // SAFETY: feature was just detected at runtime.
            return unsafe { sum_i32_to_f64_neon(values) };
        }
    }

    sum_i32_to_f64_scalar(values)
}

/// 4-way unrolled scalar accumulator. Used as the dispatch fallback and as the
/// reference for property-based equivalence testing.
#[inline]
pub fn sum_i32_to_f64_scalar(values: &[i32]) -> f64 {
    let mut s0 = 0.0_f64;
    let mut s1 = 0.0_f64;
    let mut s2 = 0.0_f64;
    let mut s3 = 0.0_f64;

    let chunks = values.chunks_exact(4);
    let remainder = chunks.remainder();
    for c in chunks {
        s0 += c[0] as f64;
        s1 += c[1] as f64;
        s2 += c[2] as f64;
        s3 += c[3] as f64;
    }
    let mut tail = 0.0_f64;
    for &v in remainder {
        tail += v as f64;
    }
    ((s0 + s1) + (s2 + s3)) + tail
}

// --- NEON (aarch64) ---------------------------------------------------------

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn sum_i32_to_f64_neon(values: &[i32]) -> f64 {
    use core::arch::aarch64::*;

    // Four independent 128-bit (2-lane f64) accumulators → 8-wide ILP.
    let mut acc0 = vdupq_n_f64(0.0);
    let mut acc1 = vdupq_n_f64(0.0);
    let mut acc2 = vdupq_n_f64(0.0);
    let mut acc3 = vdupq_n_f64(0.0);

    // Each chunk = 8 i32 → produces 8 f64 spread across 4 accumulators.
    let chunks = values.chunks_exact(8);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let p = chunk.as_ptr();
        let v0 = vld1q_s32(p); // 4× i32
        let v1 = vld1q_s32(p.add(4)); // 4× i32

        // i32x4 → f32x4 (vcvtq_f32_s32 → scvtf.4s), then split:
        // low half via vcvt_f64_f32 (2 f64), high half via vcvt_high_f64_f32 (2 f64).
        let f0_32 = vcvtq_f32_s32(v0);
        let f1_32 = vcvtq_f32_s32(v1);

        let f0_lo = vcvt_f64_f32(vget_low_f32(f0_32));
        let f0_hi = vcvt_high_f64_f32(f0_32);
        let f1_lo = vcvt_f64_f32(vget_low_f32(f1_32));
        let f1_hi = vcvt_high_f64_f32(f1_32);

        acc0 = vaddq_f64(acc0, f0_lo);
        acc1 = vaddq_f64(acc1, f0_hi);
        acc2 = vaddq_f64(acc2, f1_lo);
        acc3 = vaddq_f64(acc3, f1_hi);
    }

    let acc01 = vaddq_f64(acc0, acc1);
    let acc23 = vaddq_f64(acc2, acc3);
    let acc = vaddq_f64(acc01, acc23);
    let mut sum = vaddvq_f64(acc);

    for &v in remainder {
        sum += v as f64;
    }
    sum
}

// --- AVX2 (x86_64) ----------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn sum_i32_to_f64_avx2(values: &[i32]) -> f64 {
    use core::arch::x86_64::*;

    // Four independent 256-bit (4-lane f64) accumulators → 16-wide ILP.
    let mut acc0 = _mm256_setzero_pd();
    let mut acc1 = _mm256_setzero_pd();
    let mut acc2 = _mm256_setzero_pd();
    let mut acc3 = _mm256_setzero_pd();

    // Each chunk = 16 i32 → 16 f64 spread across 4 accumulators.
    let chunks = values.chunks_exact(16);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let p = chunk.as_ptr() as *const __m128i;
        // Each __m128i holds 4 i32; _mm256_cvtepi32_pd widens to 4 f64.
        let f0 = _mm256_cvtepi32_pd(_mm_loadu_si128(p));
        let f1 = _mm256_cvtepi32_pd(_mm_loadu_si128(p.add(1)));
        let f2 = _mm256_cvtepi32_pd(_mm_loadu_si128(p.add(2)));
        let f3 = _mm256_cvtepi32_pd(_mm_loadu_si128(p.add(3)));

        acc0 = _mm256_add_pd(acc0, f0);
        acc1 = _mm256_add_pd(acc1, f1);
        acc2 = _mm256_add_pd(acc2, f2);
        acc3 = _mm256_add_pd(acc3, f3);
    }

    let acc01 = _mm256_add_pd(acc0, acc1);
    let acc23 = _mm256_add_pd(acc2, acc3);
    let acc = _mm256_add_pd(acc01, acc23);

    let lo = _mm256_castpd256_pd128(acc);
    let hi = _mm256_extractf128_pd(acc, 1);
    let s128 = _mm_add_pd(lo, hi);
    let high = _mm_unpackhi_pd(s128, s128);
    let s = _mm_add_sd(s128, high);
    let mut sum = _mm_cvtsd_f64(s);

    for &v in remainder {
        sum += v as f64;
    }
    sum
}

// --- AVX-512F (x86_64) ------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn sum_i32_to_f64_avx512(values: &[i32]) -> f64 {
    use core::arch::x86_64::*;

    // Four independent 512-bit (8-lane f64) accumulators → 32-wide ILP.
    let mut acc0 = _mm512_setzero_pd();
    let mut acc1 = _mm512_setzero_pd();
    let mut acc2 = _mm512_setzero_pd();
    let mut acc3 = _mm512_setzero_pd();

    // Each chunk = 32 i32 → 32 f64 spread across 4 accumulators.
    let chunks = values.chunks_exact(32);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let p = chunk.as_ptr() as *const __m256i;
        // Each __m256i holds 8 i32; _mm512_cvtepi32_pd (AVX-512F) widens to 8 f64.
        let f0 = _mm512_cvtepi32_pd(_mm256_loadu_si256(p));
        let f1 = _mm512_cvtepi32_pd(_mm256_loadu_si256(p.add(1)));
        let f2 = _mm512_cvtepi32_pd(_mm256_loadu_si256(p.add(2)));
        let f3 = _mm512_cvtepi32_pd(_mm256_loadu_si256(p.add(3)));

        acc0 = _mm512_add_pd(acc0, f0);
        acc1 = _mm512_add_pd(acc1, f1);
        acc2 = _mm512_add_pd(acc2, f2);
        acc3 = _mm512_add_pd(acc3, f3);
    }

    let acc01 = _mm512_add_pd(acc0, acc1);
    let acc23 = _mm512_add_pd(acc2, acc3);
    let acc = _mm512_add_pd(acc01, acc23);
    let mut sum = _mm512_reduce_add_pd(acc);

    for &v in remainder {
        sum += v as f64;
    }
    sum
}

// --- tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn within_tolerance(a: f64, b: f64) -> bool {
        let tol = (a.abs().max(b.abs()) * 1e-15).max(1.0);
        (a - b).abs() <= tol
    }

    #[test]
    fn dispatch_empty_is_zero() {
        assert_eq!(sum_i32_to_f64(&[]), 0.0);
    }

    #[test]
    fn dispatch_small_range() {
        let values: Vec<i32> = (1..=100).collect();
        let expected = sum_i32_to_f64_scalar(&values);
        assert_eq!(sum_i32_to_f64(&values), expected);
    }

    #[test]
    fn dispatch_matches_scalar_random() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut state: u64 = 0xfeed_face_dead_beef;
        let mut values = Vec::with_capacity(10_007);
        for _ in 0..values.capacity() {
            let mut h = DefaultHasher::new();
            state.hash(&mut h);
            state = h.finish();
            values.push((state as i32) % 1_000_000);
        }
        let scalar = sum_i32_to_f64_scalar(&values);
        let dispatched = sum_i32_to_f64(&values);
        assert!(
            within_tolerance(scalar, dispatched),
            "scalar={} dispatched={}",
            scalar,
            dispatched
        );
    }

    #[cfg(target_arch = "aarch64")]
    #[test]
    fn neon_matches_scalar() {
        if !std::arch::is_aarch64_feature_detected!("neon") {
            return;
        }
        let values: Vec<i32> = (-50_000..50_000).collect();
        let scalar = sum_i32_to_f64_scalar(&values);
        let neon = unsafe { sum_i32_to_f64_neon(&values) };
        assert!(within_tolerance(scalar, neon),
            "scalar={} neon={}", scalar, neon);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn avx2_matches_scalar() {
        if !std::is_x86_feature_detected!("avx2") {
            return;
        }
        let values: Vec<i32> = (-50_000..50_000).collect();
        let scalar = sum_i32_to_f64_scalar(&values);
        let avx2 = unsafe { sum_i32_to_f64_avx2(&values) };
        assert!(within_tolerance(scalar, avx2),
            "scalar={} avx2={}", scalar, avx2);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn avx512_matches_scalar() {
        if !std::is_x86_feature_detected!("avx512f") {
            return;
        }
        let values: Vec<i32> = (-50_000..50_000).collect();
        let scalar = sum_i32_to_f64_scalar(&values);
        let avx512 = unsafe { sum_i32_to_f64_avx512(&values) };
        assert!(within_tolerance(scalar, avx512),
            "scalar={} avx512={}", scalar, avx512);
    }

    #[test]
    fn handles_extreme_values() {
        let values = [i32::MIN, i32::MAX, 0, -1, 1, i32::MIN + 1, i32::MAX - 1];
        let scalar = sum_i32_to_f64_scalar(&values);
        let dispatched = sum_i32_to_f64(&values);
        assert!(within_tolerance(scalar, dispatched));
    }

    #[test]
    fn tail_handling_short_input() {
        for len in 0..40 {
            let values: Vec<i32> = (0..len).map(|i| i * 7).collect();
            let scalar = sum_i32_to_f64_scalar(&values);
            let dispatched = sum_i32_to_f64(&values);
            assert!(within_tolerance(scalar, dispatched), "len={}", len);
        }
    }
}
