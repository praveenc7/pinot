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

//! SUM(DOUBLE) kernel — `f64 -> f64` accumulation.
//!
//! No conversion required; the kernel is a pure SIMD reduction over
//! contiguous f64 values. Lane widths per loop:
//!
//! * NEON      — `vaddq_f64`: 2-lane f64; 4 accumulators → 8-wide ILP
//! * AVX2      — `_mm256_add_pd`: 4-lane f64; 4 accumulators → 16-wide ILP
//! * AVX-512F  — `_mm512_add_pd`: 8-lane f64; 4 accumulators → 32-wide ILP

#[inline]
pub fn sum_f64_to_f64(values: &[f64]) -> f64 {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512f") {
            return unsafe { sum_f64_to_f64_avx512(values) };
        }
        if std::is_x86_feature_detected!("avx2") {
            return unsafe { sum_f64_to_f64_avx2(values) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            return unsafe { sum_f64_to_f64_neon(values) };
        }
    }

    sum_f64_to_f64_scalar(values)
}

#[inline]
pub fn sum_f64_to_f64_scalar(values: &[f64]) -> f64 {
    let mut s0 = 0.0_f64;
    let mut s1 = 0.0_f64;
    let mut s2 = 0.0_f64;
    let mut s3 = 0.0_f64;

    let chunks = values.chunks_exact(4);
    let remainder = chunks.remainder();
    for c in chunks {
        s0 += c[0];
        s1 += c[1];
        s2 += c[2];
        s3 += c[3];
    }
    let mut tail = 0.0_f64;
    for &v in remainder {
        tail += v;
    }
    ((s0 + s1) + (s2 + s3)) + tail
}

// --- NEON (aarch64) ---------------------------------------------------------

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn sum_f64_to_f64_neon(values: &[f64]) -> f64 {
    use core::arch::aarch64::*;

    let mut acc0 = vdupq_n_f64(0.0);
    let mut acc1 = vdupq_n_f64(0.0);
    let mut acc2 = vdupq_n_f64(0.0);
    let mut acc3 = vdupq_n_f64(0.0);

    // Each chunk = 8 f64 spread across 4 accumulators (2 lanes each).
    let chunks = values.chunks_exact(8);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let p = chunk.as_ptr();
        let v0 = vld1q_f64(p);
        let v1 = vld1q_f64(p.add(2));
        let v2 = vld1q_f64(p.add(4));
        let v3 = vld1q_f64(p.add(6));

        acc0 = vaddq_f64(acc0, v0);
        acc1 = vaddq_f64(acc1, v1);
        acc2 = vaddq_f64(acc2, v2);
        acc3 = vaddq_f64(acc3, v3);
    }

    let acc01 = vaddq_f64(acc0, acc1);
    let acc23 = vaddq_f64(acc2, acc3);
    let acc = vaddq_f64(acc01, acc23);
    let mut sum = vaddvq_f64(acc);

    for &v in remainder {
        sum += v;
    }
    sum
}

// --- AVX2 (x86_64) ----------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn sum_f64_to_f64_avx2(values: &[f64]) -> f64 {
    use core::arch::x86_64::*;

    let mut acc0 = _mm256_setzero_pd();
    let mut acc1 = _mm256_setzero_pd();
    let mut acc2 = _mm256_setzero_pd();
    let mut acc3 = _mm256_setzero_pd();

    // Each chunk = 16 f64 spread across 4 accumulators (4 lanes each).
    let chunks = values.chunks_exact(16);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let p = chunk.as_ptr();
        let v0 = _mm256_loadu_pd(p);
        let v1 = _mm256_loadu_pd(p.add(4));
        let v2 = _mm256_loadu_pd(p.add(8));
        let v3 = _mm256_loadu_pd(p.add(12));

        acc0 = _mm256_add_pd(acc0, v0);
        acc1 = _mm256_add_pd(acc1, v1);
        acc2 = _mm256_add_pd(acc2, v2);
        acc3 = _mm256_add_pd(acc3, v3);
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
        sum += v;
    }
    sum
}

// --- AVX-512F (x86_64) ------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn sum_f64_to_f64_avx512(values: &[f64]) -> f64 {
    use core::arch::x86_64::*;

    let mut acc0 = _mm512_setzero_pd();
    let mut acc1 = _mm512_setzero_pd();
    let mut acc2 = _mm512_setzero_pd();
    let mut acc3 = _mm512_setzero_pd();

    // Each chunk = 32 f64 spread across 4 accumulators (8 lanes each).
    let chunks = values.chunks_exact(32);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let p = chunk.as_ptr();
        let v0 = _mm512_loadu_pd(p);
        let v1 = _mm512_loadu_pd(p.add(8));
        let v2 = _mm512_loadu_pd(p.add(16));
        let v3 = _mm512_loadu_pd(p.add(24));

        acc0 = _mm512_add_pd(acc0, v0);
        acc1 = _mm512_add_pd(acc1, v1);
        acc2 = _mm512_add_pd(acc2, v2);
        acc3 = _mm512_add_pd(acc3, v3);
    }

    let acc01 = _mm512_add_pd(acc0, acc1);
    let acc23 = _mm512_add_pd(acc2, acc3);
    let acc = _mm512_add_pd(acc01, acc23);
    let mut sum = _mm512_reduce_add_pd(acc);

    for &v in remainder {
        sum += v;
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
        assert_eq!(sum_f64_to_f64(&[]), 0.0);
    }

    #[test]
    fn dispatch_small_range() {
        let values: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let expected = sum_f64_to_f64_scalar(&values);
        assert_eq!(sum_f64_to_f64(&values), expected);
    }

    #[test]
    fn dispatch_matches_scalar_random() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut state: u64 = 0xabad_1dea_dead_f00d;
        let mut values = Vec::with_capacity(10_007);
        for _ in 0..values.capacity() {
            let mut h = DefaultHasher::new();
            state.hash(&mut h);
            state = h.finish();
            values.push(((state as i64 % 2_000_001) as f64) - 1_000_000.0);
        }
        let scalar = sum_f64_to_f64_scalar(&values);
        let dispatched = sum_f64_to_f64(&values);
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
        let values: Vec<f64> = (-50_000..50_000).map(|i| i as f64 * 0.5).collect();
        let scalar = sum_f64_to_f64_scalar(&values);
        let neon = unsafe { sum_f64_to_f64_neon(&values) };
        assert!(within_tolerance(scalar, neon),
            "scalar={} neon={}", scalar, neon);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn avx2_matches_scalar() {
        if !std::is_x86_feature_detected!("avx2") {
            return;
        }
        let values: Vec<f64> = (-50_000..50_000).map(|i| i as f64 * 0.5).collect();
        let scalar = sum_f64_to_f64_scalar(&values);
        let avx2 = unsafe { sum_f64_to_f64_avx2(&values) };
        assert!(within_tolerance(scalar, avx2),
            "scalar={} avx2={}", scalar, avx2);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn avx512_matches_scalar() {
        if !std::is_x86_feature_detected!("avx512f") {
            return;
        }
        let values: Vec<f64> = (-50_000..50_000).map(|i| i as f64 * 0.5).collect();
        let scalar = sum_f64_to_f64_scalar(&values);
        let avx512 = unsafe { sum_f64_to_f64_avx512(&values) };
        assert!(within_tolerance(scalar, avx512),
            "scalar={} avx512={}", scalar, avx512);
    }

    #[test]
    fn tail_handling_short_input() {
        for len in 0..40 {
            let values: Vec<f64> = (0..len).map(|i| (i * 7) as f64 + 0.25).collect();
            let scalar = sum_f64_to_f64_scalar(&values);
            let dispatched = sum_f64_to_f64(&values);
            assert!(within_tolerance(scalar, dispatched), "len={}", len);
        }
    }
}
