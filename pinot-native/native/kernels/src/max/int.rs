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

//! MAX(INT) kernel — computes the maximum `i32` across a slice, returned as `f64`.
//!
//! Empty input returns `f64::NEG_INFINITY` to match Pinot's
//! `MaxAggregationFunction.DEFAULT_VALUE = Double.NEGATIVE_INFINITY`.
//! See [`super::super::min::int`] for the mirror MIN kernel; ISA lane widths,
//! accumulator structure, and reduction shape are identical.

#[inline]
pub fn max_i32_to_f64(values: &[i32]) -> f64 {
    if values.is_empty() {
        return f64::NEG_INFINITY;
    }
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512f") {
            return unsafe { max_i32_to_f64_avx512(values) };
        }
        if std::is_x86_feature_detected!("avx2") {
            return unsafe { max_i32_to_f64_avx2(values) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            return unsafe { max_i32_to_f64_neon(values) };
        }
    }

    max_i32_to_f64_scalar(values)
}

#[inline]
pub fn max_i32_to_f64_scalar(values: &[i32]) -> f64 {
    if values.is_empty() {
        return f64::NEG_INFINITY;
    }
    let mut m0 = i32::MIN;
    let mut m1 = i32::MIN;
    let mut m2 = i32::MIN;
    let mut m3 = i32::MIN;

    let chunks = values.chunks_exact(4);
    let remainder = chunks.remainder();
    for c in chunks {
        m0 = m0.max(c[0]);
        m1 = m1.max(c[1]);
        m2 = m2.max(c[2]);
        m3 = m3.max(c[3]);
    }
    let mut tail = i32::MIN;
    for &v in remainder {
        tail = tail.max(v);
    }
    m0.max(m1).max(m2.max(m3)).max(tail) as f64
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn max_i32_to_f64_neon(values: &[i32]) -> f64 {
    use core::arch::aarch64::*;

    let mut acc0 = vdupq_n_s32(i32::MIN);
    let mut acc1 = vdupq_n_s32(i32::MIN);
    let mut acc2 = vdupq_n_s32(i32::MIN);
    let mut acc3 = vdupq_n_s32(i32::MIN);

    let chunks = values.chunks_exact(16);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let p = chunk.as_ptr();
        let v0 = vld1q_s32(p);
        let v1 = vld1q_s32(p.add(4));
        let v2 = vld1q_s32(p.add(8));
        let v3 = vld1q_s32(p.add(12));

        acc0 = vmaxq_s32(acc0, v0);
        acc1 = vmaxq_s32(acc1, v1);
        acc2 = vmaxq_s32(acc2, v2);
        acc3 = vmaxq_s32(acc3, v3);
    }

    let acc01 = vmaxq_s32(acc0, acc1);
    let acc23 = vmaxq_s32(acc2, acc3);
    let acc = vmaxq_s32(acc01, acc23);
    let mut m = vmaxvq_s32(acc);

    for &v in remainder {
        m = m.max(v);
    }
    m as f64
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn max_i32_to_f64_avx2(values: &[i32]) -> f64 {
    use core::arch::x86_64::*;

    let init = _mm256_set1_epi32(i32::MIN);
    let mut acc0 = init;
    let mut acc1 = init;
    let mut acc2 = init;
    let mut acc3 = init;

    let chunks = values.chunks_exact(32);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let p = chunk.as_ptr() as *const __m256i;
        let v0 = _mm256_loadu_si256(p);
        let v1 = _mm256_loadu_si256(p.add(1));
        let v2 = _mm256_loadu_si256(p.add(2));
        let v3 = _mm256_loadu_si256(p.add(3));

        acc0 = _mm256_max_epi32(acc0, v0);
        acc1 = _mm256_max_epi32(acc1, v1);
        acc2 = _mm256_max_epi32(acc2, v2);
        acc3 = _mm256_max_epi32(acc3, v3);
    }

    let acc01 = _mm256_max_epi32(acc0, acc1);
    let acc23 = _mm256_max_epi32(acc2, acc3);
    let acc = _mm256_max_epi32(acc01, acc23);

    let lo = _mm256_castsi256_si128(acc);
    let hi = _mm256_extracti128_si256(acc, 1);
    let s128 = _mm_max_epi32(lo, hi);
    let shuf = _mm_shuffle_epi32(s128, 0b1110);
    let s64 = _mm_max_epi32(s128, shuf);
    let shuf2 = _mm_shuffle_epi32(s64, 0b01);
    let s32 = _mm_max_epi32(s64, shuf2);
    let mut m = _mm_cvtsi128_si32(s32);

    for &v in remainder {
        m = m.max(v);
    }
    m as f64
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn max_i32_to_f64_avx512(values: &[i32]) -> f64 {
    use core::arch::x86_64::*;

    let init = _mm512_set1_epi32(i32::MIN);
    let mut acc0 = init;
    let mut acc1 = init;
    let mut acc2 = init;
    let mut acc3 = init;

    let chunks = values.chunks_exact(64);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let p = chunk.as_ptr() as *const i32;
        let v0 = _mm512_loadu_si512(p as *const _);
        let v1 = _mm512_loadu_si512(p.add(16) as *const _);
        let v2 = _mm512_loadu_si512(p.add(32) as *const _);
        let v3 = _mm512_loadu_si512(p.add(48) as *const _);

        acc0 = _mm512_max_epi32(acc0, v0);
        acc1 = _mm512_max_epi32(acc1, v1);
        acc2 = _mm512_max_epi32(acc2, v2);
        acc3 = _mm512_max_epi32(acc3, v3);
    }

    let acc01 = _mm512_max_epi32(acc0, acc1);
    let acc23 = _mm512_max_epi32(acc2, acc3);
    let acc = _mm512_max_epi32(acc01, acc23);
    let mut m = _mm512_reduce_max_epi32(acc);

    for &v in remainder {
        m = m.max(v);
    }
    m as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_empty_returns_neg_infinity() {
        assert_eq!(max_i32_to_f64(&[]), f64::NEG_INFINITY);
        assert_eq!(max_i32_to_f64_scalar(&[]), f64::NEG_INFINITY);
    }

    #[test]
    fn dispatch_single_element() {
        assert_eq!(max_i32_to_f64(&[42]), 42.0);
        assert_eq!(max_i32_to_f64(&[-42]), -42.0);
    }

    #[test]
    fn dispatch_small_range() {
        let values: Vec<i32> = (1..=100).collect();
        assert_eq!(max_i32_to_f64(&values), 100.0);
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
            values.push((state as i32) % 2_000_001 - 1_000_000);
        }
        let scalar = max_i32_to_f64_scalar(&values);
        let dispatched = max_i32_to_f64(&values);
        assert_eq!(scalar, dispatched);
    }

    #[cfg(target_arch = "aarch64")]
    #[test]
    fn neon_matches_scalar() {
        if !std::arch::is_aarch64_feature_detected!("neon") {
            return;
        }
        let values: Vec<i32> = (-50_000..50_000).collect();
        let scalar = max_i32_to_f64_scalar(&values);
        let neon = unsafe { max_i32_to_f64_neon(&values) };
        assert_eq!(scalar, neon);
    }

    #[test]
    fn handles_positive_extreme_values() {
        let values = vec![i32::MIN, 0, i32::MAX, 1, -1];
        assert_eq!(max_i32_to_f64(&values), i32::MAX as f64);
    }

    #[test]
    fn tail_handling_short_input() {
        for len in 0..40 {
            let values: Vec<i32> = (0..len).map(|i| (i * 7) - 50).collect();
            let scalar = max_i32_to_f64_scalar(&values);
            let dispatched = max_i32_to_f64(&values);
            assert_eq!(scalar, dispatched, "len={}", len);
        }
    }

    #[test]
    fn handles_all_min_values() {
        let values = vec![i32::MIN; 64];
        assert_eq!(max_i32_to_f64(&values), i32::MIN as f64);
    }
}
