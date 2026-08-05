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

//! MAX(LONG) kernel — computes the maximum `i64` across a slice, returned as `f64`.
//!
//! Empty input returns `f64::NEG_INFINITY`. Native vector max for `i64` is
//! missing on NEON and AVX2; synthesized via compare-and-select. AVX-512F has
//! it directly. See [`super::super::min::long`] for the mirror MIN kernel.

#[inline]
pub fn max_i64_to_f64(values: &[i64]) -> f64 {
    if values.is_empty() {
        return f64::NEG_INFINITY;
    }
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512f") {
            return unsafe { max_i64_to_f64_avx512(values) };
        }
        if std::is_x86_feature_detected!("avx2") {
            return unsafe { max_i64_to_f64_avx2(values) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            return unsafe { max_i64_to_f64_neon(values) };
        }
    }

    max_i64_to_f64_scalar(values)
}

#[inline]
pub fn max_i64_to_f64_scalar(values: &[i64]) -> f64 {
    if values.is_empty() {
        return f64::NEG_INFINITY;
    }
    let mut m0 = i64::MIN;
    let mut m1 = i64::MIN;
    let mut m2 = i64::MIN;
    let mut m3 = i64::MIN;

    let chunks = values.chunks_exact(4);
    let remainder = chunks.remainder();
    for c in chunks {
        m0 = m0.max(c[0]);
        m1 = m1.max(c[1]);
        m2 = m2.max(c[2]);
        m3 = m3.max(c[3]);
    }
    let mut tail = i64::MIN;
    for &v in remainder {
        tail = tail.max(v);
    }
    m0.max(m1).max(m2.max(m3)).max(tail) as f64
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn max_i64_to_f64_neon(values: &[i64]) -> f64 {
    use core::arch::aarch64::*;

    /// 2-lane i64 max: pick a where a > b, else b.
    #[inline(always)]
    unsafe fn vmax_s64(a: int64x2_t, b: int64x2_t) -> int64x2_t {
        let mask = vcgtq_s64(a, b);
        vbslq_s64(mask, a, b)
    }

    let mut acc0 = vdupq_n_s64(i64::MIN);
    let mut acc1 = vdupq_n_s64(i64::MIN);
    let mut acc2 = vdupq_n_s64(i64::MIN);
    let mut acc3 = vdupq_n_s64(i64::MIN);

    let chunks = values.chunks_exact(8);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let p = chunk.as_ptr();
        let v0 = vld1q_s64(p);
        let v1 = vld1q_s64(p.add(2));
        let v2 = vld1q_s64(p.add(4));
        let v3 = vld1q_s64(p.add(6));

        acc0 = vmax_s64(acc0, v0);
        acc1 = vmax_s64(acc1, v1);
        acc2 = vmax_s64(acc2, v2);
        acc3 = vmax_s64(acc3, v3);
    }

    let acc01 = vmax_s64(acc0, acc1);
    let acc23 = vmax_s64(acc2, acc3);
    let acc = vmax_s64(acc01, acc23);

    let lo = vgetq_lane_s64(acc, 0);
    let hi = vgetq_lane_s64(acc, 1);
    let mut m = lo.max(hi);

    for &v in remainder {
        m = m.max(v);
    }
    m as f64
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn max_i64_to_f64_avx2(values: &[i64]) -> f64 {
    use core::arch::x86_64::*;

    /// 4-lane i64 max via compare-and-select.
    #[inline(always)]
    unsafe fn vmax_i64(a: __m256i, b: __m256i) -> __m256i {
        let mask = _mm256_cmpgt_epi64(a, b);
        _mm256_blendv_epi8(b, a, mask)
    }

    let init = _mm256_set1_epi64x(i64::MIN);
    let mut acc0 = init;
    let mut acc1 = init;
    let mut acc2 = init;
    let mut acc3 = init;

    let chunks = values.chunks_exact(16);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let p = chunk.as_ptr() as *const __m256i;
        let v0 = _mm256_loadu_si256(p);
        let v1 = _mm256_loadu_si256(p.add(1));
        let v2 = _mm256_loadu_si256(p.add(2));
        let v3 = _mm256_loadu_si256(p.add(3));

        acc0 = vmax_i64(acc0, v0);
        acc1 = vmax_i64(acc1, v1);
        acc2 = vmax_i64(acc2, v2);
        acc3 = vmax_i64(acc3, v3);
    }

    let acc01 = vmax_i64(acc0, acc1);
    let acc23 = vmax_i64(acc2, acc3);
    let acc = vmax_i64(acc01, acc23);

    let lo128 = _mm256_castsi256_si128(acc);
    let hi128 = _mm256_extracti128_si256(acc, 1);
    let cmp = _mm_cmpgt_epi64(lo128, hi128);
    let s128 = _mm_blendv_epi8(hi128, lo128, cmp);
    let l0 = _mm_extract_epi64(s128, 0);
    let l1 = _mm_extract_epi64(s128, 1);
    let mut m = l0.max(l1);

    for &v in remainder {
        m = m.max(v);
    }
    m as f64
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn max_i64_to_f64_avx512(values: &[i64]) -> f64 {
    use core::arch::x86_64::*;

    let init = _mm512_set1_epi64(i64::MIN);
    let mut acc0 = init;
    let mut acc1 = init;
    let mut acc2 = init;
    let mut acc3 = init;

    let chunks = values.chunks_exact(32);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let p = chunk.as_ptr();
        let v0 = _mm512_loadu_si512(p as *const _);
        let v1 = _mm512_loadu_si512(p.add(8) as *const _);
        let v2 = _mm512_loadu_si512(p.add(16) as *const _);
        let v3 = _mm512_loadu_si512(p.add(24) as *const _);

        acc0 = _mm512_max_epi64(acc0, v0);
        acc1 = _mm512_max_epi64(acc1, v1);
        acc2 = _mm512_max_epi64(acc2, v2);
        acc3 = _mm512_max_epi64(acc3, v3);
    }

    let acc01 = _mm512_max_epi64(acc0, acc1);
    let acc23 = _mm512_max_epi64(acc2, acc3);
    let acc = _mm512_max_epi64(acc01, acc23);
    let mut m = _mm512_reduce_max_epi64(acc);

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
        assert_eq!(max_i64_to_f64(&[]), f64::NEG_INFINITY);
        assert_eq!(max_i64_to_f64_scalar(&[]), f64::NEG_INFINITY);
    }

    #[test]
    fn dispatch_small_range() {
        let values: Vec<i64> = (1..=100).collect();
        assert_eq!(max_i64_to_f64(&values), 100.0);
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
        let scalar = max_i64_to_f64_scalar(&values);
        let dispatched = max_i64_to_f64(&values);
        assert_eq!(scalar, dispatched);
    }

    #[cfg(target_arch = "aarch64")]
    #[test]
    fn neon_matches_scalar() {
        if !std::arch::is_aarch64_feature_detected!("neon") {
            return;
        }
        let values: Vec<i64> = (-50_000..50_000).collect();
        let scalar = max_i64_to_f64_scalar(&values);
        let neon = unsafe { max_i64_to_f64_neon(&values) };
        assert_eq!(scalar, neon);
    }

    #[test]
    fn handles_positive_extreme_values() {
        let values = vec![i64::MIN, 0, i64::MAX, 1, -1];
        // i64::MAX rounds when converted to f64 (2^63 - 1 → 2^63); the scalar
        // and SIMD paths converge through the same conversion.
        assert_eq!(max_i64_to_f64(&values), i64::MAX as f64);
    }

    #[test]
    fn tail_handling_short_input() {
        for len in 0..40 {
            let values: Vec<i64> = (0..len).map(|i| (i * 7) - 50).collect();
            let scalar = max_i64_to_f64_scalar(&values);
            let dispatched = max_i64_to_f64(&values);
            assert_eq!(scalar, dispatched, "len={}", len);
        }
    }

    #[test]
    fn handles_all_min_values() {
        let values = vec![i64::MIN; 64];
        assert_eq!(max_i64_to_f64(&values), i64::MIN as f64);
    }
}
