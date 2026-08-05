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

//! MAX(FLOAT) kernel — computes the maximum `f32` across a slice, returned as `f64`.
//!
//! Empty input returns `f64::NEG_INFINITY`. NaN-propagating to match Java's
//! `Math.max(float, float)`. See [`super::super::min::float`] for the mirror
//! MIN kernel — NEON path uses NaN-propagating `vmaxq_f32`; x86 path uses
//! sticky NaN-detection because `_mm*_max_ps` is asymmetric on NaN.

#[inline]
pub fn max_f32_to_f64(values: &[f32]) -> f64 {
    if values.is_empty() {
        return f64::NEG_INFINITY;
    }
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512f") {
            return unsafe { max_f32_to_f64_avx512(values) };
        }
        if std::is_x86_feature_detected!("avx2") {
            return unsafe { max_f32_to_f64_avx2(values) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            return unsafe { max_f32_to_f64_neon(values) };
        }
    }

    max_f32_to_f64_scalar(values)
}

#[inline]
pub fn max_f32_to_f64_scalar(values: &[f32]) -> f64 {
    if values.is_empty() {
        return f64::NEG_INFINITY;
    }
    let mut m0 = f32::NEG_INFINITY;
    let mut m1 = f32::NEG_INFINITY;
    let mut m2 = f32::NEG_INFINITY;
    let mut m3 = f32::NEG_INFINITY;
    let mut saw_nan = false;

    let chunks = values.chunks_exact(4);
    let remainder = chunks.remainder();
    for c in chunks {
        if c[0].is_nan() || c[1].is_nan() || c[2].is_nan() || c[3].is_nan() {
            saw_nan = true;
        }
        if c[0] > m0 {
            m0 = c[0];
        }
        if c[1] > m1 {
            m1 = c[1];
        }
        if c[2] > m2 {
            m2 = c[2];
        }
        if c[3] > m3 {
            m3 = c[3];
        }
    }
    let mut tail = f32::NEG_INFINITY;
    for &v in remainder {
        if v.is_nan() {
            saw_nan = true;
        } else if v > tail {
            tail = v;
        }
    }
    if saw_nan {
        return f64::NAN;
    }
    let m01 = if m0 > m1 { m0 } else { m1 };
    let m23 = if m2 > m3 { m2 } else { m3 };
    let m = if m01 > m23 { m01 } else { m23 };
    let result = if m > tail { m } else { tail };
    result as f64
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn max_f32_to_f64_neon(values: &[f32]) -> f64 {
    use core::arch::aarch64::*;

    let mut acc0 = vdupq_n_f32(f32::NEG_INFINITY);
    let mut acc1 = vdupq_n_f32(f32::NEG_INFINITY);
    let mut acc2 = vdupq_n_f32(f32::NEG_INFINITY);
    let mut acc3 = vdupq_n_f32(f32::NEG_INFINITY);

    let chunks = values.chunks_exact(16);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let p = chunk.as_ptr();
        let v0 = vld1q_f32(p);
        let v1 = vld1q_f32(p.add(4));
        let v2 = vld1q_f32(p.add(8));
        let v3 = vld1q_f32(p.add(12));

        acc0 = vmaxq_f32(acc0, v0);
        acc1 = vmaxq_f32(acc1, v1);
        acc2 = vmaxq_f32(acc2, v2);
        acc3 = vmaxq_f32(acc3, v3);
    }

    let acc01 = vmaxq_f32(acc0, acc1);
    let acc23 = vmaxq_f32(acc2, acc3);
    let acc = vmaxq_f32(acc01, acc23);
    let mut m = vmaxvq_f32(acc);

    for &v in remainder {
        if v.is_nan() || m.is_nan() {
            m = f32::NAN;
        } else if v > m {
            m = v;
        }
    }
    m as f64
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn max_f32_to_f64_avx2(values: &[f32]) -> f64 {
    use core::arch::x86_64::*;

    let init = _mm256_set1_ps(f32::NEG_INFINITY);
    let mut acc0 = init;
    let mut acc1 = init;
    let mut acc2 = init;
    let mut acc3 = init;
    let mut nan_acc = _mm256_setzero_ps();

    let chunks = values.chunks_exact(32);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let p = chunk.as_ptr();
        let v0 = _mm256_loadu_ps(p);
        let v1 = _mm256_loadu_ps(p.add(8));
        let v2 = _mm256_loadu_ps(p.add(16));
        let v3 = _mm256_loadu_ps(p.add(24));

        let nan0 = _mm256_cmp_ps::<_CMP_UNORD_Q>(v0, v0);
        let nan1 = _mm256_cmp_ps::<_CMP_UNORD_Q>(v1, v1);
        let nan2 = _mm256_cmp_ps::<_CMP_UNORD_Q>(v2, v2);
        let nan3 = _mm256_cmp_ps::<_CMP_UNORD_Q>(v3, v3);
        nan_acc = _mm256_or_ps(nan_acc, _mm256_or_ps(_mm256_or_ps(nan0, nan1), _mm256_or_ps(nan2, nan3)));

        acc0 = _mm256_max_ps(acc0, v0);
        acc1 = _mm256_max_ps(acc1, v1);
        acc2 = _mm256_max_ps(acc2, v2);
        acc3 = _mm256_max_ps(acc3, v3);
    }

    if _mm256_movemask_ps(nan_acc) != 0 {
        return f64::NAN;
    }

    let acc01 = _mm256_max_ps(acc0, acc1);
    let acc23 = _mm256_max_ps(acc2, acc3);
    let acc = _mm256_max_ps(acc01, acc23);

    let lo = _mm256_castps256_ps128(acc);
    let hi = _mm256_extractf128_ps(acc, 1);
    let s128 = _mm_max_ps(lo, hi);
    let shuf = _mm_shuffle_ps::<0b1110>(s128, s128);
    let s64 = _mm_max_ps(s128, shuf);
    let shuf2 = _mm_shuffle_ps::<0b01>(s64, s64);
    let s32 = _mm_max_ss(s64, shuf2);
    let mut m = _mm_cvtss_f32(s32);

    for &v in remainder {
        if v.is_nan() {
            return f64::NAN;
        }
        if v > m {
            m = v;
        }
    }
    m as f64
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn max_f32_to_f64_avx512(values: &[f32]) -> f64 {
    use core::arch::x86_64::*;

    let init = _mm512_set1_ps(f32::NEG_INFINITY);
    let mut acc0 = init;
    let mut acc1 = init;
    let mut acc2 = init;
    let mut acc3 = init;
    let mut nan_mask: __mmask16 = 0;

    let chunks = values.chunks_exact(64);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let p = chunk.as_ptr();
        let v0 = _mm512_loadu_ps(p);
        let v1 = _mm512_loadu_ps(p.add(16));
        let v2 = _mm512_loadu_ps(p.add(32));
        let v3 = _mm512_loadu_ps(p.add(48));

        let nan0 = _mm512_cmp_ps_mask::<_CMP_UNORD_Q>(v0, v0);
        let nan1 = _mm512_cmp_ps_mask::<_CMP_UNORD_Q>(v1, v1);
        let nan2 = _mm512_cmp_ps_mask::<_CMP_UNORD_Q>(v2, v2);
        let nan3 = _mm512_cmp_ps_mask::<_CMP_UNORD_Q>(v3, v3);
        nan_mask |= nan0 | nan1 | nan2 | nan3;

        acc0 = _mm512_max_ps(acc0, v0);
        acc1 = _mm512_max_ps(acc1, v1);
        acc2 = _mm512_max_ps(acc2, v2);
        acc3 = _mm512_max_ps(acc3, v3);
    }

    if nan_mask != 0 {
        return f64::NAN;
    }

    let acc01 = _mm512_max_ps(acc0, acc1);
    let acc23 = _mm512_max_ps(acc2, acc3);
    let acc = _mm512_max_ps(acc01, acc23);
    let mut m = _mm512_reduce_max_ps(acc);

    for &v in remainder {
        if v.is_nan() {
            return f64::NAN;
        }
        if v > m {
            m = v;
        }
    }
    m as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_empty_returns_neg_infinity() {
        assert_eq!(max_f32_to_f64(&[]), f64::NEG_INFINITY);
        assert_eq!(max_f32_to_f64_scalar(&[]), f64::NEG_INFINITY);
    }

    #[test]
    fn dispatch_small_range() {
        let values: Vec<f32> = (1..=100).map(|i| i as f32).collect();
        assert_eq!(max_f32_to_f64(&values), 100.0);
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
            values.push(((state as i32 % 2_000_001) as f32) - 1_000_000.0);
        }
        let scalar = max_f32_to_f64_scalar(&values);
        let dispatched = max_f32_to_f64(&values);
        assert_eq!(scalar, dispatched);
    }

    #[cfg(target_arch = "aarch64")]
    #[test]
    fn neon_matches_scalar() {
        if !std::arch::is_aarch64_feature_detected!("neon") {
            return;
        }
        let values: Vec<f32> = (-50_000..50_000).map(|i| i as f32 * 0.5).collect();
        let scalar = max_f32_to_f64_scalar(&values);
        let neon = unsafe { max_f32_to_f64_neon(&values) };
        assert_eq!(scalar, neon);
    }

    #[test]
    fn handles_negative_values() {
        let values = vec![-1.0_f32, -2.0, -3.0, 0.0, 1.0, 2.0, 3.0];
        assert_eq!(max_f32_to_f64(&values), 3.0);
    }

    #[test]
    fn tail_handling_short_input() {
        for len in 0..40 {
            let values: Vec<f32> = (0..len).map(|i| (i * 7) as f32 + 0.25).collect();
            let expected = if len == 0 {
                f64::NEG_INFINITY
            } else {
                ((len - 1) * 7) as f64 + 0.25
            };
            assert_eq!(max_f32_to_f64(&values), expected, "len={}", len);
        }
    }

    #[test]
    fn nan_propagates_in_scalar() {
        let values = vec![1.0_f32, 2.0, f32::NAN, 3.0];
        assert!(max_f32_to_f64_scalar(&values).is_nan());
    }

    #[test]
    fn nan_propagates_in_dispatch() {
        let values = vec![1.0_f32, 2.0, f32::NAN, 3.0];
        assert!(max_f32_to_f64(&values).is_nan());
    }

    #[test]
    fn nan_propagates_at_various_positions() {
        for nan_idx in [0, 1, 15, 16, 31, 32, 100, 999] {
            let mut values: Vec<f32> = (0..1000).map(|i| (i as f32) - 500.0).collect();
            values[nan_idx] = f32::NAN;
            assert!(max_f32_to_f64(&values).is_nan(), "NaN at idx {}", nan_idx);
        }
    }

    #[test]
    fn handles_pos_infinity() {
        let values = vec![1.0_f32, 2.0, f32::INFINITY, 3.0];
        assert_eq!(max_f32_to_f64(&values), f64::INFINITY);
    }
}
