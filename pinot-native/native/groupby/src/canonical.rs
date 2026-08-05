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

//! Canonical floating-point group keys (Task #45; design doc §23 foundation
//! step 4).
//!
//! `f32` / `f64` cannot be hash-table keys directly: they implement `PartialEq`
//! but not `Eq` (NaN ≠ NaN), and raw bit patterns for "equal" values can
//! differ (every NaN payload, and ±0.0). [`CanonicalF64`] / [`CanonicalF32`]
//! are `Copy` newtypes over the **canonicalized bit pattern**, giving total
//! `Eq` + `Hash` so FP columns can be raw GROUP BY keys (at combine, and at the
//! future raw-FP segment driver, steps 1d/1e).
//!
//! ## Canonicalization
//!
//! * **NaN** — every NaN (any sign/payload) collapses to one canonical NaN, so
//!   all NaN rows fall in a single group (matches Java, whose
//!   `Double.doubleToLongBits` collapses NaN, and whose boxed-`Double` GROUP BY
//!   treats NaN == NaN).
//! * **±0.0** — `-0.0` and `+0.0` collapse to `+0.0`, so they group together
//!   (they are numerically equal under `==`). NOTE: the exact ±0.0 grouping
//!   must be confirmed against Pinot's double group-key semantics in the
//!   step-5 differential test; if Pinot keeps them distinct, drop the zero
//!   branch (one line).
//! * All other values keep their bits, so distinct finite values stay distinct.

use crate::hash::{hash_u64, HashKey};

/// Canonical `f64` GROUP BY key — a `Copy` newtype over the canonicalized
/// IEEE-754 bits (NaN and ±0.0 collapsed). Total `Eq` + `Hash`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct CanonicalF64(u64);

impl CanonicalF64 {
    /// Canonical quiet-NaN bits: positive quiet NaN with a zero payload — the
    /// exact value Java's `Double.doubleToLongBits` collapses every NaN to.
    /// Using an explicit constant (rather than `f64::NAN.to_bits()`, whose bit
    /// pattern is implementation-defined) makes the canonical group key
    /// deterministic and Java-parity by construction.
    pub const CANONICAL_NAN_BITS: u64 = 0x7ff8_0000_0000_0000;

    #[inline]
    pub fn new(v: f64) -> Self {
        let bits = if v.is_nan() {
            Self::CANONICAL_NAN_BITS
        } else if v == 0.0 {
            0.0_f64.to_bits() // +0.0; also collapses -0.0 (which == 0.0)
        } else {
            v.to_bits()
        };
        Self(bits)
    }

    #[inline]
    pub fn to_f64(self) -> f64 {
        f64::from_bits(self.0)
    }
}

impl HashKey for CanonicalF64 {
    #[inline]
    fn hash(&self) -> u64 {
        hash_u64(self.0)
    }
}

/// Canonical `f32` GROUP BY key — see [`CanonicalF64`].
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct CanonicalF32(u32);

impl CanonicalF32 {
    /// Canonical quiet-NaN bits: positive quiet NaN with a zero payload — the
    /// exact value Java's `Float.floatToIntBits` collapses every NaN to. See
    /// [`CanonicalF64::CANONICAL_NAN_BITS`] for the rationale.
    pub const CANONICAL_NAN_BITS: u32 = 0x7fc0_0000;

    #[inline]
    pub fn new(v: f32) -> Self {
        let bits = if v.is_nan() {
            Self::CANONICAL_NAN_BITS
        } else if v == 0.0 {
            0.0_f32.to_bits() // +0.0; also collapses -0.0 (which == 0.0)
        } else {
            v.to_bits()
        };
        Self(bits)
    }

    #[inline]
    pub fn to_f32(self) -> f32 {
        f32::from_bits(self.0)
    }
}

impl HashKey for CanonicalF32 {
    #[inline]
    fn hash(&self) -> u64 {
        // Widen to u64 so the same finalizer applies as for f64/i64.
        hash_u64(self.0 as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agg::{AggKind, AggState};
    use crate::combine::CombineDriver;
    use crate::HashbrownTable;

    #[test]
    fn all_nans_canonicalize_equal() {
        let a = CanonicalF64::new(f64::NAN);
        let b = CanonicalF64::new(-f64::NAN);
        let c = CanonicalF64::new(f64::from_bits(0x7ff8_0000_dead_beef)); // NaN, different payload
        assert_eq!(a, b);
        assert_eq!(a, c);
        assert!(a.to_f64().is_nan());
    }

    #[test]
    fn canonical_nan_bits_match_java() {
        // The canonical NaN is the exact bit pattern Java's Double.doubleToLongBits /
        // Float.floatToIntBits collapse every NaN to — a positive quiet NaN, zero payload.
        assert_eq!(CanonicalF64::CANONICAL_NAN_BITS, 0x7ff8_0000_0000_0000);
        assert_eq!(CanonicalF32::CANONICAL_NAN_BITS, 0x7fc0_0000);
        // Any NaN input (any sign/payload) canonicalizes to that constant.
        assert_eq!(CanonicalF64::new(f64::NAN).to_f64().to_bits(), CanonicalF64::CANONICAL_NAN_BITS);
        assert_eq!(
            CanonicalF64::new(f64::from_bits(0xffff_ffff_ffff_ffff)).to_f64().to_bits(),
            CanonicalF64::CANONICAL_NAN_BITS
        );
        assert_eq!(CanonicalF32::new(f32::NAN).to_f32().to_bits(), CanonicalF32::CANONICAL_NAN_BITS);
    }

    #[test]
    fn signed_zeros_group_together() {
        let neg = CanonicalF64::new(-0.0);
        let pos = CanonicalF64::new(0.0);
        assert_eq!(neg, pos);
        assert_eq!(neg.to_f64(), 0.0);
    }

    #[test]
    fn distinct_finite_values_stay_distinct() {
        assert_ne!(CanonicalF64::new(1.5), CanonicalF64::new(2.5));
        assert_eq!(CanonicalF64::new(1.5), CanonicalF64::new(1.5));
        assert_eq!(CanonicalF64::new(1.5).to_f64(), 1.5);
        assert_ne!(CanonicalF32::new(1.5), CanonicalF32::new(2.5));
        assert_eq!(CanonicalF32::new(-0.0), CanonicalF32::new(0.0));
        assert_eq!(CanonicalF32::new(f32::NAN), CanonicalF32::new(-f32::NAN));
    }

    /// FP raw key at combine: a DOUBLE GROUP BY key works through the combine
    /// driver, with all NaNs in one group and ±0.0 together.
    #[test]
    fn combine_over_canonical_f64_keys() {
        let kinds = [AggKind::Count];
        let mut c = CombineDriver::<CanonicalF64, HashbrownTable<CanonicalF64>>::new(&kinds);
        let k = |v: f64| CanonicalF64::new(v);
        // Segment A
        c.merge_partials(
            &[k(1.5), k(f64::NAN), k(-0.0)],
            &[AggState::Count(vec![2, 1, 3])],
        );
        // Segment B: another NaN (different payload) + +0.0 + a fresh value
        c.merge_partials(
            &[k(f64::from_bits(0x7ff8_0000_0000_0001)), k(0.0), k(9.0)],
            &[AggState::Count(vec![5, 4, 7])],
        );

        assert_eq!(c.num_groups(), 4, "groups: 1.5, NaN, 0.0, 9.0");
        let counts = c.agg_state(0).as_long_slice().unwrap();
        let keys = c.keys();
        let mut total_nan = 0i64;
        let mut total_zero = 0i64;
        for (i, key) in keys.iter().enumerate() {
            let v = key.to_f64();
            if v.is_nan() {
                total_nan += counts[i];
            } else if v == 0.0 {
                total_zero += counts[i];
            }
        }
        assert_eq!(total_nan, 1 + 5, "all NaNs in one group");
        assert_eq!(total_zero, 3 + 4, "-0.0 and +0.0 in one group");
    }
}
