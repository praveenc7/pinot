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

//! Segment group-by **key-table tier ladder** — the single, explicit place
//! that decides *how* a dict-encoded `GROUP BY` maps composite keys to
//! `group_id`s. Aggregation state is orthogonal: every tier feeds the same
//! [`crate::GroupByDriver`] + [`crate::AggState`] machinery, so a tier only
//! ever changes the `key → group_id` step.
//!
//! Three tiers, tried in this order (each a distinct, self-contained strategy
//! so the ladder is easy to read and evolve):
//!
//! | Tier | Table | Key | Gate | Lineage |
//! |------|-------|-----|------|---------|
//! | [`Tier::BitPackedDirect`] | dense slot array (no hash) | bit-packed (shift) via [`PackedKeyEncoder`] | `Σ ceil(log2 Cᵢ) ≤ `[`MAX_DIRECT_BITS`] | ClickHouse `keysN` |
//! | [`Tier::RadixDirect`]     | dense slot array (no hash) | mixed-radix (`slot·Cᵢ+dᵢ`) via [`RadixKeyEncoder`] | `∏ Cᵢ ≤ `[`DIRECT_BUDGET_SLOTS`] | DuckDB perfect-HT / CH `FixedHashMap` |
//! | [`Tier::BitPackedHash`]   | hashbrown hash table | bit-packed (i64→i128) | — (fallback) | CH `keys64/128` |
//!
//! ## Why bit-packed is tried *before* radix
//!
//! Radix is always at least as dense as bit-packed (`∏ Cᵢ ≤ 2^Σbits`), so if
//! the tiers shared one budget and radix were checked first, the bit-packed
//! direct tier could never fire. Instead the fastest-indexing direct tier that
//! fits the budget wins: bit-packed uses shift/mask, radix needs multiply +
//! mod/div. Radix therefore serves as the **denser rescue** — it fires exactly
//! when bit-rounding waste pushes `2^Σbits` over budget but the true product
//! `∏ Cᵢ` still fits. Both direct tiers size the same dense slot array
//! (`4 · domain` bytes) and reuse [`crate::DictDirectTable`]; only the key
//! encoder differs.

use crate::multi_key::{bits_for, PackedKeyEncoder, RadixKeyEncoder};

/// Direct-tier slot budget: the dense `group_id` slot array may span at most
/// this many slots (`2^20 = 1_048_576`, a 4 MB `u32` array). Both direct tiers
/// are gated by it — bit-packed as `2^Σbits ≤ B`, radix as `∏ Cᵢ ≤ B`.
pub const DIRECT_BUDGET_SLOTS: u64 = 1 << 20;

/// Bit-packed direct gate: `Σ ceil(log2 Cᵢ) ≤ 20`, equivalent to
/// `2^Σbits ≤ `[`DIRECT_BUDGET_SLOTS`]. Checked first in the ladder.
pub const MAX_DIRECT_BITS: u32 = 20;

/// The chosen key-table strategy for a dict-encoded segment `GROUP BY`. See the
/// module docs for the full ladder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    /// Tier 1 — dense slot array, bit-packed key (no hashing). Fastest index.
    BitPackedDirect,
    /// Tier 0 — dense slot array, mixed-radix key (no hashing). Denser rescue.
    RadixDirect,
    /// Tier 2 — hashbrown hash table, bit-packed key (generic fallback).
    BitPackedHash,
}

impl Tier {
    /// Stable identifier for diagnostics / marker assertions in tests.
    pub const fn name(self) -> &'static str {
        match self {
            Tier::BitPackedDirect => "bit-packed-direct",
            Tier::RadixDirect => "radix-direct",
            Tier::BitPackedHash => "bit-packed-hash",
        }
    }
}

/// Sum of per-column key bit-widths, `Σ ceil(log2 Cᵢ)`. Saturates rather than
/// overflowing for pathologically many columns.
fn sum_key_bits(cardinalities: &[i32]) -> u32 {
    cardinalities
        .iter()
        .map(|&c| bits_for(c))
        .fold(0u32, |acc, w| acc.saturating_add(w))
}

/// Product of cardinalities `∏ Cᵢ` (radix domain), or `None` if it overflows
/// `u64`. A cardinality `≤ 1` counts as `1` (constant column).
fn product_of_cardinalities(cardinalities: &[i32]) -> Option<u64> {
    let mut product: u64 = 1;
    for &c in cardinalities {
        let radix = (c as u64).max(1);
        product = product.checked_mul(radix)?;
    }
    Some(product)
}

/// Choose the key-table tier for a dict-encoded `GROUP BY` from the per-column
/// dictionary cardinalities (in grouping order). This is the single source of
/// truth for the ladder:
///
/// 1. `Σ ceil(log2 Cᵢ) ≤ `[`MAX_DIRECT_BITS`] → [`Tier::BitPackedDirect`]
/// 2. else `∏ Cᵢ ≤ `[`DIRECT_BUDGET_SLOTS`] → [`Tier::RadixDirect`]
/// 3. else → [`Tier::BitPackedHash`]
pub fn select_tier(cardinalities: &[i32]) -> Tier {
    if sum_key_bits(cardinalities) <= MAX_DIRECT_BITS {
        return Tier::BitPackedDirect;
    }
    if let Some(product) = product_of_cardinalities(cardinalities) {
        if product <= DIRECT_BUDGET_SLOTS {
            return Tier::RadixDirect;
        }
    }
    Tier::BitPackedHash
}

/// Build the [`RadixKeyEncoder`] for a [`Tier::RadixDirect`] selection. Kept
/// here so the tier→encoder pairing lives next to the ladder. Returns `None`
/// only if the product exceeds the `i32` slot bound (never, given the gate).
pub fn radix_encoder(cardinalities: &[i32]) -> Option<RadixKeyEncoder> {
    RadixKeyEncoder::new(cardinalities)
}

/// Build the [`PackedKeyEncoder`] for a bit-packed tier ([`Tier::BitPackedDirect`]
/// or [`Tier::BitPackedHash`]). Returns `None` if the packed key exceeds 64 bits
/// (only possible for the hash tier, which then needs the i128 widening).
pub fn packed_encoder(cardinalities: &[i32]) -> Option<PackedKeyEncoder> {
    PackedKeyEncoder::new(cardinalities)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agg::AggKind;
    use crate::{DictDirectTable, GroupByDriver};
    use std::collections::HashMap;

    #[test]
    fn ladder_picks_bitpacked_direct_when_bits_fit() {
        // 3 cols x 10 bits = 30... too many. Use small: 2 cols, 8+8=16 <= 20.
        assert_eq!(select_tier(&[256, 256]), Tier::BitPackedDirect);
        // Single column well within budget.
        assert_eq!(select_tier(&[1000]), Tier::BitPackedDirect);
    }

    #[test]
    fn ladder_picks_radix_when_bits_overflow_but_product_fits() {
        // Cardinalities just above powers of two waste bits: 3 cols of 33.
        // Σbits = 3*6 = 18 <= 20 -> actually BitPackedDirect. Push higher:
        // 3 cols of 129 -> bits_for(129)=8 each, Σ=24 > 20; product=129^3=
        // 2_146_689 > 2^20 -> hash. Need product <= 2^20 with Σbits > 20.
        // 2 cols of 700: bits_for(700)=10 each, Σ=20 <= 20 -> BitPackedDirect.
        // 2 cols of 1100: bits_for(1100)=11 each, Σ=22 > 20; product=1_210_000
        // > 2^20 (1_048_576) -> hash. Tighten: 2 cols of 1024 and 1000:
        // bits=10+10=20 -> direct. Use 1025 & 1000: bits=11+10=21 > 20,
        // product=1_025_000 <= 1_048_576 -> RADIX.
        assert_eq!(sum_key_bits(&[1025, 1000]), 21);
        assert!(product_of_cardinalities(&[1025, 1000]).unwrap() <= DIRECT_BUDGET_SLOTS);
        assert_eq!(select_tier(&[1025, 1000]), Tier::RadixDirect);
    }

    #[test]
    fn ladder_falls_back_to_hash_when_neither_fits() {
        // 3 cols of 10_000: Σbits = 3*14 = 42 > 20; product = 1e12 >> 2^20.
        assert_eq!(select_tier(&[10_000, 10_000, 10_000]), Tier::BitPackedHash);
    }

    /// Table-driven sweep across column counts and boundary values, asserting
    /// the ladder routes each combination to the expected tier and that it lands
    /// there for the documented reason (Σbits vs product). This is the primary
    /// coverage that the T1 -> T0 -> T2 conditional is exercised in every branch.
    #[test]
    fn select_tier_across_combinations() {
        // (cardinalities, expected tier, note)
        let cases: &[(&[i32], Tier)] = &[
            // ---- T1 BitPackedDirect: Σbits <= 20 -------------------------------
            (&[2], Tier::BitPackedDirect),                 // 1 col, 1 bit
            (&[1000], Tier::BitPackedDirect),              // 1 col, 10 bits
            (&[1_048_576], Tier::BitPackedDirect),         // 1 col, exactly 20 bits
            (&[256, 256], Tier::BitPackedDirect),          // 2 cols, 16 bits
            (&[1024, 1024], Tier::BitPackedDirect),        // 2 cols, exactly 20 bits (boundary)
            (&[1, 1, 1_000_000], Tier::BitPackedDirect),   // constant cols + 20 bits
            (&[4, 4, 4, 4, 4], Tier::BitPackedDirect),     // 5 cols, 10 bits
            // ---- T0 RadixDirect: Σbits > 20 but ∏ <= 2^20 ----------------------
            (&[1025, 1000], Tier::RadixDirect),            // 2 cols, 21 bits, ∏=1_025_000
            (&[1030, 1017], Tier::RadixDirect),            // 2 cols, 21 bits, ∏=1_047_510 (near budget)
            (&[101, 101, 101], Tier::RadixDirect),         // 3 cols, 21 bits, ∏=1_030_301
            (&[130, 130, 62], Tier::RadixDirect),          // 3 cols, 22 bits, ∏=1_047_800 (near budget)
            // ---- T2 BitPackedHash: neither gate fits ---------------------------
            (&[1025, 1025], Tier::BitPackedHash),          // 22 bits, ∏=1_050_625 just over 2^20
            (&[2000, 2000], Tier::BitPackedHash),          // 22 bits, ∏=4_000_000
            (&[65536, 65536], Tier::BitPackedHash),        // 32-bit packed key, ∏=2^32
            (&[10_000, 10_000, 10_000], Tier::BitPackedHash), // ∏=1e12
            (&[46341, 46341], Tier::BitPackedHash),        // radix ∏ > i32::MAX but u64 ok -> hash
            (&[1 << 30, 1 << 30, 1 << 30], Tier::BitPackedHash), // product overflows u64 -> hash
        ];
        for &(cards, expected) in cases {
            assert_eq!(select_tier(cards), expected, "cards {cards:?}");
        }
    }

    /// Walk the two adjacent boundaries explicitly: as the key grows by one bit /
    /// one unit of product, the selection must step T1 -> T0 -> T2 in order, and
    /// the private gate helpers must justify each step.
    #[test]
    fn select_tier_steps_t1_to_t0_to_t2_at_boundaries() {
        // T1: exactly at the bit budget. Σbits = 20, ∏ = 2^20.
        assert_eq!(sum_key_bits(&[1024, 1024]), 20);
        assert_eq!(product_of_cardinalities(&[1024, 1024]), Some(DIRECT_BUDGET_SLOTS));
        assert_eq!(select_tier(&[1024, 1024]), Tier::BitPackedDirect);

        // T0: one bit over the bit budget, product still within slot budget.
        assert!(sum_key_bits(&[1025, 1000]) > MAX_DIRECT_BITS);
        assert!(product_of_cardinalities(&[1025, 1000]).unwrap() <= DIRECT_BUDGET_SLOTS);
        assert_eq!(select_tier(&[1025, 1000]), Tier::RadixDirect);

        // T2: over both — product now exceeds the slot budget too.
        assert!(sum_key_bits(&[1025, 1025]) > MAX_DIRECT_BITS);
        assert!(product_of_cardinalities(&[1025, 1025]).unwrap() > DIRECT_BUDGET_SLOTS);
        assert_eq!(select_tier(&[1025, 1025]), Tier::BitPackedHash);
    }

    /// The radix tier must not be dead code: there must exist inputs where the
    /// bit-packed direct gate fails but the radix gate succeeds (the "denser
    /// rescue"). This guards the ordering invariant ∏Cᵢ <= 2^Σbits.
    #[test]
    fn radix_tier_is_reachable_not_dead_code() {
        let cards = [130, 130, 62];
        assert!(sum_key_bits(&cards) > MAX_DIRECT_BITS, "bit-packed direct rejects");
        assert!(
            product_of_cardinalities(&cards).unwrap() <= DIRECT_BUDGET_SLOTS,
            "radix direct accepts"
        );
        assert_eq!(select_tier(&cards), Tier::RadixDirect);
    }

    #[test]
    fn select_tier_empty_and_single_constant_columns() {
        // No columns -> Σbits 0, product 1 -> direct (degenerate).
        assert_eq!(select_tier(&[]), Tier::BitPackedDirect);
        // A single constant column contributes nothing.
        assert_eq!(select_tier(&[1]), Tier::BitPackedDirect);
    }

    /// The headline reason radix exists: when every cardinality sits *just over*
    /// a power of two (`2^k + 1`), `bits_for` rounds each column up to `k+1`
    /// bits — so the bit-packed domain `2^Σbits` explodes (nearly 2x per column)
    /// far past the budget, while the true product `∏Cᵢ` stays right under it.
    /// Bit-packed direct would need a huge sparse slot array (or be rejected to
    /// hash); radix rescues it into a compact dense array.
    #[test]
    fn radix_rescues_bit_packing_explosion_just_over_power_of_two() {
        // 6 columns of 9 (= 2^3 + 1). Each needs 4 bits but only 9 of 16 slots.
        let cards = [9, 9, 9, 9, 9, 9];
        assert_eq!(bits_for(9), 4);
        assert_eq!(sum_key_bits(&cards), 24);

        let bit_packed_domain = 1u64 << sum_key_bits(&cards); // 2^24 = 16_777_216
        let radix_domain = product_of_cardinalities(&cards).unwrap(); // 9^6 = 531_441
        assert_eq!(bit_packed_domain, 16_777_216);
        assert_eq!(radix_domain, 531_441);

        // Bit-packed explodes >16x past budget; radix stays under it.
        assert!(bit_packed_domain > DIRECT_BUDGET_SLOTS, "bit-packed exploded");
        assert!(radix_domain <= DIRECT_BUDGET_SLOTS, "radix fits");
        // The explosion factor is ~31.6x — radix is dramatically denser.
        assert!(bit_packed_domain > 30 * radix_domain);

        assert_eq!(select_tier(&cards), Tier::RadixDirect);

        // A second shape with cardinalities just over 4 (= 2^2 + 1): 7 cols of 5.
        let cards2 = [5, 5, 5, 5, 5, 5, 5];
        assert_eq!(sum_key_bits(&cards2), 21); // 2^21 = 2_097_152 > budget
        assert_eq!(product_of_cardinalities(&cards2).unwrap(), 78_125); // <= budget
        assert_eq!(select_tier(&cards2), Tier::RadixDirect);
    }

    /// The canonical *marginal* rescue: 3 columns of cardinality 100. Each 100
    /// rounds up into a 128-wide (7-bit) column, so Σbits = 21 — just ONE bit
    /// over the 20-bit budget — which doubles the bit-packed slot array to
    /// exactly 2× the budget (2^21 = 2·2^20). The true product 100³ = 1_000_000
    /// still fits under 2^20, so radix rescues it into a compact dense array.
    /// This is the common real case: any non-power-of-two cardinality rounds up,
    /// and enough columns tip Σbits over the budget while the product stays under.
    #[test]
    fn radix_rescues_three_columns_of_100_one_bit_over_budget() {
        let cards = [100, 100, 100];
        assert_eq!(bits_for(100), 7); // 100 in (64, 128] -> 7 bits
        assert_eq!(sum_key_bits(&cards), 21); // one bit over MAX_DIRECT_BITS = 20

        let bit_packed_domain = 1u64 << sum_key_bits(&cards);
        assert_eq!(bit_packed_domain, 2_097_152);
        assert_eq!(bit_packed_domain, 2 * DIRECT_BUDGET_SLOTS); // exactly 2x budget
        assert!(bit_packed_domain > DIRECT_BUDGET_SLOTS); // T1 rejected

        let radix_domain = product_of_cardinalities(&cards).unwrap();
        assert_eq!(radix_domain, 1_000_000);
        assert!(radix_domain <= DIRECT_BUDGET_SLOTS); // radix fits -> T0 rescues

        assert_eq!(select_tier(&cards), Tier::RadixDirect);
    }

    /// Worked examples from the design table — realistic column shapes mapped to
    /// the tier the ladder must pick. Examples 2 and 3 are the key contrast: the
    /// SAME product (1_000_000) lands in *different* tiers purely because of
    /// bit-rounding — 100->128 triples the bit-space (Σbits 21 -> radix rescue),
    /// while 1000->1024 wastes only ~5% (Σbits 20 -> bit-packed fits).
    #[test]
    fn select_tier_worked_examples() {
        struct Case {
            label: &'static str,
            cards: &'static [i32],
            sum_bits: u32,
            product: u64,
            tier: Tier,
        }
        let cases = [
            Case {
                label: "country 200 x browser 50",
                cards: &[200, 50],
                sum_bits: 14,
                product: 10_000,
                tier: Tier::BitPackedDirect,
            },
            Case {
                label: "3 cols of 100 (rounding 100->128 tripled the space)",
                cards: &[100, 100, 100],
                sum_bits: 21,
                product: 1_000_000,
                tier: Tier::RadixDirect,
            },
            Case {
                label: "1000 x 1000 (rounding 1000->1024 wasted ~5%)",
                cards: &[1000, 1000],
                sum_bits: 20,
                product: 1_000_000,
                tier: Tier::BitPackedDirect,
            },
            Case {
                label: "40000 x 60 (22 bits packs into a long)",
                cards: &[40_000, 60],
                sum_bits: 22,
                product: 2_400_000,
                tier: Tier::BitPackedHash,
            },
            Case {
                label: "5 cols of 20 (25 bits packs into a long)",
                cards: &[20, 20, 20, 20, 20],
                sum_bits: 25,
                product: 3_200_000,
                tier: Tier::BitPackedHash,
            },
        ];
        for c in &cases {
            assert_eq!(sum_key_bits(c.cards), c.sum_bits, "Σbits for {}", c.label);
            assert_eq!(
                product_of_cardinalities(c.cards).unwrap(),
                c.product,
                "∏ for {}",
                c.label
            );
            assert_eq!(select_tier(c.cards), c.tier, "tier for {}", c.label);
        }
    }

    /// Contrast that pins the direction: cardinalities *just under* a power of
    /// two (e.g. 15 < 16) waste almost no bits, so radix ≈ bit-packed and there
    /// is nothing to rescue — the product balloons alongside the bit domain and
    /// the ladder falls straight through to hash.
    #[test]
    fn just_under_power_of_two_has_no_explosion_to_rescue() {
        // 6 columns of 15 (= 2^4 - 1). bits_for(15) = 4, same as a full 16.
        let cards = [15, 15, 15, 15, 15, 15];
        assert_eq!(bits_for(15), 4);
        let bit_packed_domain = 1u64 << sum_key_bits(&cards); // 2^24
        let radix_domain = product_of_cardinalities(&cards).unwrap(); // 15^6 = 11_390_625
        // Radix is only ~1.47x denser here (vs ~31x for the 9s) — no rescue,
        // and the product itself already exceeds the budget.
        assert!(bit_packed_domain < 2 * radix_domain);
        assert!(radix_domain > DIRECT_BUDGET_SLOTS);
        assert_eq!(select_tier(&cards), Tier::BitPackedHash);
    }

    #[test]
    fn radix_direct_end_to_end_matches_reference() {
        // Full Tier 0 path: mixed-radix pack -> i32 dense DictDirectTable ->
        // the SAME GroupByDriver/AggState -> unpack. Must match a tuple HashMap.
        let cards = [400i32, 60];
        assert_eq!(select_tier(&cards), Tier::BitPackedDirect); // sanity: bits=9+6=15
        // Force the radix path directly (independent of the ladder choice) to
        // exercise the encoder + dense table + shared agg state end to end.
        let enc = radix_encoder(&cards).unwrap();
        let kinds = [AggKind::SumLongToDouble, AggKind::Count];
        let n = 30_000;
        let mut state: u64 = 0x0f0f_1234_dead_beef;
        let (mut c0, mut c1, mut vals) = (Vec::new(), Vec::new(), Vec::new());
        let mut reference: HashMap<(i32, i32), (f64, i64)> = HashMap::new();
        for _ in 0..n {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let a = (((state >> 33) as i32) % 400).abs();
            let b = (((state >> 17) as i32) % 60).abs();
            let v = ((state >> 5) as i64) % 1_000_000;
            c0.push(a);
            c1.push(b);
            vals.push(v);
            let e = reference.entry((a, b)).or_insert((0.0, 0));
            e.0 += v as f64;
            e.1 += 1;
        }
        // Pack to i64 then narrow to i32 slot indices (product 24_000 << i32).
        let mut packed = vec![0i64; n];
        enc.pack_block(&[&c0, &c1], &mut packed);
        let slots: Vec<i32> = packed.iter().map(|&p| p as i32).collect();

        let mut driver = GroupByDriver::<i32, DictDirectTable>::new(&kinds);
        driver.process_block_keys(&slots);
        driver.apply_long(0, &vals);
        driver.apply_count(1);

        let (keys, aggs) = driver.extract();
        assert_eq!(keys.len(), reference.len(), "group count");
        let sums = aggs[0].as_double_slice().unwrap();
        let cnts = aggs[1].as_long_slice().unwrap();
        let mut decoded = [0i32; 2];
        for (g, &slot) in keys.iter().enumerate() {
            enc.unpack(slot as i64, &mut decoded);
            let r = reference[&(decoded[0], decoded[1])];
            assert_eq!(sums[g], r.0, "sum ({},{})", decoded[0], decoded[1]);
            assert_eq!(cnts[g], r.1, "cnt ({},{})", decoded[0], decoded[1]);
        }
    }
}
