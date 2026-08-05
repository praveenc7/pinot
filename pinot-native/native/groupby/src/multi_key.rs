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

//! Multi-column key packing — the fast path for `GROUP BY a, b, c` over
//! dict-encoded columns (design doc §23 foundation step 2).
//!
//! Each dict-encoded grouping column gives a dense `dict_id` in
//! `[0, cardinality)`, needing `ceil(log2(cardinality))` bits. When the
//! per-column bit widths **sum to ≤ 64**, the whole composite key packs into a
//! single integer, and the entire single-key machinery — dict-direct (small
//! packed range) or a primitive hash table (`i64`) — applies unchanged. This is
//! ClickHouse's `keys64`/`keys128`/`keys256` and DuckDB's perfect-hash packing.
//!
//! When the widths exceed 64 bits (many columns, or a wide/raw component),
//! [`PackedKeyEncoder::new`] returns `None` and the caller falls back to
//! row-encoding the key bytes (a later foundation piece).
//!
//! ## Bit layout
//!
//! Column `i` occupies bits `[shift_i, shift_i + width_i)` of the packed value,
//! with `shift_0 = 0` and `shift_i = Σ_{j<i} width_j` (column 0 in the low
//! bits). Packing/unpacking are exact inverses, so a packed key decodes back to
//! the original per-column dict_ids at the materialization boundary.

/// Bits needed to represent dict_ids in `[0, cardinality)`. Cardinality ≤ 1
/// needs 0 bits (only id 0 exists; the column contributes nothing to the key).
#[inline]
pub fn bits_for(cardinality: i32) -> u32 {
    if cardinality <= 1 {
        0
    } else {
        64 - ((cardinality as u64) - 1).leading_zeros()
    }
}

/// Packs several dict-encoded columns' `dict_id`s into a single `i64` key.
///
/// Build with [`PackedKeyEncoder::new`] from the per-column dict cardinalities;
/// it returns `None` if the composite needs more than 64 bits (caller falls
/// back to row-encoding).
pub struct PackedKeyEncoder {
    widths: Vec<u32>,
    shifts: Vec<u32>,
    total_bits: u32,
}

impl PackedKeyEncoder {
    /// Build an encoder for columns with the given dict cardinalities (in
    /// grouping order). Returns `None` if the packed key would exceed 64 bits.
    pub fn new(cardinalities: &[i32]) -> Option<Self> {
        let mut widths = Vec::with_capacity(cardinalities.len());
        let mut shifts = Vec::with_capacity(cardinalities.len());
        let mut total: u32 = 0;
        for &card in cardinalities {
            let w = bits_for(card);
            shifts.push(total);
            widths.push(w);
            total = total.checked_add(w)?;
            if total > 64 {
                return None;
            }
        }
        Some(Self {
            widths,
            shifts,
            total_bits: total,
        })
    }

    /// Number of grouping columns.
    #[inline]
    pub fn num_columns(&self) -> usize {
        self.widths.len()
    }

    /// Total bits the packed key occupies. The packed value lies in
    /// `[0, 2^total_bits)`, so the caller can choose dict-direct on the packed
    /// key (a dense array, when `total_bits` is small) vs. a hash table.
    #[inline]
    pub fn total_bits(&self) -> u32 {
        self.total_bits
    }

    /// `true` if the packed key fits in 31 bits — i.e. it can ride the `i32`
    /// dict-direct path (the packed value used directly as a slot index).
    #[inline]
    pub fn fits_i32(&self) -> bool {
        self.total_bits <= 31
    }

    /// Pack one row's per-column `dict_ids` (one per column, in grouping order)
    /// into a single key.
    #[inline]
    pub fn pack(&self, dict_ids: &[i32]) -> i64 {
        debug_assert_eq!(dict_ids.len(), self.widths.len());
        let mut packed: u64 = 0;
        for i in 0..self.widths.len() {
            packed |= (dict_ids[i] as u64) << self.shifts[i];
        }
        packed as i64
    }

    /// Unpack a key back into per-column `dict_ids` (for the materialization
    /// boundary — each is then decoded via its column's `Dictionary`).
    #[inline]
    pub fn unpack(&self, packed: i64, out: &mut [i32]) {
        debug_assert_eq!(out.len(), self.widths.len());
        let p = packed as u64;
        for i in 0..self.widths.len() {
            let w = self.widths[i];
            let mask = if w == 64 { u64::MAX } else { (1u64 << w) - 1 };
            out[i] = ((p >> self.shifts[i]) & mask) as i32;
        }
    }

    /// Pack a whole block. `columns[c]` is column `c`'s dict_ids (column-major,
    /// all length `n`); writes one packed key per row into `out[0..n]`.
    pub fn pack_block(&self, columns: &[&[i32]], out: &mut [i64]) {
        debug_assert_eq!(columns.len(), self.widths.len());
        let n = out.len();
        debug_assert!(columns.iter().all(|c| c.len() == n));
        // Column-major outer loop keeps each column's reads sequential.
        for v in out.iter_mut() {
            *v = 0;
        }
        for c in 0..self.widths.len() {
            let shift = self.shifts[c];
            let col = columns[c];
            for i in 0..n {
                out[i] |= (col[i] as u64 as i64) << shift;
            }
        }
    }
}

/// Packs several dict-encoded columns' `dict_id`s into a single key by
/// **mixed-radix** (Horner) encoding — `slot = slot * C_i + d_i` over the
/// columns in grouping order, where `C_i` is column `i`'s dict cardinality.
///
/// This is the [`crate::tier::Tier::RadixDirect`] key encoder. Unlike
/// [`PackedKeyEncoder`] (which rounds each column up to `2^ceil(log2 C_i)`
/// bits), the radix encoding wastes nothing: the packed value ranges over
/// exactly `[0, ∏ C_i)` — the product of cardinalities — so a dense direct
/// slot array sized `∏ C_i` has no power-of-two holes. This is DuckDB's
/// perfect-hash aggregate table / ClickHouse's `FixedHashMap` packing.
///
/// The trade-off vs. [`PackedKeyEncoder`]: packing needs a multiply (not a
/// shift) and unpacking needs mod/div (not shift/mask), so it is slightly
/// slower per row — hence the tier ladder tries the bit-packed encoder first
/// and falls back to radix only when bit-rounding would overflow the budget.
///
/// # Layout
///
/// Column 0 is the most significant: `slot = Σ_i d_i · (∏_{j>i} C_j)`. Packing
/// and unpacking are exact inverses over `[0, ∏ C_i)`.
pub struct RadixKeyEncoder {
    /// Per-column cardinality (the radix), in grouping order. Held as `i64`
    /// so the running Horner product cannot overflow before the budget check.
    radices: Vec<i64>,
    /// `∏ radices` — the total number of slots the packed key ranges over.
    total_slots: i64,
}

impl RadixKeyEncoder {
    /// Build an encoder for columns with the given dict cardinalities (in
    /// grouping order). Returns `None` if the product `∏ C_i` overflows or
    /// exceeds `i32::MAX` — i.e. the packed value could not index the `i32`
    /// dense slot array. (The tier selector only routes here when the product
    /// is within the much smaller direct budget, so this bound is never the
    /// limiting factor in practice; it just keeps `pack` total.)
    pub fn new(cardinalities: &[i32]) -> Option<Self> {
        let mut total: i64 = 1;
        let mut radices = Vec::with_capacity(cardinalities.len());
        for &card in cardinalities {
            // A cardinality of 0 or 1 contributes a single value (radix 1):
            // the column is constant and adds nothing to the key.
            let radix = (card as i64).max(1);
            total = total.checked_mul(radix)?;
            if total > i32::MAX as i64 {
                return None;
            }
            radices.push(radix);
        }
        Some(Self {
            radices,
            total_slots: total,
        })
    }

    /// Number of grouping columns.
    #[inline]
    pub fn num_columns(&self) -> usize {
        self.radices.len()
    }

    /// Total number of slots the packed key ranges over, `∏ C_i`. The packed
    /// value lies in `[0, total_slots)`, so this is the dense slot-array size.
    #[inline]
    pub fn total_slots(&self) -> i64 {
        self.total_slots
    }

    /// Pack one row's per-column `dict_ids` into a single mixed-radix key.
    #[inline]
    pub fn pack(&self, dict_ids: &[i32]) -> i64 {
        debug_assert_eq!(dict_ids.len(), self.radices.len());
        let mut slot: i64 = 0;
        for i in 0..self.radices.len() {
            slot = slot * self.radices[i] + dict_ids[i] as i64;
        }
        slot
    }

    /// Unpack a key back into per-column `dict_ids` (inverse of [`Self::pack`];
    /// least-significant column — the last — recovered first).
    #[inline]
    pub fn unpack(&self, packed: i64, out: &mut [i32]) {
        debug_assert_eq!(out.len(), self.radices.len());
        let mut slot = packed;
        for i in (0..self.radices.len()).rev() {
            let radix = self.radices[i];
            out[i] = (slot % radix) as i32;
            slot /= radix;
        }
    }

    /// Pack a whole block. `columns[c]` is column `c`'s dict_ids (column-major,
    /// all length `n`); writes one packed key per row into `out[0..n]`.
    pub fn pack_block(&self, columns: &[&[i32]], out: &mut [i64]) {
        debug_assert_eq!(columns.len(), self.radices.len());
        let n = out.len();
        debug_assert!(columns.iter().all(|c| c.len() == n));
        for v in out.iter_mut() {
            *v = 0;
        }
        for c in 0..self.radices.len() {
            let radix = self.radices[c];
            let col = columns[c];
            for i in 0..n {
                out[i] = out[i] * radix + col[i] as i64;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agg::AggKind;
    use crate::{GroupByDriver, HashbrownTable};
    use std::collections::HashMap;

    #[test]
    fn bits_for_edge_cases() {
        assert_eq!(bits_for(0), 0);
        assert_eq!(bits_for(1), 0);
        assert_eq!(bits_for(2), 1);
        assert_eq!(bits_for(3), 2);
        assert_eq!(bits_for(256), 8); // max id 255
        assert_eq!(bits_for(257), 9);
        assert_eq!(bits_for(65536), 16);
    }

    #[test]
    fn pack_unpack_roundtrip() {
        let enc = PackedKeyEncoder::new(&[200, 10, 5000]).unwrap();
        assert_eq!(enc.total_bits(), 8 + 4 + 13); // 25
        let mut out = [0i32; 3];
        for &(a, b, c) in &[(0, 0, 0), (199, 9, 4999), (123, 3, 1000), (1, 0, 42)] {
            let packed = enc.pack(&[a, b, c]);
            enc.unpack(packed, &mut out);
            assert_eq!(out, [a, b, c], "roundtrip ({a},{b},{c})");
        }
    }

    #[test]
    fn rejects_keys_wider_than_64_bits() {
        // 3 columns each needing 30 bits = 90 > 64.
        assert!(PackedKeyEncoder::new(&[1 << 29, 1 << 29, 1 << 29]).is_none());
        // Exactly 64 bits is OK.
        assert!(PackedKeyEncoder::new(&[1 << 31, 1 << 31, 4]).is_some());
    }

    #[test]
    fn single_low_card_column_costs_zero_bits() {
        let enc = PackedKeyEncoder::new(&[1, 100]).unwrap();
        // First column (card 1) contributes 0 bits; only the second matters.
        assert_eq!(enc.total_bits(), bits_for(100));
        let mut out = [0i32; 2];
        let packed = enc.pack(&[0, 73]);
        enc.unpack(packed, &mut out);
        assert_eq!(out, [0, 73]);
    }

    #[test]
    fn pack_block_matches_per_row() {
        let enc = PackedKeyEncoder::new(&[300, 50]).unwrap();
        let col0 = [5i32, 12, 5, 299, 0];
        let col1 = [1i32, 49, 1, 0, 7];
        let mut out = [0i64; 5];
        enc.pack_block(&[&col0, &col1], &mut out);
        for i in 0..5 {
            assert_eq!(out[i], enc.pack(&[col0[i], col1[i]]), "row {i}");
        }
    }

    /// End-to-end: a 2-column dict GROUP BY via packed i64 keys through the
    /// generalized driver must match a tuple-keyed HashMap reference.
    #[test]
    fn multi_key_groupby_via_packed_i64_matches_reference() {
        let enc = PackedKeyEncoder::new(&[400, 60]).unwrap();
        let kinds = [AggKind::SumLongToDouble, AggKind::Count];
        let n = 30_000;
        let mut state: u64 = 0x1234_5678_9abc_def0;
        let (mut c0, mut c1, mut vals) = (Vec::new(), Vec::new(), Vec::new());
        let mut reference: HashMap<(i32, i32), (f64, i64)> = HashMap::new();
        for _ in 0..n {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
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
        let mut packed = vec![0i64; n];
        enc.pack_block(&[&c0, &c1], &mut packed);

        let mut driver = GroupByDriver::<i64, HashbrownTable<i64>>::new(&kinds);
        driver.process_block_keys(&packed);
        driver.apply_long(0, &vals);
        driver.apply_count(1);

        let (keys, aggs) = driver.extract();
        assert_eq!(keys.len(), reference.len(), "group count");
        let sums = aggs[0].as_double_slice().unwrap();
        let cnts = aggs[1].as_long_slice().unwrap();
        let mut decoded = [0i32; 2];
        for (g, &packed_key) in keys.iter().enumerate() {
            enc.unpack(packed_key, &mut decoded);
            let r = reference[&(decoded[0], decoded[1])];
            assert_eq!(sums[g], r.0, "sum for ({},{})", decoded[0], decoded[1]);
            assert_eq!(cnts[g], r.1, "cnt for ({},{})", decoded[0], decoded[1]);
        }
    }

    // ---- RadixKeyEncoder (Tier 0 mixed-radix) -------------------------------

    #[test]
    fn radix_known_slots_and_roundtrip() {
        // Worked example from the design: C0=3, C1=5 -> domain 15, no holes.
        let enc = RadixKeyEncoder::new(&[3, 5]).unwrap();
        assert_eq!(enc.total_slots(), 15);
        // slot = d0*5 + d1
        let cases = [((0, 1), 1i64), ((2, 0), 10), ((1, 4), 9), ((0, 3), 3)];
        let mut out = [0i32; 2];
        for &((d0, d1), slot) in &cases {
            assert_eq!(enc.pack(&[d0, d1]), slot, "pack ({d0},{d1})");
            enc.unpack(slot, &mut out);
            assert_eq!(out, [d0, d1], "unpack {slot}");
        }
    }

    #[test]
    fn radix_roundtrip_three_columns_full_domain() {
        // Exhaustively verify every point in a small 3-column domain is a
        // distinct slot in [0, prod) and roundtrips.
        let (c0, c1, c2) = (4, 3, 7);
        let enc = RadixKeyEncoder::new(&[c0, c1, c2]).unwrap();
        assert_eq!(enc.total_slots(), (c0 * c1 * c2) as i64);
        let mut seen = vec![false; (c0 * c1 * c2) as usize];
        let mut out = [0i32; 3];
        for a in 0..c0 {
            for b in 0..c1 {
                for d in 0..c2 {
                    let slot = enc.pack(&[a, b, d]);
                    assert!((0..enc.total_slots()).contains(&slot));
                    assert!(!seen[slot as usize], "collision at ({a},{b},{d})");
                    seen[slot as usize] = true;
                    enc.unpack(slot, &mut out);
                    assert_eq!(out, [a, b, d]);
                }
            }
        }
        assert!(seen.iter().all(|&s| s), "domain fully dense, no holes");
    }

    #[test]
    fn radix_is_denser_than_bitpacked() {
        // For the same columns, ∏C_i (radix) <= 2^Σbits (bit-packed), often far.
        let cards = [3, 5];
        let radix = RadixKeyEncoder::new(&cards).unwrap();
        let packed = PackedKeyEncoder::new(&cards).unwrap();
        assert_eq!(radix.total_slots(), 15);
        assert_eq!(1i64 << packed.total_bits(), 32);
        assert!(radix.total_slots() < (1i64 << packed.total_bits()));
    }

    #[test]
    fn radix_pack_block_matches_per_row() {
        let enc = RadixKeyEncoder::new(&[300, 50]).unwrap();
        let col0 = [5i32, 12, 5, 299, 0];
        let col1 = [1i32, 49, 1, 0, 7];
        let mut out = [0i64; 5];
        enc.pack_block(&[&col0, &col1], &mut out);
        for i in 0..5 {
            assert_eq!(out[i], enc.pack(&[col0[i], col1[i]]), "row {i}");
        }
    }

    #[test]
    fn radix_rejects_product_overflowing_i32() {
        // 46341^2 = 2_147_488_281 > i32::MAX (2_147_483_647) -> None.
        assert!(RadixKeyEncoder::new(&[46341, 46341]).is_none());
        // 40000^2 = 1_600_000_000 < i32::MAX -> Some.
        assert!(RadixKeyEncoder::new(&[40000, 40000]).is_some());
    }

    #[test]
    fn radix_constant_column_is_radix_one() {
        // Cardinality 0 or 1 contributes a single value (radix 1).
        let enc = RadixKeyEncoder::new(&[1, 100]).unwrap();
        assert_eq!(enc.total_slots(), 100);
        let mut out = [0i32; 2];
        let slot = enc.pack(&[0, 73]);
        assert_eq!(slot, 73);
        enc.unpack(slot, &mut out);
        assert_eq!(out, [0, 73]);
    }
}
