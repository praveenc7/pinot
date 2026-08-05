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

//! Multi-column server combine over **raw values** (design §7.0.2 / §17.9 /
//! §23.1), with two interchangeable, benchmarkable merge strategies:
//!
//! * [`MultiColStrategy::ColumnWise`] — **ColumnWise** (DataFusion
//!   `GroupValuesColumn` style): store each key column separately in typed form;
//!   hash by combining per-column typed hashes; dedup through a
//!   `hashbrown::HashTable<(group_id, hash)>` whose equality compares the
//!   incoming row against a stored group **column-by-column** (typed). No arena,
//!   no row-encoding, no duplicate key storage. Handles any arity and any
//!   type-mix (incl. STRING) in one path.
//! * [`MultiColStrategy::PackedKeys`] — **PackedKeys** (ClickHouse `keysN`
//!   style): when *all* columns are fixed-width and their total type width is
//!   ≤ 128 bits, pack each row's raw values into a single `i64` (≤ 64b) or `i128`
//!   (≤ 128b) key via [`RawKeyPacker`] and merge with the existing primitive
//!   [`crate::combine_parallel`]; unpack the result back to typed columns.
//!   Falls back to ColumnWise when a STRING column is present or the packed key
//!   would exceed 128 bits.
//!
//! At combine the dict_ids are segment-local, so the grouping key is the tuple
//! of raw values materialized at the segment boundary — *not* the segment's
//! packed dict_ids (that is [`crate::PackedKeyEncoder`], which packs dict_ids by
//! **cardinality**; [`RawKeyPacker`] here packs raw values by **type width**).
//!
//! Both strategies **converge to the same typed [`KeyColumn`] result**, so the
//! selection ([`MultiColumnCombineSession::select`]) and extraction are shared;
//! the strategies differ *only* in the cross-segment merge.

use crate::agg::{AggKind, AggState};
use crate::canonical::{CanonicalF32, CanonicalF64};
use crate::combine::SegmentPartial;
use crate::combine_parallel::combine_parallel;
use crate::hash::{hash_bytes, hash_combine, hash_u64};
use crate::HashbrownTable;
use crate::topk::{compute_selection, OrderTerm};
use hashbrown::hash_table::HashTable;
use rayon::prelude::*;
use std::cmp::Ordering;

/// Which multi-column merge strategy to use (selectable / benchmarkable).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MultiColStrategy {
    /// DataFusion-style column-wise typed hashing + equality.
    ColumnWise,
    /// ClickHouse-style pack-into-wide-integer (falls back to ColumnWise when a
    /// STRING column is present or the packed key exceeds 128 bits).
    PackedKeys,
}

/// A combine grouping-key column's raw type (design §23.1 raw-value domain).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum KeyColType {
    /// INT / LONG raw value.
    Long,
    /// FLOAT / DOUBLE raw value (canonicalized: NaN and ±0 collapsed).
    Double,
    /// STRING / BYTES raw value.
    String,
}

/// One grouping-key column's values, indexed by group. STRING columns store the
/// bytes flattened Arrow-style (one buffer + cumulative offsets) to avoid a
/// per-value allocation. **This is the sole key store for ColumnWise** (the
/// `hashbrown` table holds only `(group_id, hash)`), and the result
/// representation both strategies converge to.
#[derive(Clone)]
pub enum KeyColumn {
    Long(Vec<i64>),
    Double(Vec<CanonicalF64>),
    Str { buffer: Vec<u8>, offsets: Vec<i32> },
}

impl KeyColumn {
    /// An empty column builder of the given type.
    fn empty(col_type: KeyColType) -> Self {
        match col_type {
            KeyColType::Long => KeyColumn::Long(Vec::new()),
            KeyColType::Double => KeyColumn::Double(Vec::new()),
            KeyColType::String => KeyColumn::Str { buffer: Vec::new(), offsets: vec![0] },
        }
    }

    /// Build a LONG column from raw i64 values.
    pub fn from_long(values: Vec<i64>) -> Self {
        KeyColumn::Long(values)
    }

    /// Build a DOUBLE column from raw f64 values (canonicalized on the way in).
    pub fn from_double(values: Vec<f64>) -> Self {
        KeyColumn::Double(values.into_iter().map(CanonicalF64::new).collect())
    }

    /// Build a STRING column from a flattened byte buffer + cumulative offsets
    /// (length `num_groups + 1`).
    pub fn from_string(buffer: Vec<u8>, offsets: Vec<i32>) -> Self {
        KeyColumn::Str { buffer, offsets }
    }

    /// Number of values in the column.
    pub fn len(&self) -> usize {
        match self {
            KeyColumn::Long(v) => v.len(),
            KeyColumn::Double(v) => v.len(),
            KeyColumn::Str { offsets, .. } => offsets.len().saturating_sub(1),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Append `src`'s value at `idx` onto this (same-typed) column builder.
    fn push_from(&mut self, src: &KeyColumn, idx: usize) {
        match (self, src) {
            (KeyColumn::Long(dst), KeyColumn::Long(s)) => dst.push(s[idx]),
            (KeyColumn::Double(dst), KeyColumn::Double(s)) => dst.push(s[idx]),
            (KeyColumn::Str { buffer, offsets }, KeyColumn::Str { buffer: sb, offsets: so }) => {
                let bytes = &sb[so[idx] as usize..so[idx + 1] as usize];
                buffer.extend_from_slice(bytes);
                offsets.push(buffer.len() as i32);
            }
            _ => panic!("KeyColumn type mismatch in push_from"),
        }
    }

    /// Typed hash of the value at `idx` (fixed-width numerics via `hash_u64`;
    /// STRING via `hash_bytes`). Folded across columns via [`hash_combine`].
    #[inline]
    fn hash_at(&self, idx: usize) -> u64 {
        match self {
            KeyColumn::Long(v) => hash_u64(v[idx] as u64),
            KeyColumn::Double(v) => hash_u64(v[idx].to_f64().to_bits()),
            KeyColumn::Str { buffer, offsets } => {
                hash_bytes(&buffer[offsets[idx] as usize..offsets[idx + 1] as usize])
            }
        }
    }

    /// Typed equality of *this* column's value at `my_idx` against `src`'s value
    /// at `src_idx` (the incoming partial). No decode-from-bytes.
    #[inline]
    fn eq_across(&self, my_idx: usize, src: &KeyColumn, src_idx: usize) -> bool {
        match (self, src) {
            (KeyColumn::Long(a), KeyColumn::Long(b)) => a[my_idx] == b[src_idx],
            (KeyColumn::Double(a), KeyColumn::Double(b)) => a[my_idx] == b[src_idx],
            (KeyColumn::Str { buffer: ab, offsets: ao }, KeyColumn::Str { buffer: bb, offsets: bo }) => {
                ab[ao[my_idx] as usize..ao[my_idx + 1] as usize]
                    == bb[bo[src_idx] as usize..bo[src_idx + 1] as usize]
            }
            _ => panic!("KeyColumn type mismatch in eq_across"),
        }
    }

    /// Typed comparison of two groups by this column's natural order (numeric for
    /// Long/Double via `total_cmp`, lexical for String).
    fn cmp_at(&self, a: usize, b: usize) -> Ordering {
        match self {
            KeyColumn::Long(v) => v[a].cmp(&v[b]),
            KeyColumn::Double(v) => v[a].to_f64().total_cmp(&v[b].to_f64()),
            KeyColumn::Str { buffer, offsets } => {
                let ka = &buffer[offsets[a] as usize..offsets[a + 1] as usize];
                let kb = &buffer[offsets[b] as usize..offsets[b + 1] as usize];
                ka.cmp(kb)
            }
        }
    }

    /// Return a new column with groups reordered by `perm` (`out[k] = self[perm[k]]`).
    fn gather(&self, perm: &[usize]) -> KeyColumn {
        match self {
            KeyColumn::Long(v) => KeyColumn::Long(perm.iter().map(|&i| v[i]).collect()),
            KeyColumn::Double(v) => KeyColumn::Double(perm.iter().map(|&i| v[i]).collect()),
            KeyColumn::Str { buffer, offsets } => {
                let mut new_buffer = Vec::new();
                let mut new_offsets = vec![0i32];
                for &i in perm {
                    new_buffer.extend_from_slice(&buffer[offsets[i] as usize..offsets[i + 1] as usize]);
                    new_offsets.push(new_buffer.len() as i32);
                }
                KeyColumn::Str { buffer: new_buffer, offsets: new_offsets }
            }
        }
    }

    /// Combined LONG values (extraction). Empty for non-LONG columns.
    pub fn as_long(&self) -> &[i64] {
        match self {
            KeyColumn::Long(v) => v,
            _ => &[],
        }
    }

    /// Combined DOUBLE value at `g` (extraction). NaN for non-DOUBLE columns.
    pub fn double_at(&self, g: usize) -> f64 {
        match self {
            KeyColumn::Double(v) => v[g].to_f64(),
            _ => f64::NAN,
        }
    }

    /// Combined STRING buffer + offsets (extraction). Empty for non-STRING columns.
    pub fn string_parts(&self) -> (&[u8], &[i32]) {
        match self {
            KeyColumn::Str { buffer, offsets } => (buffer, offsets),
            _ => (&[], &[]),
        }
    }
}

/// Combined row hash: fold each column's typed hash at `idx` into one composite.
/// The fold is order-dependent, so `(a, b)` and `(b, a)` composite keys hash
/// differently — matching their distinct grouping.
#[inline]
fn row_hash(columns: &[KeyColumn], idx: usize) -> u64 {
    hash_combine(columns.iter().map(|col| col.hash_at(idx)))
}

/// True if the incoming row `src[*][src_idx]` equals stored group `g` column-wise.
#[inline]
fn columns_eq(stored: &[KeyColumn], g: usize, src: &[KeyColumn], src_idx: usize) -> bool {
    stored.iter().enumerate().all(|(c, col)| col.eq_across(g, &src[c], src_idx))
}

/// One segment's multi-column partial: N raw key columns + typed agg
/// accumulators, all length `num_groups`.
struct MultiColumnPartial {
    columns: Vec<KeyColumn>,
    aggs: Vec<AggState>,
}

impl MultiColumnPartial {
    fn num_groups(&self) -> usize {
        self.columns.first().map_or(0, KeyColumn::len)
    }
}

// --------------------------------------------------------------------------
// ColumnWise (design B) — per-partition driver
// --------------------------------------------------------------------------

/// Per-partition ColumnWise merge driver: a `hashbrown` table holding only
/// `(group_id, hash)`, with the key values living in the typed [`KeyColumn`]s
/// (DataFusion `GroupValuesColumn` pattern).
struct ColumnWiseDriver {
    map: HashTable<(u32, u64)>,
    columns: Vec<KeyColumn>,
    aggs: Vec<AggState>,
}

impl ColumnWiseDriver {
    fn new(col_types: &[KeyColType], agg_kinds: &[AggKind]) -> Self {
        Self {
            map: HashTable::new(),
            columns: col_types.iter().map(|&t| KeyColumn::empty(t)).collect(),
            aggs: agg_kinds.iter().map(|&k| AggState::new_for(k, 0)).collect(),
        }
    }

    fn num_groups(&self) -> usize {
        self.columns.first().map_or(0, KeyColumn::len)
    }

    /// Merge source group `src_idx` (row hash precomputed in phase 1).
    fn merge_one(&mut self, hash: u64, src_columns: &[KeyColumn], src_aggs: &[AggState], src_idx: usize) {
        // Disjoint field borrows: the closure captures `self.columns` (shared)
        // while `self.map.entry` borrows `self.map` (mut) — different fields.
        let columns = &self.columns;
        let entry = self.map.entry(
            hash,
            |&(g, h)| h == hash && columns_eq(columns, g as usize, src_columns, src_idx),
            |&(_, h)| h,
        );
        let gid = match entry {
            hashbrown::hash_table::Entry::Occupied(o) => o.get().0 as usize,
            hashbrown::hash_table::Entry::Vacant(v) => {
                let g = self.columns.first().map_or(0, KeyColumn::len);
                for (c, col) in self.columns.iter_mut().enumerate() {
                    col.push_from(&src_columns[c], src_idx);
                }
                for a in self.aggs.iter_mut() {
                    a.push_new_group();
                }
                v.insert((g as u32, hash));
                g
            }
        };
        for (i, a) in self.aggs.iter_mut().enumerate() {
            a.merge_slot(gid, &src_aggs[i], src_idx);
        }
    }
}

// --------------------------------------------------------------------------
// PackedKeys (design C) — raw-value type-width packer
// --------------------------------------------------------------------------

/// Packs a row's fixed-width raw key values into a single wide integer key by
/// **type width** (INT/FLOAT → 32 bits, LONG/DOUBLE → 64 bits), ClickHouse
/// `keysN` style. Returns `None` from [`Self::new`] when a STRING column is
/// present or the total width exceeds 128 bits (caller falls back to ColumnWise).
///
/// Distinct from [`crate::PackedKeyEncoder`], which packs segment dict_ids by
/// **cardinality**; raw combine values are not dense, so packing is by type
/// width and generally needs the full 32/64 bits per column.
pub struct RawKeyPacker {
    kinds: Vec<KeyColType>,
    widths: Vec<u32>,
    shifts: Vec<u32>,
    total_bits: u32,
}

impl RawKeyPacker {
    /// Build a packer for the given column types + per-column type widths (bits;
    /// 32 or 64 for fixed-width, ignored for STRING). `None` if any column is
    /// STRING or the total width would exceed 128 bits.
    pub fn new(col_types: &[KeyColType], col_widths: &[u32]) -> Option<Self> {
        let mut widths = Vec::with_capacity(col_types.len());
        let mut shifts = Vec::with_capacity(col_types.len());
        let mut total: u32 = 0;
        for (c, &t) in col_types.iter().enumerate() {
            if t == KeyColType::String {
                return None;
            }
            let w = col_widths[c];
            debug_assert!(w == 32 || w == 64, "unexpected fixed-width bits: {w}");
            shifts.push(total);
            widths.push(w);
            total = total.checked_add(w)?;
            if total > 128 {
                return None;
            }
        }
        Some(Self { kinds: col_types.to_vec(), widths, shifts, total_bits: total })
    }

    /// Whether the packed key fits in an `i64` (≤ 64 bits); else it needs `i128`.
    #[inline]
    pub fn fits_i64(&self) -> bool {
        self.total_bits <= 64
    }

    #[inline]
    pub fn total_bits(&self) -> u32 {
        self.total_bits
    }

    /// Raw bits (masked to the column's width) of column `c`'s value at `idx`.
    #[inline]
    fn col_bits(&self, columns: &[KeyColumn], c: usize, idx: usize) -> u64 {
        match (self.kinds[c], self.widths[c], &columns[c]) {
            (KeyColType::Long, 32, KeyColumn::Long(v)) => (v[idx] as u32) as u64,
            (KeyColType::Long, _, KeyColumn::Long(v)) => v[idx] as u64,
            (KeyColType::Double, 32, KeyColumn::Double(v)) => {
                CanonicalF32::new(v[idx].to_f64() as f32).to_f32().to_bits() as u64
            }
            (KeyColType::Double, _, KeyColumn::Double(v)) => v[idx].to_f64().to_bits(),
            _ => panic!("RawKeyPacker column/type mismatch"),
        }
    }

    /// Pack row `idx` into an `i64` (requires [`Self::fits_i64`]).
    fn pack_i64(&self, columns: &[KeyColumn], idx: usize) -> i64 {
        let mut acc: u64 = 0;
        for c in 0..self.widths.len() {
            acc |= self.col_bits(columns, c, idx) << self.shifts[c];
        }
        acc as i64
    }

    /// Pack row `idx` into an `i128`.
    fn pack_i128(&self, columns: &[KeyColumn], idx: usize) -> i128 {
        let mut acc: u128 = 0;
        for c in 0..self.widths.len() {
            acc |= (self.col_bits(columns, c, idx) as u128) << self.shifts[c];
        }
        acc as i128
    }

    /// Reconstruct one column's value from `bits` and append it to `out[c]`.
    #[inline]
    fn push_unpacked(&self, c: usize, bits: u64, out: &mut [KeyColumn]) {
        match (self.kinds[c], self.widths[c], &mut out[c]) {
            (KeyColType::Long, 32, KeyColumn::Long(v)) => v.push((bits as u32) as i32 as i64),
            (KeyColType::Long, _, KeyColumn::Long(v)) => v.push(bits as i64),
            (KeyColType::Double, 32, KeyColumn::Double(v)) => {
                v.push(CanonicalF64::new(f32::from_bits(bits as u32) as f64))
            }
            (KeyColType::Double, _, KeyColumn::Double(v)) => {
                v.push(CanonicalF64::new(f64::from_bits(bits)))
            }
            _ => panic!("RawKeyPacker unpack column/type mismatch"),
        }
    }

    #[inline]
    fn mask(width: u32) -> u64 {
        if width >= 64 {
            u64::MAX
        } else {
            (1u64 << width) - 1
        }
    }

    /// Unpack an `i64` key into the typed column builders `out`.
    fn unpack_i64_into(&self, packed: i64, out: &mut [KeyColumn]) {
        let p = packed as u64;
        for c in 0..self.widths.len() {
            let bits = (p >> self.shifts[c]) & Self::mask(self.widths[c]);
            self.push_unpacked(c, bits, out);
        }
    }

    /// Unpack an `i128` key into the typed column builders `out`.
    fn unpack_i128_into(&self, packed: i128, out: &mut [KeyColumn]) {
        let p = packed as u128;
        for c in 0..self.widths.len() {
            let bits = ((p >> self.shifts[c]) & (Self::mask(self.widths[c]) as u128)) as u64;
            self.push_unpacked(c, bits, out);
        }
    }
}

/// Radix partition of a row hash: top `radix_bits` bits (0 → one partition).
#[inline]
fn partition(hash: u64, radix_bits: u32) -> usize {
    if radix_bits == 0 {
        0
    } else {
        (hash >> (64 - radix_bits)) as usize
    }
}

/// JNI-facing orchestration for the multi-column combine — the
/// begin/set-key/set-agg/commit/finish/select/extract state machine over N
/// raw-value key columns, dispatching to [`MultiColStrategy`] at `finish`.
pub struct MultiColumnCombineSession {
    agg_kinds: Vec<AggKind>,
    col_types: Vec<KeyColType>,
    col_widths: Vec<u32>,
    strategy: MultiColStrategy,
    partials: Vec<MultiColumnPartial>,
    staging_columns: Vec<Option<KeyColumn>>,
    staging_aggs: Vec<Option<AggState>>,
    result: Option<(Vec<KeyColumn>, Vec<AggState>)>,
}

impl MultiColumnCombineSession {
    /// `col_widths` is the per-column type width in bits (32 for INT/FLOAT, 64
    /// for LONG/DOUBLE; ignored for STRING). Only used by [`MultiColStrategy::PackedKeys`].
    pub fn new(
        agg_kinds: &[AggKind],
        col_types: &[KeyColType],
        col_widths: &[u32],
        strategy: MultiColStrategy,
    ) -> Self {
        Self {
            agg_kinds: agg_kinds.to_vec(),
            col_types: col_types.to_vec(),
            col_widths: col_widths.to_vec(),
            strategy,
            partials: Vec::new(),
            staging_columns: (0..col_types.len()).map(|_| None).collect(),
            staging_aggs: (0..agg_kinds.len()).map(|_| None).collect(),
            result: None,
        }
    }

    #[inline]
    pub fn num_key_columns(&self) -> usize {
        self.col_types.len()
    }

    #[inline]
    pub fn num_aggs(&self) -> usize {
        self.agg_kinds.len()
    }

    /// Begin a new segment partial; clears the per-column / per-agg staging slots.
    pub fn begin_partial(&mut self) {
        self.staging_columns.fill_with(|| None);
        self.staging_aggs.fill_with(|| None);
    }

    pub fn set_key_long(&mut self, col_idx: usize, values: Vec<i64>) {
        assert_eq!(self.col_types[col_idx], KeyColType::Long, "key column {col_idx} is not LONG");
        self.staging_columns[col_idx] = Some(KeyColumn::from_long(values));
    }

    pub fn set_key_double(&mut self, col_idx: usize, values: Vec<f64>) {
        assert_eq!(self.col_types[col_idx], KeyColType::Double, "key column {col_idx} is not DOUBLE");
        self.staging_columns[col_idx] = Some(KeyColumn::from_double(values));
    }

    pub fn set_key_string(&mut self, col_idx: usize, buffer: Vec<u8>, offsets: Vec<i32>) {
        assert_eq!(self.col_types[col_idx], KeyColType::String, "key column {col_idx} is not STRING");
        self.staging_columns[col_idx] = Some(KeyColumn::from_string(buffer, offsets));
    }

    pub fn set_agg_long(&mut self, agg_idx: usize, v: Vec<i64>) {
        self.staging_aggs[agg_idx] = Some(AggState::from_long_vec(self.agg_kinds[agg_idx], v));
    }

    pub fn set_agg_int(&mut self, agg_idx: usize, v: Vec<i32>) {
        self.staging_aggs[agg_idx] = Some(AggState::from_int_vec(self.agg_kinds[agg_idx], v));
    }

    pub fn set_agg_double(&mut self, agg_idx: usize, v: Vec<f64>) {
        self.staging_aggs[agg_idx] = Some(AggState::from_double_vec(self.agg_kinds[agg_idx], v));
    }

    pub fn set_agg_float(&mut self, agg_idx: usize, v: Vec<f32>) {
        self.staging_aggs[agg_idx] = Some(AggState::from_float_vec(self.agg_kinds[agg_idx], v));
    }

    /// Finalize the staged partial. Panics if any key column or aggregation is
    /// unset, or if the columns/aggs have inconsistent group counts.
    pub fn commit_partial(&mut self) {
        let mut columns = Vec::with_capacity(self.staging_columns.len());
        for (c, slot) in self.staging_columns.iter_mut().enumerate() {
            columns.push(slot.take().unwrap_or_else(|| panic!("key column {c} not set before commit_partial")));
        }
        let mut aggs = Vec::with_capacity(self.staging_aggs.len());
        for (i, slot) in self.staging_aggs.iter_mut().enumerate() {
            aggs.push(slot.take().unwrap_or_else(|| panic!("aggregation {i} not set before commit_partial")));
        }
        let num_groups = columns.first().map_or(0, KeyColumn::len);
        assert!(columns.iter().all(|c| c.len() == num_groups), "inconsistent key column lengths");
        assert!(aggs.iter().all(|a| a.len() == num_groups), "agg length != key length");
        self.partials.push(MultiColumnPartial { columns, aggs });
    }

    /// Run the cross-segment merge, dispatching on [`MultiColStrategy`].
    /// `PackedKeys` falls back to `ColumnWise` when the key isn't packable.
    pub fn finish(&mut self, radix_bits: u32) {
        let packer = match self.strategy {
            MultiColStrategy::PackedKeys => RawKeyPacker::new(&self.col_types, &self.col_widths),
            MultiColStrategy::ColumnWise => None,
        };
        match packer {
            Some(p) => self.finish_packed(&p, radix_bits),
            None => self.finish_columnwise(radix_bits),
        }
    }

    /// ColumnWise (design B) — radix-partitioned parallel merge with per-column
    /// typed hashing + equality.
    fn finish_columnwise(&mut self, radix_bits: u32) {
        let partials = &self.partials;
        let col_types = &self.col_types;
        let agg_kinds = &self.agg_kinds;
        let num_partitions = 1usize << radix_bits;

        // Phase 1: partition (partial, group) by the composite row hash; carry
        // the hash so phase 2 never re-hashes.
        let mut buckets: Vec<Vec<(u32, u32, u64)>> = vec![Vec::new(); num_partitions];
        for (pi, p) in partials.iter().enumerate() {
            for g in 0..p.num_groups() {
                let h = row_hash(&p.columns, g);
                buckets[partition(h, radix_bits)].push((pi as u32, g as u32, h));
            }
        }

        // Phase 2: merge each partition independently (disjoint by hash, lock-free).
        let drivers: Vec<ColumnWiseDriver> = buckets
            .into_par_iter()
            .map(|bucket| {
                let mut driver = ColumnWiseDriver::new(col_types, agg_kinds);
                for (pi, g, h) in bucket {
                    let p = &partials[pi as usize];
                    driver.merge_one(h, &p.columns, &p.aggs, g as usize);
                }
                driver
            })
            .collect();

        self.result = Some(concat_drivers(col_types, agg_kinds, drivers));
    }

    /// PackedKeys (design C) — pack each partial into i64/i128, reuse the
    /// primitive `combine_parallel`, then unpack the merged keys to typed columns.
    fn finish_packed(&mut self, packer: &RawKeyPacker, radix_bits: u32) {
        let col_types = &self.col_types;
        let taken = std::mem::take(&mut self.partials);
        let (columns, aggs) = if packer.fits_i64() {
            let seg_partials: Vec<SegmentPartial<i64>> = taken
                .into_iter()
                .map(|p| {
                    let keys: Vec<i64> = (0..p.num_groups()).map(|g| packer.pack_i64(&p.columns, g)).collect();
                    SegmentPartial::new(keys, p.aggs)
                })
                .collect();
            let (packed_keys, aggs) = combine_parallel::<i64, HashbrownTable<i64>>(&seg_partials, &self.agg_kinds, radix_bits);
            let mut columns: Vec<KeyColumn> = col_types.iter().map(|&t| KeyColumn::empty(t)).collect();
            for &k in &packed_keys {
                packer.unpack_i64_into(k, &mut columns);
            }
            (columns, aggs)
        } else {
            let seg_partials: Vec<SegmentPartial<i128>> = taken
                .into_iter()
                .map(|p| {
                    let keys: Vec<i128> = (0..p.num_groups()).map(|g| packer.pack_i128(&p.columns, g)).collect();
                    SegmentPartial::new(keys, p.aggs)
                })
                .collect();
            let (packed_keys, aggs) =
                combine_parallel::<i128, HashbrownTable<i128>>(&seg_partials, &self.agg_kinds, radix_bits);
            let mut columns: Vec<KeyColumn> = col_types.iter().map(|&t| KeyColumn::empty(t)).collect();
            for &k in &packed_keys {
                packer.unpack_i128_into(k, &mut columns);
            }
            (columns, aggs)
        };
        self.result = Some((columns, aggs));
    }

    /// Combined group count (after `finish`).
    pub fn result_num_groups(&self) -> usize {
        self.result.as_ref().map_or(0, |(cols, _)| cols.first().map_or(0, KeyColumn::len))
    }

    /// Combined key column `col_idx` (after `finish`).
    pub fn result_key_column(&self, col_idx: usize) -> &KeyColumn {
        &self.result.as_ref().expect("finish not called").0[col_idx]
    }

    /// Combined accumulator for aggregation `agg_idx` (after `finish`).
    pub fn result_agg(&self, agg_idx: usize) -> &AggState {
        &self.result.as_ref().expect("finish not called").1[agg_idx]
    }

    /// Apply ORDER BY top-K / no-ORDER-BY cap to the combined result in place
    /// (after `finish`): reduce to `result_size` groups per `order` (§26.3).
    /// `OrderRef::Key(c)` orders by key column `c` with its typed order; the
    /// tie-break compares all key columns in grouping order then group index.
    /// Shared by both strategies (they converge to typed columns).
    pub fn select(&mut self, order: &[OrderTerm], result_size: usize) {
        if let Some((columns, aggs)) = self.result.as_mut() {
            let n = columns.first().map_or(0, KeyColumn::len);
            let cols: &[KeyColumn] = columns;
            let perm =
                compute_selection(n, cols.len(), |c, a, b| cols[c].cmp_at(a, b), aggs, order, result_size);
            let new_columns: Vec<KeyColumn> = columns.iter().map(|col| col.gather(&perm)).collect();
            *columns = new_columns;
            for a in aggs.iter_mut() {
                *a = a.gather(&perm);
            }
        }
    }
}

/// Concatenate the disjoint per-partition ColumnWise drivers into one flat
/// (typed columns, aggs) result.
fn concat_drivers(
    col_types: &[KeyColType],
    agg_kinds: &[AggKind],
    drivers: Vec<ColumnWiseDriver>,
) -> (Vec<KeyColumn>, Vec<AggState>) {
    let mut columns: Vec<KeyColumn> = col_types.iter().map(|&t| KeyColumn::empty(t)).collect();
    let mut aggs: Vec<AggState> = agg_kinds.iter().map(|&k| AggState::new_for(k, 0)).collect();
    for driver in drivers {
        let n = driver.num_groups();
        for (c, col) in columns.iter_mut().enumerate() {
            for g in 0..n {
                col.push_from(&driver.columns[c], g);
            }
        }
        let mut part_aggs = driver.aggs;
        for (i, a) in part_aggs.iter_mut().enumerate() {
            aggs[i].append(a);
        }
    }
    (columns, aggs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topk::OrderRef;
    use std::collections::HashMap;

    fn kinds() -> Vec<AggKind> {
        vec![AggKind::SumDouble, AggKind::Count]
    }

    const BOTH: [MultiColStrategy; 2] = [MultiColStrategy::ColumnWise, MultiColStrategy::PackedKeys];

    /// (LONG, STRING) composite over two segments vs a tuple reference. STRING is
    /// present, so PackedKeys must fall back to ColumnWise and match.
    #[test]
    fn merge_two_segments_long_string_key() {
        for strat in BOTH {
            let col_types = [KeyColType::Long, KeyColType::String];
            let widths = [64, 0];
            let mut s = MultiColumnCombineSession::new(&kinds(), &col_types, &widths, strat);

            s.begin_partial();
            s.set_key_long(0, vec![10, 20]);
            s.set_key_string(1, b"xy".to_vec(), vec![0, 1, 2]);
            s.set_agg_double(0, vec![1.5, 2.5]);
            s.set_agg_long(1, vec![3, 1]);
            s.commit_partial();

            s.begin_partial();
            s.set_key_long(0, vec![20, 30]);
            s.set_key_string(1, b"yz".to_vec(), vec![0, 1, 2]);
            s.set_agg_double(0, vec![4.0, 9.0]);
            s.set_agg_long(1, vec![2, 5]);
            s.commit_partial();

            s.finish(4);
            assert_eq!(s.result_num_groups(), 3, "{strat:?}");

            let mut got: HashMap<(i64, String), (f64, i64)> = HashMap::new();
            let longs = s.result_key_column(0).as_long().to_vec();
            let (buf, offs) = s.result_key_column(1).string_parts();
            let sums = s.result_agg(0).as_double_slice().unwrap().to_vec();
            let counts = s.result_agg(1).as_long_slice().unwrap().to_vec();
            for g in 0..3 {
                let str_key = String::from_utf8(buf[offs[g] as usize..offs[g + 1] as usize].to_vec()).unwrap();
                got.insert((longs[g], str_key), (sums[g], counts[g]));
            }
            let mut expected: HashMap<(i64, String), (f64, i64)> = HashMap::new();
            expected.insert((10, "x".into()), (1.5, 3));
            expected.insert((20, "y".into()), (6.5, 3));
            expected.insert((30, "z".into()), (9.0, 5));
            assert_eq!(got, expected, "{strat:?}");
        }
    }

    /// Random two-LONG merge (128-bit packed key for PackedKeys) vs tuple
    /// reference — both strategies must agree exactly.
    #[test]
    fn matches_tuple_reference_two_long_columns() {
        for strat in BOTH {
            let col_types = [KeyColType::Long, KeyColType::Long];
            let widths = [64, 64]; // 128-bit packed key → i128 path
            let mut s = MultiColumnCombineSession::new(&kinds(), &col_types, &widths, strat);
            let mut reference: HashMap<(i64, i64), (f64, i64)> = HashMap::new();
            let mut state: u64 = 0xDEADBEEF;
            for _ in 0..20 {
                let n = 200;
                let (mut c0, mut c1, mut sums) = (Vec::new(), Vec::new(), Vec::new());
                for _ in 0..n {
                    state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let a = ((state >> 33) as i64) % 25;
                    let b = ((state >> 17) as i64) % 15;
                    let v = (((state >> 5) as i64) % 1000) as f64;
                    c0.push(a);
                    c1.push(b);
                    sums.push(v);
                    let e = reference.entry((a, b)).or_insert((0.0, 0));
                    e.0 += v;
                    e.1 += 1;
                }
                s.begin_partial();
                s.set_key_long(0, c0);
                s.set_key_long(1, c1);
                s.set_agg_double(0, sums);
                s.set_agg_long(1, vec![1i64; n]);
                s.commit_partial();
            }
            s.finish(6);
            assert_eq!(s.result_num_groups(), reference.len(), "{strat:?}");
            let c0 = s.result_key_column(0).as_long().to_vec();
            let c1 = s.result_key_column(1).as_long().to_vec();
            let sums = s.result_agg(0).as_double_slice().unwrap().to_vec();
            let counts = s.result_agg(1).as_long_slice().unwrap().to_vec();
            for g in 0..c0.len() {
                let r = reference[&(c0[g], c1[g])];
                assert_eq!(sums[g], r.0, "{strat:?} sum ({},{})", c0[g], c1[g]);
                assert_eq!(counts[g], r.1, "{strat:?} count ({},{})", c0[g], c1[g]);
            }
        }
    }

    /// Two INT columns pack into a single 64-bit key (PackedKeys i64 path),
    /// including negatives → exercises the sign-extend on unpack.
    #[test]
    fn packed_i64_two_int_columns() {
        let col_types = [KeyColType::Long, KeyColType::Long];
        let widths = [32, 32]; // 64-bit packed key → i64 path
        let mut s = MultiColumnCombineSession::new(&kinds(), &col_types, &widths, MultiColStrategy::PackedKeys);
        let mut reference: HashMap<(i64, i64), (f64, i64)> = HashMap::new();
        for seg in 0..5i64 {
            let (mut c0, mut c1, mut sums) = (Vec::new(), Vec::new(), Vec::new());
            for r in 0..300i64 {
                let a = ((r + seg) % 30) - 15; // negatives included
                let b = (r * 7 + seg) % 20;
                c0.push(a);
                c1.push(b);
                sums.push((r % 100) as f64);
                let e = reference.entry((a, b)).or_insert((0.0, 0));
                e.0 += (r % 100) as f64;
                e.1 += 1;
            }
            let len = c1.len();
            s.begin_partial();
            s.set_key_long(0, c0);
            s.set_key_long(1, c1);
            s.set_agg_double(0, sums);
            s.set_agg_long(1, vec![1i64; len]);
            s.commit_partial();
        }
        s.finish(5);
        assert_eq!(s.result_num_groups(), reference.len());
        let c0 = s.result_key_column(0).as_long().to_vec();
        let c1 = s.result_key_column(1).as_long().to_vec();
        let sums = s.result_agg(0).as_double_slice().unwrap().to_vec();
        for g in 0..c0.len() {
            assert_eq!(sums[g], reference[&(c0[g], c1[g])].0, "sum ({},{})", c0[g], c1[g]);
        }
    }

    /// ORDER BY a specific key column ascending → exact top-K (both strategies).
    #[test]
    fn select_order_by_key_column() {
        for strat in BOTH {
            let col_types = [KeyColType::Long, KeyColType::Long];
            let widths = [64, 64];
            let mut s = MultiColumnCombineSession::new(&kinds(), &col_types, &widths, strat);
            s.begin_partial();
            s.set_key_long(0, vec![1, 1, 2, 2]);
            s.set_key_long(1, vec![30, 10, 20, 5]);
            s.set_agg_double(0, vec![0.0, 0.0, 0.0, 0.0]);
            s.set_agg_long(1, vec![1, 1, 1, 1]);
            s.commit_partial();
            s.finish(2);
            s.select(&[OrderTerm::new(OrderRef::Key(1), true)], 2);
            assert_eq!(s.result_num_groups(), 2, "{strat:?}");
            let c1 = s.result_key_column(1).as_long().to_vec();
            assert_eq!(c1, vec![5, 10], "{strat:?}");
        }
    }
}
