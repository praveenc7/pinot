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

//! Per-group aggregation state for plan step (1a) — Task #60.
//!
//! A multi-aggregation GROUP BY driver holds one [`AggState`] per
//! aggregation declared at construction time. Each variant carries a dense
//! `Vec<T>` indexed by `group_id`. The driver grows each vector by one
//! identity element when a new group is allocated, then mutates per-group
//! slots in-place during `apply_*` block updates.
//!
//! ## Java semantics
//!
//! * **SUM(LONG)** wraps on overflow (`wrapping_add`) to match
//!   `SumAggregationFunction` for LONG in Pinot, which does not use
//!   `Math.addExact`. SUM(INT) widens to `i64` accumulator (also wrapping).
//! * **SUM(DOUBLE/FLOAT)** uses straight IEEE add; NaN propagates naturally.
//! * **MIN/MAX(LONG/INT)** is straight comparison; identity is `MAX_VALUE`
//!   / `MIN_VALUE` so the first observed row always wins.
//! * **MIN/MAX(FLOAT/DOUBLE)** is **NaN-propagating** to match
//!   `Math.min(double, double)` / `Math.max(double, double)`. The
//!   per-group accumulator latches NaN once seen — no escape. Also handles
//!   the `-0.0 < +0.0` ordering Java applies (see [`java_min_f64`] /
//!   [`java_max_f64`]). This is consistent with the SIMD kernel's NaN
//!   handling documented in the 2026-05-31 decision log entry.
//! * **COUNT** is just per-group row count (`i64` accumulator, `+=1` per
//!   row).

/// Aggregation kind identifier. Stable u8 encoding so the JNI surface can
/// pass a `byte[]` of kinds at driver construction time.
///
/// The numeric order is internal to the Rust crate; the Java side has its
/// own `enum NativeAggKind` whose ordinals must match these values. The
/// JNI bridge validates kinds at handle creation.
#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AggKind {
    SumLong = 0,
    SumDouble = 1,
    MinInt = 2,
    MinLong = 3,
    MinFloat = 4,
    MinDouble = 5,
    MaxInt = 6,
    MaxLong = 7,
    MaxFloat = 8,
    MaxDouble = 9,
    Count = 10,
    /// SUM(INT) accumulated in f64 — reads i32 input, converts each element
    /// to f64 and accumulates in an f64 slot. Matches Pinot's group-by SUM,
    /// which reads every numeric column via `getDoubleValuesSV()` and sums
    /// in a double holder.
    SumIntToDouble = 11,
    /// SUM(LONG) accumulated in f64 — reads i64 input, widens each element
    /// to f64 (matching Pinot's i64→f64 conversion + double accumulation,
    /// including the >2^53 precision loss). Distinct from [`AggKind::SumLong`],
    /// which is the legacy i64-wrapping accumulator used by the single-SUM
    /// shim and is NOT Pinot-parity for large sums.
    SumLongToDouble = 12,
    /// SUM(FLOAT) accumulated in f64 — reads f32 input, widens each element.
    SumFloatToDouble = 13,
}

/// Error returned by [`TryFrom<u8>`] for [`AggKind`] when the byte is not a
/// recognized ordinal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownAggregation;

impl TryFrom<u8> for AggKind {
    type Error = UnknownAggregation;

    /// Decode a u8 from the JNI surface back into an [`AggKind`]. Returns
    /// `Err(UnknownAggregation)` for unknown values; callers reject the whole
    /// driver creation if any kind fails to decode (no partial-validity states).
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        Ok(match v {
            0 => Self::SumLong,
            1 => Self::SumDouble,
            2 => Self::MinInt,
            3 => Self::MinLong,
            4 => Self::MinFloat,
            5 => Self::MinDouble,
            6 => Self::MaxInt,
            7 => Self::MaxLong,
            8 => Self::MaxFloat,
            9 => Self::MaxDouble,
            10 => Self::Count,
            11 => Self::SumIntToDouble,
            12 => Self::SumLongToDouble,
            13 => Self::SumFloatToDouble,
            _ => return Err(UnknownAggregation),
        })
    }
}

impl AggKind {
    /// True if this kind accepts an `i64` value array via `apply_long`.
    #[inline]
    pub fn is_long_valued(self) -> bool {
        matches!(
            self,
            Self::SumLong | Self::MinLong | Self::MaxLong | Self::SumLongToDouble
        )
    }

    /// True if this kind accepts an `i32` value array via `apply_int`.
    #[inline]
    pub fn is_int_valued(self) -> bool {
        matches!(self, Self::MinInt | Self::MaxInt | Self::SumIntToDouble)
    }

    /// True if this kind accepts an `f64` value array via `apply_double`.
    #[inline]
    pub fn is_double_valued(self) -> bool {
        matches!(self, Self::SumDouble | Self::MinDouble | Self::MaxDouble)
    }

    /// True if this kind accepts an `f32` value array via `apply_float`.
    #[inline]
    pub fn is_float_valued(self) -> bool {
        matches!(self, Self::MinFloat | Self::MaxFloat | Self::SumFloatToDouble)
    }

    /// True if this kind takes no value array (COUNT).
    #[inline]
    pub fn is_count(self) -> bool {
        matches!(self, Self::Count)
    }
}

/// Per-group accumulator state for one aggregation. Indexed by `group_id`:
/// `state[g]` is the running accumulator for group `g`.
///
/// Variants carry the accumulator type, which is wider than the input type
/// for SUM(INT/LONG → i64) and SUM(FLOAT/DOUBLE → f64) per Pinot's
/// [`SumAggregationFunction`]; MIN/MAX accumulators match the input type
/// because there's no widening risk.
///
/// **Invariant**: all variants' inner `Vec::len()` equals the driver's
/// `num_groups()`. Maintained by [`push_new_group`].
///
/// [`SumAggregationFunction`]: https://github.com/apache/pinot
/// [`push_new_group`]: AggState::push_new_group
pub enum AggState {
    /// SUM(LONG) or SUM(INT) — i64 accumulator, wrapping add semantics.
    SumLong(Vec<i64>),
    /// SUM(DOUBLE) or SUM(FLOAT) — f64 accumulator, IEEE add.
    SumDouble(Vec<f64>),
    /// MIN(INT) — i32 accumulator, identity `i32::MAX`.
    MinInt(Vec<i32>),
    /// MIN(LONG) — i64 accumulator, identity `i64::MAX`.
    MinLong(Vec<i64>),
    /// MIN(FLOAT) — f32 accumulator, identity `f32::INFINITY`, NaN-propagating.
    MinFloat(Vec<f32>),
    /// MIN(DOUBLE) — f64 accumulator, identity `f64::INFINITY`, NaN-propagating.
    MinDouble(Vec<f64>),
    /// MAX(INT) — i32 accumulator, identity `i32::MIN`.
    MaxInt(Vec<i32>),
    /// MAX(LONG) — i64 accumulator, identity `i64::MIN`.
    MaxLong(Vec<i64>),
    /// MAX(FLOAT) — f32 accumulator, identity `f32::NEG_INFINITY`, NaN-propagating.
    MaxFloat(Vec<f32>),
    /// MAX(DOUBLE) — f64 accumulator, identity `f64::NEG_INFINITY`, NaN-propagating.
    MaxDouble(Vec<f64>),
    /// COUNT — i64 row count, identity 0.
    Count(Vec<i64>),
    /// SUM(INT)→f64 — i32 input fed via `apply_int`, f64 accumulator. Pinot
    /// group-by parity (see [`AggKind::SumIntToDouble`]).
    SumIntToDouble(Vec<f64>),
    /// SUM(LONG)→f64 — i64 input fed via `apply_long`, f64 accumulator.
    SumLongToDouble(Vec<f64>),
    /// SUM(FLOAT)→f64 — f32 input fed via `apply_float`, f64 accumulator.
    SumFloatToDouble(Vec<f64>),
}

impl AggState {
    /// Construct a new empty state for the given kind. `expected_groups` is
    /// passed to `Vec::with_capacity` to avoid early reallocation when the
    /// caller has a tight group-cardinality estimate.
    pub fn new_for(kind: AggKind, expected_groups: usize) -> Self {
        match kind {
            AggKind::SumLong => Self::SumLong(Vec::with_capacity(expected_groups)),
            AggKind::SumDouble => Self::SumDouble(Vec::with_capacity(expected_groups)),
            AggKind::MinInt => Self::MinInt(Vec::with_capacity(expected_groups)),
            AggKind::MinLong => Self::MinLong(Vec::with_capacity(expected_groups)),
            AggKind::MinFloat => Self::MinFloat(Vec::with_capacity(expected_groups)),
            AggKind::MinDouble => Self::MinDouble(Vec::with_capacity(expected_groups)),
            AggKind::MaxInt => Self::MaxInt(Vec::with_capacity(expected_groups)),
            AggKind::MaxLong => Self::MaxLong(Vec::with_capacity(expected_groups)),
            AggKind::MaxFloat => Self::MaxFloat(Vec::with_capacity(expected_groups)),
            AggKind::MaxDouble => Self::MaxDouble(Vec::with_capacity(expected_groups)),
            AggKind::Count => Self::Count(Vec::with_capacity(expected_groups)),
            AggKind::SumIntToDouble => Self::SumIntToDouble(Vec::with_capacity(expected_groups)),
            AggKind::SumLongToDouble => Self::SumLongToDouble(Vec::with_capacity(expected_groups)),
            AggKind::SumFloatToDouble => Self::SumFloatToDouble(Vec::with_capacity(expected_groups)),
        }
    }

    // --- Reconstruct a partial from a segment's extracted vector ---
    //
    // The combine path rebuilds typed partials from a segment's typed
    // extraction. Each builder is the inverse of the matching `as_*_slice`
    // extract family; the input vector becomes the accumulator directly (no
    // copy). Panics if `kind` is not in that extract family.

    /// i64 family: SumLong / MinLong / MaxLong / Count.
    pub fn from_long_vec(kind: AggKind, v: Vec<i64>) -> Self {
        match kind {
            AggKind::SumLong => Self::SumLong(v),
            AggKind::MinLong => Self::MinLong(v),
            AggKind::MaxLong => Self::MaxLong(v),
            AggKind::Count => Self::Count(v),
            _ => panic!("from_long_vec: {:?} is not an i64-extracted agg", kind),
        }
    }

    /// i32 family: MinInt / MaxInt.
    pub fn from_int_vec(kind: AggKind, v: Vec<i32>) -> Self {
        match kind {
            AggKind::MinInt => Self::MinInt(v),
            AggKind::MaxInt => Self::MaxInt(v),
            _ => panic!("from_int_vec: {:?} is not an i32-extracted agg", kind),
        }
    }

    /// f64 family: SumDouble / MinDouble / MaxDouble / Sum{Int,Long,Float}ToDouble.
    pub fn from_double_vec(kind: AggKind, v: Vec<f64>) -> Self {
        match kind {
            AggKind::SumDouble => Self::SumDouble(v),
            AggKind::MinDouble => Self::MinDouble(v),
            AggKind::MaxDouble => Self::MaxDouble(v),
            AggKind::SumIntToDouble => Self::SumIntToDouble(v),
            AggKind::SumLongToDouble => Self::SumLongToDouble(v),
            AggKind::SumFloatToDouble => Self::SumFloatToDouble(v),
            _ => panic!("from_double_vec: {:?} is not an f64-extracted agg", kind),
        }
    }

    /// f32 family: MinFloat / MaxFloat.
    pub fn from_float_vec(kind: AggKind, v: Vec<f32>) -> Self {
        match kind {
            AggKind::MinFloat => Self::MinFloat(v),
            AggKind::MaxFloat => Self::MaxFloat(v),
            _ => panic!("from_float_vec: {:?} is not an f32-extracted agg", kind),
        }
    }

    /// The [`AggKind`] this state was constructed for.
    #[inline]
    pub fn kind(&self) -> AggKind {
        match self {
            Self::SumLong(_) => AggKind::SumLong,
            Self::SumDouble(_) => AggKind::SumDouble,
            Self::MinInt(_) => AggKind::MinInt,
            Self::MinLong(_) => AggKind::MinLong,
            Self::MinFloat(_) => AggKind::MinFloat,
            Self::MinDouble(_) => AggKind::MinDouble,
            Self::MaxInt(_) => AggKind::MaxInt,
            Self::MaxLong(_) => AggKind::MaxLong,
            Self::MaxFloat(_) => AggKind::MaxFloat,
            Self::MaxDouble(_) => AggKind::MaxDouble,
            Self::Count(_) => AggKind::Count,
            Self::SumIntToDouble(_) => AggKind::SumIntToDouble,
            Self::SumLongToDouble(_) => AggKind::SumLongToDouble,
            Self::SumFloatToDouble(_) => AggKind::SumFloatToDouble,
        }
    }

    /// Number of groups currently tracked. Equal across all `AggState`s of
    /// a driver (invariant maintained by [`push_new_group`]).
    ///
    /// [`push_new_group`]: AggState::push_new_group
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::SumLong(v) => v.len(),
            Self::SumDouble(v) => v.len(),
            Self::MinInt(v) => v.len(),
            Self::MinLong(v) => v.len(),
            Self::MinFloat(v) => v.len(),
            Self::MinDouble(v) => v.len(),
            Self::MaxInt(v) => v.len(),
            Self::MaxLong(v) => v.len(),
            Self::MaxFloat(v) => v.len(),
            Self::MaxDouble(v) => v.len(),
            Self::Count(v) => v.len(),
            Self::SumIntToDouble(v) => v.len(),
            Self::SumLongToDouble(v) => v.len(),
            Self::SumFloatToDouble(v) => v.len(),
        }
    }

    /// `true` iff no groups have been allocated yet.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Push the identity element for this aggregation. Called by the driver
    /// exactly once per newly-allocated group, after `probe_or_insert_batch`
    /// reports a new group_id.
    ///
    /// Identities:
    ///
    /// | Kind          | Identity                |
    /// |---------------|-------------------------|
    /// | SumLong       | `0`                     |
    /// | SumDouble     | `0.0`                   |
    /// | MinInt        | `i32::MAX`              |
    /// | MinLong       | `i64::MAX`              |
    /// | MinFloat      | `f32::INFINITY`         |
    /// | MinDouble     | `f64::INFINITY`         |
    /// | MaxInt        | `i32::MIN`              |
    /// | MaxLong       | `i64::MIN`              |
    /// | MaxFloat      | `f32::NEG_INFINITY`     |
    /// | MaxDouble     | `f64::NEG_INFINITY`     |
    /// | Count         | `0`                     |
    ///
    /// Empty-input agg result (a group that was created but never had a
    /// value applied — currently impossible since groups are allocated
    /// *by* value-bearing rows) would surface the identity. This matters
    /// for the all-COUNT-no-other-agg case where COUNT does not consume a
    /// value column: an emptily-iterated group still reports 0 count,
    /// which is the correct identity for COUNT.
    #[inline]
    pub fn push_new_group(&mut self) {
        match self {
            Self::SumLong(v) => v.push(0),
            Self::SumDouble(v) => v.push(0.0),
            Self::MinInt(v) => v.push(i32::MAX),
            Self::MinLong(v) => v.push(i64::MAX),
            Self::MinFloat(v) => v.push(f32::INFINITY),
            Self::MinDouble(v) => v.push(f64::INFINITY),
            Self::MaxInt(v) => v.push(i32::MIN),
            Self::MaxLong(v) => v.push(i64::MIN),
            Self::MaxFloat(v) => v.push(f32::NEG_INFINITY),
            Self::MaxDouble(v) => v.push(f64::NEG_INFINITY),
            Self::Count(v) => v.push(0),
            Self::SumIntToDouble(v) => v.push(0.0),
            Self::SumLongToDouble(v) => v.push(0.0),
            Self::SumFloatToDouble(v) => v.push(0.0),
        }
    }

    /// **Combine merge:** fold the partial accumulator at `other[src]` into
    /// `self[dst]` using this aggregation's merge op. This is the cross-segment
    /// merge used by the server combine driver: each segment produces a partial
    /// accumulator per group, and combine folds partials for the same raw key
    /// together. The merge op per kind:
    ///
    /// * SUM (any input type, f64 accumulator) and COUNT (i64) → **add**
    /// * `SumLong` (legacy i64) → **wrapping add**
    /// * MIN → **min** (Java NaN-propagating for FP)
    /// * MAX → **max** (Java NaN-propagating for FP)
    ///
    /// Merging partial-into-partial is associative + commutative for all of
    /// these, which is what makes the radix-partitioned parallel merge correct:
    /// partitions are disjoint by key hash, so each final group is folded by
    /// exactly one worker in any order.
    ///
    /// `self` and `other` must be the same [`AggKind`]; `dst`/`src` must be in
    /// range. Both hold true by construction in the combine driver.
    #[inline]
    pub fn merge_slot(&mut self, dst: usize, other: &AggState, src: usize) {
        match self {
            Self::SumLong(v) => {
                v[dst] = v[dst].wrapping_add(other.as_long_slice().unwrap()[src]);
            }
            Self::Count(v) => {
                v[dst] = v[dst].wrapping_add(other.as_long_slice().unwrap()[src]);
            }
            Self::MinLong(v) => {
                v[dst] = v[dst].min(other.as_long_slice().unwrap()[src]);
            }
            Self::MaxLong(v) => {
                v[dst] = v[dst].max(other.as_long_slice().unwrap()[src]);
            }
            Self::MinInt(v) => {
                v[dst] = v[dst].min(other.as_int_slice().unwrap()[src]);
            }
            Self::MaxInt(v) => {
                v[dst] = v[dst].max(other.as_int_slice().unwrap()[src]);
            }
            Self::SumDouble(v)
            | Self::SumIntToDouble(v)
            | Self::SumLongToDouble(v)
            | Self::SumFloatToDouble(v) => {
                v[dst] += other.as_double_slice().unwrap()[src];
            }
            Self::MinDouble(v) => {
                v[dst] = java_min_f64(v[dst], other.as_double_slice().unwrap()[src]);
            }
            Self::MaxDouble(v) => {
                v[dst] = java_max_f64(v[dst], other.as_double_slice().unwrap()[src]);
            }
            Self::MinFloat(v) => {
                v[dst] = java_min_f32(v[dst], other.as_float_slice().unwrap()[src]);
            }
            Self::MaxFloat(v) => {
                v[dst] = java_max_f32(v[dst], other.as_float_slice().unwrap()[src]);
            }
        }
    }

    /// Append another **same-kind** `AggState`'s elements onto this one,
    /// draining `other`. Used to concatenate the disjoint radix partitions of
    /// the parallel combine into a single contiguous result. Panics on a kind
    /// mismatch (a programming error — partitions share the query's agg list).
    pub fn append(&mut self, other: &mut AggState) {
        use AggState::*;
        match (self, other) {
            (SumLong(a), SumLong(b)) => a.append(b),
            (SumDouble(a), SumDouble(b)) => a.append(b),
            (MinInt(a), MinInt(b)) => a.append(b),
            (MinLong(a), MinLong(b)) => a.append(b),
            (MinFloat(a), MinFloat(b)) => a.append(b),
            (MinDouble(a), MinDouble(b)) => a.append(b),
            (MaxInt(a), MaxInt(b)) => a.append(b),
            (MaxLong(a), MaxLong(b)) => a.append(b),
            (MaxFloat(a), MaxFloat(b)) => a.append(b),
            (MaxDouble(a), MaxDouble(b)) => a.append(b),
            (Count(a), Count(b)) => a.append(b),
            (SumIntToDouble(a), SumIntToDouble(b)) => a.append(b),
            (SumLongToDouble(a), SumLongToDouble(b)) => a.append(b),
            (SumFloatToDouble(a), SumFloatToDouble(b)) => a.append(b),
            _ => panic!("AggState::append kind mismatch"),
        }
    }

    // --- Phase-2 batch apply (shared by the hash-keyed and dict-direct drivers) ---
    //
    // Each folds a block of `values` into per-group slots: `state[gids[i]] op=
    // values[i]`. `gids` and `values` must be equal length; the `zip` iterates the
    // pair without per-element bounds checks. The one data-dependent access,
    // `state[gid]`, is bounds-checked in safe code (the hash driver grows slots
    // densely and the dict-direct driver pre-sizes to dict cardinality, so a valid
    // `gid` is always in range; a caller bug panics loudly rather than corrupting
    // memory). No `unsafe` / `get_unchecked` — the checked scatter is dominated by
    // its own random-access memory latency, so the bound check is not measurable.

    /// Apply an i64-valued agg (SumLong / MinLong / MaxLong / SumLongToDouble).
    pub fn apply_long_batch(&mut self, gids: &[u32], values: &[i64]) {
        debug_assert_eq!(gids.len(), values.len());
        match self {
            Self::SumLong(state) => {
                for (&gid, &v) in gids.iter().zip(values) {
                    let s = &mut state[gid as usize];
                    *s = s.wrapping_add(v);
                }
            }
            Self::MinLong(state) => {
                for (&gid, &v) in gids.iter().zip(values) {
                    let s = &mut state[gid as usize];
                    if v < *s {
                        *s = v;
                    }
                }
            }
            Self::MaxLong(state) => {
                for (&gid, &v) in gids.iter().zip(values) {
                    let s = &mut state[gid as usize];
                    if v > *s {
                        *s = v;
                    }
                }
            }
            Self::SumLongToDouble(state) => {
                // i64 input, f64 accumulator (Pinot getDoubleValuesSV parity).
                for (&gid, &v) in gids.iter().zip(values) {
                    state[gid as usize] += v as f64;
                }
            }
            other => panic!("apply_long_batch on non-long agg: kind = {:?}", other.kind()),
        }
    }

    /// Apply an i32-valued agg (MinInt / MaxInt / SumIntToDouble).
    pub fn apply_int_batch(&mut self, gids: &[u32], values: &[i32]) {
        debug_assert_eq!(gids.len(), values.len());
        match self {
            Self::MinInt(state) => {
                for (&gid, &v) in gids.iter().zip(values) {
                    let s = &mut state[gid as usize];
                    if v < *s {
                        *s = v;
                    }
                }
            }
            Self::MaxInt(state) => {
                for (&gid, &v) in gids.iter().zip(values) {
                    let s = &mut state[gid as usize];
                    if v > *s {
                        *s = v;
                    }
                }
            }
            Self::SumIntToDouble(state) => {
                for (&gid, &v) in gids.iter().zip(values) {
                    state[gid as usize] += v as f64;
                }
            }
            other => panic!("apply_int_batch on non-int agg: kind = {:?}", other.kind()),
        }
    }

    /// Apply an f64-valued agg (SumDouble / MinDouble / MaxDouble); MIN/MAX use
    /// Java NaN-propagating semantics.
    pub fn apply_double_batch(&mut self, gids: &[u32], values: &[f64]) {
        debug_assert_eq!(gids.len(), values.len());
        match self {
            Self::SumDouble(state) => {
                for (&gid, &v) in gids.iter().zip(values) {
                    state[gid as usize] += v;
                }
            }
            Self::MinDouble(state) => {
                for (&gid, &v) in gids.iter().zip(values) {
                    let s = &mut state[gid as usize];
                    *s = java_min_f64(*s, v);
                }
            }
            Self::MaxDouble(state) => {
                for (&gid, &v) in gids.iter().zip(values) {
                    let s = &mut state[gid as usize];
                    *s = java_max_f64(*s, v);
                }
            }
            other => panic!("apply_double_batch on non-double agg: kind = {:?}", other.kind()),
        }
    }

    /// Apply an f32-valued agg (MinFloat / MaxFloat / SumFloatToDouble).
    pub fn apply_float_batch(&mut self, gids: &[u32], values: &[f32]) {
        debug_assert_eq!(gids.len(), values.len());
        match self {
            Self::MinFloat(state) => {
                for (&gid, &v) in gids.iter().zip(values) {
                    let s = &mut state[gid as usize];
                    *s = java_min_f32(*s, v);
                }
            }
            Self::MaxFloat(state) => {
                for (&gid, &v) in gids.iter().zip(values) {
                    let s = &mut state[gid as usize];
                    *s = java_max_f32(*s, v);
                }
            }
            Self::SumFloatToDouble(state) => {
                for (&gid, &v) in gids.iter().zip(values) {
                    state[gid as usize] += v as f64;
                }
            }
            other => panic!("apply_float_batch on non-float agg: kind = {:?}", other.kind()),
        }
    }

    /// Apply COUNT — increment each group's count by 1 for every `gids[i]`.
    pub fn apply_count_batch(&mut self, gids: &[u32]) {
        match self {
            Self::Count(state) => {
                for &gid in gids {
                    state[gid as usize] += 1;
                }
            }
            other => panic!("apply_count_batch on non-count agg: kind = {:?}", other.kind()),
        }
    }

    // --- Read-only typed views (for tests + result extraction) ---

    pub fn as_long_slice(&self) -> Option<&[i64]> {
        match self {
            Self::SumLong(v) | Self::MinLong(v) | Self::MaxLong(v) | Self::Count(v) => Some(v),
            _ => None,
        }
    }
    pub fn as_int_slice(&self) -> Option<&[i32]> {
        match self {
            Self::MinInt(v) | Self::MaxInt(v) => Some(v),
            _ => None,
        }
    }
    pub fn as_double_slice(&self) -> Option<&[f64]> {
        match self {
            Self::SumDouble(v)
            | Self::MinDouble(v)
            | Self::MaxDouble(v)
            | Self::SumIntToDouble(v)
            | Self::SumLongToDouble(v)
            | Self::SumFloatToDouble(v) => Some(v),
            _ => None,
        }
    }
    pub fn as_float_slice(&self) -> Option<&[f32]> {
        match self {
            Self::MinFloat(v) | Self::MaxFloat(v) => Some(v),
            _ => None,
        }
    }

    /// Compare two group slots by this aggregation's natural result value, as a
    /// total order (`total_cmp` for FP so canonical NaN sorts deterministically
    /// rather than producing a partial order). Used by ORDER BY on an agg column.
    pub fn cmp_slots(&self, i: usize, j: usize) -> std::cmp::Ordering {
        match self {
            Self::SumLong(v) | Self::MinLong(v) | Self::MaxLong(v) | Self::Count(v) => v[i].cmp(&v[j]),
            Self::MinInt(v) | Self::MaxInt(v) => v[i].cmp(&v[j]),
            Self::SumDouble(v)
            | Self::MinDouble(v)
            | Self::MaxDouble(v)
            | Self::SumIntToDouble(v)
            | Self::SumLongToDouble(v)
            | Self::SumFloatToDouble(v) => v[i].total_cmp(&v[j]),
            Self::MinFloat(v) | Self::MaxFloat(v) => v[i].total_cmp(&v[j]),
        }
    }

    /// Return a new accumulator with groups reordered by `perm`
    /// (`out[k] = self[perm[k]]`) — applies a top-K / selection permutation.
    pub fn gather(&self, perm: &[usize]) -> Self {
        macro_rules! g {
            ($v:expr) => {
                perm.iter().map(|&i| $v[i]).collect()
            };
        }
        match self {
            Self::SumLong(v) => Self::SumLong(g!(v)),
            Self::SumDouble(v) => Self::SumDouble(g!(v)),
            Self::MinInt(v) => Self::MinInt(g!(v)),
            Self::MinLong(v) => Self::MinLong(g!(v)),
            Self::MinFloat(v) => Self::MinFloat(g!(v)),
            Self::MinDouble(v) => Self::MinDouble(g!(v)),
            Self::MaxInt(v) => Self::MaxInt(g!(v)),
            Self::MaxLong(v) => Self::MaxLong(g!(v)),
            Self::MaxFloat(v) => Self::MaxFloat(g!(v)),
            Self::MaxDouble(v) => Self::MaxDouble(g!(v)),
            Self::Count(v) => Self::Count(g!(v)),
            Self::SumIntToDouble(v) => Self::SumIntToDouble(g!(v)),
            Self::SumLongToDouble(v) => Self::SumLongToDouble(g!(v)),
            Self::SumFloatToDouble(v) => Self::SumFloatToDouble(g!(v)),
        }
    }
}

// --------------------------------------------------------------------------
// FP min/max with Java semantics
// --------------------------------------------------------------------------

/// Java `Math.min(double, double)` semantics:
///
/// * If either arg is NaN, returns NaN.
/// * Treats `-0.0 < +0.0` (so `min(-0.0, +0.0) == -0.0`).
/// * Otherwise the numerically smaller value.
///
/// Rust's `f64::min` does NOT propagate NaN (it returns the non-NaN arg),
/// so we cannot use it directly. The implementation here matches the
/// existing kernel-side NaN sticky-OR pattern documented in §15
/// 2026-05-31.
#[inline]
pub fn java_min_f64(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        return f64::NAN;
    }
    if a == 0.0 && b == 0.0 {
        // Java: -0.0 < +0.0 for the purpose of Math.min.
        return if a.is_sign_negative() { a } else { b };
    }
    if a < b {
        a
    } else {
        b
    }
}

/// Java `Math.max(double, double)` semantics — symmetric to [`java_min_f64`]
/// but treats `+0.0 > -0.0`.
#[inline]
pub fn java_max_f64(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        return f64::NAN;
    }
    if a == 0.0 && b == 0.0 {
        return if a.is_sign_positive() { a } else { b };
    }
    if a > b {
        a
    } else {
        b
    }
}

/// Java `Math.min(float, float)` — same rules as [`java_min_f64`] at f32.
#[inline]
pub fn java_min_f32(a: f32, b: f32) -> f32 {
    if a.is_nan() || b.is_nan() {
        return f32::NAN;
    }
    if a == 0.0 && b == 0.0 {
        return if a.is_sign_negative() { a } else { b };
    }
    if a < b {
        a
    } else {
        b
    }
}

/// Java `Math.max(float, float)` — symmetric to [`java_min_f32`].
#[inline]
pub fn java_max_f32(a: f32, b: f32) -> f32 {
    if a.is_nan() || b.is_nan() {
        return f32::NAN;
    }
    if a == 0.0 && b == 0.0 {
        return if a.is_sign_positive() { a } else { b };
    }
    if a > b {
        a
    } else {
        b
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- AggKind ----------------------------------------------------------

    #[test]
    fn agg_kind_roundtrip_through_u8() {
        let all = [
            AggKind::SumLong,
            AggKind::SumDouble,
            AggKind::MinInt,
            AggKind::MinLong,
            AggKind::MinFloat,
            AggKind::MinDouble,
            AggKind::MaxInt,
            AggKind::MaxLong,
            AggKind::MaxFloat,
            AggKind::MaxDouble,
            AggKind::Count,
            AggKind::SumIntToDouble,
            AggKind::SumLongToDouble,
            AggKind::SumFloatToDouble,
        ];
        for k in all {
            let v = k as u8;
            assert_eq!(AggKind::try_from(v), Ok(k));
        }
    }

    #[test]
    fn agg_kind_unknown_u8_returns_none() {
        assert_eq!(AggKind::try_from(14), Err(UnknownAggregation));
        assert_eq!(AggKind::try_from(255), Err(UnknownAggregation));
    }

    #[test]
    fn agg_kind_value_type_predicates_partition_kinds() {
        // Every kind is either long-, int-, double-, float-valued, or count —
        // exactly one of the five. This is a load-bearing invariant for the
        // JNI dispatch (apply_long vs apply_int vs apply_double vs apply_float
        // vs apply_count — see driver_multi_agg.rs).
        let all = [
            AggKind::SumLong,
            AggKind::SumDouble,
            AggKind::MinInt,
            AggKind::MinLong,
            AggKind::MinFloat,
            AggKind::MinDouble,
            AggKind::MaxInt,
            AggKind::MaxLong,
            AggKind::MaxFloat,
            AggKind::MaxDouble,
            AggKind::Count,
            AggKind::SumIntToDouble,
            AggKind::SumLongToDouble,
            AggKind::SumFloatToDouble,
        ];
        for k in all {
            let count =
                (k.is_long_valued() as u32)
                + (k.is_int_valued() as u32)
                + (k.is_double_valued() as u32)
                + (k.is_float_valued() as u32)
                + (k.is_count() as u32);
            assert_eq!(count, 1, "kind {:?} should match exactly one value-type predicate", k);
        }
    }

    // --- AggState identities ---------------------------------------------

    #[test]
    fn push_new_group_pushes_identity_for_every_kind() {
        let mut s = AggState::new_for(AggKind::SumLong, 0);
        s.push_new_group();
        assert_eq!(s.as_long_slice(), Some(&[0i64][..]));

        let mut s = AggState::new_for(AggKind::SumDouble, 0);
        s.push_new_group();
        assert_eq!(s.as_double_slice(), Some(&[0.0f64][..]));

        let mut s = AggState::new_for(AggKind::MinInt, 0);
        s.push_new_group();
        assert_eq!(s.as_int_slice(), Some(&[i32::MAX][..]));

        let mut s = AggState::new_for(AggKind::MinLong, 0);
        s.push_new_group();
        assert_eq!(s.as_long_slice(), Some(&[i64::MAX][..]));

        let mut s = AggState::new_for(AggKind::MinFloat, 0);
        s.push_new_group();
        assert_eq!(s.as_float_slice(), Some(&[f32::INFINITY][..]));

        let mut s = AggState::new_for(AggKind::MinDouble, 0);
        s.push_new_group();
        assert_eq!(s.as_double_slice(), Some(&[f64::INFINITY][..]));

        let mut s = AggState::new_for(AggKind::MaxInt, 0);
        s.push_new_group();
        assert_eq!(s.as_int_slice(), Some(&[i32::MIN][..]));

        let mut s = AggState::new_for(AggKind::MaxLong, 0);
        s.push_new_group();
        assert_eq!(s.as_long_slice(), Some(&[i64::MIN][..]));

        let mut s = AggState::new_for(AggKind::MaxFloat, 0);
        s.push_new_group();
        assert_eq!(s.as_float_slice(), Some(&[f32::NEG_INFINITY][..]));

        let mut s = AggState::new_for(AggKind::MaxDouble, 0);
        s.push_new_group();
        assert_eq!(s.as_double_slice(), Some(&[f64::NEG_INFINITY][..]));

        let mut s = AggState::new_for(AggKind::Count, 0);
        s.push_new_group();
        assert_eq!(s.as_long_slice(), Some(&[0i64][..]));

        let mut s = AggState::new_for(AggKind::SumIntToDouble, 0);
        s.push_new_group();
        assert_eq!(s.as_double_slice(), Some(&[0.0f64][..]));

        let mut s = AggState::new_for(AggKind::SumLongToDouble, 0);
        s.push_new_group();
        assert_eq!(s.as_double_slice(), Some(&[0.0f64][..]));

        let mut s = AggState::new_for(AggKind::SumFloatToDouble, 0);
        s.push_new_group();
        assert_eq!(s.as_double_slice(), Some(&[0.0f64][..]));
    }

    #[test]
    fn typed_slice_accessors_reject_wrong_value_type() {
        let s = AggState::new_for(AggKind::SumLong, 0);
        assert!(s.as_long_slice().is_some());
        assert!(s.as_int_slice().is_none());
        assert!(s.as_double_slice().is_none());
        assert!(s.as_float_slice().is_none());
    }

    #[test]
    fn len_starts_at_zero_grows_on_push() {
        let mut s = AggState::new_for(AggKind::MaxFloat, 0);
        assert_eq!(s.len(), 0);
        assert!(s.is_empty());
        s.push_new_group();
        s.push_new_group();
        assert_eq!(s.len(), 2);
        assert!(!s.is_empty());
    }

    // --- FP min/max Java semantics ---------------------------------------

    #[test]
    fn java_min_f64_propagates_nan() {
        assert!(java_min_f64(f64::NAN, 1.0).is_nan());
        assert!(java_min_f64(1.0, f64::NAN).is_nan());
        assert!(java_min_f64(f64::NAN, f64::NAN).is_nan());
        // Rust's f64::min would return 1.0 in the first two cases.
    }

    #[test]
    fn java_max_f64_propagates_nan() {
        assert!(java_max_f64(f64::NAN, 1.0).is_nan());
        assert!(java_max_f64(1.0, f64::NAN).is_nan());
        assert!(java_max_f64(f64::NAN, f64::NAN).is_nan());
    }

    #[test]
    fn java_min_f64_signed_zero() {
        let neg = -0.0f64;
        let pos = 0.0f64;
        // Java: min(-0.0, +0.0) == -0.0 (preserve negative-zero bit)
        let r = java_min_f64(neg, pos);
        assert_eq!(r, 0.0);
        assert!(r.is_sign_negative());
        let r = java_min_f64(pos, neg);
        assert_eq!(r, 0.0);
        assert!(r.is_sign_negative());
    }

    #[test]
    fn java_max_f64_signed_zero() {
        let neg = -0.0f64;
        let pos = 0.0f64;
        // Java: max(-0.0, +0.0) == +0.0
        let r = java_max_f64(neg, pos);
        assert_eq!(r, 0.0);
        assert!(r.is_sign_positive());
        let r = java_max_f64(pos, neg);
        assert_eq!(r, 0.0);
        assert!(r.is_sign_positive());
    }

    #[test]
    fn java_min_max_f64_ordinary_values() {
        assert_eq!(java_min_f64(1.0, 2.0), 1.0);
        assert_eq!(java_min_f64(2.0, 1.0), 1.0);
        assert_eq!(java_max_f64(1.0, 2.0), 2.0);
        assert_eq!(java_max_f64(2.0, 1.0), 2.0);
        assert_eq!(java_min_f64(-5.5, -10.0), -10.0);
        assert_eq!(java_max_f64(-5.5, -10.0), -5.5);
    }

    #[test]
    fn java_min_max_f64_infinities() {
        assert_eq!(java_min_f64(f64::NEG_INFINITY, 0.0), f64::NEG_INFINITY);
        assert_eq!(java_min_f64(f64::INFINITY, 0.0), 0.0);
        assert_eq!(java_max_f64(f64::INFINITY, 0.0), f64::INFINITY);
        assert_eq!(java_max_f64(f64::NEG_INFINITY, 0.0), 0.0);
        // ∞ vs NaN — NaN wins (propagates).
        assert!(java_min_f64(f64::INFINITY, f64::NAN).is_nan());
        assert!(java_max_f64(f64::NEG_INFINITY, f64::NAN).is_nan());
    }

    #[test]
    fn java_min_max_f32_mirrors_f64_semantics() {
        assert!(java_min_f32(f32::NAN, 1.0).is_nan());
        assert!(java_max_f32(1.0, f32::NAN).is_nan());

        let r = java_min_f32(-0.0, 0.0);
        assert_eq!(r, 0.0);
        assert!(r.is_sign_negative());

        let r = java_max_f32(-0.0, 0.0);
        assert_eq!(r, 0.0);
        assert!(r.is_sign_positive());

        assert_eq!(java_min_f32(1.0, 2.0), 1.0);
        assert_eq!(java_max_f32(1.0, 2.0), 2.0);
    }
}
