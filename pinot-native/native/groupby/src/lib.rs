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

//! Native vectorized GROUP BY engine for Apache Pinot.
//!
//! The crate is organized around a clean split: **key tables** map a composite
//! key to a dense `group_id` ([`GroupByBackend`] — [`HashbrownTable`] and the
//! no-hash [`DictDirectTable`]), while **drivers** ([`GroupByDriver`]) own the
//! per-group [`AggState`] accumulators indexed by that `group_id`. A tier only
//! changes the key→group_id step; the aggregation state is reused unchanged.
//!
//! * Segment tier ladder — [`tier`] (`select_tier`) + [`SegmentTierDriver`]:
//!   bit-packed / mixed-radix dense direct tiers and a hashbrown hash tier.
//! * Cross-segment combine — [`combine`] / [`combine_parallel`] (radix
//!   two-phase) / [`multi_combine`] (multi-column) / [`string_table`].
//! * Result selection (ORDER BY / top-K) — [`topk`].

pub mod agg;
pub mod backend;
pub mod canonical;
pub mod combine;
pub mod combine_parallel;
pub mod dict_direct;
pub mod driver_multi_agg;
pub mod hash;
pub mod hashbrown_table;
pub mod multi_combine;
pub mod multi_key;
pub mod segment_driver;
pub mod segment_tier;
pub mod string_table;
pub mod tier;
pub mod topk;

pub use agg::{java_max_f32, java_max_f64, java_min_f32, java_min_f64, AggKind, AggState};
pub use backend::GroupByBackend;
pub use canonical::{CanonicalF32, CanonicalF64};
pub use combine::{CombineDriver, CombineSession, SegmentPartial};
pub use combine_parallel::{combine_parallel, default_radix_bits};
pub use dict_direct::DictDirectTable;
pub use driver_multi_agg::{GroupByDriver, GroupByDriverDictInt};
pub use hash::{hash_bytes, hash_u64, HashKey};
pub use multi_key::{bits_for, PackedKeyEncoder, RadixKeyEncoder};
pub use hashbrown_table::HashbrownTable;
pub use multi_combine::{KeyColType, KeyColumn, MultiColStrategy, MultiColumnCombineSession, RawKeyPacker};
pub use segment_driver::GroupBySumLongByDictInt;
pub use segment_tier::SegmentTierDriver;
pub use string_table::{StringCombineDriver, StringCombineSession, StringTable};
pub use tier::{select_tier, Tier, DIRECT_BUDGET_SLOTS, MAX_DIRECT_BITS};
pub use topk::{compute_selection, OrderKey, OrderRef, OrderTerm};
