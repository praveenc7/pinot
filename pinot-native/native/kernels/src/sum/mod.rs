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

//! SUM kernels for primitive fixed-width types.
//!
//! Each submodule implements one input type, producing an `f64` accumulator
//! (matching `SumAggregationFunction.aggregateSV(<TYPE>)` semantics in Pinot
//! Java). Per-type implementation has:
//!
//! * a public runtime-dispatched entry point (`sum_<type>_to_f64`)
//! * a public 4-way unrolled scalar fallback (`sum_<type>_to_f64_scalar`),
//!   used as the dispatch fallback AND exposed for JMH attribution
//! * architecture-specific SIMD bodies guarded by `target_feature`
//!
//! ## Java semantics
//!
//! Matches `SumAggregationFunction.aggregateSV(<TYPE>)` closely but not
//! bit-exactly. Java does scalar left-to-right `s += v` accumulation; SIMD
//! adds reorder the reduction across multiple accumulator lanes. Results
//! differ only in the last ulp(s) when magnitudes exceed the f64 mantissa;
//! Pinot's differential tester allows
//! `|native - java| ≤ max(1.0, |java| × 1e-15)`.

pub mod double;
pub mod float;
pub mod int;
pub mod long;

// Re-exports preserve the pre-Phase-1.B FFI surface
// (`pinot_native_kernels::sum::sum_i64_to_f64`,
//  `pinot_native_kernels::sum::sum_i64_to_f64_scalar`).
pub use long::{sum_i64_to_f64, sum_i64_to_f64_scalar};
