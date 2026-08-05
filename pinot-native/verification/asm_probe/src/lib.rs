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

// Probe: does LLVM auto-vectorize our 4-accumulator FP SUM scalar loop?
// Each function is no_mangle + inline(never) so it shows up as a discrete
// symbol in the .s file.

// ---------------------------------------------------------------------------
// FP SUM — single accumulator (this is the "Java equivalent" loop shape)
// ---------------------------------------------------------------------------

#[no_mangle]
#[inline(never)]
pub fn sum_f64_single_acc(values: &[f64]) -> f64 {
    let mut acc = 0.0_f64;
    for &v in values {
        acc += v;
    }
    acc
}

// ---------------------------------------------------------------------------
// FP SUM — 4 independent accumulators (this is our "scalar Rust" kernel shape)
// ---------------------------------------------------------------------------

#[no_mangle]
#[inline(never)]
pub fn sum_f64_four_accs(values: &[f64]) -> f64 {
    let mut s0 = 0.0_f64;
    let mut s1 = 0.0_f64;
    let mut s2 = 0.0_f64;
    let mut s3 = 0.0_f64;
    let chunks = values.chunks_exact(4);
    let remainder = chunks.remainder();
    for c in chunks {
        s0 += c[0];
        s1 += c[1];
        s2 += c[2];
        s3 += c[3];
    }
    let mut tail = 0.0_f64;
    for &v in remainder {
        tail += v;
    }
    ((s0 + s1) + (s2 + s3)) + tail
}

// ---------------------------------------------------------------------------
// Integer SUM — single accumulator (control: this SHOULD auto-vectorize)
// ---------------------------------------------------------------------------

#[no_mangle]
#[inline(never)]
pub fn sum_i64_single_acc(values: &[i64]) -> i64 {
    let mut acc = 0_i64;
    for &v in values {
        acc = acc.wrapping_add(v);
    }
    acc
}

// ---------------------------------------------------------------------------
// FP MIN — single accumulator with Java NaN-propagating semantics
// (This is what we'd auto-vectorize. We claim HotSpot doesn't; let's see LLVM.)
// ---------------------------------------------------------------------------

#[no_mangle]
#[inline(never)]
pub fn min_f64_java_semantics(values: &[f64]) -> f64 {
    let mut acc = f64::INFINITY;
    for &v in values {
        if v.is_nan() || acc.is_nan() {
            acc = f64::NAN;
        } else if v < acc {
            acc = v;
        }
    }
    acc
}

// ---------------------------------------------------------------------------
// FP MIN — using built-in f64::min (defined to ignore NaN, NOT Java semantics)
// Should auto-vectorize more readily because NaN handling is "ignore."
// ---------------------------------------------------------------------------

#[no_mangle]
#[inline(never)]
pub fn min_f64_rust_builtin(values: &[f64]) -> f64 {
    let mut acc = f64::INFINITY;
    for &v in values {
        acc = acc.min(v);
    }
    acc
}

// ---------------------------------------------------------------------------
// Integer MIN — single accumulator (control: SHOULD auto-vectorize)
// ---------------------------------------------------------------------------

#[no_mangle]
#[inline(never)]
pub fn min_i64_single_acc(values: &[i64]) -> i64 {
    let mut acc = i64::MAX;
    for &v in values {
        if v < acc {
            acc = v;
        }
    }
    acc
}
