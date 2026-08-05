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

//! JNI bindings for Pinot's native aggregation engine.
//!
//! All exported functions follow the naming convention required by JNI:
//! `Java_<fully_qualified_class_with_underscores>_<methodName>`.
//!
//! Invariants enforced at this layer:
//! * `panic::catch_unwind` around every Rust call. A Rust panic must never
//!   unwind into the JVM.
//! * Returned scalar errors are encoded as the function's "null" sentinel
//!   (NaN for f64, 0 for integer return types) — the Java side checks for
//!   these explicitly. For richer error handling we will introduce an
//!   error-code parameter in Phase 1.D.

use std::panic::{self, AssertUnwindSafe};

use jni::objects::{
    JBooleanArray, JByteArray, JClass, JDoubleArray, JFloatArray, JIntArray, JLongArray, JObject,
    ReleaseMode,
};
use jni::sys::{jbyte, jdouble, jint, jlong};
use jni::JNIEnv;

use pinot_native_groupby::{
    AggKind, AggState, CanonicalF64, CombineSession, DictDirectTable, GroupByBackend,
    GroupByDriverDictInt, HashbrownTable, KeyColType, MultiColStrategy,
    MultiColumnCombineSession, OrderRef, OrderTerm, SegmentTierDriver,
    StringCombineSession, Tier,
};
use pinot_native_kernels::{max, min, sum};

/// Defines a JNI entry point that pins a Java primitive array via
/// `GetPrimitiveArrayCritical`, slices it to the caller-supplied length, and
/// invokes the named kernel on the slice. The kernel is referenced by path so
/// LLVM emits a direct call (no fn-pointer indirection).
///
/// `$empty_value` is the value returned when `length <= 0`. For SUM this is
/// `0.0`; for MIN it is `f64::INFINITY`; for MAX it is `f64::NEG_INFINITY` —
/// matching each aggregation's neutral element / Pinot Java-side default.
///
/// Per-call invariants enforced:
/// * `panic::catch_unwind` — a Rust panic must never unwind into the JVM.
/// * `length` is clamped to the actual Java array length.
/// * `length <= 0` short-circuits to `$empty_value`.
/// * Failure to pin returns `f64::NAN` as the Java-side sentinel.
macro_rules! define_reduce_jni {
    ($name:ident, $jarray:ty, $elem:ty, $kernel:path, $empty_value:expr) => {
        #[no_mangle]
        pub extern "system" fn $name(
            mut env: JNIEnv,
            _class: JClass,
            values: $jarray,
            length: jint,
        ) -> jdouble {
            let result = panic::catch_unwind(AssertUnwindSafe(|| -> jdouble {
                if length <= 0 {
                    return $empty_value;
                }
                // SAFETY: We hold the critical pin for the duration of the kernel call;
                // no JNI calls are made in between. The slice we cast is valid for the
                // lifetime of `auto`.
                let auto = match unsafe {
                    env.get_array_elements_critical(&values, ReleaseMode::NoCopyBack)
                } {
                    Ok(a) => a,
                    Err(_) => return f64::NAN,
                };
                let len_usize = length as usize;
                let array_len = auto.len();
                let effective = if len_usize > array_len {
                    array_len
                } else {
                    len_usize
                };
                // SAFETY: auto.as_ptr() points to a contiguous region of `array_len`
                // elements of the underlying primitive type; `effective <= array_len`.
                let slice: &[$elem] =
                    unsafe { std::slice::from_raw_parts(auto.as_ptr() as *const $elem, effective) };
                $kernel(slice)
            }));
            match result {
                Ok(v) => v,
                Err(_) => f64::NAN,
            }
        }
    };
}

// SUM(LONG) — production path (runtime-dispatched SIMD).
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_sumLong,
    JLongArray,
    i64,
    sum::long::sum_i64_to_f64,
    0.0
);

// SUM(LONG) — benchmark-only forced-scalar variant. Bypasses ISA dispatch so
// `BenchmarkNativeSumLongAggregation` can isolate the SIMD contribution from
// the Rust-language + JNI contribution. Not exposed by `NativeAggregationRouter`.
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_sumLongScalar,
    JLongArray,
    i64,
    sum::long::sum_i64_to_f64_scalar,
    0.0
);

// SUM(INT)
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_sumInt,
    JIntArray,
    i32,
    sum::int::sum_i32_to_f64,
    0.0
);

// SUM(INT) — benchmark-only forced-scalar variant.
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_sumIntScalar,
    JIntArray,
    i32,
    sum::int::sum_i32_to_f64_scalar,
    0.0
);

// SUM(FLOAT)
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_sumFloat,
    JFloatArray,
    f32,
    sum::float::sum_f32_to_f64,
    0.0
);

// SUM(FLOAT) — benchmark-only forced-scalar variant.
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_sumFloatScalar,
    JFloatArray,
    f32,
    sum::float::sum_f32_to_f64_scalar,
    0.0
);

// SUM(DOUBLE)
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_sumDouble,
    JDoubleArray,
    f64,
    sum::double::sum_f64_to_f64,
    0.0
);

// SUM(DOUBLE) — benchmark-only forced-scalar variant.
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_sumDoubleScalar,
    JDoubleArray,
    f64,
    sum::double::sum_f64_to_f64_scalar,
    0.0
);

// MIN(INT)
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_minInt,
    JIntArray,
    i32,
    min::int::min_i32_to_f64,
    f64::INFINITY
);

// MIN(INT) — benchmark-only forced-scalar variant.
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_minIntScalar,
    JIntArray,
    i32,
    min::int::min_i32_to_f64_scalar,
    f64::INFINITY
);

// MIN(LONG)
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_minLong,
    JLongArray,
    i64,
    min::long::min_i64_to_f64,
    f64::INFINITY
);

// MIN(LONG) — benchmark-only forced-scalar variant.
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_minLongScalar,
    JLongArray,
    i64,
    min::long::min_i64_to_f64_scalar,
    f64::INFINITY
);

// MIN(FLOAT)
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_minFloat,
    JFloatArray,
    f32,
    min::float::min_f32_to_f64,
    f64::INFINITY
);

// MIN(FLOAT) — benchmark-only forced-scalar variant.
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_minFloatScalar,
    JFloatArray,
    f32,
    min::float::min_f32_to_f64_scalar,
    f64::INFINITY
);

// MIN(DOUBLE)
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_minDouble,
    JDoubleArray,
    f64,
    min::double::min_f64_to_f64,
    f64::INFINITY
);

// MIN(DOUBLE) — benchmark-only forced-scalar variant.
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_minDoubleScalar,
    JDoubleArray,
    f64,
    min::double::min_f64_to_f64_scalar,
    f64::INFINITY
);

// MAX(INT)
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_maxInt,
    JIntArray,
    i32,
    max::int::max_i32_to_f64,
    f64::NEG_INFINITY
);

// MAX(INT) — benchmark-only forced-scalar variant.
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_maxIntScalar,
    JIntArray,
    i32,
    max::int::max_i32_to_f64_scalar,
    f64::NEG_INFINITY
);

// MAX(LONG)
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_maxLong,
    JLongArray,
    i64,
    max::long::max_i64_to_f64,
    f64::NEG_INFINITY
);

// MAX(LONG) — benchmark-only forced-scalar variant.
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_maxLongScalar,
    JLongArray,
    i64,
    max::long::max_i64_to_f64_scalar,
    f64::NEG_INFINITY
);

// MAX(FLOAT)
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_maxFloat,
    JFloatArray,
    f32,
    max::float::max_f32_to_f64,
    f64::NEG_INFINITY
);

// MAX(FLOAT) — benchmark-only forced-scalar variant.
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_maxFloatScalar,
    JFloatArray,
    f32,
    max::float::max_f32_to_f64_scalar,
    f64::NEG_INFINITY
);

// MAX(DOUBLE)
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_maxDouble,
    JDoubleArray,
    f64,
    max::double::max_f64_to_f64,
    f64::NEG_INFINITY
);

// MAX(DOUBLE) — benchmark-only forced-scalar variant.
define_reduce_jni!(
    Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_maxDoubleScalar,
    JDoubleArray,
    f64,
    max::double::max_f64_to_f64_scalar,
    f64::NEG_INFINITY
);

/// Probe function. Returns a known value so the Java side can verify the
/// native library is loaded and the JNI symbol resolution works before any
/// real kernel is exercised.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_probe(
    _env: JNIEnv,
    _class: JClass,
) -> jint {
    0x5049_4E4F // 'PINO' — magic number for self-test
}

// ===========================================================================
// Phase 1.D-core-D + Plan step (1a) — GROUP BY <aggs...> BY dict-encoded INT
// ===========================================================================
//
// Stateful handle-based JNI surface for the per-segment multi-aggregation
// GROUP BY driver. Two flavors of API are exposed:
//
// **Legacy single-SUM-LONG API** (kept for the existing Java tests + JMH
// harness). Internally creates a multi-agg driver with one `SumLong` agg
// and routes `processBlock` to `process_block_keys` + `apply_long(0, ...)`:
//
//   long handle = createHashbrown(capacityHint);
//   processBlock(handle, dictIds, values, n);
//   int g = numGroups(handle);
//   extractKeys(handle, outKeys);
//   extractSums(handle, outSums);       // shortcut for agg-0-as-long
//   destroy(handle);
//
// **Multi-agg API** (plan step (1a) — Task #60). Caller declares the agg
// list at handle creation time; per block, calls `processBlockKeys` once
// to probe the key column, then `applyAgg<Type>(aggIdx, ...)` once per
// declared aggregation:
//
//   byte[] aggKinds = { SUM_LONG, MIN_LONG, MAX_DOUBLE, COUNT };
//   long handle = createHashbrownMultiAgg(capacityHint, aggKinds);
//   processBlockKeys(handle, dictIds, n);
//   applyAggLong(handle, 0, longCol, n);     // SUM(longCol)
//   applyAggLong(handle, 1, longCol, n);     // MIN(longCol)
//   applyAggDouble(handle, 2, doubleCol, n); // MAX(doubleCol)
//   applyAggCount(handle, 3);                // COUNT(*) — no value column
//   extractKeys(handle, outKeys);
//   extractAggLong(handle, 0, outSumLong);
//   extractAggLong(handle, 1, outMinLong);
//   extractAggDouble(handle, 2, outMaxDouble);
//   extractAggLong(handle, 3, outCount);
//   destroy(handle);
//
// Both flavors share `BoxedDriver` — the legacy API is a thin wrapper
// over the multi-agg driver with a single `SumLong` agg pre-declared.
// Both flavors share `destroy`, `numGroups`, and `extractKeys`.

/// Backend tag stored alongside the boxed driver so the backend-agnostic
/// entry points can dispatch correctly. Stored as the first byte of the
/// boxed handle struct.
#[repr(u8)]
#[derive(Clone, Copy)]
enum DriverTag {
    Hashbrown,
    /// Dict-direct dense array (`DictDirectTable`) — single dict-encoded key only
    /// (never combined with a packed-key `encoder`). Design §22.9 lever 2.
    DictDirect,
}

/// Boxed driver — either backend, identified by the tag. Java holds a
/// `jlong` pointer to this struct.
///
/// One scratch buffer per primitive value type. We need them because
/// `jni-rs`'s `get_array_elements_critical` takes `&mut env`, so we can't
/// hold two critical pins simultaneously — each input array is copied out
/// under its own pin scope, then the scratch slices are passed to the
/// Rust driver. Scratch buffers are reused across all calls on the same
/// handle, so steady-state allocation is zero.
struct BoxedDriver {
    tag: DriverTag,
    hashbrown: Option<GroupByDriverDictInt<HashbrownTable<i32>>>,
    // Dict-direct dense-array backend: single dict-encoded i32 key, no hashing
    // (design §22.9 lever 2). Present iff `tag == DictDirect`; then the hash
    // single-key drivers above and the packed variants below are all `None`.
    dict_direct: Option<GroupByDriverDictInt<DictDirectTable>>,
    // Multi-column, tier-dispatched driver: `SegmentTierDriver` runs the tier
    // ladder (select_tier) and owns its own key encoder + backend. Present iff
    // this is a multi-column GROUP BY; then the single-key drivers are `None`.
    tiered: Option<SegmentTierDriver>,
    dict_id_scratch: Vec<i32>,
    long_scratch: Vec<i64>,
    int_scratch: Vec<i32>,
    double_scratch: Vec<f64>,
    float_scratch: Vec<f32>,
}

/// Dispatch a `&mut` driver method: to the tier-dispatched multi-column driver
/// when present, else the single-column backend selected by `tag`.
macro_rules! driver_mut_dispatch {
    ($self:expr, $method:ident $(, $arg:expr)*) => {
        if let Some(t) = $self.tiered.as_mut() {
            t.$method($($arg),*)
        } else {
            match $self.tag {
                DriverTag::Hashbrown => $self.hashbrown.as_mut().unwrap().$method($($arg),*),
                DriverTag::DictDirect => $self.dict_direct.as_mut().unwrap().$method($($arg),*),
            }
        }
    };
}

/// Dispatch a `&self` driver method: to the tier-dispatched multi-column driver
/// when present, else the single-column backend selected by `tag`.
macro_rules! driver_ref_dispatch {
    ($self:expr, $method:ident $(, $arg:expr)*) => {
        if let Some(t) = $self.tiered.as_ref() {
            t.$method($($arg),*)
        } else {
            match $self.tag {
                DriverTag::Hashbrown => $self.hashbrown.as_ref().unwrap().$method($($arg),*),
                DriverTag::DictDirect => $self.dict_direct.as_ref().unwrap().$method($($arg),*),
            }
        }
    };
}

impl BoxedDriver {
    fn new_hashbrown(capacity_hint: usize, agg_kinds: &[AggKind]) -> Box<Self> {
        Box::new(Self {
            tag: DriverTag::Hashbrown,
            hashbrown: Some(GroupByDriverDictInt::with_capacity(
                agg_kinds,
                capacity_hint,
            )),
            dict_direct: None,
            tiered: None,
            dict_id_scratch: Vec::new(),
            long_scratch: Vec::new(),
            int_scratch: Vec::new(),
            double_scratch: Vec::new(),
            float_scratch: Vec::new(),
        })
    }

    /// Dict-direct dense-array driver — single dict-encoded i32 key, no hashing
    /// (design §22.9 lever 2). `capacity_hint` should be the dictionary length so
    /// the `dict_id -> group_id` slot array is pre-sized (no resize).
    fn new_dict_direct(capacity_hint: usize, agg_kinds: &[AggKind]) -> Box<Self> {
        Box::new(Self {
            tag: DriverTag::DictDirect,
            hashbrown: None,
            dict_direct: Some(GroupByDriverDictInt::with_capacity(
                agg_kinds,
                capacity_hint,
            )),
            tiered: None,
            dict_id_scratch: Vec::new(),
            long_scratch: Vec::new(),
            int_scratch: Vec::new(),
            double_scratch: Vec::new(),
            float_scratch: Vec::new(),
        })
    }

    /// Multi-column, tier-dispatched driver: runs `select_tier` and builds the
    /// matching (key encoder x backend). Returns `None` when the packed key would
    /// exceed 64 bits (the hash tier is i64-only for now) so the caller falls
    /// back to Java.
    fn new_tiered(cardinalities: &[i32], agg_kinds: &[AggKind]) -> Option<Box<Self>> {
        let tiered = SegmentTierDriver::new(cardinalities, agg_kinds)?;
        Some(Box::new(Self {
            tag: DriverTag::Hashbrown, // unused while `tiered` is Some
            hashbrown: None,
            dict_direct: None,
            tiered: Some(tiered),
            dict_id_scratch: Vec::new(),
            long_scratch: Vec::new(),
            int_scratch: Vec::new(),
            double_scratch: Vec::new(),
            float_scratch: Vec::new(),
        }))
    }

    #[inline]
    fn process_block_keys(&mut self, dict_ids: &[i32]) {
        match self.tag {
            DriverTag::Hashbrown => self
                .hashbrown
                .as_mut()
                .unwrap()
                .process_block_keys(dict_ids),
            DriverTag::DictDirect => self
                .dict_direct
                .as_mut()
                .unwrap()
                .process_block_keys(dict_ids),
        }
    }

    /// Feed one multi-column dict-id block (column-major in `flat`, column `c` =
    /// `flat[c*n .. (c+1)*n]`) to the tier-dispatched driver, which packs each
    /// row's composite key and probe-inserts it.
    fn process_block_keys_multi(&mut self, flat: &[i32], num_columns: usize, n: usize) {
        self.tiered
            .as_mut()
            .expect("process_block_keys_multi on a single-key driver")
            .feed_block(flat, num_columns, n);
    }

    #[inline]
    fn apply_long(&mut self, agg_idx: usize, values: &[i64]) {
        driver_mut_dispatch!(self, apply_long, agg_idx, values)
    }

    #[inline]
    fn apply_int(&mut self, agg_idx: usize, values: &[i32]) {
        driver_mut_dispatch!(self, apply_int, agg_idx, values)
    }

    #[inline]
    fn apply_double(&mut self, agg_idx: usize, values: &[f64]) {
        driver_mut_dispatch!(self, apply_double, agg_idx, values)
    }

    #[inline]
    fn apply_float(&mut self, agg_idx: usize, values: &[f32]) {
        driver_mut_dispatch!(self, apply_float, agg_idx, values)
    }

    #[inline]
    fn apply_count(&mut self, agg_idx: usize) {
        driver_mut_dispatch!(self, apply_count, agg_idx)
    }

    #[inline]
    fn num_groups(&self) -> usize {
        driver_ref_dispatch!(self, num_groups)
    }

    #[inline]
    fn num_aggs(&self) -> usize {
        driver_ref_dispatch!(self, num_aggs)
    }

    #[inline]
    fn agg_kind(&self, idx: usize) -> AggKind {
        driver_ref_dispatch!(self, agg_kind, idx)
    }

    #[inline]
    fn keys(&self) -> &[i32] {
        match self.tag {
            DriverTag::Hashbrown => self.hashbrown.as_ref().unwrap().keys(),
            DriverTag::DictDirect => self.dict_direct.as_ref().unwrap().keys(),
        }
    }

    /// Unpack the per-group composite keys into `num_columns` per-column dict-id
    /// arrays (column-major in `out`), inverse of [`Self::process_block_keys_multi`].
    fn extract_keys_multi(&self, out: &mut [i32], num_columns: usize) {
        self.tiered
            .as_ref()
            .expect("extract_keys_multi on a single-key driver")
            .extract_keys(out, num_columns);
    }

    #[inline]
    fn agg_state(&self, idx: usize) -> &AggState {
        driver_ref_dispatch!(self, agg_state, idx)
    }

    /// Selected tier for a multi-column driver, for test observability:
    /// `0 = BitPackedDirect (T1)`, `1 = RadixDirect (T0)`, `2 = BitPackedHash (T2)`,
    /// `-1 = single-column / not tier-dispatched`.
    fn tier_tag(&self) -> i32 {
        match self.tiered.as_ref() {
            Some(t) => match t.tier() {
                Tier::BitPackedDirect => 0,
                Tier::RadixDirect => 1,
                Tier::BitPackedHash => 2,
            },
            None => -1,
        }
    }
}

/// Convert a `jlong` handle to a `&mut BoxedDriver`.
///
/// # Safety
///
/// `handle` must be a valid pointer previously returned by one of the
/// `create*` JNI entries and not yet passed to `destroy`. Caller must
/// ensure no other reference (mutable or shared) exists for the duration.
#[inline]
unsafe fn driver_mut<'a>(handle: jlong) -> &'a mut BoxedDriver {
    &mut *(handle as *mut BoxedDriver)
}

/// Convert a `jlong` handle to a `&BoxedDriver`.
///
/// # Safety
///
/// See [`driver_mut`].
#[inline]
unsafe fn driver_ref<'a>(handle: jlong) -> &'a BoxedDriver {
    &*(handle as *const BoxedDriver)
}

/// Create a SUM(LONG) GROUP BY driver backed by the HashbrownTable wrapper.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupBy_createHashbrown(
    _env: JNIEnv,
    _class: JClass,
    capacity_hint: jint,
) -> jlong {
    let cap = if capacity_hint > 0 {
        capacity_hint as usize
    } else {
        0
    };
    let boxed = BoxedDriver::new_hashbrown(cap, &[AggKind::SumLong]);
    Box::into_raw(boxed) as jlong
}

/// Process one block of `(dict_id, value)` rows for the legacy single-agg
/// driver. Equivalent to `processBlockKeys(handle, dictIds, n)` followed
/// by `applyAggLong(handle, 0, values, n)` on a driver created via
/// `createHashbrown` (which pre-declares agg 0 as
/// `SumLong`). New code should use the explicit multi-agg API.
///
/// JNI marshalling: we can't hold two critical pins simultaneously
/// (`get_array_elements_critical` takes `&mut env`), so each input array
/// is copied into a reusable scratch buffer under its own pin scope, then
/// the scratch slices are passed to the Rust driver. The memcpy cost is
/// minor (~0.5 ns/element at L1 bandwidth) and the scratch buffers are
/// reused across all calls on the same handle, so steady-state allocation
/// is zero.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupBy_processBlock(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    dict_ids: JIntArray,
    values: JLongArray,
    n: jint,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if n <= 0 || handle == 0 {
            return;
        }
        let n = n as usize;
        // Clamp n to the actual Java array lengths before any set_len (Dino review).
        // copy_n below is pin.len().min(n); if the caller passed n larger than either
        // array, we would set_len(n) and then read the uninitialized [copy_n..n) tail.
        // JNI array lengths are immutable, so this also can't race with the pins.
        let n = match (env.get_array_length(&dict_ids), env.get_array_length(&values)) {
            (Ok(dl), Ok(vl)) if dl >= 0 && vl >= 0 => n.min(dl as usize).min(vl as usize),
            _ => return,
        };
        if n == 0 {
            return;
        }
        // SAFETY: handle invariant per `driver_mut`.
        let driver = unsafe { driver_mut(handle) };

        // Size the scratch buffers to exactly n (already clamped to both array lengths
        // above) and zero-fill; the copy_nonoverlapping below overwrites all n elements.
        // resize avoids the set_len-on-uninitialized-memory pattern (clippy::uninit_vec)
        // with no unsafe, and is a no-op memset when the buffer is reused at the same size.
        driver.dict_id_scratch.resize(n, 0);
        driver.long_scratch.resize(n, 0);

        // Phase 1: copy dict_ids under one critical pin scope, then drop.
        {
            let pin = match unsafe {
                env.get_array_elements_critical(&dict_ids, ReleaseMode::NoCopyBack)
            } {
                Ok(p) => p,
                Err(_) => return,
            };
            let copy_n = pin.len().min(n);
            unsafe {
                std::ptr::copy_nonoverlapping(
                    pin.as_ptr(),
                    driver.dict_id_scratch.as_mut_ptr(),
                    copy_n,
                );
            }
        }

        // Phase 2: copy values under a second critical pin scope.
        {
            let pin = match unsafe {
                env.get_array_elements_critical(&values, ReleaseMode::NoCopyBack)
            } {
                Ok(p) => p,
                Err(_) => return,
            };
            let copy_n = pin.len().min(n);
            unsafe {
                std::ptr::copy_nonoverlapping(
                    pin.as_ptr(),
                    driver.long_scratch.as_mut_ptr(),
                    copy_n,
                );
            }
        }

        // Phase 3: hand off to the multi-agg driver. The legacy contract
        // is "probe + sum into agg 0" — implemented as two driver calls.
        // Swap the filled scratch buffers out into locals so the &mut self driver
        // calls do not alias the driver's own fields (see the aliasing note in
        // apply_agg_jni). mem::take is a pointer swap; the buffers are restored
        // afterwards so their capacity is reused on the next block.
        let driver_inner = unsafe { driver_mut(handle) };
        let dict_scratch = std::mem::take(&mut driver_inner.dict_id_scratch);
        let long_scratch = std::mem::take(&mut driver_inner.long_scratch);
        driver_inner.process_block_keys(&dict_scratch);
        driver_inner.apply_long(0, &long_scratch);
        driver_inner.dict_id_scratch = dict_scratch;
        driver_inner.long_scratch = long_scratch;
    }));
}

/// Number of distinct groups accumulated so far. Java uses this to size
/// the output arrays before calling `extractKeys` / `extractSums`.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupBy_numGroups(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jint {
    if handle == 0 {
        return 0;
    }
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: handle invariant per `driver_ref`.
        unsafe { driver_ref(handle) }.num_groups() as jint
    }));
    result.unwrap_or(-1)
}

/// Selected tier for a multi-column driver (test observability): `0` =
/// BitPackedDirect (T1), `1` = RadixDirect (T0), `2` = BitPackedHash (T2),
/// `-1` = single-column / invalid handle.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupBy_tierTag(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jint {
    if handle == 0 {
        return -1;
    }
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: handle invariant per `driver_ref`.
        unsafe { driver_ref(handle) }.tier_tag() as jint
    }));
    result.unwrap_or(-1)
}

/// Copy the per-group keys (the dict_id that created each group, in
/// group_id order) into the caller-supplied output array. `out` must have
/// length >= numGroups; excess slots are not written.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupBy_extractKeys(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    out: JIntArray,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 {
            return;
        }
        // SAFETY: handle invariant per `driver_ref`.
        let driver = unsafe { driver_ref(handle) };
        let keys = driver.keys();

        let out_pin = match unsafe { env.get_array_elements_critical(&out, ReleaseMode::CopyBack) }
        {
            Ok(p) => p,
            Err(_) => return,
        };
        let copy_n = out_pin.len().min(keys.len());
        // SAFETY: out_pin sized >= copy_n; keys sized >= copy_n.
        unsafe {
            std::ptr::copy_nonoverlapping(keys.as_ptr(), out_pin.as_ptr(), copy_n);
        }
    }));
}

/// Copy the per-group sums (parallel to `extractKeys` — `out[g]` is the
/// SUM accumulated for the group whose key is `extractKeys` at index `g`).
///
/// **Legacy single-agg entry point** — extracts agg 0 as a long slice
/// (the `SumLong` agg pre-declared by `createHashbrown`).
/// For multi-agg drivers, use `extractAggLong(handle, 0, out)` or higher.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupBy_extractSums(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    out: JLongArray,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 {
            return;
        }
        let driver = unsafe { driver_ref(handle) };
        let sums = match driver.agg_state(0).as_long_slice() {
            Some(s) => s,
            None => return,
        };

        let out_pin = match unsafe { env.get_array_elements_critical(&out, ReleaseMode::CopyBack) }
        {
            Ok(p) => p,
            Err(_) => return,
        };
        let copy_n = out_pin.len().min(sums.len());
        unsafe {
            std::ptr::copy_nonoverlapping(sums.as_ptr(), out_pin.as_ptr(), copy_n);
        }
    }));
}

/// Free the native driver. The caller MUST invoke this exactly once per
/// `create*` return value, with the handle zeroed on the caller side under a
/// lock BEFORE (or atomically with) this call so it cannot be issued twice for
/// the same pointer. This entry point cannot self-guard against a double free:
/// once `Box::from_raw` reclaims and drops the box, the handle is a dangling
/// pointer, so a second call would be undefined behavior (the `handle == 0`
/// check only rejects a null handle). The Java side enforces exactly-once via
/// the executor's `HandleCleanup` (a synchronized, idempotent claim).
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupBy_destroy(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    if handle == 0 {
        return;
    }
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: handle was produced by Box::into_raw in one of the
        // `create*` entries; calling Box::from_raw exactly once
        // reconstitutes ownership and drops the driver.
        unsafe {
            drop(Box::from_raw(handle as *mut BoxedDriver));
        }
    }));
}

// ===========================================================================
// Plan step (1a) — multi-aggregation JNI entry points (Task #60)
// ===========================================================================
//
// New API for queries with one or more aggregations declared at handle
// creation time. See the module-level docs near `BoxedDriver` for the
// per-block workflow.
//
// The Java side encodes the agg list as a `byte[]` using the same u8
// ordinals as `AggKind` (see `agg.rs`). The Java `enum NativeAggKind` is
// the source of truth for the encoding from Java's side.

/// Decode a Java `byte[]` into a `Vec<AggKind>`. Returns `None` if any
/// byte is not a recognized [`AggKind`] ordinal — the caller rejects the
/// whole driver creation rather than silently dropping unknown aggs.
fn decode_agg_kinds(env: &mut JNIEnv, jarray: &JByteArray) -> Option<Vec<AggKind>> {
    let pin = unsafe { env.get_array_elements_critical(jarray, ReleaseMode::NoCopyBack) }.ok()?;
    let n = pin.len();
    let mut out = Vec::with_capacity(n);
    // SAFETY: pin is a critical pin over a valid Java byte[]; reading n
    // bytes from its start is safe for the duration of the pin.
    let bytes: &[u8] = unsafe { std::slice::from_raw_parts(pin.as_ptr() as *const u8, n) };
    for &b in bytes {
        out.push(AggKind::try_from(b).ok()?);
    }
    Some(out)
}

/// Multi-agg driver constructor (HashbrownTable backend).
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupBy_createHashbrownMultiAgg(
    mut env: JNIEnv,
    _class: JClass,
    capacity_hint: jint,
    agg_kinds: JByteArray,
) -> jlong {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let cap = if capacity_hint > 0 {
            capacity_hint as usize
        } else {
            0
        };
        let kinds = match decode_agg_kinds(&mut env, &agg_kinds) {
            Some(k) => k,
            None => return 0,
        };
        let boxed = BoxedDriver::new_hashbrown(cap, &kinds);
        Box::into_raw(boxed) as jlong
    }));
    result.unwrap_or(0)
}

/// Multi-agg driver constructor (dict-direct dense-array backend) — the SOTA path
/// for a single dict-encoded key (design §22.9 lever 2). No hashing: `dict_id`
/// maps to a dense `group_id` through a slot array. `capacity_hint` should be the
/// dictionary length so the slot array is pre-sized. Otherwise identical contract
/// to [`Java_..._createHashbrownMultiAgg`]. Returns 0 if any kind byte is invalid.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupBy_createDictDirectMultiAgg(
    mut env: JNIEnv,
    _class: JClass,
    capacity_hint: jint,
    agg_kinds: JByteArray,
) -> jlong {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let cap = if capacity_hint > 0 {
            capacity_hint as usize
        } else {
            0
        };
        let kinds = match decode_agg_kinds(&mut env, &agg_kinds) {
            Some(k) => k,
            None => return 0,
        };
        let boxed = BoxedDriver::new_dict_direct(cap, &kinds);
        Box::into_raw(boxed) as jlong
    }));
    result.unwrap_or(0)
}

/// Create a segment GROUP BY driver for any number of dict-encoded key columns
/// (single or multi). Runs the tier ladder internally (`select_tier`) and builds
/// the matching direct/hash backend. Returns 0 if the composite key exceeds 64
/// bits (hash tier is i64-only) — caller falls back to Java.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupBy_createGroupBy(
    mut env: JNIEnv,
    _class: JClass,
    agg_kinds: JByteArray,
    cardinalities: JIntArray,
) -> jlong {
    create_multi_column(&mut env, &agg_kinds, &cardinalities)
}

fn create_multi_column(env: &mut JNIEnv, agg_kinds: &JByteArray, cardinalities: &JIntArray) -> jlong {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let kinds = match decode_agg_kinds(env, agg_kinds) {
            Some(k) => k,
            None => return 0,
        };
        let cards = unsafe { read_int_vec(env, cardinalities) };
        if cards.is_empty() {
            return 0;
        }
        match BoxedDriver::new_tiered(&cards, &kinds) {
            Some(boxed) => Box::into_raw(boxed) as jlong,
            None => 0, // key > 64 bits (hash tier i64-only) — Java falls back.
        }
    }));
    result.unwrap_or(0)
}

/// **Phase 1 (multi-column):** pack `num_columns` dict-id columns (column-major in
/// `dict_ids`, column `c` = `dict_ids[c*n .. (c+1)*n]`) into one i64 key per row and
/// probe/insert them, caching group_ids for the subsequent `applyAgg*` calls.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupBy_processBlockKeys(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    dict_ids: JIntArray,
    num_columns: jint,
    n: jint,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 || num_columns <= 0 || n <= 0 {
            if handle != 0 {
                unsafe { driver_mut(handle) }.process_block_keys_multi(
                    &[],
                    num_columns.max(0) as usize,
                    0,
                );
            }
            return;
        }
        let flat = unsafe { read_int_vec(&mut env, &dict_ids) };
        let cols = num_columns as usize;
        let rows = n as usize;
        if flat.len() < cols * rows {
            return;
        }
        unsafe { driver_mut(handle) }.process_block_keys_multi(&flat, cols, rows);
    }));
}

/// Unpack the combined packed keys into `num_columns` per-column dict-id arrays
/// (column-major in `out`; `out.len() >= num_columns * numGroups`). Inverse of
/// [`Java_..._processBlockKeys`]; each dict_id is then decoded via its column's
/// `Dictionary` on the Java side.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupBy_extractGroupKeys(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    out: JIntArray,
    num_columns: jint,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 || num_columns <= 0 {
            return;
        }
        let driver = unsafe { driver_ref(handle) };
        let cols = num_columns as usize;
        let num_groups = driver.num_groups();
        let needed = cols * num_groups;
        let mut buf = vec![0i32; needed];
        driver.extract_keys_multi(&mut buf, cols);
        if let Ok(pin) = unsafe { env.get_array_elements_critical(&out, ReleaseMode::CopyBack) } {
            let copy_n = pin.len().min(needed);
            unsafe {
                std::ptr::copy_nonoverlapping(buf.as_ptr(), pin.as_ptr(), copy_n)
            };
        }
    }));
}

/// Number of aggregations declared at driver creation time. Returns -1
/// on invalid handle.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupBy_numAggs(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jint {
    if handle == 0 {
        return -1;
    }
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        unsafe { driver_ref(handle) }.num_aggs() as jint
    }));
    result.unwrap_or(-1)
}

/// [`AggKind`] ordinal of the aggregation at `agg_idx`. Returns -1 on
/// invalid handle or out-of-range index.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupBy_aggKindAt(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
    agg_idx: jint,
) -> jbyte {
    if handle == 0 || agg_idx < 0 {
        return -1;
    }
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let d = unsafe { driver_ref(handle) };
        let idx = agg_idx as usize;
        if idx >= d.num_aggs() {
            return -1i8;
        }
        d.agg_kind(idx) as i8
    }));
    result.unwrap_or(-1)
}

/// Macro for the apply_<type> JNI entry points. All share the same
/// shape: pin the value array, copy to typed scratch, dispatch to driver.
macro_rules! apply_agg_jni {
    ($name:ident, $jty:ty, $rty:ty, $scratch:ident, $driver_method:ident) => {
        #[no_mangle]
        pub extern "system" fn $name(
            mut env: JNIEnv,
            _class: JClass,
            handle: jlong,
            agg_idx: jint,
            values: $jty,
            n: jint,
        ) {
            let _ = panic::catch_unwind(AssertUnwindSafe(|| {
                if handle == 0 || agg_idx < 0 {
                    return;
                }
                let n = if n > 0 { n as usize } else { 0 };
                if n == 0 {
                    return;
                }
                // Clamp n to the value array length before set_len (Dino review) so
                // copy_n == n below and the [copy_n..n) tail can't be uninitialized.
                let n = match env.get_array_length(&values) {
                    Ok(vl) if vl >= 0 => n.min(vl as usize),
                    _ => return,
                };
                if n == 0 {
                    return;
                }
                let driver = unsafe { driver_mut(handle) };
                // Size + zero-fill exactly n (clamped above); the copy below overwrites all
                // n. resize avoids clippy::uninit_vec / unsafe set_len (see processBlock).
                driver.$scratch.resize(n, <$rty>::default());

                {
                    let pin = match unsafe {
                        env.get_array_elements_critical(&values, ReleaseMode::NoCopyBack)
                    } {
                        Ok(p) => p,
                        Err(_) => return,
                    };
                    let copy_n = pin.len().min(n);
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            pin.as_ptr() as *const $rty,
                            driver.$scratch.as_mut_ptr(),
                            copy_n,
                        );
                    }
                }

                // Swap the (now-filled) scratch out into a local so the &mut self
                // driver call below does not alias the driver's own field. Passing a
                // slice that borrows driver_inner.$scratch while also taking &mut
                // driver_inner is an aliasing violation (the from_raw_parts launder
                // only hid it from the borrow checker); mem::take removes the alias
                // entirely and is just a pointer swap (capacity preserved on restore).
                let driver_inner = unsafe { driver_mut(handle) };
                let scratch = std::mem::take(&mut driver_inner.$scratch);
                driver_inner.$driver_method(agg_idx as usize, &scratch);
                driver_inner.$scratch = scratch;
            }));
        }
    };
}

apply_agg_jni!(
    Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupBy_applyAggLong,
    JLongArray,
    i64,
    long_scratch,
    apply_long
);
apply_agg_jni!(
    Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupBy_applyAggInt,
    JIntArray,
    i32,
    int_scratch,
    apply_int
);
apply_agg_jni!(
    Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupBy_applyAggDouble,
    JDoubleArray,
    f64,
    double_scratch,
    apply_double
);
apply_agg_jni!(
    Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupBy_applyAggFloat,
    JFloatArray,
    f32,
    float_scratch,
    apply_float
);

/// COUNT — no value array. Increments per-group count by the number of
/// rows in the last processBlockKeys call.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupBy_applyAggCount(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
    agg_idx: jint,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 || agg_idx < 0 {
            return;
        }
        let driver = unsafe { driver_mut(handle) };
        driver.apply_count(agg_idx as usize);
    }));
}

/// Extract macro for the typed result arrays. `out` is a Java primitive
/// array (length >= numGroups); excess slots are not written.
macro_rules! extract_agg_jni {
    ($name:ident, $jty:ty, $rty:ty, $accessor:ident) => {
        #[no_mangle]
        pub extern "system" fn $name(
            mut env: JNIEnv,
            _class: JClass,
            handle: jlong,
            agg_idx: jint,
            out: $jty,
        ) {
            let _ = panic::catch_unwind(AssertUnwindSafe(|| {
                if handle == 0 || agg_idx < 0 {
                    return;
                }
                let driver = unsafe { driver_ref(handle) };
                let idx = agg_idx as usize;
                if idx >= driver.num_aggs() {
                    return;
                }
                let src = match driver.agg_state(idx).$accessor() {
                    Some(s) => s,
                    None => return,
                };
                let pin =
                    match unsafe { env.get_array_elements_critical(&out, ReleaseMode::CopyBack) } {
                        Ok(p) => p,
                        Err(_) => return,
                    };
                let copy_n = pin.len().min(src.len());
                unsafe {
                    std::ptr::copy_nonoverlapping(src.as_ptr(), pin.as_ptr() as *mut $rty, copy_n);
                }
            }));
        }
    };
}

extract_agg_jni!(
    Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupBy_extractAggLong,
    JLongArray,
    i64,
    as_long_slice
);
extract_agg_jni!(
    Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupBy_extractAggInt,
    JIntArray,
    i32,
    as_int_slice
);
extract_agg_jni!(
    Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupBy_extractAggDouble,
    JDoubleArray,
    f64,
    as_double_slice
);
extract_agg_jni!(
    Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupBy_extractAggFloat,
    JFloatArray,
    f32,
    as_float_slice
);

// Suppress unused-import warnings for the GroupByBackend trait, which is
// referenced only indirectly through `BoxedDriver`'s typed members.
const _: fn() = || {
    fn _backend_used<B: GroupByBackend<i32>>(_: &B) {}
};

// ===========================================================================
// Server-level combine JNI surface — all key types (Task #54, step 5 wiring)
// ===========================================================================
//
// Drives the native cross-segment combine from Pinot's combine operator, over
// any grouping-key type: INT/LONG -> i64, FLOAT/DOUBLE -> CanonicalF64, STRING
// -> arena. Per segment the Java side materializes dict_id -> raw value and
// feeds the partial: beginPartial<KeyType>(keys) + setAgg<Type>(aggIdx, values)
// per aggregation + commitPartial(). After all segments, finish() runs the
// radix-partitioned parallel merge; extract* drains the combined result.

/// Combine key type tag passed from Java at session creation.
const KEY_TYPE_LONG: jint = 0;
const KEY_TYPE_DOUBLE: jint = 1;
const KEY_TYPE_STRING: jint = 2;

/// Multi-column merge strategy tag (design B / C), passed to `createCombineMulti`.
const MULTICOL_COLUMNWISE: jint = 0;
const MULTICOL_PACKED: jint = 1;

/// Native fixed-width DataTable-V4 serialize op codes (design "native-serialize").
/// Each output column maps to one op that fully determines how a group's value is
/// read from the merged result and written (big-endian) into the row-major buffer.
/// Aggregations are always read as f64 (combine merges in double); the op casts to
/// the DataTable's stored intermediate type. Keys are read as i64 (INT/LONG surface)
/// or f64 (FLOAT/DOUBLE surface) and cast to the stored key type.
const SER_AGG_TO_I64: i32 = 0; // COUNT (intermediate LONG)
const SER_AGG_TO_F64: i32 = 1; // SUM / MIN / MAX (intermediate DOUBLE)
const SER_AGG_TO_I32: i32 = 2; // agg intermediate INT (future kernels)
const SER_AGG_TO_F32: i32 = 3; // agg intermediate FLOAT (future kernels)
const SER_KEY_LONG_TO_I32: i32 = 4; // INT key
const SER_KEY_LONG_TO_I64: i32 = 5; // LONG key
const SER_KEY_DOUBLE_TO_F32: i32 = 6; // FLOAT key
const SER_KEY_DOUBLE_TO_F64: i32 = 7; // DOUBLE key

/// Combine session, monomorphized per key type. Strings use a single arena-backed session.
enum BoxedCombine {
    LongHashbrown(CombineSession<i64, HashbrownTable<i64>>),
    DoubleHashbrown(CombineSession<CanonicalF64, HashbrownTable<CanonicalF64>>),
    Strings(StringCombineSession),
    /// Multi-column: groups on the tuple of N raw key columns (design §17.9 / §23.1).
    MultiColumn(MultiColumnCombineSession),
}

/// Delegate a method call across every variant (the agg + finish + result-count
/// surface is identical for `CombineSession<K,_>`, `StringCombineSession`, and
/// `MultiColumnCombineSession`).
macro_rules! delegate_all {
    ($self:expr, $method:ident $(, $arg:expr)*) => {
        match $self {
            BoxedCombine::LongHashbrown(s) => s.$method($($arg),*),
            BoxedCombine::DoubleHashbrown(s) => s.$method($($arg),*),
            BoxedCombine::Strings(s) => s.$method($($arg),*),
            BoxedCombine::MultiColumn(s) => s.$method($($arg),*),
        }
    };
}

impl BoxedCombine {
    fn set_agg_long(&mut self, i: usize, v: Vec<i64>) {
        delegate_all!(self, set_agg_long, i, v)
    }
    fn set_agg_int(&mut self, i: usize, v: Vec<i32>) {
        delegate_all!(self, set_agg_int, i, v)
    }
    fn set_agg_double(&mut self, i: usize, v: Vec<f64>) {
        delegate_all!(self, set_agg_double, i, v)
    }
    fn set_agg_float(&mut self, i: usize, v: Vec<f32>) {
        delegate_all!(self, set_agg_float, i, v)
    }
    fn commit_partial(&mut self) {
        delegate_all!(self, commit_partial)
    }
    fn finish(&mut self, radix_bits: u32) {
        delegate_all!(self, finish, radix_bits)
    }
    fn result_num_groups(&self) -> usize {
        delegate_all!(self, result_num_groups)
    }
    fn result_agg(&self, i: usize) -> &AggState {
        delegate_all!(self, result_agg, i)
    }
    /// Apply ORDER BY top-K / no-ORDER-BY cap to the combined result in place
    /// (after `finish`), reducing it to `result_size` groups per `order`
    /// (design §26.3). Uniform across every key variant: i64 and `CanonicalF64`
    /// both implement `OrderKey`, and `StringCombineSession::select` orders keys
    /// lexicographically — so the delegate covers all surfaces.
    fn select(&mut self, order: &[OrderTerm], result_size: usize) {
        delegate_all!(self, select, order, result_size)
    }

    fn begin_partial_long(&mut self, keys: Vec<i64>) {
        match self {
            BoxedCombine::LongHashbrown(s) => s.begin_partial(keys),
            _ => panic!("beginPartialLong on a non-long combine session"),
        }
    }
    fn begin_partial_double(&mut self, keys: Vec<f64>) {
        let canon: Vec<CanonicalF64> = keys.into_iter().map(CanonicalF64::new).collect();
        match self {
            BoxedCombine::DoubleHashbrown(s) => s.begin_partial(canon),
            _ => panic!("beginPartialDouble on a non-double combine session"),
        }
    }
    fn begin_partial_string(&mut self, buffer: Vec<u8>, offsets: Vec<i32>) {
        match self {
            BoxedCombine::Strings(s) => s.begin_partial(buffer, offsets),
            _ => panic!("beginPartialString on a non-string combine session"),
        }
    }

    // --- multi-column key surface (BoxedCombine::MultiColumn only) ---
    fn begin_partial_multi(&mut self) {
        match self {
            BoxedCombine::MultiColumn(s) => s.begin_partial(),
            _ => panic!("beginPartialMulti on a non-multi-column combine session"),
        }
    }
    fn set_key_long(&mut self, col_idx: usize, values: Vec<i64>) {
        match self {
            BoxedCombine::MultiColumn(s) => s.set_key_long(col_idx, values),
            _ => panic!("setKeyLong on a non-multi-column combine session"),
        }
    }
    fn set_key_double(&mut self, col_idx: usize, values: Vec<f64>) {
        match self {
            BoxedCombine::MultiColumn(s) => s.set_key_double(col_idx, values),
            _ => panic!("setKeyDouble on a non-multi-column combine session"),
        }
    }
    fn set_key_string(&mut self, col_idx: usize, buffer: Vec<u8>, offsets: Vec<i32>) {
        match self {
            BoxedCombine::MultiColumn(s) => s.set_key_string(col_idx, buffer, offsets),
            _ => panic!("setKeyString on a non-multi-column combine session"),
        }
    }
    fn result_key_column_long(&self, col_idx: usize) -> &[i64] {
        match self {
            BoxedCombine::MultiColumn(s) => s.result_key_column(col_idx).as_long(),
            _ => &[],
        }
    }
    fn result_key_column_double_at(&self, col_idx: usize, g: usize) -> f64 {
        match self {
            BoxedCombine::MultiColumn(s) => s.result_key_column(col_idx).double_at(g),
            _ => f64::NAN,
        }
    }
    fn result_key_column_string_parts(&self, col_idx: usize) -> (&[u8], &[i32]) {
        match self {
            BoxedCombine::MultiColumn(s) => s.result_key_column(col_idx).string_parts(),
            _ => (&[], &[]),
        }
    }
    fn result_keys_long(&self) -> &[i64] {
        match self {
            BoxedCombine::LongHashbrown(s) => s.result_keys(),
            _ => &[],
        }
    }
    fn result_key_double_at(&self, g: usize) -> f64 {
        match self {
            BoxedCombine::DoubleHashbrown(s) => s.result_keys()[g].to_f64(),
            _ => f64::NAN,
        }
    }
    fn result_string_buffer(&self) -> &[u8] {
        match self {
            BoxedCombine::Strings(s) => s.result_string_buffer(),
            _ => &[],
        }
    }
    fn result_string_offsets(&self) -> &[i32] {
        match self {
            BoxedCombine::Strings(s) => s.result_string_offsets(),
            _ => &[],
        }
    }

    /// Merged+selected i64 key slice for column `col` (single-key sessions ignore
    /// `col`). Used by the fixed-width native serialize for INT/LONG key columns.
    fn key_i64_slice(&self, col: usize) -> &[i64] {
        match self {
            BoxedCombine::LongHashbrown(s) => s.result_keys(),
            BoxedCombine::MultiColumn(s) => s.result_key_column(col).as_long(),
            _ => &[],
        }
    }

    /// Merged+selected f64 key value at group `g` for column `col` (FLOAT/DOUBLE
    /// key surface). Single-key double sessions ignore `col`.
    fn key_f64_at(&self, col: usize, g: usize) -> f64 {
        match self {
            BoxedCombine::DoubleHashbrown(s) => s.result_keys()[g].to_f64(),
            BoxedCombine::MultiColumn(s) => s.result_key_column(col).double_at(g),
            _ => f64::NAN,
        }
    }

    /// Build the DataTable-V4 fixed-size row-major byte buffer directly from the
    /// merged+selected native result — the "native-serialize" fast path. `ops`,
    /// `indices`, `offsets` are parallel per-output-column: `ops[c]` is a `SER_*`
    /// code, `indices[c]` the key-column or aggregation index it reads, `offsets[c]`
    /// its byte offset within a `row_size`-byte row. Fixed-width columns only (the
    /// Java side gates STRING/BYTES and `serverReturnFinalResult` to the boxed path).
    /// Values are written big-endian to match Pinot's `ByteBuffer` DataTable layout.
    fn serialize_fixed_width(
        &self,
        ops: &[i32],
        indices: &[i32],
        offsets: &[i32],
        row_size: usize,
    ) -> Vec<u8> {
        let n = self.result_num_groups();
        let mut buf = vec![0u8; n * row_size];
        for c in 0..ops.len() {
            let idx = indices[c] as usize;
            let off = offsets[c] as usize;
            match ops[c] {
                SER_AGG_TO_I64 => {
                    let src = self.result_agg(idx).as_double_slice().unwrap_or(&[]);
                    for g in 0..n {
                        let p = g * row_size + off;
                        buf[p..p + 8].copy_from_slice(&(src[g] as i64).to_be_bytes());
                    }
                }
                SER_AGG_TO_F64 => {
                    let src = self.result_agg(idx).as_double_slice().unwrap_or(&[]);
                    for g in 0..n {
                        let p = g * row_size + off;
                        buf[p..p + 8].copy_from_slice(&src[g].to_be_bytes());
                    }
                }
                SER_AGG_TO_I32 => {
                    let src = self.result_agg(idx).as_double_slice().unwrap_or(&[]);
                    for g in 0..n {
                        let p = g * row_size + off;
                        buf[p..p + 4].copy_from_slice(&(src[g] as i32).to_be_bytes());
                    }
                }
                SER_AGG_TO_F32 => {
                    let src = self.result_agg(idx).as_double_slice().unwrap_or(&[]);
                    for g in 0..n {
                        let p = g * row_size + off;
                        buf[p..p + 4].copy_from_slice(&(src[g] as f32).to_be_bytes());
                    }
                }
                SER_KEY_LONG_TO_I64 => {
                    let keys = self.key_i64_slice(idx);
                    for g in 0..n {
                        let p = g * row_size + off;
                        buf[p..p + 8].copy_from_slice(&keys[g].to_be_bytes());
                    }
                }
                SER_KEY_LONG_TO_I32 => {
                    let keys = self.key_i64_slice(idx);
                    for g in 0..n {
                        let p = g * row_size + off;
                        buf[p..p + 4].copy_from_slice(&(keys[g] as i32).to_be_bytes());
                    }
                }
                SER_KEY_DOUBLE_TO_F64 => {
                    for g in 0..n {
                        let p = g * row_size + off;
                        buf[p..p + 8].copy_from_slice(&self.key_f64_at(idx, g).to_be_bytes());
                    }
                }
                SER_KEY_DOUBLE_TO_F32 => {
                    for g in 0..n {
                        let p = g * row_size + off;
                        buf[p..p + 4]
                            .copy_from_slice(&(self.key_f64_at(idx, g) as f32).to_be_bytes());
                    }
                }
                _ => {}
            }
        }
        buf
    }
}

unsafe fn combine_mut<'a>(handle: jlong) -> &'a mut BoxedCombine {
    &mut *(handle as *mut BoxedCombine)
}
unsafe fn combine_ref<'a>(handle: jlong) -> &'a BoxedCombine {
    &*(handle as *const BoxedCombine)
}

// --- input array readers (Java array -> owned Vec, via a critical pin) ---

unsafe fn read_long_vec(env: &mut JNIEnv, arr: &JLongArray) -> Vec<i64> {
    match env.get_array_elements_critical(arr, ReleaseMode::NoCopyBack) {
        Ok(pin) => {
            let n = pin.len();
            let mut v = vec![0i64; n];
            std::ptr::copy_nonoverlapping(pin.as_ptr(), v.as_mut_ptr(), n);
            v
        }
        Err(_) => Vec::new(),
    }
}
unsafe fn read_int_vec(env: &mut JNIEnv, arr: &JIntArray) -> Vec<i32> {
    match env.get_array_elements_critical(arr, ReleaseMode::NoCopyBack) {
        Ok(pin) => {
            let n = pin.len();
            let mut v = vec![0i32; n];
            std::ptr::copy_nonoverlapping(pin.as_ptr(), v.as_mut_ptr(), n);
            v
        }
        Err(_) => Vec::new(),
    }
}
unsafe fn read_double_vec(env: &mut JNIEnv, arr: &JDoubleArray) -> Vec<f64> {
    match env.get_array_elements_critical(arr, ReleaseMode::NoCopyBack) {
        Ok(pin) => {
            let n = pin.len();
            let mut v = vec![0.0f64; n];
            std::ptr::copy_nonoverlapping(pin.as_ptr(), v.as_mut_ptr(), n);
            v
        }
        Err(_) => Vec::new(),
    }
}
unsafe fn read_float_vec(env: &mut JNIEnv, arr: &JFloatArray) -> Vec<f32> {
    match env.get_array_elements_critical(arr, ReleaseMode::NoCopyBack) {
        Ok(pin) => {
            let n = pin.len();
            let mut v = vec![0.0f32; n];
            std::ptr::copy_nonoverlapping(pin.as_ptr(), v.as_mut_ptr(), n);
            v
        }
        Err(_) => Vec::new(),
    }
}
unsafe fn read_byte_vec(env: &mut JNIEnv, arr: &JByteArray) -> Vec<u8> {
    match env.get_array_elements_critical(arr, ReleaseMode::NoCopyBack) {
        Ok(pin) => {
            let n = pin.len();
            let mut v = vec![0u8; n];
            std::ptr::copy_nonoverlapping(pin.as_ptr() as *const u8, v.as_mut_ptr(), n);
            v
        }
        Err(_) => Vec::new(),
    }
}
unsafe fn read_bool_vec(env: &mut JNIEnv, arr: &JBooleanArray) -> Vec<bool> {
    match env.get_array_elements_critical(arr, ReleaseMode::NoCopyBack) {
        Ok(pin) => {
            let n = pin.len();
            let ptr = pin.as_ptr() as *const u8;
            (0..n).map(|i| *ptr.add(i) != 0).collect()
        }
        Err(_) => Vec::new(),
    }
}

/// Order-term ref code base for group-by key columns: key column `c` is encoded
/// as `ORDER_REF_KEY - c` (i.e. -1 → key col 0, -2 → key col 1, …), so any
/// negative code selects a key column and non-negative codes are aggregation
/// indices. Single-key ORDER BY uses `ORDER_REF_KEY` (key col 0).
const ORDER_REF_KEY: i32 = -1;

/// Decode the JNI order spec (`refs[t]` + `ascending[t]`, parallel arrays) into
/// `OrderTerm`s. `refs[t] < 0` → `OrderRef::Key((ORDER_REF_KEY - refs[t]))`;
/// `refs[t] >= 0` → `OrderRef::Agg(refs[t])`. A missing ascending flag defaults
/// to ascending.
fn build_order_terms(refs: &[i32], ascending: &[bool]) -> Vec<OrderTerm> {
    refs.iter()
        .enumerate()
        .map(|(t, &r)| {
            let order_ref = if r <= ORDER_REF_KEY {
                OrderRef::Key((ORDER_REF_KEY - r) as usize)
            } else {
                OrderRef::Agg(r as usize)
            };
            OrderTerm::new(order_ref, ascending.get(t).copied().unwrap_or(true))
        })
        .collect()
}

#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_createCombine(
    mut env: JNIEnv,
    _class: JClass,
    agg_kinds: JByteArray,
    key_type: jint,
) -> jlong {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let kinds = match decode_agg_kinds(&mut env, &agg_kinds) {
            Some(k) => k,
            None => return 0,
        };
        let boxed = match key_type {
            KEY_TYPE_LONG => Box::new(BoxedCombine::LongHashbrown(CombineSession::new(&kinds))),
            KEY_TYPE_DOUBLE => Box::new(BoxedCombine::DoubleHashbrown(CombineSession::new(&kinds))),
            KEY_TYPE_STRING => Box::new(BoxedCombine::Strings(StringCombineSession::new(&kinds))),
            _ => return 0,
        };
        Box::into_raw(boxed) as jlong
    }));
    result.unwrap_or(0)
}

/// Create a multi-column combine session over `col_types` (one byte per grouping
/// Create a multi-column combine session over `col_types` (one byte per grouping
/// column: {@code KEY_TYPE_LONG=0 / DOUBLE=1 / STRING=2}), with per-column type
/// widths in bits `col_widths` (32 for INT/FLOAT, 64 for LONG/DOUBLE, 0 for
/// STRING — used only by the PackedKeys strategy) and a `strategy` selector
/// ({@code MULTICOL_COLUMNWISE=0 / MULTICOL_PACKED=1}). Groups on the tuple of N
/// raw key values (design §17.9 / §23.1). Returns 0 on invalid input.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_createCombineMulti(
    mut env: JNIEnv,
    _class: JClass,
    agg_kinds: JByteArray,
    col_types: JByteArray,
    col_widths: JIntArray,
    strategy: jint,
) -> jlong {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let kinds = match decode_agg_kinds(&mut env, &agg_kinds) {
            Some(k) => k,
            None => return 0,
        };
        let raw = unsafe { read_byte_vec(&mut env, &col_types) };
        if raw.is_empty() {
            return 0;
        }
        let mut types = Vec::with_capacity(raw.len());
        for b in raw {
            let t = match b as jint {
                KEY_TYPE_LONG => KeyColType::Long,
                KEY_TYPE_DOUBLE => KeyColType::Double,
                KEY_TYPE_STRING => KeyColType::String,
                _ => return 0,
            };
            types.push(t);
        }
        let widths_i32 = unsafe { read_int_vec(&mut env, &col_widths) };
        if widths_i32.len() != types.len() {
            return 0;
        }
        let widths: Vec<u32> = widths_i32.iter().map(|&w| w.max(0) as u32).collect();
        let strat = match strategy {
            MULTICOL_PACKED => MultiColStrategy::PackedKeys,
            MULTICOL_COLUMNWISE => MultiColStrategy::ColumnWise,
            _ => MultiColStrategy::ColumnWise,
        };
        let boxed = Box::new(BoxedCombine::MultiColumn(MultiColumnCombineSession::new(
            &kinds, &types, &widths, strat,
        )));
        Box::into_raw(boxed) as jlong
    }));
    result.unwrap_or(0)
}

/// Begin a multi-column segment partial. Follow with one {@code setKey*(colIdx, ...)}
/// per key column and one {@code setAgg*(aggIdx, ...)} per aggregation, then commit.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_beginPartialMulti(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 {
            return;
        }
        unsafe { combine_mut(handle) }.begin_partial_multi();
    }));
}

#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_setKeyLong(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    col_idx: jint,
    values: JLongArray,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 || col_idx < 0 {
            return;
        }
        let v = unsafe { read_long_vec(&mut env, &values) };
        unsafe { combine_mut(handle) }.set_key_long(col_idx as usize, v);
    }));
}

#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_setKeyDouble(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    col_idx: jint,
    values: JDoubleArray,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 || col_idx < 0 {
            return;
        }
        let v = unsafe { read_double_vec(&mut env, &values) };
        unsafe { combine_mut(handle) }.set_key_double(col_idx as usize, v);
    }));
}

#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_setKeyString(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    col_idx: jint,
    buffer: JByteArray,
    offsets: JIntArray,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 || col_idx < 0 {
            return;
        }
        let buf = unsafe { read_byte_vec(&mut env, &buffer) };
        let offs = unsafe { read_int_vec(&mut env, &offsets) };
        unsafe { combine_mut(handle) }.set_key_string(col_idx as usize, buf, offs);
    }));
}

/// Copy combined key column `col_idx`'s LONG values into `out` (parallel to the
/// aggregation extraction; `out.length >= numGroups`).
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_extractKeyColumnLong(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    col_idx: jint,
    out: JLongArray,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 || col_idx < 0 {
            return;
        }
        let keys = unsafe { combine_ref(handle) }.result_key_column_long(col_idx as usize);
        if let Ok(pin) = unsafe { env.get_array_elements_critical(&out, ReleaseMode::CopyBack) } {
            let n = pin.len().min(keys.len());
            unsafe { std::ptr::copy_nonoverlapping(keys.as_ptr(), pin.as_ptr(), n) };
        }
    }));
}

/// Copy combined key column `col_idx`'s DOUBLE values into `out`.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_extractKeyColumnDouble(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    col_idx: jint,
    out: JDoubleArray,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 || col_idx < 0 {
            return;
        }
        let session = unsafe { combine_ref(handle) };
        let g = session.result_num_groups();
        if let Ok(pin) = unsafe { env.get_array_elements_critical(&out, ReleaseMode::CopyBack) } {
            let n = pin.len().min(g);
            let dst = pin.as_ptr();
            for i in 0..n {
                unsafe { *dst.add(i) = session.result_key_column_double_at(col_idx as usize, i) };
            }
        }
    }));
}

/// Total bytes of combined key column `col_idx`'s STRING values — the caller sizes its buffer.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_keyColumnStringTotalBytes(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
    col_idx: jint,
) -> jint {
    if handle == 0 || col_idx < 0 {
        return 0;
    }
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let (buffer, _) =
            unsafe { combine_ref(handle) }.result_key_column_string_parts(col_idx as usize);
        buffer.len() as jint
    }));
    result.unwrap_or(0)
}

/// Copy combined key column `col_idx`'s STRING keys: `bufferOut` (>= totalBytes)
/// gets the concatenated bytes, `offsetsOut` (>= numGroups + 1) the cumulative offsets.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_extractKeyColumnString(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    col_idx: jint,
    buffer_out: JByteArray,
    offsets_out: JIntArray,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 || col_idx < 0 {
            return;
        }
        let (buffer, offsets) =
            unsafe { combine_ref(handle) }.result_key_column_string_parts(col_idx as usize);
        if let Ok(pin) =
            unsafe { env.get_array_elements_critical(&buffer_out, ReleaseMode::CopyBack) }
        {
            let n = pin.len().min(buffer.len());
            unsafe { std::ptr::copy_nonoverlapping(buffer.as_ptr(), pin.as_ptr() as *mut u8, n) };
        }
        if let Ok(pin) =
            unsafe { env.get_array_elements_critical(&offsets_out, ReleaseMode::CopyBack) }
        {
            let n = pin.len().min(offsets.len());
            unsafe { std::ptr::copy_nonoverlapping(offsets.as_ptr(), pin.as_ptr(), n) };
        }
    }));
}

#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_beginPartialLong(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    keys: JLongArray,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 {
            return;
        }
        let v = unsafe { read_long_vec(&mut env, &keys) };
        unsafe { combine_mut(handle) }.begin_partial_long(v);
    }));
}

#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_beginPartialDouble(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    keys: JDoubleArray,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 {
            return;
        }
        let v = unsafe { read_double_vec(&mut env, &keys) };
        unsafe { combine_mut(handle) }.begin_partial_double(v);
    }));
}

#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_beginPartialString(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    buffer: JByteArray,
    offsets: JIntArray,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 {
            return;
        }
        let buf = unsafe { read_byte_vec(&mut env, &buffer) };
        let offs = unsafe { read_int_vec(&mut env, &offsets) };
        unsafe { combine_mut(handle) }.begin_partial_string(buf, offs);
    }));
}

#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_setAggLong(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    agg_idx: jint,
    values: JLongArray,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 || agg_idx < 0 {
            return;
        }
        let v = unsafe { read_long_vec(&mut env, &values) };
        unsafe { combine_mut(handle) }.set_agg_long(agg_idx as usize, v);
    }));
}

#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_setAggInt(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    agg_idx: jint,
    values: JIntArray,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 || agg_idx < 0 {
            return;
        }
        let v = unsafe { read_int_vec(&mut env, &values) };
        unsafe { combine_mut(handle) }.set_agg_int(agg_idx as usize, v);
    }));
}

#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_setAggDouble(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    agg_idx: jint,
    values: JDoubleArray,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 || agg_idx < 0 {
            return;
        }
        let v = unsafe { read_double_vec(&mut env, &values) };
        unsafe { combine_mut(handle) }.set_agg_double(agg_idx as usize, v);
    }));
}

#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_setAggFloat(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    agg_idx: jint,
    values: JFloatArray,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 || agg_idx < 0 {
            return;
        }
        let v = unsafe { read_float_vec(&mut env, &values) };
        unsafe { combine_mut(handle) }.set_agg_float(agg_idx as usize, v);
    }));
}

#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_commitPartial(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 {
            return;
        }
        unsafe { combine_mut(handle) }.commit_partial();
    }));
}

#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_finish(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
    radix_bits: jint,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 {
            return;
        }
        let bits = if radix_bits < 0 { 0 } else { radix_bits as u32 };
        unsafe { combine_mut(handle) }.finish(bits);
    }));
}

/// Apply ORDER BY top-K / no-ORDER-BY cap to the combined result, in place,
/// between `finish` and the `extract*` drain (design §26.3/§26.7).
///
/// * `order_refs` + `order_ascending` are parallel arrays, one entry per ORDER
///   BY term. `order_refs[t] < 0` orders by the group key; `>= 0` orders by
///   aggregation-result column `order_refs[t]`. Empty → no-ORDER-BY cap.
/// * `result_size` is the number of groups to keep (`LIMIT` with no ORDER BY;
///   `trimSize = max(LIMIT*5, 5000)` with ORDER BY). Negative is clamped to 0.
///
/// After this call `numGroups` / `extractKeys*` / `extractAgg*` reflect the
/// selected (and, for ORDER BY, sorted) subset.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_select(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    order_refs: JIntArray,
    order_ascending: JBooleanArray,
    result_size: jint,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 {
            return;
        }
        let refs = unsafe { read_int_vec(&mut env, &order_refs) };
        let ascending = unsafe { read_bool_vec(&mut env, &order_ascending) };
        let order = build_order_terms(&refs, &ascending);
        let rs = if result_size < 0 {
            0
        } else {
            result_size as usize
        };
        unsafe { combine_mut(handle) }.select(&order, rs);
    }));
}

#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_numGroups(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jint {
    if handle == 0 {
        return -1;
    }
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        unsafe { combine_ref(handle) }.result_num_groups() as jint
    }));
    result.unwrap_or(-1)
}

/// Native-serialize the merged+selected result into a DataTable-V4 fixed-size
/// row-major byte buffer (big-endian), returned as a fresh Java `byte[]`. Skips the
/// per-group Java `Record`/`Object[]` materialization entirely. `ops`/`indices`/
/// `offsets` are parallel per-output-column descriptors (see `SER_*` and
/// `BoxedCombine::serialize_fixed_width`). Returns a null array on invalid handle or
/// panic (the Java side then falls back to the boxed path).
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_serializeFixedWidth<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    ops: JIntArray<'local>,
    indices: JIntArray<'local>,
    offsets: JIntArray<'local>,
    row_size: jint,
) -> JByteArray<'local> {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 || row_size <= 0 {
            return JByteArray::from(JObject::null());
        }
        let ops_v = unsafe { read_int_vec(&mut env, &ops) };
        let idx_v = unsafe { read_int_vec(&mut env, &indices) };
        let off_v = unsafe { read_int_vec(&mut env, &offsets) };
        let bytes = unsafe { combine_ref(handle) }.serialize_fixed_width(
            &ops_v,
            &idx_v,
            &off_v,
            row_size as usize,
        );
        env.byte_array_from_slice(&bytes)
            .unwrap_or_else(|_| JByteArray::from(JObject::null()))
    }));
    result.unwrap_or_else(|_| JByteArray::from(JObject::null()))
}

#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_extractKeysLong(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    out: JLongArray,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 {
            return;
        }
        let keys = unsafe { combine_ref(handle) }.result_keys_long();
        if let Ok(pin) = unsafe { env.get_array_elements_critical(&out, ReleaseMode::CopyBack) } {
            let n = pin.len().min(keys.len());
            unsafe { std::ptr::copy_nonoverlapping(keys.as_ptr(), pin.as_ptr(), n) };
        }
    }));
}

#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_extractKeysDouble(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    out: JDoubleArray,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 {
            return;
        }
        let session = unsafe { combine_ref(handle) };
        let g = session.result_num_groups();
        if let Ok(pin) = unsafe { env.get_array_elements_critical(&out, ReleaseMode::CopyBack) } {
            let n = pin.len().min(g);
            let dst = pin.as_ptr();
            for i in 0..n {
                unsafe { *dst.add(i) = session.result_key_double_at(i) };
            }
        }
    }));
}

/// Total bytes of all combined string keys — the caller sizes its key buffer.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_stringKeysTotalBytes(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jint {
    if handle == 0 {
        return -1;
    }
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        unsafe { combine_ref(handle) }.result_string_buffer().len() as jint
    }));
    result.unwrap_or(-1)
}

/// Copy the combined string keys: `bufferOut` (>= stringKeysTotalBytes) gets the
/// concatenated bytes, `offsetsOut` (>= numGroups + 1) gets cumulative offsets.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_extractKeysString(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    buffer_out: JByteArray,
    offsets_out: JIntArray,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 {
            return;
        }
        let session = unsafe { combine_ref(handle) };
        let buffer = session.result_string_buffer();
        let offsets = session.result_string_offsets();
        if let Ok(pin) =
            unsafe { env.get_array_elements_critical(&buffer_out, ReleaseMode::CopyBack) }
        {
            let n = pin.len().min(buffer.len());
            unsafe { std::ptr::copy_nonoverlapping(buffer.as_ptr(), pin.as_ptr() as *mut u8, n) };
        }
        if let Ok(pin) =
            unsafe { env.get_array_elements_critical(&offsets_out, ReleaseMode::CopyBack) }
        {
            let n = pin.len().min(offsets.len());
            unsafe { std::ptr::copy_nonoverlapping(offsets.as_ptr(), pin.as_ptr(), n) };
        }
    }));
}

#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_extractAggLong(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    agg_idx: jint,
    out: JLongArray,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 || agg_idx < 0 {
            return;
        }
        let src = match unsafe { combine_ref(handle) }
            .result_agg(agg_idx as usize)
            .as_long_slice()
        {
            Some(s) => s,
            None => return,
        };
        if let Ok(pin) = unsafe { env.get_array_elements_critical(&out, ReleaseMode::CopyBack) } {
            let n = pin.len().min(src.len());
            unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), pin.as_ptr(), n) };
        }
    }));
}

#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_extractAggInt(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    agg_idx: jint,
    out: JIntArray,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 || agg_idx < 0 {
            return;
        }
        let src = match unsafe { combine_ref(handle) }
            .result_agg(agg_idx as usize)
            .as_int_slice()
        {
            Some(s) => s,
            None => return,
        };
        if let Ok(pin) = unsafe { env.get_array_elements_critical(&out, ReleaseMode::CopyBack) } {
            let n = pin.len().min(src.len());
            unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), pin.as_ptr(), n) };
        }
    }));
}

#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_extractAggDouble(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    agg_idx: jint,
    out: JDoubleArray,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 || agg_idx < 0 {
            return;
        }
        let src = match unsafe { combine_ref(handle) }
            .result_agg(agg_idx as usize)
            .as_double_slice()
        {
            Some(s) => s,
            None => return,
        };
        if let Ok(pin) = unsafe { env.get_array_elements_critical(&out, ReleaseMode::CopyBack) } {
            let n = pin.len().min(src.len());
            unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), pin.as_ptr(), n) };
        }
    }));
}

#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_extractAggFloat(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    agg_idx: jint,
    out: JFloatArray,
) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if handle == 0 || agg_idx < 0 {
            return;
        }
        let src = match unsafe { combine_ref(handle) }
            .result_agg(agg_idx as usize)
            .as_float_slice()
        {
            Some(s) => s,
            None => return,
        };
        if let Ok(pin) = unsafe { env.get_array_elements_critical(&out, ReleaseMode::CopyBack) } {
            let n = pin.len().min(src.len());
            unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), pin.as_ptr(), n) };
        }
    }));
}

/// Free the native combine session. Exactly-once contract identical to the
/// segment driver `destroy` above: the caller must zero the handle under a lock
/// before/with this call (the combine operator's synchronized `destroyHandle`);
/// this entry point cannot self-guard against a double free.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_groupby_PinotNativeGroupByCombine_destroy(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    if handle == 0 {
        return;
    }
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        drop(unsafe { Box::from_raw(handle as *mut BoxedCombine) });
    }));
}
