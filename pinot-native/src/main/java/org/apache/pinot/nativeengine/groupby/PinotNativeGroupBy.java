/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.nativeengine.groupby;

import org.apache.pinot.nativeengine.agg.PinotNativeAgg;


/**
 * Java entry points to Pinot's native GROUP BY engine.
 *
 * <p>The Java side creates a native per-segment driver (hashbrown or dict-direct), feeds dict-encoded
 * keys and aggregation inputs block-by-block, then extracts grouped keys and aggregation results for
 * downstream combine.
 *
 * <p>The native library is loaded lazily on first reference to this class. We piggyback on
 * {@link PinotNativeAgg}'s static initializer so the library is loaded exactly once per
 * JVM regardless of which entry-point class the caller touches first.
 *
 * <p><b>Thread safety:</b> handles are NOT thread-safe. The native driver maintains internal
 * state across {@code processBlock} calls; concurrent access on the same handle is
 * undefined behavior. Per-segment, single-threaded use only — which matches Pinot's
 * segment-level GROUP BY execution model.
 */
public final class PinotNativeGroupBy {

  private static final boolean AVAILABLE;

  static {
    // Touching PinotNativeAgg triggers its static block, which calls
    // NativeLibLoader.tryLoad(). All native symbols (including ours below) resolve
    // from the same loaded libpinot_native.{dylib,so,dll}.
    AVAILABLE = PinotNativeAgg.isAvailable();
  }

  private PinotNativeGroupBy() {
  }

  /**
   * @return {@code true} if the native library was loaded successfully and group-by
   * entry points are callable; callers must fall back to the Java GROUP BY path otherwise.
   */
  public static boolean isAvailable() {
    return AVAILABLE;
  }

  // --- Handle lifecycle ---------------------------------------------------

  /**
   * Create a SUM(LONG) GROUP BY driver backed by the hashbrown-wrapped backend
   * (DataFusion-style: hashbrown::hash_table::HashTable + parallel Vec&lt;K&gt; for keys).
   *
   * @param capacityHint upper bound on the per-segment group count.
   * @return opaque native handle; caller must pass to {@link #destroy(long)} exactly once.
   */
  public static native long createHashbrown(int capacityHint);

  /**
   * Free the native driver. After this call, the handle is invalid and must not be
   * passed to any other method. Calling {@code destroy} on an already-destroyed handle
   * or on zero is a no-op (safe).
   */
  public static native void destroy(long handle);

  // --- Block processing ---------------------------------------------------

  /**
   * Process one block of {@code (dictId, value)} rows. For each row {@code i}:
   * <ul>
   *   <li>Probe-or-insert {@code dictIds[i]} into the table to obtain its {@code group_id}.</li>
   *   <li>Accumulate {@code sums[group_id] += values[i]} (wrapping i64 addition — matches
   *       Pinot's {@code SumAggregationFunction} LONG semantics).</li>
   * </ul>
   *
   * <p>Both arrays must have length {@code >= n}. Extra trailing elements are ignored.
   *
   * @param handle a handle from {@link #createHashbrown(int)}.
   * @param dictIds dict-encoded column values for this block, one per row.
   * @param values  SUM column values for this block, one per row.
   * @param n       number of rows in the block to process.
   */
  public static native void processBlock(long handle, int[] dictIds, long[] values, int n);

  // --- Result extraction --------------------------------------------------

  /**
   * @return number of distinct groups accumulated so far across all {@code processBlock}
   * calls on this handle. Returns {@code -1} on invalid handle.
   */
  public static native int numGroups(long handle);

  /**
   * Selected key-table tier for a multi-column handle (test observability):
   * {@code 0} = BitPackedDirect (T1), {@code 1} = RadixDirect (T0),
   * {@code 2} = BitPackedHash (T2), {@code -1} = single-column / invalid handle.
   */
  public static native int tierTag(long handle);

  /**
   * Copy the per-group dict_ids (the dict_id that created each group, in group_id order)
   * into {@code out}. {@code out.length} must be {@code >=} {@link #numGroups(long)};
   * excess trailing slots are not written.
   *
   * <p>Phase 1.D-core-I (Task #52) consumes this output to materialize raw column values
   * via the segment's local {@code Dictionary} for cross-segment combine compatibility.
   */
  public static native void extractKeys(long handle, int[] out);

  /**
   * Copy the per-group sums into {@code out}, parallel to {@link #extractKeys(long, int[])} —
   * {@code out[g]} is the SUM accumulated for the group whose key is at index {@code g}
   * in the output of {@code extractKeys}.
   */
  public static native void extractSums(long handle, long[] out);

  // ===========================================================================
  // Multi-aggregation API (plan step (1a) — Task #60)
  // ===========================================================================
  //
  // Caller declares the aggregation list at handle creation time, then per
  // block calls processBlockKeys once to probe the key column, followed by
  // one applyAgg<Type>() call per declared aggregation. Each apply call
  // reuses the cached group_ids from processBlockKeys — so the probe cost
  // is paid once per block regardless of agg count.
  //
  // Example: SELECT SUM(longCol), MIN(longCol), MAX(doubleCol), COUNT(*)
  //                FROM t GROUP BY dictCol
  //
  //   byte[] aggKinds = {
  //       NativeAggKind.SUM_LONG.ordinal,
  //       NativeAggKind.MIN_LONG.ordinal,
  //       NativeAggKind.MAX_DOUBLE.ordinal,
  //       NativeAggKind.COUNT.ordinal,
  //   };
  //   long h = PinotNativeGroupBy.createHashbrownMultiAgg(capacityHint, aggKinds);
  //   try {
  //     for (Block block : segment.blocks()) {
  //       PinotNativeGroupBy.processBlockKeys(h, block.dictIds(), block.n());
  //       PinotNativeGroupBy.applyAggLong(h, 0, block.longCol(), block.n());
  //       PinotNativeGroupBy.applyAggLong(h, 1, block.longCol(), block.n());
  //       PinotNativeGroupBy.applyAggDouble(h, 2, block.doubleCol(), block.n());
  //       PinotNativeGroupBy.applyAggCount(h, 3);
  //     }
  //     int g = PinotNativeGroupBy.numGroups(h);
  //     int[] keys = new int[g];
  //     long[] sums = new long[g];
  //     long[] mins = new long[g];
  //     double[] maxes = new double[g];
  //     long[] counts = new long[g];
  //     PinotNativeGroupBy.extractKeys(h, keys);
  //     PinotNativeGroupBy.extractAggLong(h, 0, sums);
  //     PinotNativeGroupBy.extractAggLong(h, 1, mins);
  //     PinotNativeGroupBy.extractAggDouble(h, 2, maxes);
  //     PinotNativeGroupBy.extractAggLong(h, 3, counts);
  //   } finally {
  //     PinotNativeGroupBy.destroy(h);
  //   }

  /**
   * Aggregation kind ordinals — must match the Rust {@code AggKind} u8 encoding
   * in {@code pinot-native/native/groupby/src/agg.rs}. The byte value of each
   * enum constant is what gets passed to {@code createHashbrownMultiAgg} /
   * {@code createDictDirectMultiAgg} via the {@code aggKinds} byte array.
   */
  public enum NativeAggKind {
    SUM_LONG((byte) 0),
    SUM_DOUBLE((byte) 1),
    MIN_INT((byte) 2),
    MIN_LONG((byte) 3),
    MIN_FLOAT((byte) 4),
    MIN_DOUBLE((byte) 5),
    MAX_INT((byte) 6),
    MAX_LONG((byte) 7),
    MAX_FLOAT((byte) 8),
    MAX_DOUBLE((byte) 9),
    COUNT((byte) 10),
    // SUM accumulated in f64 with a typed input array — matches Pinot's
    // group-by SUM, which reads every numeric column via getDoubleValuesSV()
    // and accumulates in a double holder. SUM_LONG_TO_DOUBLE is the
    // Pinot-parity counterpart to SUM_LONG (which wraps in i64).
    SUM_INT_TO_DOUBLE((byte) 11),
    SUM_LONG_TO_DOUBLE((byte) 12),
    SUM_FLOAT_TO_DOUBLE((byte) 13);

    public final byte _ordinalByte;

    NativeAggKind(byte ordinalByte) {
      _ordinalByte = ordinalByte;
    }

    /** Convenience: pack a list of kinds into the {@code byte[]} the JNI surface expects. */
    public static byte[] toBytes(NativeAggKind... kinds) {
      byte[] out = new byte[kinds.length];
      for (int i = 0; i < kinds.length; i++) {
        out[i] = kinds[i]._ordinalByte;
      }
      return out;
    }

    /** Decode a byte returned by {@link PinotNativeGroupBy#aggKindAt(long, int)} back to an enum. */
    public static NativeAggKind fromByte(byte b) {
      for (NativeAggKind k : values()) {
        if (k._ordinalByte == b) {
          return k;
        }
      }
      throw new IllegalArgumentException("Unknown native agg kind ordinal: " + b);
    }
  }

  /**
   * Create a multi-aggregation GROUP BY driver backed by the HashbrownTable wrapper.
   *
   * @param capacityHint upper bound on per-segment group count.
   * @param aggKinds     one byte per aggregation, in the order subsequent
   *                     {@code applyAgg*(aggIdx, ...)} calls will reference them.
   */
  public static native long createHashbrownMultiAgg(int capacityHint, byte[] aggKinds);

  /**
   * Create a multi-aggregation GROUP BY driver backed by the <b>dict-direct</b> dense-array backend —
   * the SOTA path for a single dict-encoded key (design §22.9 lever 2). Instead of hashing the
   * {@code dict_id}, it maps {@code dict_id -> group_id} through a slot array (no hash, no probe), which
   * is what Pinot's Java {@code ArrayBasedHolder} does. Prefer this for single dict-encoded fixed-width
   * keys whose cardinality is small enough that a {@code u32}-per-dict-id slot array is affordable;
   * above that threshold the caller falls back to the hash backends.
   *
   * @param capacityHint the dictionary length ({@code Dictionary.length()}) so the slot array is
   *                     pre-sized (no resize).
   * @param aggKinds     one {@link NativeAggKind} ordinal byte per aggregation (same contract as
   *                     {@link #createHashbrownMultiAgg(int, byte[])}).
   */
  public static native long createDictDirectMultiAgg(int capacityHint, byte[] aggKinds);

  /**
   * Create a segment GROUP BY driver for any number of dict-encoded key columns (single or multi), running the tier
   * ladder internally.
   *
   * @param aggKinds      one {@link NativeAggKind} ordinal byte per aggregation.
   * @param cardinalities one dict cardinality per grouping column, in grouping order.
   * @return opaque handle, or {@code 0} if the packed key would exceed 64 bits or on invalid input.
   */
  public static native long createGroupBy(byte[] aggKinds, int[] cardinalities);

  /**
   * <b>Phase 1 (multi-column):</b> pack {@code numColumns} dict-id columns into one {@code i64} key
   * per row and probe-or-insert them, caching {@code group_ids} for the subsequent {@code applyAgg*}
   * calls. {@code dictIds} is <b>column-major</b>: column {@code c} occupies
   * {@code dictIds[c*n .. (c+1)*n]} ({@code dictIds.length >= numColumns * n}).
   */
  public static native void processBlockKeys(long handle, int[] dictIds, int numColumns, int n);

  /**
   * Unpack the combined packed keys into {@code numColumns} per-column dict-id arrays, <b>column-major</b>
   * in {@code out} (column {@code c} = {@code out[c*numGroups .. (c+1)*numGroups]};
   * {@code out.length >= numColumns * numGroups}). Each dict_id is then decoded via its column's
   * {@code Dictionary}. Inverse of {@link #processBlockKeys(long, int[], int, int)}.
   */
  public static native void extractGroupKeys(long handle, int[] out, int numColumns);

  /** @return number of aggregations declared at driver creation, or {@code -1} on invalid handle. */
  public static native int numAggs(long handle);

  /**
   * @return the {@link NativeAggKind} ordinal byte for the aggregation at {@code aggIdx},
   * or {@code -1} on invalid handle / out-of-range index.
   */
  public static native byte aggKindAt(long handle, int aggIdx);

  /**
   * <b>Phase 2:</b> apply an i64-valued aggregation (SUM_LONG, MIN_LONG, MAX_LONG) at
   * index {@code aggIdx} to the {@code values[0..n)} array, using the cached
   * {@code group_ids} from the most recent {@link #processBlockKeys(long, int[], int, int)}.
   * {@code values.length} must equal the {@code n} passed to {@code processBlockKeys}.
   */
  public static native void applyAggLong(long handle, int aggIdx, long[] values, int n);

  /** <b>Phase 2:</b> apply an i32-valued aggregation (MIN_INT, MAX_INT). See {@link #applyAggLong}. */
  public static native void applyAggInt(long handle, int aggIdx, int[] values, int n);

  /**
   * <b>Phase 2:</b> apply an f64-valued aggregation (SUM_DOUBLE, MIN_DOUBLE, MAX_DOUBLE).
   * MIN / MAX propagate NaN per Java {@code Math.min} / {@code Math.max} semantics.
   * See {@link #applyAggLong}.
   */
  public static native void applyAggDouble(long handle, int aggIdx, double[] values, int n);

  /** <b>Phase 2:</b> apply an f32-valued aggregation (MIN_FLOAT, MAX_FLOAT). See {@link #applyAggDouble}
   *  for NaN semantics. */
  public static native void applyAggFloat(long handle, int aggIdx, float[] values, int n);

  /**
   * <b>Phase 2:</b> apply COUNT — increments per-group count by 1 for each row in the
   * last {@link #processBlockKeys(long, int[], int, int)} call. Takes no value array because
   * COUNT only depends on which group each row landed in.
   */
  public static native void applyAggCount(long handle, int aggIdx);

  /**
   * Copy the per-group i64 result for aggregation {@code aggIdx} into {@code out}.
   * Works for SUM_LONG, MIN_LONG, MAX_LONG, COUNT. Silent no-op if {@code aggIdx}
   * doesn't refer to an i64-valued agg or if {@code out.length < numGroups()}.
   */
  public static native void extractAggLong(long handle, int aggIdx, long[] out);

  /** Copy the per-group i32 result for MIN_INT / MAX_INT. See {@link #extractAggLong}. */
  public static native void extractAggInt(long handle, int aggIdx, int[] out);

  /** Copy the per-group f64 result for SUM_DOUBLE / MIN_DOUBLE / MAX_DOUBLE. */
  public static native void extractAggDouble(long handle, int aggIdx, double[] out);

  /** Copy the per-group f32 result for MIN_FLOAT / MAX_FLOAT. */
  public static native void extractAggFloat(long handle, int aggIdx, float[] out);
}
