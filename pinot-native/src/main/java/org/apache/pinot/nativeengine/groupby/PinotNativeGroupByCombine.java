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
 * Java entry points to Pinot's native server-level GROUP BY <b>combine</b> engine (Task #54) — a
 * radix-partitioned, work-stealing parallel cross-segment merge in Rust, over any grouping-key type.
 *
 * <p>Combine keys are raw values (dict ids are segment-local; the segment boundary materializes
 * {@code dict_id → raw value}). The key type is fixed at session creation via {@link #createCombine}:
 * <ul>
 *   <li>{@link #KEY_TYPE_LONG} — INT/LONG keys (raw value as {@code long}).</li>
 *   <li>{@link #KEY_TYPE_DOUBLE} — FLOAT/DOUBLE keys (raw value as {@code double}, canonicalized).</li>
 *   <li>{@link #KEY_TYPE_STRING} — STRING/BYTES keys (UTF-8 / raw bytes via an arena).</li>
 * </ul>
 *
 * <p>Per segment: {@code beginPartial<KeyType>(keys)} + one {@code setAgg<Type>(aggIdx, values)} per
 * aggregation (typed per the agg's accumulator: i64 for SUM_LONG/MIN_LONG/MAX_LONG/COUNT, f64 for
 * SUM_*_TO_DOUBLE/SUM_DOUBLE/MIN_DOUBLE/MAX_DOUBLE, etc.) + {@code commitPartial()}. After all
 * segments, {@code finish(radixBits)} runs the parallel merge; {@code extract*} drains the result.
 *
 * <p>String keys cross the boundary flattened: a byte buffer plus a cumulative {@code int[]} offset
 * array (length {@code numGroups + 1}), so there is no per-key allocation.
 *
 * <p><b>Thread safety:</b> a handle is not thread-safe; drive it from one thread. The merge itself
 * parallelizes internally inside {@link #finish}.
 */
public final class PinotNativeGroupByCombine {
  public static final int KEY_TYPE_LONG = 0;
  public static final int KEY_TYPE_DOUBLE = 1;
  public static final int KEY_TYPE_STRING = 2;

  /**
   * ORDER BY term ref code selecting the (single) group-by key column, for {@link #select}. Any
   * negative code orders by the group key; non-negative codes are aggregation-result column indices.
   */
  public static final int ORDER_REF_KEY = -1;

  /** Multi-column merge strategy: DataFusion-style column-wise typed hashing/equality (design B). */
  public static final int MULTICOL_STRATEGY_COLUMNWISE = 0;
  /** Multi-column merge strategy: ClickHouse-style pack-into-wide-integer keys (design C). */
  public static final int MULTICOL_STRATEGY_PACKED = 1;

  /**
   * Native-serialize op codes for {@link #serializeFixedWidth}. Each output column maps to one op that
   * fully determines how a group's value is read from the merged result and written (big-endian) into
   * the DataTable-V4 row-major buffer. Aggregations are read as {@code double} (combine merges in
   * double) and cast to the stored intermediate type; keys are read as {@code long} (INT/LONG) or
   * {@code double} (FLOAT/DOUBLE) and cast to the stored key type.
   */
  public static final int SER_AGG_TO_I64 = 0;
  public static final int SER_AGG_TO_F64 = 1;
  public static final int SER_AGG_TO_I32 = 2;
  public static final int SER_AGG_TO_F32 = 3;
  public static final int SER_KEY_LONG_TO_I32 = 4;
  public static final int SER_KEY_LONG_TO_I64 = 5;
  public static final int SER_KEY_DOUBLE_TO_F32 = 6;
  public static final int SER_KEY_DOUBLE_TO_F64 = 7;

  private static final boolean AVAILABLE;

  static {
    AVAILABLE = PinotNativeAgg.isAvailable();
  }

  private PinotNativeGroupByCombine() {
  }

  public static boolean isAvailable() {
    return AVAILABLE;
  }

  /**
   * Create a combine session.
   *
   * @param aggKinds one {@link PinotNativeGroupBy.NativeAggKind} ordinal byte per aggregation.
   * @param keyType  one of {@link #KEY_TYPE_LONG} / {@link #KEY_TYPE_DOUBLE} / {@link #KEY_TYPE_STRING}.
   * @return opaque handle; pass to {@link #destroy(long)} exactly once. {@code 0} on invalid input.
   */
  public static native long createCombine(byte[] aggKinds, int keyType);

  /**
   * Create a <b>multi-column</b> combine session that groups on the tuple of N raw key columns
   * (design §17.9 / §23.1 — combine keys are raw values, since dict-ids are segment-local).
   *
   * @param aggKinds  one {@link PinotNativeGroupBy.NativeAggKind} ordinal byte per aggregation.
   * @param colTypes  one byte per grouping column: {@link #KEY_TYPE_LONG} / {@link #KEY_TYPE_DOUBLE} /
   *                  {@link #KEY_TYPE_STRING} (in grouping order).
   * @param colWidths per-column type width in bits (32 for INT/FLOAT, 64 for LONG/DOUBLE, 0 for
   *                  STRING); used only by {@link #MULTICOL_STRATEGY_PACKED}.
   * @param strategy  {@link #MULTICOL_STRATEGY_COLUMNWISE} (B) or {@link #MULTICOL_STRATEGY_PACKED} (C).
   *                  PackedKeys falls back to column-wise when a STRING column is present or the packed
   *                  key would exceed 128 bits.
   * @return opaque handle; pass to {@link #destroy(long)} exactly once. {@code 0} on invalid input.
   */
  public static native long createCombineMulti(byte[] aggKinds, byte[] colTypes, int[] colWidths, int strategy);

  /**
   * Begin a multi-column segment partial. Follow with one {@code setKey*(colIdx, values)} per key
   * column and one {@code setAgg*(aggIdx, values)} per aggregation, then {@link #commitPartial(long)}.
   */
  public static native void beginPartialMulti(long handle);

  /** Set key column {@code colIdx}'s LONG values for the staged multi-column partial. */
  public static native void setKeyLong(long handle, int colIdx, long[] values);

  /** Set key column {@code colIdx}'s DOUBLE values for the staged multi-column partial. */
  public static native void setKeyDouble(long handle, int colIdx, double[] values);

  /**
   * Set key column {@code colIdx}'s STRING values for the staged multi-column partial: {@code buffer}
   * holds all values concatenated, {@code offsets} is cumulative (length {@code numGroups + 1}).
   */
  public static native void setKeyString(long handle, int colIdx, byte[] buffer, int[] offsets);

  /** Copy combined key column {@code colIdx}'s LONG values into {@code out} ({@code >= numGroups}). */
  public static native void extractKeyColumnLong(long handle, int colIdx, long[] out);

  /** Copy combined key column {@code colIdx}'s DOUBLE values into {@code out} ({@code >= numGroups}). */
  public static native void extractKeyColumnDouble(long handle, int colIdx, double[] out);

  /** @return total bytes of combined key column {@code colIdx}'s STRING values — size your buffer. */
  public static native int keyColumnStringTotalBytes(long handle, int colIdx);

  /**
   * Copy combined key column {@code colIdx}'s STRING keys: {@code bufferOut}
   * (>= {@link #keyColumnStringTotalBytes}) gets the concatenated bytes, {@code offsetsOut}
   * (>= {@code numGroups + 1}) the cumulative offsets.
   */
  public static native void extractKeyColumnString(long handle, int colIdx, byte[] bufferOut, int[] offsetsOut);

  /** Begin a LONG-keyed segment partial. */
  public static native void beginPartialLong(long handle, long[] keys);

  /** Begin a DOUBLE-keyed segment partial. */
  public static native void beginPartialDouble(long handle, double[] keys);

  /**
   * Begin a STRING-keyed segment partial: {@code buffer} holds all keys concatenated, {@code offsets}
   * is cumulative (length {@code numGroups + 1}; group g = {@code buffer[offsets[g]..offsets[g+1]]}).
   */
  public static native void beginPartialString(long handle, byte[] buffer, int[] offsets);

  public static native void setAggLong(long handle, int aggIdx, long[] values);

  public static native void setAggInt(long handle, int aggIdx, int[] values);

  public static native void setAggDouble(long handle, int aggIdx, double[] values);

  public static native void setAggFloat(long handle, int aggIdx, float[] values);

  /** Finalize the staged partial and add it to the set to be combined. */
  public static native void commitPartial(long handle);

  /** Run the radix-partitioned parallel merge over all committed partials ({@code 2^radixBits} partitions). */
  public static native void finish(long handle, int radixBits);

  /**
   * Apply the ORDER BY top-K / no-ORDER-BY cap to the combined result, in place, between
   * {@link #finish} and the {@code extract*} drain (design §26.3/§26.7).
   *
   * <p>{@code orderRefs} and {@code orderAscending} are parallel arrays, one entry per ORDER BY
   * term: {@code orderRefs[t] < 0} (see {@link #ORDER_REF_KEY}) orders by the group key, while
   * {@code orderRefs[t] >= 0} orders by aggregation-result column {@code orderRefs[t]};
   * {@code orderAscending[t]} is the per-term direction. An <b>empty</b> {@code orderRefs} means
   * no ORDER BY — the result is capped to the first {@code resultSize} groups in storage order.
   *
   * <p>{@code resultSize} is the number of groups to keep: {@code LIMIT} for no ORDER BY,
   * {@code trimSize = max(LIMIT * 5, 5000)} for ORDER BY. After this call {@link #numGroups},
   * {@code extractKeys*} and {@code extractAgg*} reflect the selected (and, for ORDER BY, sorted)
   * subset. No-op if {@link #finish} has not run.
   */
  public static native void select(long handle, int[] orderRefs, boolean[] orderAscending, int resultSize);

  /** @return combined group count after {@link #finish}, or {@code -1} on invalid handle. */
  public static native int numGroups(long handle);

  /**
   * Native-serialize the merged+selected result directly into a DataTable-V4 fixed-size row-major
   * byte buffer (big-endian), returned as a fresh {@code byte[]} of {@code numGroups * rowSize} bytes.
   * Skips the per-group Java {@code Record}/{@code Object[]} materialization entirely (design
   * "native-serialize"). Fixed-width output columns only — the caller must gate STRING/BYTES columns
   * and {@code serverReturnFinalResult} to the boxed path.
   *
   * @param ops     parallel per-output-column {@code SER_*} op code (see {@link #SER_AGG_TO_I64} etc.).
   * @param indices parallel per-column source index (aggregation index, or key-column index for keys).
   * @param offsets parallel per-column byte offset within a row (from {@code DataTableUtils.
   *                computeColumnOffsets}).
   * @param rowSize total bytes per row.
   * @return the fixed-size byte buffer, or {@code null} on invalid handle / internal error.
   */
  public static native byte[] serializeFixedWidth(long handle, int[] ops, int[] indices, int[] offsets,
      int rowSize);

  /** Copy combined LONG keys into {@code out} ({@code out.length >= numGroups}). */
  public static native void extractKeysLong(long handle, long[] out);

  /** Copy combined DOUBLE keys into {@code out} ({@code out.length >= numGroups}). */
  public static native void extractKeysDouble(long handle, double[] out);

  /** @return total bytes of all combined STRING keys — the caller sizes its key buffer. */
  public static native int stringKeysTotalBytes(long handle);

  /**
   * Copy combined STRING keys: {@code bufferOut} (>= {@link #stringKeysTotalBytes}) gets the
   * concatenated bytes, {@code offsetsOut} (>= {@code numGroups + 1}) gets cumulative offsets.
   */
  public static native void extractKeysString(long handle, byte[] bufferOut, int[] offsetsOut);

  /** Copy aggregation {@code aggIdx}'s combined i64 result (SUM_LONG/MIN_LONG/MAX_LONG/COUNT). */
  public static native void extractAggLong(long handle, int aggIdx, long[] out);

  /** Copy aggregation {@code aggIdx}'s combined i32 result (MIN_INT/MAX_INT). */
  public static native void extractAggInt(long handle, int aggIdx, int[] out);

  /** Copy aggregation {@code aggIdx}'s combined f64 result (SUM_*_TO_DOUBLE/SUM_DOUBLE/MIN/MAX double). */
  public static native void extractAggDouble(long handle, int aggIdx, double[] out);

  /** Copy aggregation {@code aggIdx}'s combined f32 result (MIN_FLOAT/MAX_FLOAT). */
  public static native void extractAggFloat(long handle, int aggIdx, float[] out);

  /** Free the combine session. No-op on {@code 0}. */
  public static native void destroy(long handle);
}
