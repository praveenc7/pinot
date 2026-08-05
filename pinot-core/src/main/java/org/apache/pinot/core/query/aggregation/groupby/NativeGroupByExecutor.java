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
package org.apache.pinot.core.query.aggregation.groupby;

import java.lang.ref.Cleaner;
import java.util.Collection;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.TableResizer;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.nativeengine.groupby.PinotNativeGroupBy;
import org.apache.pinot.nativeengine.groupby.PinotNativeGroupBy.NativeAggKind;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * A {@link GroupByExecutor} that delegates the per-segment GROUP BY (key probe + per-group
 * aggregation) to the native Rust engine via {@link PinotNativeGroupBy}'s multi-aggregation API.
 *
 * <p>Scope (design doc §19 step 1b/1c, §17.9): one or more dict-encoded single-value group-by keys
 * of a fixed-width type (INT/LONG/FLOAT/DOUBLE) — a single key uses the i32 dict-id driver, multiple
 * keys pack their dict-ids into one i64 key ({@code createMultiColumn*}) — and aggregations drawn
 * from {SUM, MIN, MAX, COUNT} over simple single-value fixed-width columns. Eligibility (including
 * the multi-column packability bound) is enforced by {@link NativeGroupByRouter} before this executor
 * is constructed.
 *
 * <p>Per block the executor probes the key column's dict ids once ({@code processBlockKeys}) and
 * then applies each aggregation's value column ({@code applyAgg*}); the probe cost is amortized
 * across all aggregations. On {@link #getResult()} the native per-group results are drained into
 * standard double {@link GroupByResultHolder}s and the group keys are exposed via a
 * {@link NativeGroupKeyGenerator}, so the upstream combine path is unchanged.
 *
 * <p><b>SUM semantics:</b> Pinot's Java group-by SUM reads every numeric column as {@code double}
 * ({@code getDoubleValuesSV()}) and accumulates in a double holder. To match bit-for-bit while
 * avoiding a per-block {@code double[]} widening on the Java side, SUM is routed through the
 * typed-input / f64-accumulator native kinds ({@code SUM_*_TO_DOUBLE}). MIN/MAX stay typed (lossless
 * when later viewed as double) and COUNT stays i64. See the design doc §15 entry dated 2026-06-19.
 *
 * <p><b>Threading / lifecycle:</b> the native handle is not thread-safe; this executor is
 * single-threaded per segment (matching Pinot's segment GROUP BY model). The handle is freed eagerly
 * at the end of {@link #getResult()} (after all data is materialized into Java structures) and, as a
 * safety net for paths that never call {@code getResult}, by a {@link Cleaner} when the executor is
 * collected.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class NativeGroupByExecutor implements GroupByExecutor {
  // Per-aggregation value families used to dispatch the typed apply / extract JNI calls.
  private static final int FAMILY_INT = 0;
  private static final int FAMILY_LONG = 1;
  private static final int FAMILY_FLOAT = 2;
  private static final int FAMILY_DOUBLE = 3;
  private static final int FAMILY_COUNT = 4;

  /**
   * The segment key-table backend is chosen natively by the tier ladder
   * ({@code select_tier}): dense no-hash direct addressing (bit-packed T1 /
   * mixed-radix T0) for low-cardinality dict keys, else a hashbrown hash table
   * (T2). Single- and multi-column dict-encoded GROUP BY share this one path;
   * there is no Java-side backend override.
   */

  private static final Cleaner CLEANER = Cleaner.create();

  /**
   * When {@code -Dpinot.native.groupby.profile=true}, the executor accumulates per-phase wall time
   * (key probe / agg apply / result drain) and logs a breakdown when the segment result is
   * materialized. Off by default — gated so the production path has no timing overhead. Used to
   * attribute where the segment-operator time goes (the §22.9 lever-0 attribution).
   */
  private static final boolean PROFILE = Boolean.getBoolean("pinot.native.groupby.profile");

  private final AggregationFunction[] _aggregationFunctions;
  private final ExpressionContext[] _keyExpressions;
  private final Dictionary[] _keyDictionaries;
  private final int _numKeyColumns;
  // Reused per-block column-major buffer for the multi-column packed path (numKeyColumns * blockLen).
  private int[] _multiKeyScratch;

  // Per-aggregation metadata, parallel to _aggregationFunctions.
  private final ExpressionContext[] _aggInputExpressions; // null for COUNT
  private final int[] _applyFamily;                       // how to read + apply the value column
  private final int[] _extractFamily;                     // how to read back the native result

  private final HandleCleanup _cleanup;
  // Native multi-agg driver handle; 0 once released.
  private long _nativeHandle;
  private boolean _materialized;
  private NativeGroupKeyGenerator _groupKeyGenerator;
  private GroupByResultHolder[] _groupByResultHolders;

  // Profiling accumulators (ns); only written when PROFILE.
  private long _nsKeyProbe;
  private long _nsAggApply;
  private long _nsDrain;
  private long _rowsProcessed;
  private int _blocks;
  // Tier chosen by the native ladder for this segment: 0=T1 bit-packed-direct, 1=T0 radix-direct,
  // 2=T2 bit-packed-hash, -1=single-column / invalid. Captured before the handle is released.
  private int _tier = -1;

  public NativeGroupByExecutor(QueryContext queryContext, ExpressionContext[] groupByExpressions,
      BaseProjectOperator<?> projectOperator) {
    _aggregationFunctions = queryContext.getAggregationFunctions();
    assert _aggregationFunctions != null;
    assert groupByExpressions.length >= 1 : "NativeGroupByExecutor requires at least one group-by key";

    _numKeyColumns = groupByExpressions.length;
    _keyExpressions = groupByExpressions;
    _keyDictionaries = new Dictionary[_numKeyColumns];
    int[] cardinalities = new int[_numKeyColumns];
    for (int c = 0; c < _numKeyColumns; c++) {
      ColumnContext keyContext = projectOperator.getResultColumnContext(groupByExpressions[c]);
      Dictionary dictionary = keyContext.getDictionary();
      assert dictionary != null : "NativeGroupByExecutor requires dict-encoded group-by keys";
      _keyDictionaries[c] = dictionary;
      cardinalities[c] = dictionary.length();
    }

    int numAggregations = _aggregationFunctions.length;
    _aggInputExpressions = new ExpressionContext[numAggregations];
    _applyFamily = new int[numAggregations];
    _extractFamily = new int[numAggregations];
    byte[] aggKinds = new byte[numAggregations];

    for (int i = 0; i < numAggregations; i++) {
      AggregationFunction aggregationFunction = _aggregationFunctions[i];
      AggregationFunctionType type = aggregationFunction.getType();
      if (type == AggregationFunctionType.COUNT) {
        _aggInputExpressions[i] = null;
        _applyFamily[i] = FAMILY_COUNT;
        _extractFamily[i] = FAMILY_LONG;
        aggKinds[i] = NativeAggKind.COUNT._ordinalByte;
        continue;
      }
      List<ExpressionContext> inputExpressions = aggregationFunction.getInputExpressions();
      ExpressionContext inputExpression = inputExpressions.get(0);
      _aggInputExpressions[i] = inputExpression;
      DataType storedType = projectOperator.getResultColumnContext(inputExpression).getDataType().getStoredType();
      _applyFamily[i] = valueFamily(storedType);
      _extractFamily[i] = (type == AggregationFunctionType.SUM) ? FAMILY_DOUBLE : valueFamily(storedType);
      aggKinds[i] = nativeAggKind(type, storedType)._ordinalByte;
    }

    // One create path for any number of dict-encoded key columns: the native
    // side runs the tier ladder (select_tier) and picks the backend — dense
    // no-hash (T1 bit-packed / T0 radix) or hashbrown (T2) — including the
    // single-column no-pack fast path. Returns 0 only when a multi-column key
    // exceeds 64 bits (Java falls back).
    _nativeHandle = PinotNativeGroupBy.createGroupBy(aggKinds, cardinalities);
    if (_nativeHandle == 0) {
      throw new IllegalStateException("Native GROUP BY driver creation failed (invalid agg kinds or unpackable key)");
    }
    _cleanup = new HandleCleanup(_nativeHandle);
    CLEANER.register(this, _cleanup);
    if (PROFILE) {
      // Single-column drivers report their underlying tier tag (bit-packed / hash); label them "single"
      // for an honest profile line since they take the no-pack single-column fast path.
      _tier = _numKeyColumns == 1 ? -1 : PinotNativeGroupBy.tierTag(_nativeHandle);
    }
  }

  private static int valueFamily(DataType storedType) {
    switch (storedType) {
      case INT:
        return FAMILY_INT;
      case LONG:
        return FAMILY_LONG;
      case FLOAT:
        return FAMILY_FLOAT;
      case DOUBLE:
        return FAMILY_DOUBLE;
      default:
        throw new IllegalStateException("Unsupported native GROUP BY value type: " + storedType);
    }
  }

  private static NativeAggKind nativeAggKind(AggregationFunctionType type, DataType storedType) {
    switch (type) {
      case SUM:
        switch (storedType) {
          case INT:
            return NativeAggKind.SUM_INT_TO_DOUBLE;
          case LONG:
            return NativeAggKind.SUM_LONG_TO_DOUBLE;
          case FLOAT:
            return NativeAggKind.SUM_FLOAT_TO_DOUBLE;
          case DOUBLE:
            return NativeAggKind.SUM_DOUBLE;
          default:
            break;
        }
        break;
      case MIN:
        switch (storedType) {
          case INT:
            return NativeAggKind.MIN_INT;
          case LONG:
            return NativeAggKind.MIN_LONG;
          case FLOAT:
            return NativeAggKind.MIN_FLOAT;
          case DOUBLE:
            return NativeAggKind.MIN_DOUBLE;
          default:
            break;
        }
        break;
      case MAX:
        switch (storedType) {
          case INT:
            return NativeAggKind.MAX_INT;
          case LONG:
            return NativeAggKind.MAX_LONG;
          case FLOAT:
            return NativeAggKind.MAX_FLOAT;
          case DOUBLE:
            return NativeAggKind.MAX_DOUBLE;
          default:
            break;
        }
        break;
      default:
        break;
    }
    throw new IllegalStateException("Unsupported native GROUP BY aggregation: " + type + " over " + storedType);
  }

  @Override
  public void process(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    long t0 = PROFILE ? System.nanoTime() : 0L;
    // Gather the key columns' dict-ids column-major into `flat` and feed one
    // block. Single-column skips the copy (the block's dict-id array is already
    // the column); the native side packs multi-column keys and picks the tier.
    int[] flat;
    if (_numKeyColumns == 1) {
      flat = valueBlock.getBlockValueSet(_keyExpressions[0]).getDictionaryIdsSV();
    } else {
      int needed = _numKeyColumns * length;
      if (_multiKeyScratch == null || _multiKeyScratch.length < needed) {
        _multiKeyScratch = new int[needed];
      }
      for (int c = 0; c < _numKeyColumns; c++) {
        int[] dictIds = valueBlock.getBlockValueSet(_keyExpressions[c]).getDictionaryIdsSV();
        System.arraycopy(dictIds, 0, _multiKeyScratch, c * length, length);
      }
      flat = _multiKeyScratch;
    }
    PinotNativeGroupBy.processBlockKeys(_nativeHandle, flat, _numKeyColumns, length);
    long t1 = PROFILE ? System.nanoTime() : 0L;

    for (int i = 0; i < _aggregationFunctions.length; i++) {
      if (_applyFamily[i] == FAMILY_COUNT) {
        PinotNativeGroupBy.applyAggCount(_nativeHandle, i);
        continue;
      }
      BlockValSet valueSet = valueBlock.getBlockValueSet(_aggInputExpressions[i]);
      switch (_applyFamily[i]) {
        case FAMILY_INT:
          PinotNativeGroupBy.applyAggInt(_nativeHandle, i, valueSet.getIntValuesSV(), length);
          break;
        case FAMILY_LONG:
          PinotNativeGroupBy.applyAggLong(_nativeHandle, i, valueSet.getLongValuesSV(), length);
          break;
        case FAMILY_FLOAT:
          PinotNativeGroupBy.applyAggFloat(_nativeHandle, i, valueSet.getFloatValuesSV(), length);
          break;
        case FAMILY_DOUBLE:
          PinotNativeGroupBy.applyAggDouble(_nativeHandle, i, valueSet.getDoubleValuesSV(), length);
          break;
        default:
          throw new IllegalStateException("Unexpected value family: " + _applyFamily[i]);
      }
    }
    if (PROFILE) {
      _nsKeyProbe += t1 - t0;
      _nsAggApply += System.nanoTime() - t1;
      _rowsProcessed += length;
      _blocks++;
    }
  }

  /**
   * Drain native per-group results into Java structures exactly once, then free the native handle.
   * Idempotent: subsequent calls (and the interface getters below) reuse the materialized state.
   */
  private void materialize() {
    if (_materialized) {
      return;
    }
    long tDrain0 = PROFILE ? System.nanoTime() : 0L;
    int numGroups = PinotNativeGroupBy.numGroups(_nativeHandle);

    int numAggregations = _aggregationFunctions.length;
    GroupByResultHolder[] holders = new GroupByResultHolder[numAggregations];
    int holderCapacity = Math.max(1, numGroups);
    for (int i = 0; i < numAggregations; i++) {
      GroupByResultHolder holder = _aggregationFunctions[i].createGroupByResultHolder(holderCapacity, holderCapacity);
      holder.ensureCapacity(holderCapacity);
      drainAggregation(i, numGroups, holder);
      holders[i] = holder;
    }

    _groupKeyGenerator = buildGroupKeyGenerator(numGroups);
    _groupByResultHolders = holders;
    _materialized = true;
    releaseNative();
    if (PROFILE) {
      _nsDrain = System.nanoTime() - tDrain0;
      double probeMs = _nsKeyProbe / 1e6;
      double aggMs = _nsAggApply / 1e6;
      double drainMs = _nsDrain / 1e6;
      double total = probeMs + aggMs + drainMs;
      System.out.printf(
          "[native-groupby profile] tier=%s groups=%d rows=%d blocks=%d | keyProbe=%.2fms (%.0f%%) "
              + "aggApply=%.2fms (%.0f%%) drain=%.2fms (%.0f%%) | segTotal=%.2fms%n",
          tierName(_tier), numGroups, _rowsProcessed, _blocks,
          probeMs, 100 * probeMs / total, aggMs, 100 * aggMs / total,
          drainMs, 100 * drainMs / total, total);
    }
  }

  private static String tierName(int tier) {
    switch (tier) {
      case 0:
        return "T1-bitpacked-direct";
      case 1:
        return "T0-radix-direct";
      case 2:
        return "T2-bitpacked-hash";
      default:
        return "single";
    }
  }

  /**
   * Extract the native combined keys and build the group-key generator. Single-column uses the i32
   * dict-id keys directly; multi-column unpacks the packed i64 keys into N per-column dict-id arrays.
   */
  private NativeGroupKeyGenerator buildGroupKeyGenerator(int numGroups) {
    int[] flat = new int[_numKeyColumns * numGroups];
    PinotNativeGroupBy.extractGroupKeys(_nativeHandle, flat, _numKeyColumns);
    if (_numKeyColumns == 1) {
      return new NativeGroupKeyGenerator(_keyDictionaries[0], flat, numGroups);
    }
    int[][] dictIdsPerColumn = new int[_numKeyColumns][];
    for (int c = 0; c < _numKeyColumns; c++) {
      int[] column = new int[numGroups];
      System.arraycopy(flat, c * numGroups, column, 0, numGroups);
      dictIdsPerColumn[c] = column;
    }
    return new NativeGroupKeyGenerator(_keyDictionaries, dictIdsPerColumn, numGroups);
  }

  private void drainAggregation(int aggIndex, int numGroups, GroupByResultHolder holder) {
    switch (_extractFamily[aggIndex]) {
      case FAMILY_LONG: {
        long[] results = new long[numGroups];
        PinotNativeGroupBy.extractAggLong(_nativeHandle, aggIndex, results);
        for (int g = 0; g < numGroups; g++) {
          holder.setValueForKey(g, (double) results[g]);
        }
        break;
      }
      case FAMILY_INT: {
        int[] results = new int[numGroups];
        PinotNativeGroupBy.extractAggInt(_nativeHandle, aggIndex, results);
        for (int g = 0; g < numGroups; g++) {
          holder.setValueForKey(g, (double) results[g]);
        }
        break;
      }
      case FAMILY_FLOAT: {
        float[] results = new float[numGroups];
        PinotNativeGroupBy.extractAggFloat(_nativeHandle, aggIndex, results);
        for (int g = 0; g < numGroups; g++) {
          holder.setValueForKey(g, (double) results[g]);
        }
        break;
      }
      case FAMILY_DOUBLE: {
        double[] results = new double[numGroups];
        PinotNativeGroupBy.extractAggDouble(_nativeHandle, aggIndex, results);
        for (int g = 0; g < numGroups; g++) {
          holder.setValueForKey(g, results[g]);
        }
        break;
      }
      default:
        throw new IllegalStateException("Unexpected extract family: " + _extractFamily[aggIndex]);
    }
  }

  /** Extract aggregation {@code aggIndex} as a {@code double[]} in native group_id order (combine merges in double). */
  private double[] extractAggregationAsDouble(int aggIndex, int numGroups) {
    double[] out = new double[numGroups];
    switch (_extractFamily[aggIndex]) {
      case FAMILY_LONG: {
        long[] results = new long[numGroups];
        PinotNativeGroupBy.extractAggLong(_nativeHandle, aggIndex, results);
        for (int g = 0; g < numGroups; g++) {
          out[g] = (double) results[g];
        }
        break;
      }
      case FAMILY_INT: {
        int[] results = new int[numGroups];
        PinotNativeGroupBy.extractAggInt(_nativeHandle, aggIndex, results);
        for (int g = 0; g < numGroups; g++) {
          out[g] = (double) results[g];
        }
        break;
      }
      case FAMILY_FLOAT: {
        float[] results = new float[numGroups];
        PinotNativeGroupBy.extractAggFloat(_nativeHandle, aggIndex, results);
        for (int g = 0; g < numGroups; g++) {
          out[g] = (double) results[g];
        }
        break;
      }
      case FAMILY_DOUBLE:
        PinotNativeGroupBy.extractAggDouble(_nativeHandle, aggIndex, out);
        break;
      default:
        throw new IllegalStateException("Unexpected extract family: " + _extractFamily[aggIndex]);
    }
    return out;
  }

  private void releaseNative() {
    _nativeHandle = 0;
    _cleanup.destroy();
  }

  @Override
  public AggregationGroupByResult getResult() {
    materialize();
    return new AggregationGroupByResult(_groupKeyGenerator, _aggregationFunctions, _groupByResultHolders);
  }

  /**
   * Boundary-1 fast path: snapshot the native per-group result into primitive columnar arrays
   * (per-column dict-ids in native group_id order + the segment key dictionaries + {@code double}
   * aggregate columns) for the native combine, skipping the boxed {@link AggregationGroupByResult}
   * ({@link GroupByResultHolder} per aggregation + {@link NativeGroupKeyGenerator} {@code Object[]}
   * keys). Releases the native handle (mirrors {@link #materialize()}); no native method may be called
   * afterwards. Only valid for the native combine path; the standalone/Java-combine path uses
   * {@link #getResult()}.
   */
  public NativeSegmentResult buildNativeSegmentResult() {
    int numGroups = PinotNativeGroupBy.numGroups(_nativeHandle);
    int[] flat = new int[_numKeyColumns * numGroups];
    PinotNativeGroupBy.extractGroupKeys(_nativeHandle, flat, _numKeyColumns);
    int[][] dictIds = new int[_numKeyColumns][];
    for (int c = 0; c < _numKeyColumns; c++) {
      int[] column = new int[numGroups];
      System.arraycopy(flat, c * numGroups, column, 0, numGroups);
      dictIds[c] = column;
    }
    double[][] aggValues = new double[_aggregationFunctions.length][];
    for (int a = 0; a < _aggregationFunctions.length; a++) {
      aggValues[a] = extractAggregationAsDouble(a, numGroups);
    }
    releaseNative();
    return new NativeSegmentResult(numGroups, dictIds, _keyDictionaries, aggValues);
  }

  @Override
  public int getNumGroups() {
    if (_materialized) {
      return _groupKeyGenerator.getNumKeys();
    }
    return PinotNativeGroupBy.numGroups(_nativeHandle);
  }

  @Override
  public Collection<IntermediateRecord> trimGroupByResult(int trimSize, TableResizer tableResizer) {
    // Segment-level group trim (ORDER BY path) is excluded by NativeGroupByRouter for now.
    throw new UnsupportedOperationException("Native GROUP BY does not support in-segment trim yet");
  }

  @Override
  public GroupKeyGenerator getGroupKeyGenerator() {
    materialize();
    return _groupKeyGenerator;
  }

  @Override
  public GroupByResultHolder[] getGroupByResultHolders() {
    materialize();
    return _groupByResultHolders;
  }

  /**
   * Holds the native handle for {@link Cleaner}-based release. Must not reference the enclosing
   * executor (or the executor would never be collected and the cleaner never run).
   */
  private static final class HandleCleanup implements Runnable {
    private long _handle;

    HandleCleanup(long handle) {
      _handle = handle;
    }

    synchronized void destroy() {
      if (_handle != 0) {
        PinotNativeGroupBy.destroy(_handle);
        _handle = 0;
      }
    }

    @Override
    public void run() {
      destroy();
    }
  }
}
