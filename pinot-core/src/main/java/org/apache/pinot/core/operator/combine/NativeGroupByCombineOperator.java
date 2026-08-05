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
package org.apache.pinot.core.operator.combine;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.datatable.DataTableImplV4;
import org.apache.pinot.common.datatable.DataTableUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.BaseTable;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.aggregation.groupby.NativeSegmentResult;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.nativeengine.groupby.PinotNativeGroupBy.NativeAggKind;
import org.apache.pinot.nativeengine.groupby.PinotNativeGroupByCombine;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryErrorMessage;
import org.apache.pinot.spi.utils.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Native (Rust/JNI) server-level GROUP BY combine — the radix-partitioned, work-stealing parallel
 * cross-segment merge (Task #54, step-5 wiring, Pattern A). Substituted for {@link
 * GroupByCombineOperator} by {@link NativeGroupByCombineRouter} when eligible.
 *
 * <p><b>Multi-type, multi-column.</b> Each group-by key column may be INT/LONG (→ native LONG
 * surface), FLOAT/DOUBLE (→ DOUBLE surface), or STRING/BYTES (→ arena surface) — discovered lazily
 * from the first segment's {@link DataSchema}. A single key uses the specialized single-key session;
 * multiple keys use the multi-column combine (raw-value tuple key, design §17.9 / §23.1), whose merge
 * strategy is selectable via {@code -Dpinot.native.groupby.combine.multicol} = {@code columnwise}
 * (default) or {@code packed}. All aggregations merge in {@code double} (a segment's intermediate is a
 * Java double holder): SUM/COUNT → add, MIN/MAX → min/max.
 *
 * <p><b>Pattern A hand-off.</b> Each JVM worker runs its segment's group-by (parallel), extracts the
 * segment's per-group {@code (raw key, per-agg double intermediate)} arrays, and hands the whole
 * partial to the native session in one synchronized burst — a few JNI calls per segment, not per
 * group. The native side collects partials; the merge runs inside {@link
 * PinotNativeGroupByCombine#finish} (Rust threads).
 *
 * <p><b>Result path (Option 1, design §24.2/§26.3/§27).</b> After {@code finish}, the operator calls
 * {@link PinotNativeGroupByCombine#select} to apply the ORDER BY top-K / no-ORDER-BY cap natively
 * (to {@code resultSize} groups), then builds the {@link GroupByResultsBlock} <b>directly</b> from
 * native's already-merged, already-selected column arrays — no Java {@link IndexedTable} re-hash.
 * Each aggregation is boxed to its real <b>intermediate</b> type (COUNT → {@code Long},
 * SUM/MIN/MAX → {@code Double}) so the downstream {@code DataTable} serialization is type-correct.
 */
@SuppressWarnings("rawtypes")
public class NativeGroupByCombineOperator extends GroupByCombineOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(NativeGroupByCombineOperator.class);
  /** Multi-column merge strategy selector: {@code columnwise} (default, design B) or {@code packed} (design C). */
  private static final String MULTICOL_PROPERTY = "pinot.native.groupby.combine.multicol";
  /**
   * When {@code true} (default), the combine emits the DataTable bytes natively for all-fixed-width
   * results ("native-serialize", {@link #tryNativeSerialize}), skipping the per-group Java Record /
   * DataTableBuilder pass. Set {@code -Dpinot.native.groupby.combine.serialize=false} to force the
   * boxed Record path (A/B measurement, or a safety switch).
   */
  private static final boolean NATIVE_SERIALIZE_ENABLED =
      !"false".equalsIgnoreCase(System.getProperty("pinot.native.groupby.combine.serialize", "true"));
  private static final int RADIX_BITS = 6;

  private final int _numAggFunctions;
  private final int _numGroupByColumns;
  private final int _numColumns;
  private final byte[] _combineAggKinds;
  private final int _multiColStrategy;
  private final CountDownLatch _latch;
  private final Object _feedLock = new Object();

  // Created lazily on the first segment (key types come from its DataSchema).
  private volatile long _combineHandle;
  private volatile int[] _combineColTypes;
  private volatile ColumnDataType[] _keyColumnTypes;
  private volatile GroupByResultsBlock _sampleBlock;
  private volatile boolean _nativeNumGroupsLimitReached;
  private volatile boolean _nativeNumGroupsWarningLimitReached;

  public NativeGroupByCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService) {
    super(operators, queryContext, executorService);
    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
    assert aggregationFunctions != null;
    _numAggFunctions = aggregationFunctions.length;
    assert queryContext.getGroupByExpressions() != null;
    _numGroupByColumns = queryContext.getGroupByExpressions().size();
    _numColumns = _numGroupByColumns + _numAggFunctions;
    _combineAggKinds = combineAggKinds(aggregationFunctions);
    _multiColStrategy = "packed".equalsIgnoreCase(System.getProperty(MULTICOL_PROPERTY, "columnwise"))
        ? PinotNativeGroupByCombine.MULTICOL_STRATEGY_PACKED
        : PinotNativeGroupByCombine.MULTICOL_STRATEGY_COLUMNWISE;
    _latch = new CountDownLatch(_numTasks);
  }

  /** Combine merges in double (segment intermediate is a double holder): SUM/COUNT add, MIN min, MAX max. */
  private static byte[] combineAggKinds(AggregationFunction[] functions) {
    byte[] kinds = new byte[functions.length];
    for (int i = 0; i < functions.length; i++) {
      AggregationFunctionType type = functions[i].getType();
      switch (type) {
        case SUM:
        case COUNT:
          kinds[i] = NativeAggKind.SUM_DOUBLE._ordinalByte;
          break;
        case MIN:
          kinds[i] = NativeAggKind.MIN_DOUBLE._ordinalByte;
          break;
        case MAX:
          kinds[i] = NativeAggKind.MAX_DOUBLE._ordinalByte;
          break;
        default:
          throw new IllegalStateException("Unsupported native combine aggregation: " + type);
      }
    }
    return kinds;
  }

  private static int keyTypeFor(ColumnDataType columnType) {
    switch (columnType) {
      case INT:
      case LONG:
        return PinotNativeGroupByCombine.KEY_TYPE_LONG;
      case FLOAT:
      case DOUBLE:
        return PinotNativeGroupByCombine.KEY_TYPE_DOUBLE;
      case STRING:
      case BYTES:
        return PinotNativeGroupByCombine.KEY_TYPE_STRING;
      default:
        throw new IllegalStateException("Unsupported native combine key type: " + columnType);
    }
  }

  /** Per-column type width in bits for the PackedKeys strategy (32 for INT/FLOAT, 64 for LONG/DOUBLE, 0 STRING). */
  private static int keyWidthFor(ColumnDataType columnType) {
    switch (columnType) {
      case INT:
      case FLOAT:
        return 32;
      case LONG:
      case DOUBLE:
        return 64;
      default:
        return 0;
    }
  }

  @Override
  protected void processSegments() {
    int operatorId;
    while (_processingException.get() == null && (operatorId = _nextOperatorId.getAndIncrement()) < _numOperators) {
      Operator operator = _operators.get(operatorId);
      try {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).acquire();
        }
        GroupByResultsBlock resultsBlock = (GroupByResultsBlock) operator.nextBlock();
        ensureHandle(resultsBlock);
        if (resultsBlock.isNumGroupsLimitReached()) {
          _nativeNumGroupsLimitReached = true;
        }
        if (resultsBlock.isNumGroupsWarningLimitReached()) {
          _nativeNumGroupsWarningLimitReached = true;
        }
        NativeSegmentResult nativeResult = resultsBlock.getNativeSegmentResult();
        if (nativeResult != null) {
          feedSegmentNative(nativeResult);
        } else {
          AggregationGroupByResult aggResult = resultsBlock.getAggregationGroupByResult();
          if (aggResult == null) {
            throw new IllegalStateException("Native combine requires AggregationGroupByResult (no in-segment trim)");
          }
          feedSegment(aggResult);
        }
      } catch (RuntimeException e) {
        throw wrapOperatorException(operator, e);
      } finally {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).release();
        }
      }
    }
  }

  private void ensureHandle(GroupByResultsBlock block) {
    if (_combineHandle == 0) {
      synchronized (this) {
        if (_combineHandle == 0) {
          _sampleBlock = block;
          DataSchema schema = block.getDataSchema();
          int[] combineColTypes = new int[_numGroupByColumns];
          ColumnDataType[] keyColumnTypes = new ColumnDataType[_numGroupByColumns];
          for (int c = 0; c < _numGroupByColumns; c++) {
            keyColumnTypes[c] = schema.getColumnDataType(c);
            combineColTypes[c] = keyTypeFor(keyColumnTypes[c]);
          }
          _keyColumnTypes = keyColumnTypes;
          _combineColTypes = combineColTypes;
          long handle;
          if (_numGroupByColumns == 1) {
            handle = PinotNativeGroupByCombine.createCombine(_combineAggKinds, combineColTypes[0]);
          } else {
            byte[] colTypeBytes = new byte[_numGroupByColumns];
            int[] colWidths = new int[_numGroupByColumns];
            for (int c = 0; c < _numGroupByColumns; c++) {
              colTypeBytes[c] = (byte) combineColTypes[c];
              colWidths[c] = keyWidthFor(keyColumnTypes[c]);
            }
            handle = PinotNativeGroupByCombine.createCombineMulti(_combineAggKinds, colTypeBytes, colWidths,
                _multiColStrategy);
          }
          if (handle == 0) {
            throw new IllegalStateException("Native combine session creation failed");
          }
          _combineHandle = handle;
        }
      }
    }
  }

  /** Extract this segment's partial (parallel) and hand it off in one synchronized burst. */
  private void feedSegment(AggregationGroupByResult aggResult) {
    int numGroups = aggResult.getNumGroups();
    double[][] aggValues = new double[_numAggFunctions][numGroups];
    // Per key column, only the builder matching its combine type is allocated.
    long[][] longKeys = new long[_numGroupByColumns][];
    double[][] doubleKeys = new double[_numGroupByColumns][];
    // String keys: accumulate UTF-8 bytes once per key into a growing buffer (offsets = running size).
    // Using a byte stream (not a StringBuilder re-encoded per group) keeps this O(total bytes), not O(n²).
    java.io.ByteArrayOutputStream[] stringByteStreams = new java.io.ByteArrayOutputStream[_numGroupByColumns];
    int[][] stringOffsets = new int[_numGroupByColumns][];
    for (int c = 0; c < _numGroupByColumns; c++) {
      switch (_combineColTypes[c]) {
        case PinotNativeGroupByCombine.KEY_TYPE_LONG:
          longKeys[c] = new long[numGroups];
          break;
        case PinotNativeGroupByCombine.KEY_TYPE_DOUBLE:
          doubleKeys[c] = new double[numGroups];
          break;
        default:
          stringByteStreams[c] = new java.io.ByteArrayOutputStream();
          stringOffsets[c] = new int[numGroups + 1];
          break;
      }
    }

    Iterator<GroupKeyGenerator.GroupKey> it = aggResult.getGroupKeyIterator();
    int idx = 0;
    while (it.hasNext()) {
      GroupKeyGenerator.GroupKey groupKey = it.next();
      for (int c = 0; c < _numGroupByColumns; c++) {
        Object keyObject = groupKey._keys[c];
        switch (_combineColTypes[c]) {
          case PinotNativeGroupByCombine.KEY_TYPE_LONG:
            longKeys[c][idx] = ((Number) keyObject).longValue();
            break;
          case PinotNativeGroupByCombine.KEY_TYPE_DOUBLE:
            doubleKeys[c][idx] = ((Number) keyObject).doubleValue();
            break;
          default:
            byte[] keyBytes = keyString(keyObject).getBytes(StandardCharsets.UTF_8);
            stringByteStreams[c].write(keyBytes, 0, keyBytes.length);
            stringOffsets[c][idx + 1] = stringByteStreams[c].size();
            break;
        }
      }
      int groupId = groupKey._groupId;
      for (int a = 0; a < _numAggFunctions; a++) {
        aggValues[a][idx] = ((Number) aggResult.getResultForGroupId(a, groupId)).doubleValue();
      }
      idx++;
    }
    byte[][] stringBytes = new byte[_numGroupByColumns][];
    for (int c = 0; c < _numGroupByColumns; c++) {
      if (stringByteStreams[c] != null) {
        stringBytes[c] = stringByteStreams[c].toByteArray();
      }
    }

    synchronized (_feedLock) {
      stageAndCommitLocked(longKeys, doubleKeys, stringBytes, stringOffsets, aggValues);
    }
  }

  /**
   * Boundary-1 fast path: decode the native primitive segment result directly into the combine — the
   * per-column dict-ids are widened straight into the primitive key arrays fed across JNI, and the
   * {@code double} aggregate columns are handed over as-is. No {@code AggregationGroupByResult}, no
   * {@link org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder}, no per-group
   * {@code Object} boxing or {@code GroupKey} iterator allocation.
   */
  private void feedSegmentNative(NativeSegmentResult result) {
    int numGroups = result.getNumGroups();
    int[][] dictIds = result.getDictIds();
    Dictionary[] dictionaries = result.getDictionaries();
    double[][] aggValues = result.getAggValues();

    long[][] longKeys = new long[_numGroupByColumns][];
    double[][] doubleKeys = new double[_numGroupByColumns][];
    byte[][] stringBytes = new byte[_numGroupByColumns][];
    int[][] stringOffsets = new int[_numGroupByColumns][];
    for (int c = 0; c < _numGroupByColumns; c++) {
      Dictionary dictionary = dictionaries[c];
      int[] ids = dictIds[c];
      switch (_combineColTypes[c]) {
        case PinotNativeGroupByCombine.KEY_TYPE_LONG: {
          long[] keys = new long[numGroups];
          for (int g = 0; g < numGroups; g++) {
            keys[g] = dictionary.getLongValue(ids[g]);
          }
          longKeys[c] = keys;
          break;
        }
        case PinotNativeGroupByCombine.KEY_TYPE_DOUBLE: {
          double[] keys = new double[numGroups];
          for (int g = 0; g < numGroups; g++) {
            keys[g] = dictionary.getDoubleValue(ids[g]);
          }
          doubleKeys[c] = keys;
          break;
        }
        default: {
          java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
          int[] offsets = new int[numGroups + 1];
          for (int g = 0; g < numGroups; g++) {
            byte[] keyBytes = dictionary.getStringValue(ids[g]).getBytes(StandardCharsets.UTF_8);
            bytes.write(keyBytes, 0, keyBytes.length);
            offsets[g + 1] = bytes.size();
          }
          stringBytes[c] = bytes.toByteArray();
          stringOffsets[c] = offsets;
          break;
        }
      }
    }

    synchronized (_feedLock) {
      stageAndCommitLocked(longKeys, doubleKeys, stringBytes, stringOffsets, aggValues);
    }
  }

  /**
   * Stage one segment's partial (per-column keys + double aggregate columns) into the native combine
   * and commit it. Caller must hold {@link #_feedLock}.
   */
  private void stageAndCommitLocked(long[][] longKeys, double[][] doubleKeys, byte[][] stringBytes,
      int[][] stringOffsets, double[][] aggValues) {
    if (_numGroupByColumns == 1) {
      switch (_combineColTypes[0]) {
        case PinotNativeGroupByCombine.KEY_TYPE_LONG:
          PinotNativeGroupByCombine.beginPartialLong(_combineHandle, longKeys[0]);
          break;
        case PinotNativeGroupByCombine.KEY_TYPE_DOUBLE:
          PinotNativeGroupByCombine.beginPartialDouble(_combineHandle, doubleKeys[0]);
          break;
        default:
          PinotNativeGroupByCombine.beginPartialString(_combineHandle, stringBytes[0], stringOffsets[0]);
          break;
      }
    } else {
      PinotNativeGroupByCombine.beginPartialMulti(_combineHandle);
      for (int c = 0; c < _numGroupByColumns; c++) {
        switch (_combineColTypes[c]) {
          case PinotNativeGroupByCombine.KEY_TYPE_LONG:
            PinotNativeGroupByCombine.setKeyLong(_combineHandle, c, longKeys[c]);
            break;
          case PinotNativeGroupByCombine.KEY_TYPE_DOUBLE:
            PinotNativeGroupByCombine.setKeyDouble(_combineHandle, c, doubleKeys[c]);
            break;
          default:
            PinotNativeGroupByCombine.setKeyString(_combineHandle, c, stringBytes[c], stringOffsets[c]);
            break;
        }
      }
    }
    for (int a = 0; a < _numAggFunctions; a++) {
      PinotNativeGroupByCombine.setAggDouble(_combineHandle, a, aggValues[a]);
    }
    PinotNativeGroupByCombine.commitPartial(_combineHandle);
  }

  private static String keyString(Object keyObject) {
    if (keyObject instanceof byte[]) {
      return new String((byte[]) keyObject, StandardCharsets.UTF_8);
    }
    if (keyObject instanceof ByteArray) {
      return new String(((ByteArray) keyObject).getBytes(), StandardCharsets.UTF_8);
    }
    return keyObject.toString();
  }

  @Override
  public void onProcessSegmentsFinish() {
    _latch.countDown();
  }

  @Override
  public BaseResultsBlock mergeResults()
      throws Exception {
    long timeoutMs = _queryContext.getEndTimeMs() - System.currentTimeMillis();
    if (!_latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
      destroyHandle();
      String userError = "Timed out while combining group-by results after " + timeoutMs + "ms";
      LOGGER.error("{}, queryContext = {}", userError, _queryContext);
      return new ExceptionResultsBlock(new QueryErrorMessage(QueryErrorCode.EXECUTION_TIMEOUT, userError, userError));
    }
    Throwable ex = _processingException.get();
    if (ex != null) {
      destroyHandle();
      String devError = "Caught exception while processing native group-by combine: " + ex.getMessage();
      return new ExceptionResultsBlock(new QueryErrorMessage(QueryErrorCode.QUERY_EXECUTION, devError, devError));
    }
    if (_combineHandle == 0) {
      // No segments produced a block (e.g. empty result) — fall back to an empty Java table.
      IndexedTable empty = GroupByUtils.createIndexedTableForCombineOperator(
          new GroupByResultsBlock(new DataSchema(new String[0], new ColumnDataType[0]), _queryContext),
          _queryContext, 1, _executorService);
      empty.finish(false);
      return new GroupByResultsBlock(empty, _queryContext);
    }

    // Merge all committed partials (Rust radix-parallel merge).
    PinotNativeGroupByCombine.finish(_combineHandle, RADIX_BITS);

    // (c) Native ORDER BY top-K / no-ORDER-BY cap to resultSize (design §26.3). The router validated
    // that every order term resolves to a group column or an aggregation, so this is never null.
    OrderSpec orderSpec = resolveOrderSpec(_queryContext);
    assert orderSpec != null : "router accepted an unsupported ORDER BY";
    boolean hasOrderBy = _queryContext.getOrderByExpressions() != null;
    int resultSize = computeResultSize(_queryContext, hasOrderBy);
    PinotNativeGroupByCombine.select(_combineHandle, orderSpec._refs, orderSpec._ascending, resultSize);

    int numGroups = PinotNativeGroupByCombine.numGroups(_combineHandle);
    DataSchema dataSchema = _sampleBlock.getDataSchema().clone();

    // (a)+(b)+(c) FAST PATH — "native-serialize": when every output column is fixed-width, emit the
    // DataTable V4 row-major bytes DIRECTLY from native's merged/selected result (no Java Record /
    // Object[] materialization, no per-row ByteBuffer). Falls back to the boxed path below for
    // STRING/BYTES keys, null handling, or server-return-final-result.
    GroupByResultsBlock nativeSerialized = tryNativeSerialize(dataSchema, numGroups);
    if (nativeSerialized != null) {
      destroyHandle();
      return nativeSerialized;
    }

    // Drain the already-merged, already-selected column arrays out of native (one JNI copy each).
    double[][] aggResults = new double[_numAggFunctions][];
    for (int a = 0; a < _numAggFunctions; a++) {
      aggResults[a] = new double[numGroups];
      PinotNativeGroupByCombine.extractAggDouble(_combineHandle, a, aggResults[a]);
    }
    Object[][] keyColumns = extractKeyColumns(numGroups);
    destroyHandle();

    // (a)+(b) Build the result table DIRECTLY from native's column arrays — no IndexedTable re-hash.
    // Box each aggregation to its real intermediate type (COUNT → Long, SUM/MIN/MAX → Double) so the
    // downstream DataTable serialization is type-correct.
    ColumnDataType[] aggIntermediateTypes = new ColumnDataType[_numAggFunctions];
    for (int a = 0; a < _numAggFunctions; a++) {
      aggIntermediateTypes[a] = dataSchema.getColumnDataType(_numGroupByColumns + a);
    }
    List<Record> records = new ArrayList<>(numGroups);
    for (int i = 0; i < numGroups; i++) {
      Object[] values = new Object[_numColumns];
      for (int c = 0; c < _numGroupByColumns; c++) {
        values[c] = keyColumns[c][i];
      }
      for (int a = 0; a < _numAggFunctions; a++) {
        values[_numGroupByColumns + a] = boxAggIntermediate(aggIntermediateTypes[a], aggResults[a][i]);
      }
      records.add(new Record(values));
    }
    NativeMaterializedTable table =
        new NativeMaterializedTable(dataSchema, records, _numGroupByColumns, _queryContext.getAggregationFunctions());

    // Mirror the Java finish()'s final-result semantics (identity for SUM/MIN/MAX/COUNT). Native has
    // already applied the sort/trim, so no re-sort happens here.
    if (_queryContext.isServerReturnFinalResult()) {
      table.finish(true, true);
    } else if (_queryContext.isServerReturnFinalResultKeyUnpartitioned()) {
      table.finish(false, true);
    } else {
      table.finish(false);
    }

    GroupByResultsBlock mergedBlock = new GroupByResultsBlock(table, _queryContext);
    mergedBlock.setNumGroupsLimitReached(_nativeNumGroupsLimitReached);
    mergedBlock.setNumGroupsWarningLimitReached(_nativeNumGroupsWarningLimitReached);
    return mergedBlock;
  }

  private static final String[] EMPTY_STRING_DICTIONARY = new String[0];
  private static final byte[] EMPTY_VARIABLE_BYTES = new byte[0];

  /**
   * "native-serialize" fast path: when every output column is fixed-width (INT/LONG/FLOAT/DOUBLE),
   * emit the DataTable V4 row-major byte buffer DIRECTLY from native's merged/selected result,
   * skipping the per-group Java {@link Record}/{@code Object[]} materialization and the per-row
   * {@code ByteBuffer} the DataTableBuilder allocates. Returns {@code null} (→ boxed fallback) when a
   * column is STRING/BYTES/other, when null handling is on, or when the server returns final results
   * (those need per-value transforms the raw fixed-width emit does not apply).
   */
  private GroupByResultsBlock tryNativeSerialize(DataSchema dataSchema, int numGroups) {
    if (!NATIVE_SERIALIZE_ENABLED || _queryContext.isNullHandlingEnabled()
        || _queryContext.isServerReturnFinalResult() || _queryContext.isServerReturnFinalResultKeyUnpartitioned()) {
      return null;
    }
    ColumnDataType[] storedTypes = dataSchema.getStoredColumnDataTypes();
    int[] ops = new int[_numColumns];
    int[] indices = new int[_numColumns];
    for (int c = 0; c < _numColumns; c++) {
      boolean isKey = c < _numGroupByColumns;
      int op = serializeOp(storedTypes[c], isKey);
      if (op < 0) {
        return null; // non-fixed-width column → boxed fallback
      }
      ops[c] = op;
      indices[c] = isKey ? c : c - _numGroupByColumns;
    }
    int[] offsets = new int[_numColumns];
    int rowSize = DataTableUtils.computeColumnOffsets(dataSchema, offsets, DataTableFactory.VERSION_4);
    byte[] fixed =
        PinotNativeGroupByCombine.serializeFixedWidth(_combineHandle, ops, indices, offsets, rowSize);
    if (fixed == null) {
      return null;
    }
    DataTable dataTable =
        new DataTableImplV4(numGroups, dataSchema, EMPTY_STRING_DICTIONARY, fixed, EMPTY_VARIABLE_BYTES);
    NativeSerializedGroupByResultsBlock block =
        new NativeSerializedGroupByResultsBlock(dataSchema, _queryContext, dataTable, numGroups);
    block.setNumGroupsLimitReached(_nativeNumGroupsLimitReached);
    block.setNumGroupsWarningLimitReached(_nativeNumGroupsWarningLimitReached);
    return block;
  }

  /** Map a stored column type + key/agg role to a {@code SER_*} op, or {@code -1} if not fixed-width. */
  private static int serializeOp(ColumnDataType storedType, boolean isKey) {
    if (isKey) {
      switch (storedType) {
        case INT:
          return PinotNativeGroupByCombine.SER_KEY_LONG_TO_I32;
        case LONG:
          return PinotNativeGroupByCombine.SER_KEY_LONG_TO_I64;
        case FLOAT:
          return PinotNativeGroupByCombine.SER_KEY_DOUBLE_TO_F32;
        case DOUBLE:
          return PinotNativeGroupByCombine.SER_KEY_DOUBLE_TO_F64;
        default:
          return -1;
      }
    }
    switch (storedType) {
      case INT:
        return PinotNativeGroupByCombine.SER_AGG_TO_I32;
      case LONG:
        return PinotNativeGroupByCombine.SER_AGG_TO_I64;
      case FLOAT:
        return PinotNativeGroupByCombine.SER_AGG_TO_F32;
      case DOUBLE:
        return PinotNativeGroupByCombine.SER_AGG_TO_F64;
      default:
        return -1;
    }
  }

  /**
   * An instance-level group-by results block whose DataTable was built natively (fixed-width
   * {@link #tryNativeSerialize}). Holds the pre-built {@link DataTable} and returns it from {@link
   * #getDataTable()} with zero further materialization; metadata flows through the inherited {@link
   * GroupByResultsBlock#getResultsMetadata()} unchanged. {@link #getRows()} (off the SSE hot path)
   * reconstructs intermediate rows from the fixed-width DataTable on demand.
   */
  private static final class NativeSerializedGroupByResultsBlock extends GroupByResultsBlock {
    private final DataTable _prebuiltDataTable;
    private final int _numRows;

    NativeSerializedGroupByResultsBlock(DataSchema dataSchema, QueryContext queryContext, DataTable dataTable,
        int numRows) {
      super(dataSchema, queryContext);
      _prebuiltDataTable = dataTable;
      _numRows = numRows;
    }

    @Override
    public DataTable getDataTable() {
      return _prebuiltDataTable;
    }

    @Override
    public int getNumRows() {
      return _numRows;
    }

    @Override
    public List<Object[]> getRows() {
      ColumnDataType[] storedTypes = getDataSchema().getStoredColumnDataTypes();
      int numColumns = storedTypes.length;
      List<Object[]> rows = new ArrayList<>(_numRows);
      for (int row = 0; row < _numRows; row++) {
        Object[] values = new Object[numColumns];
        for (int col = 0; col < numColumns; col++) {
          switch (storedTypes[col]) {
            case INT:
              values[col] = _prebuiltDataTable.getInt(row, col);
              break;
            case LONG:
              values[col] = _prebuiltDataTable.getLong(row, col);
              break;
            case FLOAT:
              values[col] = _prebuiltDataTable.getFloat(row, col);
              break;
            case DOUBLE:
              values[col] = _prebuiltDataTable.getDouble(row, col);
              break;
            default:
              throw new IllegalStateException("Unexpected fixed-width type: " + storedTypes[col]);
          }
        }
        rows.add(values);
      }
      return rows;
    }
  }

  /**
   * The {@code resultSize} to keep after the merge, matching {@link GroupByUtils#createIndexedTableForCombineOperator}
   * (design §26.3). The router gates HAVING out, so: no ORDER BY → {@code LIMIT}; ORDER BY →
   * {@code LIMIT} when the server returns the final result, else
   * {@code trimSize = max(LIMIT*5, minServerGroupTrimSize)} (or unbounded when server trim is disabled).
   */
  private static int computeResultSize(QueryContext queryContext, boolean hasOrderBy) {
    int limit = queryContext.getLimit();
    if (!hasOrderBy) {
      return limit;
    }
    int minTrimSize = queryContext.getMinServerGroupTrimSize();
    int trimSize = minTrimSize > 0 ? GroupByUtils.getTableCapacity(limit, minTrimSize) : Integer.MAX_VALUE;
    return queryContext.isServerReturnFinalResult() ? limit : trimSize;
  }

  /** Box a native double accumulator back to the aggregation's Java intermediate type. */
  private static Object boxAggIntermediate(ColumnDataType intermediateType, double value) {
    switch (intermediateType) {
      case LONG:
        return (long) value;
      case DOUBLE:
        return value;
      case INT:
        return (int) value;
      case FLOAT:
        return (float) value;
      default:
        throw new IllegalStateException("Unsupported native combine intermediate type: " + intermediateType);
    }
  }

  /**
   * Resolve the query's ORDER BY into native order-term refs (mirroring {@link
   * org.apache.pinot.core.data.table.TableResizer}'s order-by resolution), or {@code null} if any term is
   * unsupported (literal, non-group-by identifier, filtered aggregation, or post-aggregation expression) — in
   * which case the router must fall back to the Java combine.
   *
   * <p>A term matching the (single) group-by expression maps to {@link PinotNativeGroupByCombine#ORDER_REF_KEY};
   * a term matching an aggregation maps to that aggregation's column index (same order as {@code
   * getAggregationFunctions()}). No ORDER BY → empty spec (native applies the no-ORDER-BY cap).
   */
  static OrderSpec resolveOrderSpec(QueryContext queryContext) {
    List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
    if (orderByExpressions == null || orderByExpressions.isEmpty()) {
      return new OrderSpec(new int[0], new boolean[0]);
    }
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    assert groupByExpressions != null;
    Map<ExpressionContext, Integer> groupByIndex = new HashMap<>();
    for (int i = 0; i < groupByExpressions.size(); i++) {
      groupByIndex.put(groupByExpressions.get(i), i);
    }
    Map<Pair<FunctionContext, FilterContext>, Integer> aggregationIndex =
        queryContext.getFilteredAggregationsIndexMap();
    if (aggregationIndex == null) {
      return null;
    }
    int numTerms = orderByExpressions.size();
    int[] refs = new int[numTerms];
    boolean[] ascending = new boolean[numTerms];
    for (int t = 0; t < numTerms; t++) {
      OrderByExpressionContext orderBy = orderByExpressions.get(t);
      ExpressionContext expression = orderBy.getExpression();
      ascending[t] = orderBy.isAsc();
      Integer groupByColumn = groupByIndex.get(expression);
      if (groupByColumn != null) {
        // ORDER BY a group-by key column c → native ref (ORDER_REF_KEY - c): -1 → col 0, -2 → col 1, …
        refs[t] = PinotNativeGroupByCombine.ORDER_REF_KEY - groupByColumn;
        continue;
      }
      FunctionContext function = expression.getFunction();
      if (function == null || function.getType() != FunctionContext.Type.AGGREGATION) {
        // Literal, non-group-by identifier, filtered aggregation (TRANSFORM FILTER), or post-aggregation.
        return null;
      }
      Integer aggColumn = aggregationIndex.get(Pair.of(function, (FilterContext) null));
      if (aggColumn == null) {
        return null;
      }
      refs[t] = aggColumn;
    }
    return new OrderSpec(refs, ascending);
  }

  /** Native ORDER BY spec: parallel {@code refs} (ORDER_REF_KEY or agg column index) + {@code ascending}. */
  static final class OrderSpec {
    final int[] _refs;
    final boolean[] _ascending;

    OrderSpec(int[] refs, boolean[] ascending) {
      _refs = refs;
      _ascending = ascending;
    }
  }

  /**
   * A read-only, already-selected (top-K/capped and sorted) materialized result table built straight from
   * native's column arrays. No hashing/merging happens here — native already produced unique, merged,
   * ordered groups — so this deliberately avoids the {@link IndexedTable} re-hash (design §27.1). {@link
   * GroupByResultsBlock#getDataTable} consumes it via {@link #iterator()} / {@link #size()} / {@link
   * #getDataSchema()}.
   */
  private static final class NativeMaterializedTable extends BaseTable {
    private final List<Record> _records;
    private final int _numKeyColumns;
    private final AggregationFunction[] _aggregationFunctions;

    NativeMaterializedTable(DataSchema dataSchema, List<Record> records, int numKeyColumns,
        AggregationFunction[] aggregationFunctions) {
      super(dataSchema);
      _records = records;
      _numKeyColumns = numKeyColumns;
      _aggregationFunctions = aggregationFunctions;
    }

    @Override
    public boolean upsert(Record record) {
      throw new UnsupportedOperationException("NativeMaterializedTable is read-only");
    }

    @Override
    public boolean upsert(org.apache.pinot.core.data.table.Key key, Record record) {
      throw new UnsupportedOperationException("NativeMaterializedTable is read-only");
    }

    @Override
    public int size() {
      return _records.size();
    }

    @Override
    public Iterator<Record> iterator() {
      return _records.iterator();
    }

    /**
     * Native already applied the top-K/cap and ordering, so {@code sort} is a no-op. When {@code
     * storeFinalResult} is set (server-return-final-result), extract each aggregation's final result and
     * update the schema's aggregation column types — mirroring {@link IndexedTable#finish}.
     */
    @Override
    public void finish(boolean sort, boolean storeFinalResult) {
      if (!storeFinalResult) {
        return;
      }
      ColumnDataType[] columnDataTypes = _dataSchema.getColumnDataTypes();
      for (int i = 0; i < _aggregationFunctions.length; i++) {
        columnDataTypes[_numKeyColumns + i] = _aggregationFunctions[i].getFinalResultColumnType();
      }
      for (Record record : _records) {
        Object[] values = record.getValues();
        for (int i = 0; i < _aggregationFunctions.length; i++) {
          int colId = _numKeyColumns + i;
          values[colId] = _aggregationFunctions[i].extractFinalResult(values[colId]);
        }
      }
    }
  }

  /** Extract all combined key columns, boxed to each group-by column's Java type: {@code [column][group]}. */
  private Object[][] extractKeyColumns(int numGroups) {
    Object[][] columns = new Object[_numGroupByColumns][];
    for (int c = 0; c < _numGroupByColumns; c++) {
      columns[c] = extractOneKeyColumn(c, numGroups);
    }
    return columns;
  }

  private Object[] extractOneKeyColumn(int c, int numGroups) {
    Object[] values = new Object[numGroups];
    boolean single = _numGroupByColumns == 1;
    ColumnDataType columnType = _keyColumnTypes[c];
    switch (_combineColTypes[c]) {
      case PinotNativeGroupByCombine.KEY_TYPE_LONG: {
        long[] keys = new long[numGroups];
        if (single) {
          PinotNativeGroupByCombine.extractKeysLong(_combineHandle, keys);
        } else {
          PinotNativeGroupByCombine.extractKeyColumnLong(_combineHandle, c, keys);
        }
        boolean isInt = columnType == ColumnDataType.INT;
        for (int i = 0; i < numGroups; i++) {
          values[i] = isInt ? (Object) (int) keys[i] : (Object) keys[i];
        }
        break;
      }
      case PinotNativeGroupByCombine.KEY_TYPE_DOUBLE: {
        double[] keys = new double[numGroups];
        if (single) {
          PinotNativeGroupByCombine.extractKeysDouble(_combineHandle, keys);
        } else {
          PinotNativeGroupByCombine.extractKeyColumnDouble(_combineHandle, c, keys);
        }
        boolean isFloat = columnType == ColumnDataType.FLOAT;
        for (int i = 0; i < numGroups; i++) {
          values[i] = isFloat ? (Object) (float) keys[i] : (Object) keys[i];
        }
        break;
      }
      default: {
        int totalBytes = single
            ? PinotNativeGroupByCombine.stringKeysTotalBytes(_combineHandle)
            : PinotNativeGroupByCombine.keyColumnStringTotalBytes(_combineHandle, c);
        byte[] buffer = new byte[totalBytes];
        int[] offsets = new int[numGroups + 1];
        if (single) {
          PinotNativeGroupByCombine.extractKeysString(_combineHandle, buffer, offsets);
        } else {
          PinotNativeGroupByCombine.extractKeyColumnString(_combineHandle, c, buffer, offsets);
        }
        boolean isBytes = columnType == ColumnDataType.BYTES;
        for (int i = 0; i < numGroups; i++) {
          int from = offsets[i];
          int len = offsets[i + 1] - from;
          if (isBytes) {
            byte[] keyBytes = new byte[len];
            System.arraycopy(buffer, from, keyBytes, 0, len);
            values[i] = new ByteArray(keyBytes);
          } else {
            values[i] = new String(buffer, from, len, StandardCharsets.UTF_8);
          }
        }
        break;
      }
    }
    return values;
  }

  /**
   * Release the native combine handle exactly once. {@code synchronized} plus the non-zero check make
   * this idempotent and safe under concurrent invocation: the handle is atomically claimed (read +
   * zeroed under the lock) before the native free, so no two callers can observe the same non-zero
   * value and issue {@link PinotNativeGroupByCombine#destroy} twice. This is the Java-side guarantee
   * backing the "destroy exactly once" contract of the native entry point, which cannot self-guard —
   * after the box is freed the handle is a dangling pointer and a second free would be undefined
   * behavior. Mirrors the {@code HandleCleanup} pattern on the segment executor.
   */
  private synchronized void destroyHandle() {
    long handle = _combineHandle;
    if (handle != 0) {
      _combineHandle = 0;
      PinotNativeGroupByCombine.destroy(handle);
    }
  }
}
