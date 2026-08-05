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

import org.apache.pinot.segment.spi.index.reader.Dictionary;


/**
 * A primitive, columnar snapshot of a native segment GROUP BY result, produced by
 * {@link NativeGroupByExecutor#buildNativeSegmentResult()} for the native combine path
 * ({@link org.apache.pinot.core.operator.combine.NativeGroupByCombineOperator}).
 *
 * <p>This is the "boundary-1" fast path: instead of draining the native per-group results into a
 * boxed {@link AggregationGroupByResult} (a {@link GroupByResultHolder} per aggregation plus a
 * {@link NativeGroupKeyGenerator} that materializes an {@code Object[]} key per group), the executor
 * hands over the raw per-column dict-ids (in native {@code group_id} order), the segment-local
 * {@link Dictionary} per key column, and the already-widened {@code double} aggregate columns. The
 * combine operator decodes the dict-ids directly into the primitive key arrays it feeds across JNI —
 * no {@code Object} boxing, no {@code GroupByResultHolder}, no per-group iterator allocation.
 *
 * <p>All arrays are indexed by dense native {@code group_id} (0..numGroups-1), so key column {@code c}
 * and aggregation {@code a} for group {@code g} are {@code dictIds[c][g]} and {@code aggValues[a][g]}.
 */
public final class NativeSegmentResult {
  private final int _numGroups;
  // _dictIds[c][g] = dict-id of key column c for native group_id g (group_id order).
  private final int[][] _dictIds;
  // One segment-local dictionary per key column, in grouping order, to decode _dictIds -> raw values.
  private final Dictionary[] _dictionaries;
  // _aggValues[a][g] = aggregation a's value for group g, already widened to double (combine merges in double).
  private final double[][] _aggValues;

  public NativeSegmentResult(int numGroups, int[][] dictIds, Dictionary[] dictionaries, double[][] aggValues) {
    _numGroups = numGroups;
    _dictIds = dictIds;
    _dictionaries = dictionaries;
    _aggValues = aggValues;
  }

  public int getNumGroups() {
    return _numGroups;
  }

  public int[][] getDictIds() {
    return _dictIds;
  }

  public Dictionary[] getDictionaries() {
    return _dictionaries;
  }

  public double[][] getAggValues() {
    return _aggValues;
  }
}
