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
package org.apache.pinot.core.query.aggregation.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.nativeengine.agg.PinotNativeAgg;


/**
 * SUM aggregation accelerated by the native (Rust+JNI) engine. Phase 1.B scope: handles
 * single-value {@code INT}, {@code LONG}, {@code FLOAT}, {@code DOUBLE} columns by
 * dispatching to the corresponding {@link PinotNativeAgg} kernel. All other type /
 * encoding combinations (multi-value, {@code BIG_DECIMAL}, etc.) defer to the Java
 * parent class.
 *
 * <p>Construction is gated by {@link NativeAggregationRouter#shouldAccelerate}; this class
 * is never instantiated directly by user code.
 *
 * <p>This class extends {@link SumAggregationFunction} so it inherits identical intermediate
 * and final result types, merge semantics, and group-by hooks. Mixed-version clusters
 * (native server + Java server) produce byte-for-byte identical intermediate results.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class NativeSumAggregationFunction extends SumAggregationFunction {

  public NativeSumAggregationFunction(List<ExpressionContext> arguments,
      boolean nullHandlingEnabled) {
    super(arguments, nullHandlingEnabled);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (blockValSet.isSingleValue()) {
      double nativeSum = Double.NaN;
      switch (blockValSet.getValueType().getStoredType()) {
        case INT:
          nativeSum = PinotNativeAgg.sumInt(blockValSet.getIntValuesSV(), length);
          break;
        case LONG:
          nativeSum = PinotNativeAgg.sumLong(blockValSet.getLongValuesSV(), length);
          break;
        case FLOAT:
          nativeSum = PinotNativeAgg.sumFloat(blockValSet.getFloatValuesSV(), length);
          break;
        case DOUBLE:
          nativeSum = PinotNativeAgg.sumDouble(blockValSet.getDoubleValuesSV(), length);
          break;
        default:
          // BIG_DECIMAL, STRING-as-numeric, etc. — Java parent handles them.
          break;
      }
      // NaN is the native sentinel for "kernel error" — fall through to Java in that case
      // rather than propagate the sentinel into the result holder.
      if (!Double.isNaN(nativeSum)) {
        double prev = aggregationResultHolder.getDoubleResult();
        aggregationResultHolder.setValue(prev + nativeSum);
        return;
      }
    }
    super.aggregate(length, aggregationResultHolder, blockValSetMap);
  }
}
