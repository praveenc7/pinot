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
package org.apache.pinot.perf.aggregation;

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.MinAggregationFunction;
import org.apache.pinot.nativeengine.agg.PinotNativeAgg;


/**
 * Benchmark-only sibling of {@code NativeMinAggregationFunction} that calls the forced-scalar
 * JNI entry points ({@link PinotNativeAgg#minIntScalar}, {@code minLongScalar},
 * {@code minFloatScalar}, {@code minDoubleScalar}) instead of the SIMD-dispatched ones. Used by
 * {@link BenchmarkNativeMinMaxAggregation} to attribute the speedup between JNI cost,
 * Rust-language code-gen, and explicit SIMD intrinsics.
 *
 * <p>Lives in pinot-perf so the benchmark-only class does not widen the production module's
 * public API surface.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class NativeScalarMinAggregationFunction extends MinAggregationFunction {

  public NativeScalarMinAggregationFunction(List<ExpressionContext> arguments,
      boolean nullHandlingEnabled) {
    super(arguments, nullHandlingEnabled);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (blockValSet.isSingleValue()) {
      double blockMin = Double.NaN;
      switch (blockValSet.getValueType().getStoredType()) {
        case INT:
          blockMin = PinotNativeAgg.minIntScalar(blockValSet.getIntValuesSV(), length);
          break;
        case LONG:
          blockMin = PinotNativeAgg.minLongScalar(blockValSet.getLongValuesSV(), length);
          break;
        case FLOAT:
          blockMin = PinotNativeAgg.minFloatScalar(blockValSet.getFloatValuesSV(), length);
          break;
        case DOUBLE:
          blockMin = PinotNativeAgg.minDoubleScalar(blockValSet.getDoubleValuesSV(), length);
          break;
        default:
          break;
      }
      if (!Double.isNaN(blockMin)) {
        double prev = aggregationResultHolder.getDoubleResult();
        aggregationResultHolder.setValue(Math.min(blockMin, prev));
        return;
      }
    }
    super.aggregate(length, aggregationResultHolder, blockValSetMap);
  }
}
