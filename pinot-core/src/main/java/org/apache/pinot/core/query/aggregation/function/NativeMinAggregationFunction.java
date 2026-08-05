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
 * MIN aggregation accelerated by the native (Rust+JNI) engine. Dispatches single-value
 * {@code INT}, {@code LONG}, {@code FLOAT}, {@code DOUBLE} columns to the corresponding
 * {@link PinotNativeAgg} kernel. Other type / encoding combinations fall through to the
 * Java parent.
 *
 * <p>NaN semantics match the parent's {@code Math.min(...)} behavior — NaN propagates.
 * Empty input returns {@link Double#POSITIVE_INFINITY}, the same default value the parent
 * seeds its holder with.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class NativeMinAggregationFunction extends MinAggregationFunction {

  public NativeMinAggregationFunction(List<ExpressionContext> arguments,
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
          blockMin = PinotNativeAgg.minInt(blockValSet.getIntValuesSV(), length);
          break;
        case LONG:
          blockMin = PinotNativeAgg.minLong(blockValSet.getLongValuesSV(), length);
          break;
        case FLOAT:
          blockMin = PinotNativeAgg.minFloat(blockValSet.getFloatValuesSV(), length);
          break;
        case DOUBLE:
          blockMin = PinotNativeAgg.minDouble(blockValSet.getDoubleValuesSV(), length);
          break;
        default:
          break;
      }
      // NaN here can mean two things:
      //   1. JNI sentinel for kernel failure -> fall through to Java parent.
      //   2. The block legitimately contained NaN -> the holder should become NaN.
      // We can't distinguish (2) from (1) at the boundary, so we conservatively
      // fall through to Java on NaN. Java's path will compute the same NaN
      // result for case (2) using its own NaN-propagating Math.min path.
      if (!Double.isNaN(blockMin)) {
        double prev = aggregationResultHolder.getDoubleResult();
        // Math.min handles -0.0 < +0.0 strict ordering, matching the Java parent.
        aggregationResultHolder.setValue(Math.min(blockMin, prev));
        return;
      }
    }
    super.aggregate(length, aggregationResultHolder, blockValSetMap);
  }
}
