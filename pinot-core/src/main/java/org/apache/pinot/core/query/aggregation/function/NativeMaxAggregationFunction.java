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
 * MAX aggregation accelerated by the native (Rust+JNI) engine. Mirror of
 * {@link NativeMinAggregationFunction} for {@code MAX} semantics: dispatches
 * single-value INT/LONG/FLOAT/DOUBLE columns to the matching {@link PinotNativeAgg}
 * kernel; other type / encoding combinations fall through to the Java parent.
 *
 * <p>NaN semantics match the parent's {@code Math.max(...)} — NaN propagates.
 * Empty input returns {@link Double#NEGATIVE_INFINITY}.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class NativeMaxAggregationFunction extends MaxAggregationFunction {

  public NativeMaxAggregationFunction(List<ExpressionContext> arguments,
      boolean nullHandlingEnabled) {
    super(arguments, nullHandlingEnabled);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (blockValSet.isSingleValue()) {
      double blockMax = Double.NaN;
      switch (blockValSet.getValueType().getStoredType()) {
        case INT:
          blockMax = PinotNativeAgg.maxInt(blockValSet.getIntValuesSV(), length);
          break;
        case LONG:
          blockMax = PinotNativeAgg.maxLong(blockValSet.getLongValuesSV(), length);
          break;
        case FLOAT:
          blockMax = PinotNativeAgg.maxFloat(blockValSet.getFloatValuesSV(), length);
          break;
        case DOUBLE:
          blockMax = PinotNativeAgg.maxDouble(blockValSet.getDoubleValuesSV(), length);
          break;
        default:
          break;
      }
      // See NativeMinAggregationFunction for the NaN-vs-error rationale.
      if (!Double.isNaN(blockMax)) {
        double prev = aggregationResultHolder.getDoubleResult();
        aggregationResultHolder.setValue(Math.max(blockMax, prev));
        return;
      }
    }
    super.aggregate(length, aggregationResultHolder, blockValSetMap);
  }
}
