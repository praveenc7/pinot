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


/**
 * COUNT aggregation accelerated by the native engine — but notably without a kernel.
 *
 * <p>Under {@link NativeAggregationRouter}'s gating (null handling disabled, simple
 * column identifier or absent expression), both {@code COUNT(*)} and {@code COUNT(col)}
 * collapse to "block length added to the result holder". Crossing JNI for this would
 * add the ~85 ns FFI fixed cost from §11.A of the design doc with zero kernel benefit.
 *
 * <p>So the "native" path here is just: {@code holder.setValue(holder.getDoubleResult() + length)},
 * inline in Java. The class still exists so the routing infra in
 * {@link NativeAggregationRouter} can be uniform across SUM/MIN/MAX/COUNT — every routed
 * function gets its own {@code Native*AggregationFunction} class, even when the
 * implementation is trivial.
 *
 * <p>Star-tree pre-aggregated case (where {@code blockValSetMap} is not empty and
 * {@code nullHandlingEnabled=false} but the expression is the star-tree-internal
 * {@code COUNT_STAR_EXPRESSION}, requiring a {@code long[]} sum) and null-handling case
 * both fall through to the Java parent. {@link NativeAggregationRouter#shouldAccelerate}
 * already disqualifies them, but we double-guard here for safety.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class NativeCountAggregationFunction extends CountAggregationFunction {

  public NativeCountAggregationFunction(List<ExpressionContext> arguments,
      boolean nullHandlingEnabled) {
    super(arguments, nullHandlingEnabled);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    // Fast path: COUNT(*) — blockValSetMap is empty per CountAggregationFunction's contract,
    // so the value to add IS the block length.
    if (blockValSetMap.isEmpty()) {
      aggregationResultHolder.setValue(aggregationResultHolder.getDoubleResult() + length);
      return;
    }
    // Anything else (null-handling enabled, star-tree pre-aggregated) — defer to Java.
    // NativeAggregationRouter.shouldAccelerate should disqualify these at construction time,
    // but we double-guard.
    super.aggregate(length, aggregationResultHolder, blockValSetMap);
  }
}
