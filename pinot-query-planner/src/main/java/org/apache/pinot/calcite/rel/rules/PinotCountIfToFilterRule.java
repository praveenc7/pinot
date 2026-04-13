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
package org.apache.pinot.calcite.rel.rules;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.exception.BadQueryRequestException;


/**
 * Rule that rewrites COUNTIF(boolean_expr) aggregate calls into COUNT(*) with a filter argument.
 *
 * In Calcite's plan, the boolean expression that COUNTIF takes as input is already projected as a column
 * in the Aggregate's input. This rule simply moves that column reference from the aggregate's argList
 * to its filterArg, and changes the function from COUNTIF to COUNT.
 *
 * Before: Aggregate(COUNTIF, argList=[3])  -- where column 3 is a boolean expression
 * After:  Aggregate(COUNT, argList=[], filterArg=3)
 */
public class PinotCountIfToFilterRule extends RelOptRule {
  public static final PinotCountIfToFilterRule INSTANCE =
      new PinotCountIfToFilterRule(PinotRuleUtils.PINOT_REL_FACTORY);

  private PinotCountIfToFilterRule(RelBuilderFactory factory) {
    super(operand(Aggregate.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Aggregate agg = call.rel(0);
    for (AggregateCall aggCall : agg.getAggCallList()) {
      if (aggCall.getAggregation().getName().equalsIgnoreCase(AggregationFunctionType.COUNTIF.getName())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate agg = call.rel(0);
    // matches() guarantees at least one COUNTIF exists. Rebuild the agg call list,
    // rewriting COUNTIF calls and preserving all other aggregations unchanged.
    List<AggregateCall> newAggCalls = new ArrayList<>();
    for (AggregateCall aggCall : agg.getAggCallList()) {
      if (aggCall.getAggregation().getName().equalsIgnoreCase(AggregationFunctionType.COUNTIF.getName())) {
        if (aggCall.isDistinct()) {
          throw new BadQueryRequestException(
              "Function 'COUNTIF' on DISTINCT is not supported.");
        }
        // COUNTIF has exactly one argument: the boolean column index
        int filterArgIdx = aggCall.getArgList().get(0);
        AggregateCall newCall = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            aggCall.isApproximate(),
            aggCall.ignoreNulls(),
            aggCall.rexList,
            ImmutableIntList.of(),
            filterArgIdx,
            aggCall.distinctKeys,
            aggCall.collation,
            agg.getGroupCount(),
            agg.getInput(),
            null,
            aggCall.getName());
        newAggCalls.add(newCall);
      } else {
        // Non-COUNTIF aggregation (e.g., SUM, AVG) — keep unchanged
        newAggCalls.add(aggCall);
      }
    }
    call.transformTo(
        agg.copy(agg.getTraitSet(), agg.getInput(), agg.getGroupSet(), agg.getGroupSets(), newAggCalls));
  }
}
