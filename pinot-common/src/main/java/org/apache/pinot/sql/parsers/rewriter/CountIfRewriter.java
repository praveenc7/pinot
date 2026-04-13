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
package org.apache.pinot.sql.parsers.rewriter;

import java.util.List;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.sql.parsers.SqlCompilationException;


/**
 * Rewrites COUNTIF(predicate) into COUNT(*) FILTER(WHERE predicate).
 *
 * In the PinotQuery AST, COUNT(*) FILTER(WHERE predicate) is represented as:
 *   filter(count(*), predicate)
 *
 * This rewriter traverses the select list, having list, and order-by list
 * to find and rewrite all occurrences of COUNTIF.
 */
public class CountIfRewriter implements QueryRewriter {
  private static final String COUNT_IF = "countif";
  private static final String FILTER = "filter";
  private static final String COUNT = "count";
  private static final String STAR = "*";

  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    List<Expression> selectList = pinotQuery.getSelectList();
    if (selectList != null) {
      for (Expression expression : selectList) {
        rewriteExpression(expression);
      }
    }
    List<Expression> havingList = pinotQuery.getHavingExpression() != null
        ? List.of(pinotQuery.getHavingExpression()) : null;
    if (havingList != null) {
      for (Expression expression : havingList) {
        rewriteExpression(expression);
      }
    }
    List<Expression> orderByList = pinotQuery.getOrderByList();
    if (orderByList != null) {
      for (Expression expression : orderByList) {
        rewriteExpression(expression);
      }
    }
    return pinotQuery;
  }

  private static void rewriteExpression(Expression expression) {
    Function function = expression.getFunctionCall();
    if (function == null) {
      return;
    }
    String operator = function.getOperator();
    if (operator.equals(COUNT_IF)) {
      List<Expression> operands = function.getOperands();
      if (operands == null || operands.size() != 1) {
        throw new IllegalArgumentException("COUNTIF expects exactly one boolean predicate argument");
      }
      Expression predicate = operands.get(0);
      // Validate that the predicate does not contain aggregate functions (nested aggregation)
      validateNoNestedAggregates(predicate);
      // Build count(*) expression
      Expression countStar = RequestUtils.getFunctionExpression(COUNT, RequestUtils.getIdentifierExpression(STAR));
      // Rewrite this expression to filter(count(*), predicate)
      Function filterFunction = new Function(FILTER);
      filterFunction.setOperands(List.of(countStar, predicate));
      expression.setFunctionCall(filterFunction);
      return;
    }
    // Recurse into operands to handle nested expressions (e.g., inside AS, ORDER BY, etc.)
    List<Expression> operands = function.getOperands();
    if (operands != null) {
      for (Expression operand : operands) {
        rewriteExpression(operand);
      }
    }
  }

  /**
   * Validates that the given expression does not contain any aggregate functions.
   * This catches invalid nesting like COUNTIF(COUNTIF(x > 5)) or COUNTIF(SUM(x) > 5)
   * at rewrite time rather than failing with a confusing error at execution time.
   */
  private static void validateNoNestedAggregates(Expression expression) {
    Function function = expression.getFunctionCall();
    if (function == null) {
      return;
    }
    String operator = function.getOperator();
    if (operator.equals(COUNT_IF) || AggregationFunctionType.isAggregationFunction(operator)) {
      throw new SqlCompilationException("Aggregate expressions cannot be nested");
    }
    List<Expression> operands = function.getOperands();
    if (operands != null) {
      for (Expression operand : operands) {
        validateNoNestedAggregates(operand);
      }
    }
  }
}
