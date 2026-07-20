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
package org.apache.pinot.core.query.optimizer.filter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.FilterKind;


/**
 * The {@code DistributivityFilterOptimizer} is the home for filter rewrites based on the distributive laws relating
 * AND and OR. Distributivity is a general algebraic principle; each concrete rewrite it enables is implemented as a
 * separate method so that additional laws (e.g. the OR-over-AND direction) can be added over time without changing the
 * chain wiring.
 *
 * <p>Laws currently implemented:
 * <ul>
 *   <li><b>{@link #undistributeANDOverOR} — factor a conjunct common to every branch of an OR out of the OR</b>
 *       (the "un-distribute AND over OR" direction, {@code (p AND a) OR (p AND b) == p AND (a OR b)}). This lets the
 *       common predicate be evaluated once instead of once per branch, and lets the downstream
 *       {@link MergeEqInFilterOptimizer} collapse the residual OR of equalities into a single IN predicate. Examples:
 *       <ul>
 *         <li>{@code (a = 1 AND b = 2) OR (a = 1 AND b = 3)} becomes {@code a = 1 AND (b = 2 OR b = 3)}, which
 *             {@link MergeEqInFilterOptimizer} then turns into {@code a = 1 AND b IN (2, 3)}.</li>
 *         <li>{@code (a = 1 AND b = 2) OR a = 1} becomes {@code a = 1} (a branch that consists solely of the common
 *             conjuncts is always-true once they are factored out, which absorbs the OR).</li>
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p>The dual direction (factoring a common disjunct out of an AND of ORs, {@code (p OR a) AND (p OR b) == p OR
 * (a AND b)}) is equally valid but is not yet implemented; it can be added as a sibling method when needed.
 *
 * <p>Scope limitation: {@link #undistributeANDOverOR} only factors a conjunct that is common to <b>every</b> branch of
 * the OR. If the common conjunct appears in only a subset of branches (e.g. {@code (A AND B) OR (A AND C) OR D} where
 * {@code D} shares nothing) the OR is left unchanged rather than partially factored to {@code (A AND (B OR C)) OR D}.
 * This matches DuckDB's {@code DistributivityRule} and ClickHouse's common-expression extraction, both of which factor
 * only the globally-common conjunct. Partial / grouped factoring (grouping branches by shared factors) is a possible
 * future enhancement.
 *
 * <p>Every rewrite here is a pure boolean restructuring that never inspects how an individual leaf predicate is
 * evaluated, so it is correct in Kleene three-valued logic (NULL handling) and for both single-value and multi-value
 * columns. It therefore requires no {@link Schema} and no single-value gating.
 *
 * <p>NOTE: This optimizer follows the {@link FlattenAndOrFilterOptimizer}, so the AND/OR branches are already on the
 * same level. A second {@link FlattenAndOrFilterOptimizer} pass is run after this optimizer to flatten the AND that
 * {@link #undistributeANDOverOR} introduces before {@link MergeEqInFilterOptimizer} runs.
 */
public class DistributivityFilterOptimizer implements FilterOptimizer {

  @Override
  public Expression optimize(Expression filterExpression, @Nullable Schema schema) {
    return filterExpression.getType() == ExpressionType.FUNCTION ? optimize(filterExpression) : filterExpression;
  }

  private Expression optimize(Expression filterExpression) {
    Function function = filterExpression.getFunctionCall();
    if (function == null) {
      return filterExpression;
    }
    String operator = function.getOperator();
    if (operator.equals(FilterKind.AND.name()) || operator.equals(FilterKind.NOT.name())) {
      // Recursively optimize the children.
      function.getOperands().replaceAll(this::optimize);
      return filterExpression;
    } else if (operator.equals(FilterKind.OR.name())) {
      // Recursively optimize the children first (bottom-up), then apply the distributive rewrites to this OR.
      function.getOperands().replaceAll(this::optimize);
      return undistributeANDOverOR(filterExpression);
    } else {
      return filterExpression;
    }
  }

  /**
   * Undistributes AND over OR by factoring the conjuncts common to every branch of the given OR out of the OR (the
   * distributive identity {@code OR(AND(p, q_i)...) == AND(p, OR(q_i...))}). Returns the (possibly rewritten)
   * expression.
   */
  private Expression undistributeANDOverOR(Expression orExpression) {
    Function orFunction = orExpression.getFunctionCall();
    List<Expression> branches = orFunction.getOperands();
    int numBranches = branches.size();
    if (numBranches < 2) {
      return orExpression;
    }

    // Split each branch into its set of AND conjuncts (a branch that is not an AND is treated as a single conjunct).
    List<List<Expression>> branchConjuncts = new ArrayList<>(numBranches);
    for (Expression branch : branches) {
      branchConjuncts.add(getConjuncts(branch));
    }

    // Compute the conjuncts common to every branch as the intersection of the per-branch conjunct sets. Order is
    // preserved from the first branch for deterministic output.
    LinkedHashSet<Expression> commonConjuncts = new LinkedHashSet<>(branchConjuncts.get(0));
    for (int i = 1; i < numBranches && !commonConjuncts.isEmpty(); i++) {
      commonConjuncts.retainAll(new HashSet<>(branchConjuncts.get(i)));
    }
    if (commonConjuncts.isEmpty()) {
      // No common conjunct: nothing to factor.
      return orExpression;
    }

    // Build the residual of each branch: the branch's conjuncts with the common ones removed. The rewrite applies the
    // identity OR(AND(common, r_i)...) == AND(common, OR(r_i...)), where r_i is the residual of branch i. Examples
    // (common = {a=5}):
    //   (a=5 AND b=1) OR (a=5 AND b=2)                  -> residuals [b=1], [b=2]                (single conjunct each)
    //   (a=5 AND b=1 AND c=1) OR (a=5 AND b=2 AND c=2)  -> residuals [AND(b=1,c=1)], [AND(b=2,c=2)] (multi-conjunct)
    // A residual is emitted bare when it has one conjunct, or re-wrapped in AND when it has several (see below).
    //
    // If any branch has an EMPTY residual, that branch was exactly the common conjuncts, i.e. its residual is TRUE.
    // Since OR(..., TRUE, ...) == TRUE and AND(common, TRUE) == common, the whole OR is absorbed into the common
    // conjuncts and we can stop. Examples:
    //   (a=5 AND b=1) OR a=5                            -> common={a=5}, branch 2 residual empty -> result: a=5
    //   (a=5 AND b=1) OR (a=5 AND b=1)                  -> common={a=5,b=1}, residual empty     -> result: AND(a=5,b=1)
    List<Expression> residualBranches = new ArrayList<>(numBranches);
    boolean absorbed = false;
    for (List<Expression> conjuncts : branchConjuncts) {
      List<Expression> residual = new ArrayList<>(conjuncts.size());
      for (Expression conjunct : conjuncts) {
        if (!commonConjuncts.contains(conjunct)) {
          residual.add(conjunct);
        }
      }
      if (residual.isEmpty()) {
        absorbed = true;
        break;
      }
      // Single-conjunct residual is emitted bare; multi-conjunct residual is re-wrapped in an AND (e.g. b=1 AND c=1)
      // so the enclosing OR sees one operand per branch.
      residualBranches.add(residual.size() == 1 ? residual.get(0) : and(residual));
    }

    // Reassemble as AND(common..., OR(residuals...)). When absorbed, the OR is dropped entirely and only the common
    // conjuncts remain. The size checks keep the tree well-formed (Pinot does not expect a 1-operand AND/OR):
    //   - a lone surviving residual is emitted bare rather than OR(x)
    //   - a single overall operand (e.g. absorption left just one common conjunct) is returned bare rather than AND(x)
    List<Expression> andOperands = new ArrayList<>(commonConjuncts.size() + 1);
    andOperands.addAll(commonConjuncts);
    if (!absorbed) {
      andOperands.add(residualBranches.size() == 1 ? residualBranches.get(0) : or(residualBranches));
    }
    return andOperands.size() == 1 ? andOperands.get(0) : and(andOperands);
  }

  /**
   * Returns the list of AND conjuncts of the given expression. If the expression is not an AND, it is returned as a
   * single-element list.
   */
  private static List<Expression> getConjuncts(Expression expression) {
    Function function = expression.getFunctionCall();
    if (function != null && function.getOperator().equals(FilterKind.AND.name())) {
      return function.getOperands();
    }
    List<Expression> conjuncts = new ArrayList<>(1);
    conjuncts.add(expression);
    return conjuncts;
  }

  private static Expression and(List<Expression> operands) {
    return RequestUtils.getFunctionExpression(FilterKind.AND.name(), operands);
  }

  private static Expression or(List<Expression> operands) {
    return RequestUtils.getFunctionExpression(FilterKind.OR.name(), operands);
  }
}
