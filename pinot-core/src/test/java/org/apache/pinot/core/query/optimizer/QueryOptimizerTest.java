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
package org.apache.pinot.core.query.optimizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.Query.Range;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class QueryOptimizerTest {
  private static final QueryOptimizer OPTIMIZER = new QueryOptimizer();
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("int", DataType.INT)
          .addSingleValueDimension("long", DataType.LONG).addSingleValueDimension("float", DataType.FLOAT)
          .addSingleValueDimension("double", DataType.DOUBLE).addSingleValueDimension("string", DataType.STRING)
          .addSingleValueDimension("bytes", DataType.BYTES).addMultiValueDimension("mvInt", DataType.INT).build();

  @Test
  public void testNoFilter() {
    String query = "SELECT * FROM testTable";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    assertNull(pinotQuery.getFilterExpression());
  }

  @Test
  public void testFlattenAndOrFilter() {
    String query =
        "SELECT * FROM testTable WHERE ((int = 4 OR (long = 5 AND (float = 9 AND double = 7.5))) OR string = 'foo') "
            + "OR bytes = 'abc'";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    Function filterFunction = pinotQuery.getFilterExpression().getFunctionCall();
    assertEquals(filterFunction.getOperator(), FilterKind.OR.name());
    List<Expression> children = filterFunction.getOperands();
    assertEquals(children.size(), 4);
    assertEquals(children.get(0), getEqFilterExpression("int", 4));
    assertEquals(children.get(2), getEqFilterExpression("string", "foo"));
    assertEquals(children.get(3), getEqFilterExpression("bytes", "abc"));

    Function secondChildFunction = children.get(1).getFunctionCall();
    assertEquals(secondChildFunction.getOperator(), FilterKind.AND.name());
    List<Expression> secondChildChildren = secondChildFunction.getOperands();
    assertEquals(secondChildChildren.size(), 3);
    assertEquals(secondChildChildren.get(0), getEqFilterExpression("long", 5));
    assertEquals(secondChildChildren.get(1), getEqFilterExpression("float", 9));
    assertEquals(secondChildChildren.get(2), getEqFilterExpression("double", 7.5));
  }

  private static Expression getEqFilterExpression(String column, Object value) {
    return RequestUtils.getFunctionExpression(FilterKind.EQUALS.name(), RequestUtils.getIdentifierExpression(column),
        RequestUtils.getLiteralExpression(value));
  }

  @Test
  public void testMergeEqInFilter() {
    String query =
        "SELECT * FROM testTable WHERE int IN (1, 1) AND (long IN (2, 3) OR long IN (3, 4) OR long = 2) AND (float = "
            + "3.5 OR double IN (1.1, 1.2) OR float = 4.5 OR float > 5.5 OR double = 1.3)";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    Function filterFunction = pinotQuery.getFilterExpression().getFunctionCall();
    assertEquals(filterFunction.getOperator(), FilterKind.AND.name());
    List<Expression> children = filterFunction.getOperands();
    assertEquals(children.size(), 3);
    assertEquals(children.get(0), getEqFilterExpression("int", 1));
    checkInFilterFunction(children.get(1).getFunctionCall(), "long", Arrays.asList(2, 3, 4));

    Function thirdChildFunction = children.get(2).getFunctionCall();
    assertEquals(thirdChildFunction.getOperator(), FilterKind.OR.name());
    List<Expression> thirdChildChildren = thirdChildFunction.getOperands();
    assertEquals(thirdChildChildren.size(), 3);
    assertEquals(thirdChildChildren.get(0).getFunctionCall().getOperator(), FilterKind.GREATER_THAN.name());

    // Order of second and third child is not deterministic
    Function secondGrandChildFunction = thirdChildChildren.get(1).getFunctionCall();
    assertEquals(secondGrandChildFunction.getOperator(), FilterKind.IN.name());
    Function thirdGrandChildFunction = thirdChildChildren.get(2).getFunctionCall();
    assertEquals(thirdGrandChildFunction.getOperator(), FilterKind.IN.name());
    if (secondGrandChildFunction.getOperands().get(0).getIdentifier().getName().equals("float")) {
      checkInFilterFunction(secondGrandChildFunction, "float", Arrays.asList(3.5, 4.5));
      checkInFilterFunction(thirdGrandChildFunction, "double", Arrays.asList(1.1, 1.2, 1.3));
    } else {
      checkInFilterFunction(secondGrandChildFunction, "double", Arrays.asList(1.1, 1.2, 1.3));
      checkInFilterFunction(thirdGrandChildFunction, "float", Arrays.asList(3.5, 4.5));
    }
  }

  private static void checkInFilterFunction(Function inFilterFunction, String column, List<Object> values) {
    assertEquals(inFilterFunction.getOperator(), FilterKind.IN.name());
    List<Expression> operands = inFilterFunction.getOperands();
    int numOperands = operands.size();
    assertEquals(numOperands, values.size() + 1);
    assertEquals(operands.get(0).getIdentifier().getName(), column);
    Set<Expression> valueExpressions = new HashSet<>();
    for (Object value : values) {
      valueExpressions.add(RequestUtils.getLiteralExpression(value));
    }
    for (int i = 1; i < numOperands; i++) {
      assertTrue(valueExpressions.contains(operands.get(i)));
    }
  }

  @Test
  public void testMergeRangeFilter() {
    String query =
        "SELECT * FROM testTable WHERE (int > 10 AND int <= 100 AND int BETWEEN 10 AND 20) OR (float BETWEEN 5.5 AND "
            + "7.5 AND float = 6 AND float < 6.5 AND float BETWEEN 6 AND 8) OR (string > '123' AND string > '23') OR "
            + "(mvInt > 5 AND mvInt < 0)";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    Function filterFunction = pinotQuery.getFilterExpression().getFunctionCall();
    assertEquals(filterFunction.getOperator(), FilterKind.OR.name());
    List<Expression> operands = filterFunction.getOperands();
    assertEquals(operands.size(), 4);
    assertEquals(operands.get(0), getRangeFilterExpression("int", "(10\00020]"));
    // Alphabetical order for STRING column ('23' > '123')
    assertEquals(operands.get(2), getRangeFilterExpression("string", "(23\000*)"));

    Function secondChildFunction = operands.get(1).getFunctionCall();
    assertEquals(secondChildFunction.getOperator(), FilterKind.AND.name());
    List<Expression> secondChildChildren = secondChildFunction.getOperands();
    assertEquals(secondChildChildren.size(), 2);
    assertEquals(secondChildChildren.get(0), getEqFilterExpression("float", 6));
    assertEquals(secondChildChildren.get(1), getRangeFilterExpression("float", "[6.0\0006.5)"));

    // Range filter on multi-value column should not be merged ([-5, 10] can match this filter)
    Function fourthChildFunction = operands.get(3).getFunctionCall();
    assertEquals(fourthChildFunction.getOperator(), FilterKind.AND.name());
    List<Expression> fourthChildChildren = fourthChildFunction.getOperands();
    assertEquals(fourthChildChildren.size(), 2);
    assertEquals(fourthChildChildren.get(0).getFunctionCall().getOperator(), FilterKind.GREATER_THAN.name());
    assertEquals(fourthChildChildren.get(1).getFunctionCall().getOperator(), FilterKind.LESS_THAN.name());
  }

  /**
   * Original:  (int = 5 AND long = 1) OR (int = 5 AND long = 2) OR (int = 5 AND long = 3)
   * Rewritten: int = 5 AND long IN (1, 2, 3)
   */
  @Test
  public void testFactorCommonPredicateOutOfOr() {
    String query =
        "SELECT * FROM testTable WHERE (int = 5 AND long = 1) OR (int = 5 AND long = 2) OR (int = 5 AND long = 3)";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    Function filterFunction = pinotQuery.getFilterExpression().getFunctionCall();
    assertEquals(filterFunction.getOperator(), FilterKind.AND.name());
    List<Expression> children = filterFunction.getOperands();
    assertEquals(children.size(), 2);
    assertEquals(children.get(0), getEqFilterExpression("int", 5));
    checkInFilterFunction(children.get(1).getFunctionCall(), "long", Arrays.asList(1, 2, 3));
  }

  /**
   * Original:  (int = 5 AND string = 'foo' AND long = 1) OR (int = 5 AND string = 'foo' AND long = 2)
   * Rewritten: int = 5 AND string = 'foo' AND long IN (1, 2)
   */
  @Test
  public void testFactorCommonPredicateOutOfOrMultipleCommon() {
    String query = "SELECT * FROM testTable WHERE (int = 5 AND string = 'foo' AND long = 1) OR (int = 5 AND string = "
        + "'foo' AND long = 2)";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    Function filterFunction = pinotQuery.getFilterExpression().getFunctionCall();
    assertEquals(filterFunction.getOperator(), FilterKind.AND.name());
    List<Expression> children = filterFunction.getOperands();
    assertEquals(children.size(), 3);
    Set<Expression> expectedCommon = new HashSet<>();
    expectedCommon.add(getEqFilterExpression("int", 5));
    expectedCommon.add(getEqFilterExpression("string", "foo"));
    assertTrue(expectedCommon.contains(children.get(0)));
    assertTrue(expectedCommon.contains(children.get(1)));
    checkInFilterFunction(children.get(2).getFunctionCall(), "long", Arrays.asList(1, 2));
  }

  /**
   * Original:  (int = 5 AND long = 1) OR int = 5
   * Rewritten: int = 5            (the branch that is only the common conjunct absorbs the OR)
   */
  @Test
  public void testFactorCommonPredicateAbsorption() {
    String query = "SELECT * FROM testTable WHERE (int = 5 AND long = 1) OR int = 5";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    assertEquals(pinotQuery.getFilterExpression(), getEqFilterExpression("int", 5));
  }

  /**
   * Original:  (int = 1 AND long = 2) OR (int = 3 AND long = 4)
   * Rewritten: (int = 1 AND long = 2) OR (int = 3 AND long = 4)   (unchanged: no conjunct common to all branches)
   */
  @Test
  public void testFactorCommonPredicateNoCommonConjunct() {
    String query = "SELECT * FROM testTable WHERE (int = 1 AND long = 2) OR (int = 3 AND long = 4)";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    Function filterFunction = pinotQuery.getFilterExpression().getFunctionCall();
    assertEquals(filterFunction.getOperator(), FilterKind.OR.name());
    List<Expression> children = filterFunction.getOperands();
    assertEquals(children.size(), 2);
    for (Expression child : children) {
      assertEquals(child.getFunctionCall().getOperator(), FilterKind.AND.name());
    }
  }

  /**
   * Original:  (int = 5 AND long = 1) OR (int = 5 AND long = 2) OR double = 9
   * Rewritten: (int = 5 AND long = 1) OR (int = 5 AND long = 2) OR double = 9   (unchanged)
   *
   * <p>Boundary case: int = 5 is common to the first two branches but not to the third (double = 9), so there is no
   * conjunct common to *every* branch. This optimizer only factors a globally-common conjunct (matching DuckDB and
   * ClickHouse), so it leaves the OR untouched here. Partial / grouped factoring —
   * {@code (A AND B) OR (A AND C) OR D -> (A AND (B OR C)) OR D} — is a deferred future enhancement.
   */
  @Test
  public void testFactorCommonPredicatePartialCommonNotFactored() {
    String query = "SELECT * FROM testTable WHERE (int = 5 AND long = 1) OR (int = 5 AND long = 2) OR double = 9";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    Function filterFunction = pinotQuery.getFilterExpression().getFunctionCall();
    // Still a 3-way OR; no factoring happened.
    assertEquals(filterFunction.getOperator(), FilterKind.OR.name());
    List<Expression> children = filterFunction.getOperands();
    assertEquals(children.size(), 3);
    // int = 5 must NOT have been factored out to a sibling of the OR (it is not common to the double = 9 branch).
    assertTrue(!children.contains(getEqFilterExpression("int", 5)),
        "int = 5 must not be factored out when it is not common to every OR branch");
    // The two AND-of-EQ branches are preserved as ANDs.
    int numAndBranches = 0;
    for (Expression child : children) {
      if (child.getFunctionCall().getOperator().equals(FilterKind.AND.name())) {
        numAndBranches++;
      }
    }
    assertEquals(numAndBranches, 2);
  }

  /**
   * Original:  (mvInt = 1 AND long = 2) OR (mvInt = 1 AND long = 3)   (mvInt is a multi-value column)
   * Rewritten: mvInt = 1 AND long IN (2, 3)
   *
   * <p>The rewrite is a pure boolean restructuring, so it is correct for multi-value columns and needs no
   * single-value gating.
   */
  @Test
  public void testFactorCommonPredicateMultiValueColumn() {
    String query = "SELECT * FROM testTable WHERE (mvInt = 1 AND long = 2) OR (mvInt = 1 AND long = 3)";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    Function filterFunction = pinotQuery.getFilterExpression().getFunctionCall();
    assertEquals(filterFunction.getOperator(), FilterKind.AND.name());
    List<Expression> children = filterFunction.getOperands();
    assertEquals(children.size(), 2);
    assertEquals(children.get(0), getEqFilterExpression("mvInt", 1));
    checkInFilterFunction(children.get(1).getFunctionCall(), "long", Arrays.asList(2, 3));
  }

  /**
   * Deeply-nested machine-generated shape (the one that motivated this optimizer): the wide OR-of-ANDs is one conjunct
   * several levels down inside a top-level AND, and every branch shares the conjunct int = 5.
   *
   * Original:  double >= 10 AND double <= 20 AND long IN (1000..1300) AND int > 0 AND long > 0
   *              AND ( (int = 5 AND long = 1000) OR (int = 5 AND long = 1001) OR ... OR (int = 5 AND long = 1099) )
   * Rewritten: double >= 10 AND double <= 20 AND long IN (1000..1300) AND int > 0 AND long > 0
   *              AND int = 5 AND long IN (1000..1099)
   *
   * <p>Note: this optimizer only factors the OR and merges the residual into an IN; it does not remove the
   * now-redundant superset {@code long IN (1000..1300)} or the {@code > 0} predicates (that subsumption is a separate
   * optimizer).
   */
  @Test
  public void testFactorCommonPredicateDeepOriginalShape() {
    int numOrBranches = 100;
    StringBuilder orBlock = new StringBuilder();
    List<Object> residualCampaignIds = new ArrayList<>(numOrBranches);
    for (int i = 0; i < numOrBranches; i++) {
      int campaignId = 1000 + i;
      residualCampaignIds.add(campaignId);
      if (i > 0) {
        orBlock.append(" OR ");
      }
      orBlock.append("(int = 5 AND long = ").append(campaignId).append(")");
    }
    StringBuilder inList = new StringBuilder();
    for (int v = 1000; v <= 1300; v++) {
      if (v > 1000) {
        inList.append(", ");
      }
      inList.append(v);
    }
    String query = "SELECT * FROM testTable WHERE double >= 10 AND double <= 20 AND long IN (" + inList + ") "
        + "AND int > 0 AND long > 0 AND (" + orBlock + ")";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Expression filter = pinotQuery.getFilterExpression();
    Function filterFunction = filter.getFunctionCall();
    assertEquals(filterFunction.getOperator(), FilterKind.AND.name());
    // The wide OR-of-ANDs must be completely eliminated: no OR node should remain anywhere in the tree.
    assertNoOperator(filter, FilterKind.OR);
    // The common conjunct (int = 5) must be factored up to the top-level AND.
    assertTrue(filterFunction.getOperands().contains(getEqFilterExpression("int", 5)),
        "Expected the common conjunct int = 5 to be factored out to the top-level AND");
    // The residual campaignId equalities must be merged into a single IN predicate with exactly the 100 branch values.
    Function residualIn = findInFilterOnColumn(filterFunction.getOperands(), "long", residualCampaignIds.size());
    assertTrue(residualIn != null, "Expected a merged IN predicate on 'long' with the 100 residual campaignId values");
    checkInFilterFunction(residualIn, "long", residualCampaignIds);
  }

  /**
   * Recursively asserts that the given filter expression contains no function node with the given operator.
   */
  private static void assertNoOperator(Expression expression, FilterKind operator) {
    Function function = expression.getFunctionCall();
    if (function == null) {
      return;
    }
    assertNotEquals(function.getOperator(), operator.name(),
        "Did not expect any " + operator.name() + " node to remain in the optimized filter");
    for (Expression operand : function.getOperands()) {
      assertNoOperator(operand, operator);
    }
  }

  /**
   * Returns the IN filter function on the given column with the given number of values from the list of top-level
   * conjuncts, or {@code null} if none matches.
   */
  private static Function findInFilterOnColumn(List<Expression> conjuncts, String column, int numValues) {
    for (Expression conjunct : conjuncts) {
      Function function = conjunct.getFunctionCall();
      if (function != null && function.getOperator().equals(FilterKind.IN.name())
          && function.getOperands().get(0).getIdentifier().getName().equals(column)
          && function.getOperands().size() == numValues + 1) {
        return function;
      }
    }
    return null;
  }

  @Test
  public void testMergeTextMatchFilter() {
    String query =
        "SELECT * FROM testTable WHERE TEXT_MATCH(string, 'foo') AND TEXT_MATCH(string, 'bar') OR TEXT_MATCH(string, "
            + "'baz')";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    Function filterFunction = pinotQuery.getFilterExpression().getFunctionCall();
    assertEquals(filterFunction.getOperator(), FilterKind.TEXT_MATCH.name());
    List<Expression> operands = filterFunction.getOperands();
    assertEquals(operands.size(), 2);
    assertEquals(operands.get(0), RequestUtils.getIdentifierExpression("string"));
    assertEquals(operands.get(1), RequestUtils.getLiteralExpression("((foo) AND (bar)) OR (baz)"));
  }

  private static Expression getRangeFilterExpression(String column, String rangeString) {
    return RequestUtils.getFunctionExpression(FilterKind.RANGE.name(), RequestUtils.getIdentifierExpression(column),
        RequestUtils.getLiteralExpression(rangeString));
  }

  @Test
  public void testQueries() {
    // Distributivity: factor a conjunct common to all OR branches out of the OR (then MergeEqIn collapses to IN)
    testQuery("SELECT * FROM testTable WHERE (int = 5 AND long = 1) OR (int = 5 AND long = 2) OR (int = 5 AND long "
            + "= 3)",
        "SELECT * FROM testTable WHERE int = 5 AND long IN (1, 2, 3)");
    testQuery("SELECT * FROM testTable WHERE (int = 5 AND long = 1) OR int = 5",
        "SELECT * FROM testTable WHERE int = 5");
    testQuery("SELECT * FROM testTable WHERE (int = 5 AND string = 'foo' AND long = 1) OR (int = 5 AND string = 'foo' "
            + "AND long = 2)",
        "SELECT * FROM testTable WHERE int = 5 AND string = 'foo' AND long IN (1, 2)");
    testCannotOptimizeQuery("SELECT * FROM testTable WHERE (int = 1 AND long = 2) OR (int = 3 AND long = 4)");

    // Distributivity edge cases (robustness): each runs the full optimizer, so it also exercises the no-crash path.
    // The common conjunct is order-independent across branches.
    testQuery("SELECT * FROM testTable WHERE (long = 1 AND int = 5) OR (int = 5 AND long = 2)",
        "SELECT * FROM testTable WHERE int = 5 AND long IN (1, 2)");
    // The common conjunct is a complex (IN) predicate, not just an EQ.
    testQuery("SELECT * FROM testTable WHERE (int IN (7, 8) AND long = 1) OR (int IN (7, 8) AND long = 2)",
        "SELECT * FROM testTable WHERE int IN (7, 8) AND long IN (1, 2)");
    // The common conjunct is a range predicate.
    testQuery("SELECT * FROM testTable WHERE (int > 5 AND long = 1) OR (int > 5 AND long = 2)",
        "SELECT * FROM testTable WHERE int > 5 AND long IN (1, 2)");
    // Residual of each branch is itself multi-conjunct.
    testQuery("SELECT * FROM testTable WHERE (int = 5 AND long = 1 AND float = 1.5) OR (int = 5 AND long = 2 AND float "
            + "= 2.5)",
        "SELECT * FROM testTable WHERE int = 5 AND ((long = 1 AND float = 1.5) OR (long = 2 AND float = 2.5))");
    // Nested OR-of-ANDs several levels deep inside a top-level AND (exercises bottom-up recursion + re-flatten).
    testQuery("SELECT * FROM testTable WHERE double = 9 AND ((int = 5 AND long = 1) OR (int = 5 AND long = 2))",
        "SELECT * FROM testTable WHERE double = 9 AND int = 5 AND long IN (1, 2)");
    // All branches identical: absorbed via full dedup down to a single AND.
    testQuery("SELECT * FROM testTable WHERE (int = 5 AND long = 1) OR (int = 5 AND long = 1)",
        "SELECT * FROM testTable WHERE int = 5 AND long = 1");
    // NOT wrapping an OR-of-ANDs: distributivity still factors inside the NOT. (MergeEqInFilterOptimizer does not
    // recurse into a top-level NOT, so the residual OR is left as-is rather than collapsed to IN; the result is still
    // correct, just not merged.)
    testQuery("SELECT * FROM testTable WHERE NOT ((int = 5 AND long = 1) OR (int = 5 AND long = 2))",
        "SELECT * FROM testTable WHERE NOT (int = 5 AND (long = 1 OR long = 2))");
    // No-op / crash-safety cases: nothing to factor, optimizer must leave these untouched.
    testCannotOptimizeQuery("SELECT * FROM testTable WHERE int = 5");
    testCannotOptimizeQuery("SELECT * FROM testTable WHERE (int = 1 AND long = 2) OR (float = 3.5 AND double = 4.5)");
    // Partial common conjunct (int = 5 shared by 2 of 3 branches): only a globally-common conjunct is factored, so
    // this is left unchanged (partial/grouped factoring is a deferred future enhancement).
    testCannotOptimizeQuery("SELECT * FROM testTable WHERE (int = 5 AND long = 1) OR (int = 5 AND long = 2) OR double "
        + "= 9");

    // MergeEqInFilter
    testQuery("SELECT * FROM testTable WHERE int = 1 OR int = 2 OR int = 3",
        "SELECT * FROM testTable WHERE int IN (1, 2, 3)");
    testQuery("SELECT * FROM testTable WHERE int = 1 OR int = 2 OR int = 3 AND long = 4",
        "SELECT * FROM testTable WHERE int IN (1, 2) OR (int = 3 AND long = 4)");
    testQuery("SELECT * FROM testTable WHERE int = 1 OR int = 2 OR int = 3 OR long = 4 OR long = 5 OR long = 6",
        "SELECT * FROM testTable WHERE int IN (1, 2, 3) OR long IN (4, 5, 6)");
    testQuery("SELECT * FROM testTable WHERE int = 1 OR long = 4 OR int = 2 OR long = 5 OR int = 3 OR long = 6",
        "SELECT * FROM testTable WHERE int IN (1, 2, 3) OR long IN (4, 5, 6)");
    testQuery("SELECT * FROM testTable WHERE int = 1 OR int = 1", "SELECT * FROM testTable WHERE int = 1");
    testQuery("SELECT * FROM testTable WHERE (int = 1 OR int = 1) AND long = 2",
        "SELECT * FROM testTable WHERE int = 1 AND long = 2");
    testQuery("SELECT * FROM testTable WHERE int = 1 OR int IN (2, 3, 4, 5)",
        "SELECT * FROM testTable WHERE int IN (1, 2, 3, 4, 5)");
    testQuery("SELECT * FROM testTable WHERE int IN (1, 1) OR int = 1", "SELECT * FROM testTable WHERE int = 1");
    testQuery("SELECT * FROM testTable WHERE string = 'foo' OR string = 'bar' OR string = 'foobar'",
        "SELECT * FROM testTable WHERE string IN ('foo', 'bar', 'foobar')");
    testQuery("SELECT * FROM testTable WHERE bytes = 'dead' OR bytes = 'beef' OR bytes = 'deadbeef'",
        "SELECT * FROM testTable WHERE bytes IN ('dead', 'beef', 'deadbeef')");

    // MergeRangeFilter
    testQuery("SELECT * FROM testTable WHERE int >= 10 AND int <= 20",
        "SELECT * FROM testTable WHERE int BETWEEN 10 AND 20");
    testQuery("SELECT * FROM testTable WHERE int BETWEEN 10 AND 20 AND int > 7 AND int <= 17 OR int > 20",
        "SELECT * FROM testTable WHERE int BETWEEN 10 AND 17 OR int > 20");
    testQuery("SELECT * FROM testTable WHERE long BETWEEN 10 AND 20 AND long > 7 AND long <= 17 OR long > 20",
        "SELECT * FROM testTable WHERE long BETWEEN 10 AND 17 OR long > 20");
    testQuery("SELECT * FROM testTable WHERE float BETWEEN 10.5 AND 20 AND float > 7 AND float <= 17.5 OR float > 20",
        "SELECT * FROM testTable WHERE float BETWEEN 10.5 AND 17.5 OR float > 20");
    testQuery(
        "SELECT * FROM testTable WHERE double BETWEEN 10.5 AND 20 AND double > 7 AND double <= 17.5 OR double > 20",
        "SELECT * FROM testTable WHERE double BETWEEN 10.5 AND 17.5 OR double > 20");
    testQuery(
        "SELECT * FROM testTable WHERE string BETWEEN '10' AND '20' AND string > '7' AND string <= '17' OR string > "
            + "'20'", "SELECT * FROM testTable WHERE string > '7' AND string <= '17' OR string > '20'");
    testQuery(
        "SELECT * FROM testTable WHERE bytes BETWEEN '10' AND '20' AND bytes > '07' AND bytes <= '17' OR bytes > '20'",
        "SELECT * FROM testTable WHERE bytes BETWEEN '10' AND '17' OR bytes > '20'");
    testQuery(
        "SELECT * FROM testTable WHERE int > 10 AND long > 20 AND int <= 30 AND long <= 40 AND int >= 15 AND long >= "
            + "25", "SELECT * FROM testTable WHERE int BETWEEN 15 AND 30 AND long BETWEEN 25 AND 40");
    testQuery("SELECT * FROM testTable WHERE int > 10 AND int > 20 OR int < 30 AND int < 40",
        "SELECT * FROM testTable WHERE int > 20 OR int < 30");
    testQuery("SELECT * FROM testTable WHERE int > 10 AND int > 20 OR long < 30 AND long < 40",
        "SELECT * FROM testTable WHERE int > 20 OR long < 30");

    // Mixed
    testQuery(
        "SELECT * FROM testTable WHERE int >= 20 AND (int > 10 AND (int IN (1, 2) OR (int = 2 OR int = 3)) AND int <="
            + " 30)", "SELECT * FROM testTable WHERE int BETWEEN 20 AND 30 AND int IN (1, 2, 3)");

    // IdenticalPredicateOptimizer
    testQuery("SELECT * FROM testTable WHERE 1=1", "SELECT * FROM testTable WHERE true");
    testQuery("SELECT * FROM testTable WHERE 1!=1", "SELECT * FROM testTable WHERE false");
    testQuery("SELECT * FROM testTable WHERE 1=1 AND 1!=1", "SELECT * FROM testTable WHERE false");
    testQuery("SELECT * FROM testTable WHERE 1=1 OR 1!=1", "SELECT * FROM testTable WHERE true");

    testQuery("SELECT * FROM testTable WHERE \"a\"=\"a\"", "SELECT * FROM testTable WHERE true");
    testQuery("SELECT * FROM testTable WHERE \"a\"!=\"a\"", "SELECT * FROM testTable WHERE false");
    testQuery("SELECT * FROM testTable WHERE \"a\"=\"a\" AND \"a\"!=\"a\"", "SELECT * FROM testTable WHERE false");
    testQuery("SELECT * FROM testTable WHERE \"a\"=\"a\" OR \"a\"!=\"a\"", "SELECT * FROM testTable WHERE true");

    testQuery("SELECT * FROM testTable WHERE 1=1 AND \"a\"=\"a\"", "SELECT * FROM testTable WHERE true");
    testQuery("SELECT * FROM testTable WHERE 1=1 OR \"a\"=\"a\"", "SELECT * FROM testTable WHERE true");
    testQuery("SELECT * FROM testTable WHERE 1!=1 AND \"a\"=\"a\"", "SELECT * FROM testTable WHERE false");
    testQuery("SELECT * FROM testTable WHERE 1=1 AND \"a\"!=\"a\"", "SELECT * FROM testTable WHERE false");
    testQuery("SELECT * FROM testTable WHERE 1!=1 OR \"a\"=\"a\"", "SELECT * FROM testTable WHERE true");
    testQuery("SELECT * FROM testTable WHERE 1=1 OR \"a\"!=\"a\"", "SELECT * FROM testTable WHERE true");

    testQuery("SELECT * FROM testTable WHERE 1.0=1.0", "SELECT * FROM testTable WHERE true");
    testQuery("SELECT * FROM testTable WHERE 1.0=1", "SELECT * FROM testTable WHERE true");
    testQuery("SELECT * FROM testTable WHERE 1.01=1", "SELECT * FROM testTable WHERE false");

    testQuery("SELECT * FROM testTable WHERE 1=1 AND true", "SELECT * FROM testTable WHERE true");
    testQuery("SELECT * FROM testTable WHERE \"a\"=\"a\" AND true", "SELECT * FROM testTable WHERE true");

    // TextMatchFilterOptimizer
    testQuery("SELECT * FROM testTable WHERE TEXT_MATCH(string, 'foo') AND TEXT_MATCH(string, 'bar')",
        "SELECT * FROM testTable WHERE TEXT_MATCH(string, '(foo) AND (bar)')");
    testQuery("SELECT * FROM testTable WHERE TEXT_MATCH(string, '\"foo bar\"') AND TEXT_MATCH(string, 'baz')",
        "SELECT * FROM testTable WHERE TEXT_MATCH(string, '(\"foo bar\") AND (baz)')");
    testQuery("SELECT * FROM testTable WHERE TEXT_MATCH(string, '\"foo bar\"') AND TEXT_MATCH(string, '/.*ooba.*/')",
        "SELECT * FROM testTable WHERE TEXT_MATCH(string, '(\"foo bar\") AND (/.*ooba.*/)')");
    testQuery("SELECT * FROM testTable WHERE int = 1 AND TEXT_MATCH(string, 'foo') AND TEXT_MATCH(string, 'bar')",
        "SELECT * FROM testTable WHERE int = 1 AND TEXT_MATCH(string, '(foo) AND (bar)')");
    testQuery("SELECT * FROM testTable WHERE int = 1 OR TEXT_MATCH(string, 'foo') AND TEXT_MATCH(string, 'bar')",
        "SELECT * FROM testTable WHERE int = 1 OR TEXT_MATCH(string, '(foo) AND (bar)')");
    testQuery("SELECT * FROM testTable WHERE TEXT_MATCH(string, 'foo') AND NOT TEXT_MATCH(string, 'bar')",
        "SELECT * FROM testTable WHERE TEXT_MATCH(string, '(foo) AND NOT (bar)')");
    testQuery("SELECT * FROM testTable WHERE NOT TEXT_MATCH(string, 'foo') AND TEXT_MATCH(string, 'bar')",
        "SELECT * FROM testTable WHERE TEXT_MATCH(string, 'NOT (foo) AND (bar)')");
    testQuery("SELECT * FROM testTable WHERE NOT TEXT_MATCH(string, 'foo') AND NOT TEXT_MATCH(string, 'bar')",
        "SELECT * FROM testTable WHERE NOT TEXT_MATCH(string, '(foo) OR (bar)')");
    testQuery("SELECT * FROM testTable WHERE NOT TEXT_MATCH(string, 'foo') OR NOT TEXT_MATCH(string, 'bar')",
        "SELECT * FROM testTable WHERE NOT TEXT_MATCH(string, '(foo) AND (bar)')");
    testQuery("SELECT * FROM testTable WHERE TEXT_MATCH(string, 'foo') AND TEXT_MATCH(string, 'bar') OR "
            + "TEXT_MATCH(string, 'baz')",
        "SELECT * FROM testTable WHERE TEXT_MATCH(string, '((foo) AND (bar)) OR (baz)')");
    testQuery("SELECT * FROM testTable WHERE TEXT_MATCH(string, 'foo') AND (TEXT_MATCH(string, 'bar') OR "
            + "TEXT_MATCH(string, 'baz'))",
        "SELECT * FROM testTable WHERE TEXT_MATCH(string, '(foo) AND ((bar) OR (baz))')");
    testQuery("SELECT * FROM testTable WHERE TEXT_MATCH(string1, 'foo1') AND TEXT_MATCH(string1, 'bar1') OR "
            + "TEXT_MATCH(string1, 'baz1') AND TEXT_MATCH(string2, 'foo')",
        "SELECT * FROM testTable WHERE TEXT_MATCH(string1, '(foo1) AND (bar1)') OR TEXT_MATCH(string1, 'baz1') AND "
            + "TEXT_MATCH(string2, 'foo')");
    testQuery("SELECT * FROM testTable WHERE TEXT_MATCH(string1, 'foo1') AND TEXT_MATCH(string1, 'bar1')"
            + "AND TEXT_MATCH(string2, 'foo2') AND TEXT_MATCH(string2, 'bar2')",
        "SELECT * FROM testTable WHERE TEXT_MATCH(string1, '(foo1) AND (bar1)') AND TEXT_MATCH(string2, '(foo2) AND "
            + "(bar2)')");
    testQuery("SELECT * FROM testTable WHERE TEXT_MATCH(string, 'foo') OR NOT TEXT_MATCH(string, 'bar')",
        "SELECT * FROM testTable WHERE NOT TEXT_MATCH(string, 'bar') OR TEXT_MATCH(string, 'foo')");
    testQuery(
        "select * from testTable where intCol > 1 AND (text_match(string, 'foo') OR NOT text_match(string, 'bar'))",
        "select * from testTable where intCol > 1 AND (NOT text_match(string, 'bar') OR text_match(string, 'foo'))");
    testQuery(
        "select * from testTable where text_match(string, 'foo') AND text_match(string, 'bar OR baz')",
        "select * from testTable where text_match(string, '(foo) AND (bar OR baz)')"
    );
    testCannotOptimizeQuery("SELECT * FROM testTable WHERE TEXT_MATCH(string1, 'foo') OR TEXT_MATCH(string2, 'bar')");
    testCannotOptimizeQuery(
        "SELECT * FROM testTable WHERE int = 1 AND TEXT_MATCH(string, 'foo') OR TEXT_MATCH(string, 'bar')");
    testCannotOptimizeQuery("SELECT * FROM testTable WHERE NOT TEXT_MATCH(string, 'foo')");
  }

  private static void testQuery(String actual, String expected) {
    assertNotEquals(actual, expected, "You must provide different queries to test");
    PinotQuery actualPinotQuery = CalciteSqlParser.compileToPinotQuery(actual);
    OPTIMIZER.optimize(actualPinotQuery, SCHEMA);
    // Also optimize the expected query because the expected range can only be generate via optimizer
    PinotQuery expectedPinotQuery = CalciteSqlParser.compileToPinotQuery(expected);
    OPTIMIZER.optimize(expectedPinotQuery, SCHEMA);
    comparePinotQuery(actualPinotQuery, expectedPinotQuery);
  }

  private static void testCannotOptimizeQuery(String query) {
    PinotQuery actualPinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(actualPinotQuery, SCHEMA);
    PinotQuery expectedPinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    comparePinotQuery(actualPinotQuery, expectedPinotQuery);
  }

  private static void comparePinotQuery(PinotQuery actual, PinotQuery expected) {
    if (expected.getFilterExpression() == null) {
      assertNull(actual.getFilterExpression());
      return;
    }
    compareFilterExpression(actual.getFilterExpression(), expected.getFilterExpression());
  }

  private static void compareFilterExpression(Expression actual, Expression expected) {
    if (actual.isSetLiteral()) {
      assertNull(actual.getFunctionCall());
      assertNull(expected.getFunctionCall());
      assertTrue(expected.isSetLiteral());
      assertEquals(actual.getLiteral(), expected.getLiteral());
    } else {
      Function actualFilterFunction = actual.getFunctionCall();
      Function expectedFilterFunction = expected.getFunctionCall();
      FilterKind actualFilterKind = FilterKind.valueOf(actualFilterFunction.getOperator());
      FilterKind expectedFilterKind = FilterKind.valueOf(expectedFilterFunction.getOperator());
      List<Expression> actualOperands = actualFilterFunction.getOperands();
      List<Expression> expectedOperands = expectedFilterFunction.getOperands();
      if (!actualFilterKind.isRange()) {
        assertEquals(actualFilterKind, expectedFilterKind);
        assertEquals(actualOperands.size(), expectedOperands.size());
        if (actualFilterKind == FilterKind.AND || actualFilterKind == FilterKind.OR) {
          compareFilterExpressionChildren(actualOperands, expectedOperands);
        } else {
          assertEquals(actualOperands.get(0), expectedOperands.get(0));
          if (actualFilterKind == FilterKind.IN || actualFilterKind == FilterKind.NOT_IN) {
            // Handle different order of values
            assertEqualsNoOrder(actualOperands.toArray(), expectedOperands.toArray());
          } else {
            assertEquals(actualOperands, expectedOperands);
          }
        }
      } else {
        assertTrue(expectedFilterKind.isRange());
        assertEquals(getRangeString(actualFilterKind, actualOperands),
            getRangeString(expectedFilterKind, expectedOperands));
      }
    }
  }

  /**
   * Handles different order of children under AND/OR filter.
   */
  private static void compareFilterExpressionChildren(List<Expression> actual, List<Expression> expected) {
    assertEquals(actual.size(), expected.size());
    List<Expression> unmatchedExpectedChildren = new ArrayList<>(expected);
    for (Expression actualChild : actual) {
      Iterator<Expression> iterator = unmatchedExpectedChildren.iterator();
      boolean findMatchingChild = false;
      while (iterator.hasNext()) {
        try {
          compareFilterExpression(actualChild, iterator.next());
          iterator.remove();
          findMatchingChild = true;
          break;
        } catch (AssertionError e) {
          // Ignore
        }
      }
      if (!findMatchingChild) {
        fail("Failed to find matching child");
      }
    }
  }

  private static String getRangeString(FilterKind filterKind, List<Expression> operands) {
    switch (filterKind) {
      case GREATER_THAN:
        return Range.LOWER_EXCLUSIVE + RequestUtils.getLiteralString(operands.get(1)) + Range.UPPER_UNBOUNDED;
      case GREATER_THAN_OR_EQUAL:
        return Range.LOWER_INCLUSIVE + RequestUtils.getLiteralString(operands.get(1)) + Range.UPPER_UNBOUNDED;
      case LESS_THAN:
        return Range.LOWER_UNBOUNDED + RequestUtils.getLiteralString(operands.get(1)) + Range.UPPER_EXCLUSIVE;
      case LESS_THAN_OR_EQUAL:
        return Range.LOWER_UNBOUNDED + RequestUtils.getLiteralString(operands.get(1)) + Range.UPPER_INCLUSIVE;
      case BETWEEN:
        return Range.LOWER_INCLUSIVE + RequestUtils.getLiteralString(operands.get(1)) + Range.DELIMITER
            + RequestUtils.getLiteralString(operands.get(2)) + Range.UPPER_INCLUSIVE;
      case RANGE:
        return operands.get(1).getLiteral().getStringValue();
      default:
        throw new IllegalStateException();
    }
  }

  /**
   * Coarse-grained measurement of the compile-path overhead the filter-optimizer chain adds, across several filter
   * shapes including the pathological wide OR-of-ANDs. This is intentionally a lightweight timing loop (not a JMH
   * benchmark): for each shape it measures the time to (deep-copy + optimize) a PinotQuery and subtracts the time to
   * (deep-copy) only, isolating the optimizer cost per pass.
   *
   * <p>Disabled from the automated suite ({@code enabled = false}) because timing loops are inherently machine- and
   * load-dependent and would add noise/flakiness to CI. Run it manually (e.g. from the IDE, or with
   * {@code -Dtest=QueryOptimizerTest#testOptimizerOverhead -DdisableTestNGGroups=... } after temporarily enabling it)
   * to re-measure overhead; representative numbers are recorded in the design doc and the PR description.
   */
  @Test(enabled = false)
  public void testOptimizerOverhead() {
    Map<String, String> shapes = new LinkedHashMap<>();
    shapes.put("point (no OR)", "SELECT * FROM testTable WHERE int = 5 AND long > 0");
    shapes.put("factorable OR x100", buildFactorableOrOfAnds(100));
    shapes.put("non-factorable OR x100", buildNonFactorableOrOfAnds(100));
    shapes.put("original shape (IN + OR x100)", buildOriginalShape(100));

    int warmupIterations = 2000;
    int measuredIterations = 5000;
    long accumulator = 0;

    System.out.println("=== QueryOptimizer per-pass overhead (deep-copy + optimize, minus deep-copy) ===");
    for (Map.Entry<String, String> shape : shapes.entrySet()) {
      PinotQuery base = CalciteSqlParser.compileToPinotQuery(shape.getValue());

      // Warmup to let the JIT compile the hot path.
      for (int i = 0; i < warmupIterations; i++) {
        PinotQuery copy = new PinotQuery(base);
        OPTIMIZER.optimize(copy, SCHEMA);
        accumulator += copy.getFilterExpression().getFunctionCall().getOperandsSize();
      }

      // Measure deep-copy only (the baseline we subtract out).
      long copyOnlyNanos = System.nanoTime();
      for (int i = 0; i < measuredIterations; i++) {
        PinotQuery copy = new PinotQuery(base);
        accumulator += copy.getFilterExpression().getFunctionCall().getOperandsSize();
      }
      copyOnlyNanos = System.nanoTime() - copyOnlyNanos;

      // Measure deep-copy + optimize.
      long copyAndOptimizeNanos = System.nanoTime();
      for (int i = 0; i < measuredIterations; i++) {
        PinotQuery copy = new PinotQuery(base);
        OPTIMIZER.optimize(copy, SCHEMA);
        accumulator += copy.getFilterExpression().getFunctionCall().getOperandsSize();
      }
      copyAndOptimizeNanos = System.nanoTime() - copyAndOptimizeNanos;

      double perPassMicros = Math.max(0, copyAndOptimizeNanos - copyOnlyNanos) / 1000.0 / measuredIterations;
      System.out.printf("  %-32s %8.3f us/pass%n", shape.getKey(), perPassMicros);
      // Loose guard: a single optimize pass must stay well under a millisecond even on a slow machine.
      assertTrue(perPassMicros < 1000.0,
          "Optimizer pass for shape '" + shape.getKey() + "' took " + perPassMicros + " us, unexpectedly high");
    }
    // Prevent dead-code elimination of the optimize() calls.
    assertTrue(accumulator > 0);
  }

  private static String buildFactorableOrOfAnds(int numBranches) {
    StringBuilder sb = new StringBuilder("SELECT * FROM testTable WHERE ");
    for (int i = 0; i < numBranches; i++) {
      if (i > 0) {
        sb.append(" OR ");
      }
      sb.append("(int = 5 AND long = ").append(1000 + i).append(')');
    }
    return sb.toString();
  }

  private static String buildNonFactorableOrOfAnds(int numBranches) {
    StringBuilder sb = new StringBuilder("SELECT * FROM testTable WHERE ");
    for (int i = 0; i < numBranches; i++) {
      if (i > 0) {
        sb.append(" OR ");
      }
      // No conjunct is common across branches (int value varies per branch).
      sb.append("(int = ").append(i).append(" AND long = ").append(1000 + i).append(')');
    }
    return sb.toString();
  }

  private static String buildOriginalShape(int numBranches) {
    StringBuilder inList = new StringBuilder();
    StringBuilder orBlock = new StringBuilder();
    for (int i = 0; i < numBranches; i++) {
      long campaignId = 1000 + i;
      if (i > 0) {
        inList.append(", ");
        orBlock.append(" OR ");
      }
      inList.append(campaignId);
      orBlock.append("(int = 5 AND long = ").append(campaignId).append(')');
    }
    return "SELECT * FROM testTable WHERE double >= 10 AND double <= 20 AND long IN (" + inList + ") "
        + "AND int > 0 AND long > 0 AND (" + orBlock + ")";
  }
}
