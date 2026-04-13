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

import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlCompilationException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class CountIfRewriterTest {
  private static final QueryRewriter QUERY_REWRITER = new CountIfRewriter();

  /**
   * Tests that COUNTIF(predicate) is rewritten to the equivalent COUNT(*) FILTER(WHERE predicate).
   * We compare the rewritten PinotQuery AST against the AST produced by parsing the equivalent
   * FILTER query directly, using compileToPinotQueryWithoutRewrites to avoid the rewriter chain
   * affecting the expected output.
   */
  @Test
  public void testSimpleCountIf() {
    // Simple comparison predicate
    testRewrite(
        "SELECT COUNTIF(col1 > 5) FROM myTable",
        "SELECT COUNT(*) FILTER(WHERE col1 > 5) FROM myTable");

    // Equality predicate
    testRewrite(
        "SELECT COUNTIF(col1 = 10) FROM myTable",
        "SELECT COUNT(*) FILTER(WHERE col1 = 10) FROM myTable");

    // Boolean column
    testRewrite(
        "SELECT COUNTIF(is_active) FROM myTable",
        "SELECT COUNT(*) FILTER(WHERE is_active) FROM myTable");
  }

  @Test
  public void testCountIfWithAlias() {
    testRewrite(
        "SELECT COUNTIF(col1 > 5) AS active_count FROM myTable",
        "SELECT COUNT(*) FILTER(WHERE col1 > 5) AS active_count FROM myTable");
  }

  @Test
  public void testCountIfWithWhereClause() {
    testRewrite(
        "SELECT COUNTIF(col1 > 5) FROM myTable WHERE col2 = 'abc'",
        "SELECT COUNT(*) FILTER(WHERE col1 > 5) FROM myTable WHERE col2 = 'abc'");
  }

  @Test
  public void testCountIfWithComplexPredicate() {
    // AND predicate
    testRewrite(
        "SELECT COUNTIF(col1 > 5 AND col2 < 100) FROM myTable",
        "SELECT COUNT(*) FILTER(WHERE col1 > 5 AND col2 < 100) FROM myTable");

    // OR predicate
    testRewrite(
        "SELECT COUNTIF(col1 > 5 OR col2 < 100) FROM myTable",
        "SELECT COUNT(*) FILTER(WHERE col1 > 5 OR col2 < 100) FROM myTable");

    // Function in predicate
    testRewrite(
        "SELECT COUNTIF(STARTSWITH(col1, 'abc')) FROM myTable",
        "SELECT COUNT(*) FILTER(WHERE STARTSWITH(col1, 'abc')) FROM myTable");
  }

  @Test
  public void testMultipleCountIfs() {
    testRewrite(
        "SELECT COUNTIF(col1 > 5), COUNTIF(col2 < 10) FROM myTable",
        "SELECT COUNT(*) FILTER(WHERE col1 > 5), COUNT(*) FILTER(WHERE col2 < 10) FROM myTable");
  }

  @Test
  public void testCountIfMixedWithOtherAggregations() {
    testRewrite(
        "SELECT SUM(col1), COUNTIF(col2 > 5), COUNT(*) FROM myTable",
        "SELECT SUM(col1), COUNT(*) FILTER(WHERE col2 > 5), COUNT(*) FROM myTable");
  }

  @Test
  public void testCountIfWithGroupBy() {
    testRewrite(
        "SELECT region, COUNTIF(status = 'active') FROM myTable GROUP BY region",
        "SELECT region, COUNT(*) FILTER(WHERE status = 'active') FROM myTable GROUP BY region");
  }

  @Test
  public void testCountIfWithGroupByAndOrderBy() {
    testRewrite(
        "SELECT region, COUNTIF(status = 'active') AS cnt FROM myTable GROUP BY region ORDER BY cnt DESC",
        "SELECT region, COUNT(*) FILTER(WHERE status = 'active') AS cnt FROM myTable "
            + "GROUP BY region ORDER BY cnt DESC");
  }

  @Test
  public void testCountIfInOrderByExpression() {
    // COUNTIF directly in ORDER BY (no alias) — recursion rewrites inside desc() wrapper
    testRewrite(
        "SELECT region, COUNTIF(status = 'active') FROM myTable "
            + "GROUP BY region ORDER BY COUNTIF(status = 'active') DESC",
        "SELECT region, COUNT(*) FILTER(WHERE status = 'active') FROM myTable "
            + "GROUP BY region ORDER BY COUNT(*) FILTER(WHERE status = 'active') DESC");
  }

  @Test
  public void testCountIfInArithmeticExpression() {
    // COUNTIF inside arithmetic — recursion rewrites inside plus() wrapper
    testRewrite(
        "SELECT COUNTIF(col1 > 5) + COUNTIF(col1 < 0) FROM myTable",
        "SELECT COUNT(*) FILTER(WHERE col1 > 5) + COUNT(*) FILTER(WHERE col1 < 0) FROM myTable");
  }

  @Test
  public void testCountIfInCaseWhen() {
    // COUNTIF inside CASE WHEN — recursion rewrites inside case() wrapper
    testRewrite(
        "SELECT CASE WHEN COUNTIF(col1 > 5) > 0 THEN 'yes' ELSE 'no' END FROM myTable",
        "SELECT CASE WHEN COUNT(*) FILTER(WHERE col1 > 5) > 0 THEN 'yes' ELSE 'no' END "
            + "FROM myTable");
  }

  @Test
  public void testCountIfInCoalesce() {
    testRewrite(
        "SELECT COALESCE(COUNTIF(col1 > 5), 0) FROM myTable",
        "SELECT COALESCE(COUNT(*) FILTER(WHERE col1 > 5), 0) FROM myTable");
  }

  @Test
  public void testCountIfInCast() {
    testRewrite(
        "SELECT CAST(COUNTIF(col1 > 5) AS DOUBLE) FROM myTable",
        "SELECT CAST(COUNT(*) FILTER(WHERE col1 > 5) AS DOUBLE) FROM myTable");
  }

  @Test
  public void testCountIfInAbs() {
    testRewrite(
        "SELECT ABS(COUNTIF(col1 > 5) - 10) FROM myTable",
        "SELECT ABS(COUNT(*) FILTER(WHERE col1 > 5) - 10) FROM myTable");
  }

  @Test
  public void testCountIfWithHaving() {
    testRewrite(
        "SELECT region, COUNTIF(status = 'active') AS cnt FROM myTable "
            + "GROUP BY region HAVING cnt > 0",
        "SELECT region, COUNT(*) FILTER(WHERE status = 'active') AS cnt FROM myTable "
            + "GROUP BY region HAVING cnt > 0");
  }

  @Test
  public void testCountIfInHavingClause() {
    testRewrite(
        "SELECT region FROM myTable GROUP BY region HAVING COUNTIF(status = 'active') > 0",
        "SELECT region FROM myTable GROUP BY region "
            + "HAVING COUNT(*) FILTER(WHERE status = 'active') > 0");
  }

  @Test
  public void testCountIfUnderscoreVariant() {
    // COUNT_IF should also work since Calcite normalizes it to countif
    testRewrite(
        "SELECT COUNT_IF(col1 > 5) FROM myTable",
        "SELECT COUNT(*) FILTER(WHERE col1 > 5) FROM myTable");
  }

  @Test
  public void testNoRewriteForNonCountIf() {
    // Regular aggregations should be unchanged
    String query = "SELECT COUNT(*) FROM myTable";
    PinotQuery original = CalciteSqlParser.compileToPinotQueryWithoutRewrites(query);
    PinotQuery rewritten = QUERY_REWRITER.rewrite(CalciteSqlParser.compileToPinotQueryWithoutRewrites(query));
    assertEquals(rewritten, original);

    query = "SELECT SUM(col1) FROM myTable";
    original = CalciteSqlParser.compileToPinotQueryWithoutRewrites(query);
    rewritten = QUERY_REWRITER.rewrite(CalciteSqlParser.compileToPinotQueryWithoutRewrites(query));
    assertEquals(rewritten, original);
  }

  // ==================== Error cases ====================

  @Test
  public void testCountIfNoArguments() {
    // COUNTIF() with no arguments should fail at parse time
    assertThrows(IllegalArgumentException.class,
        () -> CalciteSqlParser.compileToPinotQuery("SELECT COUNTIF() FROM myTable"));
  }

  @Test
  public void testCountIfMultipleArguments() {
    // COUNTIF(a, b) with multiple arguments should fail
    assertThrows(IllegalArgumentException.class,
        () -> CalciteSqlParser.compileToPinotQuery("SELECT COUNTIF(col1 > 5, col2 < 10) FROM myTable"));
  }

  @Test
  public void testNestedCountIfThrows() {
    // COUNTIF(COUNTIF(x > 5)) is nested aggregation and should fail at rewrite time
    assertThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToPinotQuery("SELECT COUNTIF(COUNTIF(col1 > 5)) FROM myTable"));
  }

  @Test
  public void testCountIfWithNestedAggregateThrows() {
    // COUNTIF(SUM(x) > 5) contains an aggregate in the predicate — should fail
    assertThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToPinotQuery("SELECT COUNTIF(SUM(col1) > 5) FROM myTable"));
  }

  @Test
  public void testCountIfDistinctThrows() {
    // COUNTIF(DISTINCT ...) is rejected by the v1 parser before the rewriter runs
    assertThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToPinotQuery("SELECT COUNTIF(DISTINCT col1 > 5) FROM myTable"));
  }

  private void testRewrite(String countIfQuery, String expectedFilterQuery) {
    PinotQuery rewritten = QUERY_REWRITER.rewrite(
        CalciteSqlParser.compileToPinotQueryWithoutRewrites(countIfQuery));
    PinotQuery expected = CalciteSqlParser.compileToPinotQueryWithoutRewrites(expectedFilterQuery);
    assertEquals(rewritten, expected);
  }
}
