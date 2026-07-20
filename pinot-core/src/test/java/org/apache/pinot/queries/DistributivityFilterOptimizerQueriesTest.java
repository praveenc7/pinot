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
package org.apache.pinot.queries;

import java.util.List;
import org.apache.pinot.common.response.broker.ResultTable;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Execution-level equivalence test for the
 * {@link org.apache.pinot.core.query.optimizer.filter.DistributivityFilterOptimizer}.
 *
 * <p>Reuses the {@link BaseSingleValueQueriesTest} segment, which has inverted indexes on the INT dimensions
 * {@code column6} and {@code column7} (used here as the "campaignId" and "accountId" analogs) plus the metrics
 * {@code column1} / {@code column3}. It reproduces the machine-generated pathological filter shape that motivated the
 * optimizer — a wide OR of ANDs where every branch shares the same conjunct, combined with a superset IN and
 * redundant range predicates — and asserts that executing the raw (unoptimized) filter tree produces exactly the same
 * results as executing the tree after the {@link org.apache.pinot.core.query.optimizer.QueryOptimizer} chain (which
 * includes the DistributivityFilterOptimizer) rewrites it.
 *
 * <p>{@link #getBrokerResponse} executes the query as written (no filter optimization), while
 * {@link #getBrokerResponseForOptimizedQuery} runs the optimizer chain first; comparing the two isolates the effect of
 * the rewrite on real segments.
 */
public class DistributivityFilterOptimizerQueriesTest extends BaseSingleValueQueriesTest {

  /**
   * Original:  daysSinceEpoch range AND column6 IN (c1..cN) AND column7 > 0 AND column6 > 0
   *              AND ( (column7 = A AND column6 = c1) OR ... OR (column7 = A AND column6 = cN) )
   * Rewritten: daysSinceEpoch range AND column6 IN (c1..cN) AND column7 > 0 AND column6 > 0
   *              AND column7 = A AND column6 IN (c1..cN)
   *
   * <p>Executes the original (unoptimized) tree and the rewritten (optimized) tree on real segments and asserts the
   * results are identical. A (accountId) and the c_i (campaignId) values are discovered from the data so the OR
   * branches match actual rows and the comparison is not vacuously empty.
   */
  @Test
  public void testDeepOrOfAndsRewriteProducesSameResults() {
    // Discover a real column7 ("accountId") value and the column6 ("campaignId") values that co-occur with it, so that
    // every branch of the generated OR-of-ANDs matches actual rows and the comparison is not vacuously empty.
    List<Object[]> accountRows = getBrokerResponse(
        "SELECT column7 FROM testTable GROUP BY column7 ORDER BY COUNT(*) DESC LIMIT 1").getResultTable().getRows();
    int accountId = (int) accountRows.get(0)[0];

    List<Object[]> campaignRows = getBrokerResponse(
        "SELECT column6 FROM testTable WHERE column7 = " + accountId + " GROUP BY column6 ORDER BY column6 LIMIT 40")
        .getResultTable().getRows();
    assertTrue(campaignRows.size() >= 2,
        "Expected the chosen accountId to have at least 2 distinct campaignId values to form a wide OR");

    // Build the OR of ANDs: (column7 = A AND column6 = c1) OR (column7 = A AND column6 = c2) OR ... and a matching
    // superset IN list on column6.
    StringBuilder orBlock = new StringBuilder();
    StringBuilder inList = new StringBuilder();
    for (int i = 0; i < campaignRows.size(); i++) {
      int campaignId = (int) campaignRows.get(i)[0];
      if (i > 0) {
        orBlock.append(" OR ");
        inList.append(", ");
      }
      orBlock.append("(column7 = ").append(accountId).append(" AND column6 = ").append(campaignId).append(")");
      inList.append(campaignId);
    }

    // The motivating pathological shape: a time range, a superset IN, redundant "> 0" predicates, and the wide
    // OR-of-ANDs whose branches all share column7 = accountId.
    String query = "SELECT column7, column6, SUM(column1), SUM(column3) FROM testTable "
        + "WHERE daysSinceEpoch >= 126164076 AND daysSinceEpoch <= 167572854 "
        + "AND column6 IN (" + inList + ") AND column7 > 0 AND column6 > 0 AND (" + orBlock + ") "
        + "GROUP BY column7, column6 ORDER BY column7, column6 LIMIT 1000";

    ResultTable originalResult = getBrokerResponse(query).getResultTable();
    ResultTable rewrittenResult = getBrokerResponseForOptimizedQuery(query, TABLE_CONFIG, SCHEMA).getResultTable();

    assertEquals(rewrittenResult.getDataSchema(), originalResult.getDataSchema());
    List<Object[]> originalRows = originalResult.getRows();
    List<Object[]> rewrittenRows = rewrittenResult.getRows();
    assertTrue(originalRows.size() > 0, "Expected the deep OR-of-ANDs query to match some rows");
    assertEquals(rewrittenRows.size(), originalRows.size());
    for (int i = 0; i < originalRows.size(); i++) {
      assertEquals(rewrittenRows.get(i), originalRows.get(i));
    }
  }
}
