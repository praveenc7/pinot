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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for COUNTIF aggregation function, which is rewritten to COUNT(*) FILTER(WHERE predicate).
 * Each test verifies COUNTIF produces the same result as the equivalent FILTER, CASE WHEN, or WHERE query.
 */
public class CountIfQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "CountIfQueriesTest");
  private static final String TABLE_NAME = "MyTable";
  private static final String FIRST_SEGMENT_NAME = "firstTestSegment";
  private static final String SECOND_SEGMENT_NAME = "secondTestSegment";
  private static final String INT_COL_NAME = "INT_COL";
  private static final String NO_INDEX_INT_COL_NAME = "NO_INDEX_COL";
  private static final String BOOLEAN_COL_NAME = "BOOLEAN_COL";
  private static final String STRING_COL_NAME = "STRING_COL";
  private static final String NULLABLE_INT_COL_NAME = "NULLABLE_INT_COL";
  private static final int NUM_ROWS = 30000;
  private static final long RANDOM_SEED = 42L;

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
      .addSingleValueDimension(NO_INDEX_INT_COL_NAME, FieldSpec.DataType.INT)
      .addSingleValueDimension(BOOLEAN_COL_NAME, FieldSpec.DataType.BOOLEAN)
      .addSingleValueDimension(STRING_COL_NAME, FieldSpec.DataType.STRING)
      .addSingleValueDimension(NULLABLE_INT_COL_NAME, FieldSpec.DataType.INT)
      .addMetric(INT_COL_NAME, FieldSpec.DataType.INT).build();

  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
      .setInvertedIndexColumns(Collections.singletonList(INT_COL_NAME))
      .setRangeIndexColumns(List.of(INT_COL_NAME)).build();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    buildSegment(FIRST_SEGMENT_NAME);
    buildSegment(SECOND_SEGMENT_NAME);

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(TABLE_CONFIG, SCHEMA);
    ImmutableSegment firstImmutableSegment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, FIRST_SEGMENT_NAME), indexLoadingConfig);
    ImmutableSegment secondImmutableSegment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SECOND_SEGMENT_NAME), indexLoadingConfig);
    _indexSegment = firstImmutableSegment;
    _indexSegments = Arrays.asList(firstImmutableSegment, secondImmutableSegment);
  }

  @AfterClass
  public void tearDown() {
    for (IndexSegment segment : _indexSegments) {
      segment.destroy();
    }
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private List<GenericRow> createTestData() {
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    Random random = new Random(RANDOM_SEED);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue(INT_COL_NAME, i);
      row.putValue(NO_INDEX_INT_COL_NAME, i);
      row.putValue(BOOLEAN_COL_NAME, random.nextBoolean());
      row.putValue(STRING_COL_NAME, RandomStringUtils.random(4, 0, 0, true, false, null, random));
      // Make ~1/3 of nullable column values NULL
      if (i % 3 == 0) {
        row.putValue(NULLABLE_INT_COL_NAME, null);
      } else {
        row.putValue(NULLABLE_INT_COL_NAME, i);
      }
      rows.add(row);
    }
    return rows;
  }

  private void buildSegment(String segmentName)
      throws Exception {
    List<GenericRow> rows = createTestData();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  /**
   * Compares numeric values across queries, tolerating type differences (e.g., Long vs Double).
   * Useful when comparing COUNTIF (returns Long) with SUM(CASE WHEN...) (returns Double).
   */
  private void testEquivalentValues(String countIfQuery, String equivalentQuery) {
    ResultTable countIfResult = getBrokerResponse(countIfQuery).getResultTable();
    ResultTable equivalentResult = getBrokerResponse(equivalentQuery).getResultTable();
    assertNotNull(countIfResult);
    assertNotNull(equivalentResult);
    List<Object[]> countIfRows = countIfResult.getRows();
    List<Object[]> equivalentRows = equivalentResult.getRows();
    assertEquals(countIfRows.size(), equivalentRows.size());
    for (int i = 0; i < countIfRows.size(); i++) {
      Object[] row1 = countIfRows.get(i);
      Object[] row2 = equivalentRows.get(i);
      assertEquals(row1.length, row2.length);
      for (int j = 0; j < row1.length; j++) {
        assertEquals(((Number) row1[j]).doubleValue(), ((Number) row2[j]).doubleValue(),
            "Value mismatch at row " + i + " col " + j);
      }
    }
  }

  private void testEquivalentQueries(String countIfQuery, String equivalentQuery) {
    ResultTable countIfResult = getBrokerResponse(countIfQuery).getResultTable();
    ResultTable equivalentResult = getBrokerResponse(equivalentQuery).getResultTable();
    assertNotNull(countIfResult, "COUNTIF query returned null result for: " + countIfQuery);
    assertNotNull(equivalentResult, "Equivalent query returned null result for: " + equivalentQuery);
    List<Object[]> countIfRows = countIfResult.getRows();
    List<Object[]> equivalentRows = equivalentResult.getRows();
    assertEquals(countIfRows.size(), equivalentRows.size(),
        "Row count mismatch between COUNTIF and equivalent query");
    for (int i = 0; i < countIfRows.size(); i++) {
      assertEquals(countIfRows.get(i), equivalentRows.get(i),
          "Row " + i + " mismatch between COUNTIF and equivalent query");
    }
  }

  // ==================== COUNTIF vs COUNT(*) FILTER(WHERE ...) ====================

  @Test
  public void testCountIfVsFilter() {
    // Simple comparison
    testEquivalentQueries(
        "SELECT COUNTIF(INT_COL > 9999) FROM MyTable",
        "SELECT COUNT(*) FILTER(WHERE INT_COL > 9999) FROM MyTable");

    // Equality
    testEquivalentQueries(
        "SELECT COUNTIF(INT_COL = 4) FROM MyTable",
        "SELECT COUNT(*) FILTER(WHERE INT_COL = 4) FROM MyTable");

    // Range
    testEquivalentQueries(
        "SELECT COUNTIF(INT_COL > 1000 AND INT_COL < 5000) FROM MyTable",
        "SELECT COUNT(*) FILTER(WHERE INT_COL > 1000 AND INT_COL < 5000) FROM MyTable");

    // Boolean column
    testEquivalentQueries(
        "SELECT COUNTIF(BOOLEAN_COL) FROM MyTable",
        "SELECT COUNT(*) FILTER(WHERE BOOLEAN_COL) FROM MyTable");

    // Non-indexed column
    testEquivalentQueries(
        "SELECT COUNTIF(NO_INDEX_COL > 20000) FROM MyTable",
        "SELECT COUNT(*) FILTER(WHERE NO_INDEX_COL > 20000) FROM MyTable");
  }

  // ==================== COUNTIF vs CASE WHEN ====================

  @Test
  public void testCountIfVsCaseWhen() {
    // COUNTIF returns Long (via COUNT), SUM(CASE WHEN...) returns Double, so compare values not types
    testEquivalentValues(
        "SELECT COUNTIF(INT_COL > 9999) FROM MyTable",
        "SELECT SUM(CASE WHEN INT_COL > 9999 THEN 1 ELSE 0 END) FROM MyTable");

    testEquivalentValues(
        "SELECT COUNTIF(INT_COL > 1234 AND INT_COL < 22000) FROM MyTable",
        "SELECT SUM(CASE WHEN (INT_COL > 1234 AND INT_COL < 22000) THEN 1 ELSE 0 END) FROM MyTable");
  }

  // ==================== COUNTIF vs COUNT(*) WHERE ====================

  @Test
  public void testCountIfVsWhere() {
    testEquivalentQueries(
        "SELECT COUNTIF(INT_COL > 25000) FROM MyTable",
        "SELECT COUNT(*) FROM MyTable WHERE INT_COL > 25000");

    testEquivalentQueries(
        "SELECT COUNTIF(INT_COL = 100) FROM MyTable",
        "SELECT COUNT(*) FROM MyTable WHERE INT_COL = 100");
  }

  // ==================== COUNTIF with WHERE clause ====================

  @Test
  public void testCountIfWithWhereClause() {
    testEquivalentQueries(
        "SELECT COUNTIF(INT_COL > 9999) FROM MyTable WHERE INT_COL < 20000",
        "SELECT COUNT(*) FILTER(WHERE INT_COL > 9999) FROM MyTable WHERE INT_COL < 20000");

    testEquivalentQueries(
        "SELECT COUNTIF(INT_COL > 5000) FROM MyTable WHERE NO_INDEX_COL < 25000",
        "SELECT COUNT(*) FILTER(WHERE INT_COL > 5000) FROM MyTable WHERE NO_INDEX_COL < 25000");
  }

  // ==================== Multiple COUNTIFs ====================

  @Test
  public void testMultipleCountIfs() {
    testEquivalentQueries(
        "SELECT COUNTIF(INT_COL > 10000), COUNTIF(INT_COL < 5000) FROM MyTable",
        "SELECT COUNT(*) FILTER(WHERE INT_COL > 10000), COUNT(*) FILTER(WHERE INT_COL < 5000) FROM MyTable");
  }

  // ==================== COUNTIF mixed with other aggregations ====================

  @Test
  public void testCountIfMixedWithOtherAggs() {
    testEquivalentQueries(
        "SELECT COUNT(*), COUNTIF(INT_COL > 10000), SUM(INT_COL) FROM MyTable",
        "SELECT COUNT(*), COUNT(*) FILTER(WHERE INT_COL > 10000), SUM(INT_COL) FROM MyTable");

    testEquivalentQueries(
        "SELECT SUM(INT_COL), COUNTIF(NO_INDEX_COL > 5000), MAX(INT_COL) FROM MyTable",
        "SELECT SUM(INT_COL), COUNT(*) FILTER(WHERE NO_INDEX_COL > 5000), MAX(INT_COL) FROM MyTable");
  }

  // ==================== COUNTIF with GROUP BY ====================

  @Test
  public void testCountIfGroupBy() {
    testEquivalentQueries(
        "SELECT BOOLEAN_COL, COUNTIF(INT_COL > 25000) FROM MyTable GROUP BY BOOLEAN_COL ORDER BY BOOLEAN_COL",
        "SELECT BOOLEAN_COL, COUNT(*) FILTER(WHERE INT_COL > 25000) FROM MyTable "
            + "GROUP BY BOOLEAN_COL ORDER BY BOOLEAN_COL");
  }

  @Test
  public void testCountIfGroupByMixedAggs() {
    testEquivalentQueries(
        "SELECT BOOLEAN_COL, COUNT(*), COUNTIF(INT_COL > 25000) FROM MyTable "
            + "GROUP BY BOOLEAN_COL ORDER BY BOOLEAN_COL",
        "SELECT BOOLEAN_COL, COUNT(*), COUNT(*) FILTER(WHERE INT_COL > 25000) FROM MyTable "
            + "GROUP BY BOOLEAN_COL ORDER BY BOOLEAN_COL");
  }

  @Test
  public void testCountIfGroupByMultipleCountIfs() {
    testEquivalentQueries(
        "SELECT BOOLEAN_COL, COUNTIF(INT_COL > 10000), COUNTIF(INT_COL < 5000) FROM MyTable "
            + "GROUP BY BOOLEAN_COL ORDER BY BOOLEAN_COL",
        "SELECT BOOLEAN_COL, COUNT(*) FILTER(WHERE INT_COL > 10000), COUNT(*) FILTER(WHERE INT_COL < 5000) "
            + "FROM MyTable GROUP BY BOOLEAN_COL ORDER BY BOOLEAN_COL");
  }

  // ==================== COUNTIF with complex predicates ====================

  @Test
  public void testCountIfComplexPredicates() {
    // AND predicate
    testEquivalentQueries(
        "SELECT COUNTIF(INT_COL > 5000 AND NO_INDEX_COL < 20000) FROM MyTable",
        "SELECT COUNT(*) FILTER(WHERE INT_COL > 5000 AND NO_INDEX_COL < 20000) FROM MyTable");

    // OR predicate
    testEquivalentQueries(
        "SELECT COUNTIF(INT_COL < 100 OR INT_COL > 29900) FROM MyTable",
        "SELECT COUNT(*) FILTER(WHERE INT_COL < 100 OR INT_COL > 29900) FROM MyTable");

    // Function in predicate
    testEquivalentQueries(
        "SELECT COUNTIF(STARTSWITH(STRING_COL, 'abc')) FROM MyTable",
        "SELECT COUNT(*) FILTER(WHERE STARTSWITH(STRING_COL, 'abc')) FROM MyTable");

    // Modulo
    testEquivalentQueries(
        "SELECT COUNTIF(MOD(INT_COL, 10) = 0) FROM MyTable",
        "SELECT COUNT(*) FILTER(WHERE MOD(INT_COL, 10) = 0) FROM MyTable");
  }

  // ==================== COUNTIF with ORDER BY on result ====================

  @Test
  public void testCountIfOrderByResult() {
    testEquivalentQueries(
        "SELECT BOOLEAN_COL, COUNTIF(INT_COL > 20000) AS cnt FROM MyTable "
            + "GROUP BY BOOLEAN_COL ORDER BY cnt DESC",
        "SELECT BOOLEAN_COL, COUNT(*) FILTER(WHERE INT_COL > 20000) AS cnt FROM MyTable "
            + "GROUP BY BOOLEAN_COL ORDER BY cnt DESC");
  }

  // ==================== COUNTIF in wrapper expressions ====================

  @Test
  public void testCountIfInArithmeticExpression() {
    testEquivalentQueries(
        "SELECT COUNTIF(INT_COL > 10000) + COUNTIF(INT_COL < 100) FROM MyTable",
        "SELECT COUNT(*) FILTER(WHERE INT_COL > 10000) + COUNT(*) FILTER(WHERE INT_COL < 100) FROM MyTable");
  }

  @Test
  public void testCountIfInOrderByExpression() {
    testEquivalentQueries(
        "SELECT BOOLEAN_COL, COUNTIF(INT_COL > 25000) FROM MyTable "
            + "GROUP BY BOOLEAN_COL ORDER BY COUNTIF(INT_COL > 25000) DESC",
        "SELECT BOOLEAN_COL, COUNT(*) FILTER(WHERE INT_COL > 25000) FROM MyTable "
            + "GROUP BY BOOLEAN_COL ORDER BY COUNT(*) FILTER(WHERE INT_COL > 25000) DESC");
  }

  @Test
  public void testCountIfInCaseWhen() {
    testEquivalentQueries(
        "SELECT CASE WHEN COUNTIF(INT_COL > 10000) > 0 THEN 'yes' ELSE 'no' END FROM MyTable",
        "SELECT CASE WHEN COUNT(*) FILTER(WHERE INT_COL > 10000) > 0 THEN 'yes' ELSE 'no' END "
            + "FROM MyTable");
  }

  @Test
  public void testCountIfInCoalesce() {
    testEquivalentQueries(
        "SELECT COALESCE(COUNTIF(INT_COL > 10000), 0) FROM MyTable",
        "SELECT COALESCE(COUNT(*) FILTER(WHERE INT_COL > 10000), 0) FROM MyTable");
  }

  @Test
  public void testCountIfInCast() {
    testEquivalentValues(
        "SELECT CAST(COUNTIF(INT_COL > 10000) AS DOUBLE) FROM MyTable",
        "SELECT CAST(COUNT(*) FILTER(WHERE INT_COL > 10000) AS DOUBLE) FROM MyTable");
  }

  @Test
  public void testCountIfInAbs() {
    testEquivalentQueries(
        "SELECT ABS(COUNTIF(INT_COL > 10000) - 100000) FROM MyTable",
        "SELECT ABS(COUNT(*) FILTER(WHERE INT_COL > 10000) - 100000) FROM MyTable");
  }

  // ==================== COUNT_IF variant (underscore) ====================

  @Test
  public void testCountIfUnderscoreVariant() {
    testEquivalentQueries(
        "SELECT COUNT_IF(INT_COL > 9999) FROM MyTable",
        "SELECT COUNTIF(INT_COL > 9999) FROM MyTable");
  }

  // ==================== EXPLAIN PLAN equivalence ====================
  // Since COUNTIF is rewritten to COUNT(*) FILTER(WHERE ...) before execution,
  // the v1 explain plans should be identical.

  @Test
  public void testExplainPlanEquivalence() {
    testExplainPlanMatch(
        "EXPLAIN PLAN FOR SELECT COUNTIF(INT_COL > 9999) FROM MyTable",
        "EXPLAIN PLAN FOR SELECT COUNT(*) FILTER(WHERE INT_COL > 9999) FROM MyTable");
  }

  @Test
  public void testExplainPlanEquivalenceWithGroupBy() {
    testExplainPlanMatch(
        "EXPLAIN PLAN FOR SELECT BOOLEAN_COL, COUNTIF(INT_COL > 25000) FROM MyTable "
            + "GROUP BY BOOLEAN_COL",
        "EXPLAIN PLAN FOR SELECT BOOLEAN_COL, COUNT(*) FILTER(WHERE INT_COL > 25000) "
            + "FROM MyTable GROUP BY BOOLEAN_COL");
  }

  @Test
  public void testExplainPlanEquivalenceMixedAggs() {
    testExplainPlanMatch(
        "EXPLAIN PLAN FOR SELECT COUNT(*), COUNTIF(INT_COL > 10000), SUM(INT_COL) FROM MyTable",
        "EXPLAIN PLAN FOR SELECT COUNT(*), COUNT(*) FILTER(WHERE INT_COL > 10000), "
            + "SUM(INT_COL) FROM MyTable");
  }

  // ==================== Null handling ====================
  // COUNTIF should match COUNT(*) FILTER behavior with both null handling modes:
  // - NULL predicate values are treated as non-matching (not counted)
  // - COUNTIF returns 0, not NULL, when no rows match

  @Test
  public void testCountIfWithNullsNullHandlingEnabled() {
    // NULLABLE_INT_COL has ~1/3 NULL values. With null handling enabled,
    // COUNTIF(NULLABLE_INT_COL > 10000) should skip NULL rows and match FILTER behavior.
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("enableNullHandling", "true");

    ResultTable countIfResult = getBrokerResponse(
        "SELECT COUNTIF(NULLABLE_INT_COL > 10000) FROM MyTable", queryOptions).getResultTable();
    ResultTable filterResult = getBrokerResponse(
        "SELECT COUNT(*) FILTER(WHERE NULLABLE_INT_COL > 10000) FROM MyTable",
        queryOptions).getResultTable();
    assertNotNull(countIfResult);
    assertNotNull(filterResult);
    assertEquals(countIfResult.getRows().get(0), filterResult.getRows().get(0),
        "COUNTIF with nulls should match FILTER with nulls (null handling enabled)");
    long count = ((Number) countIfResult.getRows().get(0)[0]).longValue();
    assertTrue(count > 0, "COUNTIF should count some non-null rows matching > 10000");
  }

  @Test
  public void testCountIfWithNullsNullHandlingDisabled() {
    // Same test with null handling disabled (default). NULL values in Pinot are treated
    // as default values (0 for INT) when null handling is off.
    ResultTable countIfResult = getBrokerResponse(
        "SELECT COUNTIF(NULLABLE_INT_COL > 10000) FROM MyTable").getResultTable();
    ResultTable filterResult = getBrokerResponse(
        "SELECT COUNT(*) FILTER(WHERE NULLABLE_INT_COL > 10000) FROM MyTable").getResultTable();
    assertNotNull(countIfResult);
    assertNotNull(filterResult);
    assertEquals(countIfResult.getRows().get(0), filterResult.getRows().get(0),
        "COUNTIF with nulls should match FILTER with nulls (null handling disabled)");
  }

  @Test
  public void testCountIfNullHandlingNoMatchEnabled() {
    // A predicate that matches no rows should return 0 (not NULL) for COUNTIF
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("enableNullHandling", "true");

    ResultTable countIfResult = getBrokerResponse(
        "SELECT COUNTIF(INT_COL > 999999) FROM MyTable", queryOptions).getResultTable();
    assertNotNull(countIfResult);
    long count = ((Number) countIfResult.getRows().get(0)[0]).longValue();
    assertEquals(count, 0L, "COUNTIF with no matching rows should return 0");
  }

  @Test
  public void testCountIfNullHandlingNoMatchDisabled() {
    // Same no-match test with null handling disabled
    ResultTable countIfResult = getBrokerResponse(
        "SELECT COUNTIF(INT_COL > 999999) FROM MyTable").getResultTable();
    assertNotNull(countIfResult);
    long count = ((Number) countIfResult.getRows().get(0)[0]).longValue();
    assertEquals(count, 0L,
        "COUNTIF with no matching rows should return 0 (null handling disabled)");
  }

  @Test
  public void testCountIfNullHandlingGroupByEnabled() {
    // GROUP BY with null handling enabled
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("enableNullHandling", "true");

    ResultTable countIfResult = getBrokerResponse(
        "SELECT BOOLEAN_COL, COUNTIF(NULLABLE_INT_COL > 10000) FROM MyTable "
            + "GROUP BY BOOLEAN_COL ORDER BY BOOLEAN_COL", queryOptions).getResultTable();
    ResultTable filterResult = getBrokerResponse(
        "SELECT BOOLEAN_COL, COUNT(*) FILTER(WHERE NULLABLE_INT_COL > 10000) FROM MyTable "
            + "GROUP BY BOOLEAN_COL ORDER BY BOOLEAN_COL", queryOptions).getResultTable();
    assertNotNull(countIfResult);
    assertNotNull(filterResult);
    List<Object[]> countIfRows = countIfResult.getRows();
    List<Object[]> filterRows = filterResult.getRows();
    assertEquals(countIfRows.size(), filterRows.size());
    for (int i = 0; i < countIfRows.size(); i++) {
      assertEquals(
          ((Number) countIfRows.get(i)[1]).longValue(),
          ((Number) filterRows.get(i)[1]).longValue(),
          "COUNTIF and FILTER GROUP BY should match (null handling enabled)");
    }
  }

  @Test
  public void testCountIfNullHandlingGroupByDisabled() {
    // GROUP BY with null handling disabled
    ResultTable countIfResult = getBrokerResponse(
        "SELECT BOOLEAN_COL, COUNTIF(NULLABLE_INT_COL > 10000) FROM MyTable "
            + "GROUP BY BOOLEAN_COL ORDER BY BOOLEAN_COL").getResultTable();
    ResultTable filterResult = getBrokerResponse(
        "SELECT BOOLEAN_COL, COUNT(*) FILTER(WHERE NULLABLE_INT_COL > 10000) FROM MyTable "
            + "GROUP BY BOOLEAN_COL ORDER BY BOOLEAN_COL").getResultTable();
    assertNotNull(countIfResult);
    assertNotNull(filterResult);
    List<Object[]> countIfRows = countIfResult.getRows();
    List<Object[]> filterRows = filterResult.getRows();
    assertEquals(countIfRows.size(), filterRows.size());
    for (int i = 0; i < countIfRows.size(); i++) {
      assertEquals(
          ((Number) countIfRows.get(i)[1]).longValue(),
          ((Number) filterRows.get(i)[1]).longValue(),
          "COUNTIF and FILTER GROUP BY should match (null handling disabled)");
    }
  }

  private void testExplainPlanMatch(String countIfQuery, String filterQuery) {
    ResultTable countIfPlan = getBrokerResponse(countIfQuery).getResultTable();
    ResultTable filterPlan = getBrokerResponse(filterQuery).getResultTable();
    assertNotNull(countIfPlan);
    assertNotNull(filterPlan);
    List<Object[]> countIfRows = countIfPlan.getRows();
    List<Object[]> filterRows = filterPlan.getRows();
    assertEquals(countIfRows.size(), filterRows.size(),
        "Explain plan row count mismatch");
    for (int i = 0; i < countIfRows.size(); i++) {
      assertEquals(countIfRows.get(i), filterRows.get(i),
          "Explain plan row " + i + " mismatch");
    }
  }
}
