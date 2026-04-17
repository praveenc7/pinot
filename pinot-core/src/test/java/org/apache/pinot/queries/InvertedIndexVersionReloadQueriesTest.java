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
import java.net.URL;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitmapInvertedIndexWriter;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.index.InvertedIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests that query execution produces identical results before and after converting inverted indexes
 * from VERSION_0 to VERSION_1. Verifies that filter execution on columns with mixed inverted index
 * versions (some V0, some V1) works correctly.
 *
 * <p>Uses the same test data as {@link ForwardIndexHandlerReloadQueriesTest}. The segment has:
 * <ul>
 *   <li>column8 (SV INT, dictionary, inverted index) — converted to V1 on reload</li>
 *   <li>column9 (SV INT, dictionary, inverted index) — converted to V1 on reload</li>
 *   <li>column7 (MV INT, no dictionary) — no inverted index, used in filters via scan</li>
 * </ul>
 */
public class InvertedIndexVersionReloadQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), InvertedIndexVersionReloadQueriesTest.class.getSimpleName());
  private static final String AVRO_DATA = "data" + File.separator + "test_data-mv.avro";
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  //@formatter:off
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addMetric("column1", DataType.INT)
      .addMetric("column2", DataType.INT)
      .addSingleValueDimension("column3", DataType.STRING)
      .addSingleValueDimension("column5", DataType.STRING)
      .addMultiValueDimension("column6", DataType.INT)
      .addMultiValueDimension("column7", DataType.INT)
      .addSingleValueDimension("column8", DataType.INT)
      .addMetric("column9", DataType.INT)
      .addMetric("column10", DataType.INT)
      .addDateTime("daysSinceEpoch", DataType.INT, "EPOCH|DAYS", "1:DAYS")
      .build();

  // Columns that will have inverted indexes (dictionary-encoded, unsorted)
  private static final List<String> INVERTED_INDEX_COLUMNS = List.of("column8", "column9");
  // Columns without dictionary
  private static final List<String> NO_DICTIONARY_COLUMNS = List.of("column1", "column2", "column3", "column5",
      "column7", "column10");
  //@formatter:on

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @BeforeMethod
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    // Build segment with VERSION_0 inverted indexes (the default)
    TableConfig tableConfig = createTableConfig(InvertedIndexConfig.DEFAULT_VERSION);
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);

    SegmentGeneratorConfig generatorConfig = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    generatorConfig.setInputFilePath(resource.getFile());
    generatorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
    generatorConfig.setSegmentName(SEGMENT_NAME);
    generatorConfig.setSkipTimeValueCheck(true);
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(generatorConfig);
    driver.build();

    ImmutableSegment segment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), new IndexLoadingConfig(tableConfig, SCHEMA));

    // Verify inverted indexes exist and are VERSION_0
    for (String column : INVERTED_INDEX_COLUMNS) {
      assertNotNull(segment.getInvertedIndex(column), "Inverted index should exist for " + column);
    }
    verifyInvertedIndexVersions(InvertedIndexConfig.VERSION_0);

    _indexSegment = segment;
    _indexSegments = List.of(segment, segment);
  }

  @AfterMethod
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

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

  private TableConfig createTableConfig(int invertedIndexVersion) {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setTimeColumnName("daysSinceEpoch")
        .setNoDictionaryColumns(NO_DICTIONARY_COLUMNS)
        .setInvertedIndexColumns(INVERTED_INDEX_COLUMNS)
        .build();
    tableConfig.getIndexingConfig().setInvertedIndexVersion(invertedIndexVersion);
    return tableConfig;
  }

  /**
   * Tests filter queries using inverted index columns before and after V0→V1 conversion.
   * Verifies that query results are identical.
   */
  @Test
  public void testFilterQueriesAfterVersionConversion()
      throws Exception {
    // Queries that exercise inverted index filters on column8 and column9
    String query1 = "SELECT column1, column8, column9 FROM testTable WHERE column8 = 890282370 ORDER BY column1"
        + " LIMIT 100";
    String query2 = "SELECT column1, column8, column9 FROM testTable WHERE column9 IN (890282370, 890662862)"
        + " ORDER BY column1 LIMIT 100";
    String query3 = "SELECT column1, column8, column9 FROM testTable WHERE column8 != 890282370 AND column9 = 890662862"
        + " ORDER BY column1 LIMIT 100";

    // Run queries before conversion (V0)
    List<Object[]> beforeRows1 = runQuery(query1);
    List<Object[]> beforeRows2 = runQuery(query2);
    List<Object[]> beforeRows3 = runQuery(query3);

    // Convert inverted indexes from V0 to V1
    reloadWithVersion(InvertedIndexConfig.VERSION_1);

    // Verify indexes are now VERSION_1
    verifyInvertedIndexVersions(InvertedIndexConfig.VERSION_1);

    // Run the same queries after conversion (V1)
    List<Object[]> afterRows1 = runQuery(query1);
    List<Object[]> afterRows2 = runQuery(query2);
    List<Object[]> afterRows3 = runQuery(query3);

    // Results must be identical
    validateResults(beforeRows1, afterRows1);
    validateResults(beforeRows2, afterRows2);
    validateResults(beforeRows3, afterRows3);
  }

  /**
   * Tests aggregation queries with inverted index filters before and after conversion.
   */
  @Test
  public void testAggregationQueriesAfterVersionConversion()
      throws Exception {
    String query1 = "SELECT COUNT(*), SUM(column1), MAX(column1), MIN(column1) FROM testTable"
        + " WHERE column8 = 890282370";
    String query2 = "SELECT column8, COUNT(*), SUM(column9) FROM testTable WHERE column9 > 100000000 GROUP BY column8"
        + " ORDER BY column8 LIMIT 100";
    String query3 = "SELECT COUNT(*) FROM testTable WHERE column8 = 890282370 OR column9 = 890662862";

    List<Object[]> beforeRows1 = runQuery(query1);
    List<Object[]> beforeRows2 = runQuery(query2);
    List<Object[]> beforeRows3 = runQuery(query3);

    reloadWithVersion(InvertedIndexConfig.VERSION_1);
    verifyInvertedIndexVersions(InvertedIndexConfig.VERSION_1);

    List<Object[]> afterRows1 = runQuery(query1);
    List<Object[]> afterRows2 = runQuery(query2);
    List<Object[]> afterRows3 = runQuery(query3);

    validateResults(beforeRows1, afterRows1);
    validateResults(beforeRows2, afterRows2);
    validateResults(beforeRows3, afterRows3);
  }

  /**
   * Tests queries with exclusive (NOT IN / !=) predicates that trigger bitmap flip() on the
   * inverted index. This exercises the mutation path in InvertedIndexFilterOperator.
   */
  @Test
  public void testExclusiveFilterQueriesAfterVersionConversion()
      throws Exception {
    String query1 = "SELECT COUNT(*) FROM testTable WHERE column8 NOT IN (890282370, 890662862)";
    String query2 = "SELECT column8, COUNT(*) FROM testTable WHERE column8 != 890282370 GROUP BY column8"
        + " ORDER BY column8 LIMIT 100";

    List<Object[]> beforeRows1 = runQuery(query1);
    List<Object[]> beforeRows2 = runQuery(query2);

    reloadWithVersion(InvertedIndexConfig.VERSION_1);
    verifyInvertedIndexVersions(InvertedIndexConfig.VERSION_1);

    List<Object[]> afterRows1 = runQuery(query1);
    List<Object[]> afterRows2 = runQuery(query2);

    validateResults(beforeRows1, afterRows1);
    validateResults(beforeRows2, afterRows2);
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private List<Object[]> runQuery(String query) {
    BrokerResponseNative response = getBrokerResponse(query);
    assertTrue(response.getExceptions() == null || response.getExceptions().isEmpty(),
        "Query should not throw exceptions: " + query);
    ResultTable resultTable = response.getResultTable();
    assertNotNull(resultTable);
    return resultTable.getRows();
  }

  private void reloadWithVersion(int version)
      throws Exception {
    TableConfig tableConfig = createTableConfig(version);
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, SCHEMA);
    File indexDir = new File(INDEX_DIR, SEGMENT_NAME);
    ImmutableSegment segment = reloadSegment(indexDir, indexLoadingConfig, SCHEMA);
    _indexSegment.destroy();
    _indexSegment = segment;
    _indexSegments = List.of(segment, segment);
  }

  private void verifyInvertedIndexVersions(int expectedVersion)
      throws Exception {
    File segmentDir = new File(INDEX_DIR, SEGMENT_NAME);
    try (SegmentDirectory segmentDirectory =
        new org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      for (String column : INVERTED_INDEX_COLUMNS) {
        assertTrue(reader.hasIndexFor(column, StandardIndexes.inverted()));
        PinotDataBuffer buffer = reader.getIndexFor(column, StandardIndexes.inverted());
        int firstInt = buffer.getInt(0);
        if (expectedVersion == InvertedIndexConfig.VERSION_1) {
          assertEquals(firstInt, BitmapInvertedIndexWriter.MAGIC_NUMBER,
              column + " should be VERSION_1");
        } else {
          assertNotEquals(firstInt, BitmapInvertedIndexWriter.MAGIC_NUMBER,
              column + " should be VERSION_0");
        }
      }
    }
  }

  private void validateResults(List<Object[]> beforeResults, List<Object[]> afterResults) {
    assertEquals(beforeResults.size(), afterResults.size(), "Row count should match");
    for (int i = 0; i < beforeResults.size(); i++) {
      Object[] beforeRow = beforeResults.get(i);
      Object[] afterRow = afterResults.get(i);
      assertEquals(beforeRow.length, afterRow.length, "Column count should match at row " + i);
      for (int j = 0; j < beforeRow.length; j++) {
        assertEquals(afterRow[j], beforeRow[j],
            "Value mismatch at row " + i + ", column " + j);
      }
    }
  }
}
