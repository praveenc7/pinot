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
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * End-to-end query test for {@code =} (EQ) and {@code IN} filters on a single-value STRING column.
 *
 * <p>The segment carries the same data in two columns: {@code NO_DICT_STRING_COL} (no dictionary) and
 * {@code DICT_STRING_COL} (dictionary-encoded). A filter on the no-dictionary column drives the byte-path scan matcher
 * (the raw-STRING branch of {@code SVScanDocIdIterator}'s {@code BytesMatcher} -> {@code applySV(byte[])}); the same
 * filter on the dictionary
 * column uses the dictionary path. Every query is run against both columns and the results must match, which confirms
 * the byte path is wired into the filter operator and is correct for ASCII, multibyte UTF-8, and mixed-length values.
 */
public class RawStringFilterQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "RawStringFilterQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String NO_DICT_STRING_COL = "noDictStringCol";
  private static final String DICT_STRING_COL = "dictStringCol";
  private static final String ID_COLUMN = "id";

  // Mixed lengths plus a multibyte (UTF-8) value, so the byte path and the IN length-gate are exercised end-to-end.
  private static final String[] VALUES = {"a", "bb", "ccc", "café", "naïve", "uid-12345"};
  private static final int NUM_RECORDS = 1200;                 // divisible by VALUES.length -> PER_VALUE of each
  private static final int PER_VALUE = NUM_RECORDS / VALUES.length;

  // BaseQueriesTest runs each query as if over 4 identical segments (OFFLINE + REALTIME x 2-segment list), so a
  // per-segment match count of PER_VALUE surfaces as 4 * PER_VALUE in the broker response. See BaseQueriesTest Javadoc.
  private static final int SEGMENT_MULTIPLIER = 4;

  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(NO_DICT_STRING_COL, DataType.STRING)
      .addSingleValueDimension(DICT_STRING_COL, DataType.STRING)
      .addMetric(ID_COLUMN, DataType.INT)
      .build();

  // Only NO_DICT_STRING_COL is raw; DICT_STRING_COL keeps its (default) dictionary.
  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName(RAW_TABLE_NAME)
      .setNoDictionaryColumns(List.of(NO_DICT_STRING_COL))
      .build();

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
    FileUtils.deleteDirectory(INDEX_DIR);

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      String value = VALUES[i % VALUES.length];
      GenericRow record = new GenericRow();
      record.putValue(NO_DICT_STRING_COL, value);
      record.putValue(DICT_STRING_COL, value);
      record.putValue(ID_COLUMN, i);
      records.add(record);
    }

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setTableName(RAW_TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);
    config.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(records));
    driver.build();

    ImmutableSegment segment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = segment;
    _indexSegments = Arrays.asList(segment, segment);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    if (_indexSegment != null) {
      _indexSegment.destroy();
    }
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Test
  public void testEqFilter() {
    // ASCII, multibyte, and a no-match.
    assertRawMatchesDict("= 'café'", SEGMENT_MULTIPLIER * PER_VALUE);
    assertRawMatchesDict("= 'uid-12345'", SEGMENT_MULTIPLIER * PER_VALUE);
    assertRawMatchesDict("= 'no-such-value'", 0);
  }

  @Test
  public void testInFilter() {
    // Multiple values (mixed lengths + multibyte), an IN with a non-member, and an all-miss IN.
    assertRawMatchesDict("IN ('a', 'naïve', 'uid-12345')", SEGMENT_MULTIPLIER * 3 * PER_VALUE);
    assertRawMatchesDict("IN ('a', 'no-such-value')", SEGMENT_MULTIPLIER * PER_VALUE);
    assertRawMatchesDict("IN ('no-such-value')", 0);
  }

  @Test
  public void testEqSelectsSameRowsAsDictionary() {
    // Selection-path check: the byte path must select EXACTLY the same rows as the dictionary path, not just the
    // same count. (testEqFilter covers the aggregation path; this covers SELECT/projection.)
    assertSameRowsAsDictionary("= 'café'", SEGMENT_MULTIPLIER * PER_VALUE);
    assertSameRowsAsDictionary("= 'uid-12345'", SEGMENT_MULTIPLIER * PER_VALUE);
    assertSameRowsAsDictionary("= 'no-such-value'", 0);
  }

  @Test
  public void testInSelectsSameRowsAsDictionary() {
    assertSameRowsAsDictionary("IN ('a', 'naïve', 'uid-12345')", SEGMENT_MULTIPLIER * 3 * PER_VALUE);
    assertSameRowsAsDictionary("IN ('a', 'no-such-value')", SEGMENT_MULTIPLIER * PER_VALUE);
    assertSameRowsAsDictionary("IN ('no-such-value')", 0);
  }

  /**
   * Runs the same {@code COUNT(*)} filter against the no-dictionary column (byte path) and the dictionary column,
   * asserting the two agree and equal the expected count.
   */
  private void assertRawMatchesDict(String predicate, long expectedCount) {
    long rawCount = count("SELECT COUNT(*) FROM testTable WHERE " + NO_DICT_STRING_COL + " " + predicate);
    long dictCount = count("SELECT COUNT(*) FROM testTable WHERE " + DICT_STRING_COL + " " + predicate);
    assertEquals(rawCount, dictCount, "byte path disagrees with dictionary path for predicate: " + predicate);
    assertEquals(rawCount, expectedCount, "unexpected match count for predicate: " + predicate);
  }

  private long count(String query) {
    BrokerResponseNative response = getBrokerResponse(query);
    ResultTable resultTable = response.getResultTable();
    return ((Number) resultTable.getRows().get(0)[0]).longValue();
  }

  /**
   * Selects the matched row ids via the no-dictionary column (byte path) and via the dictionary column, asserting the
   * two id sequences are identical (same rows, not merely the same count) and of the expected size.
   */
  private void assertSameRowsAsDictionary(String predicate, long expectedCount) {
    List<Integer> rawIds = selectIds(NO_DICT_STRING_COL, predicate);
    List<Integer> dictIds = selectIds(DICT_STRING_COL, predicate);
    assertEquals(rawIds, dictIds, "byte path matched different rows than dictionary path for predicate: " + predicate);
    assertEquals((long) rawIds.size(), expectedCount, "unexpected match count for predicate: " + predicate);
  }

  private List<Integer> selectIds(String column, String predicate) {
    ResultTable resultTable = getBrokerResponse(
        "SELECT id FROM testTable WHERE " + column + " " + predicate + " ORDER BY id LIMIT 100000").getResultTable();
    List<Integer> ids = new ArrayList<>(resultTable.getRows().size());
    for (Object[] row : resultTable.getRows()) {
      ids.add(((Number) row[0]).intValue());
    }
    return ids;
  }
}
