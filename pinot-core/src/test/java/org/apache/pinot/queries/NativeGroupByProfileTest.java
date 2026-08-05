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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.query.aggregation.groupby.NativeGroupByRouter;
import org.apache.pinot.nativeengine.groupby.PinotNativeGroupBy;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Attribution harness (design doc §22.9 lever 0): splits the native segment GROUP BY time into
 * key-probe / agg-apply / drain and prints the broker-reduce residual, to confirm whether the
 * per-group <b>drain</b> is the high-cardinality culprit behind the §22.2 regression.
 *
 * <p>Not a CI test. Run explicitly with the profile flag set as a JVM arg (it is read once at
 * class-load in {@code NativeGroupByExecutor}):
 *
 * <pre>
 *   ./mvnw -pl pinot-core -am test -Dtest=NativeGroupByProfileTest \
 *       -Dsurefire.failIfNoSpecifiedTests=false \
 *       -Dpinot.native.groupby.profile.run=true -Dpinot.native.groupby.profile=true
 * </pre>
 */
public class NativeGroupByProfileTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "NativeGroupByProfileTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String LIB_PATH_PROP = "pinot.native.lib.path";

  private static final String KEY_COL = "keyCol";
  private static final String INT_VAL = "intVal";
  private static final String LONG_VAL = "longVal";
  private static final String DOUBLE_VAL = "doubleVal";

  private static final int NUM_ROWS = 1_000_000;
  private static final int[] CARDINALITIES = {1_000, 100_000, 1_000_000};
  private static final int WARMUP = 5;
  private static final int TIMED = 5;

  private static final String QUERY =
      "SELECT " + KEY_COL + ", SUM(" + LONG_VAL + "), MIN(" + INT_VAL + "), MAX(" + DOUBLE_VAL + "), COUNT(*) "
          + "FROM " + RAW_TABLE_NAME + " GROUP BY " + KEY_COL + " LIMIT 2000000";

  static {
    String resolved = resolveDevLibPath();
    if (resolved != null && System.getProperty(LIB_PATH_PROP) == null) {
      System.setProperty(LIB_PATH_PROP, resolved);
    }
  }

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
  public void guard() {
    if (!Boolean.getBoolean("pinot.native.groupby.profile.run")) {
      throw new SkipException("profiling harness disabled; pass -Dpinot.native.groupby.profile.run=true "
          + "and -Dpinot.native.groupby.profile=true");
    }
    if (!PinotNativeGroupBy.isAvailable()) {
      throw new SkipException("pinot-native library not loadable; build with './mvnw -pl pinot-native package'");
    }
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    System.clearProperty(NativeGroupByRouter.ENABLED_PROPERTY);
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Test
  public void profileAcrossCardinalities()
      throws Exception {
    System.setProperty(NativeGroupByRouter.ENABLED_PROPERTY, "true");
    for (int cardinality : CARDINALITIES) {
      buildSegment(cardinality);
      // Warmup (the executor prints its phase breakdown on every getResult; we read the warm ones).
      for (int i = 0; i < WARMUP; i++) {
        getBrokerResponse(QUERY);
      }
      long total = 0;
      for (int i = 0; i < TIMED; i++) {
        long t0 = System.nanoTime();
        getBrokerResponse(QUERY);
        total += System.nanoTime() - t0;
      }
      double queryMs = total / 1e6 / TIMED;
      System.out.printf("[native-groupby profile] >>> cardinality=%d : full getBrokerResponse=%.2f ms/query "
          + "(segTotal above is the executor-internal portion; residual = reduce + plan + serialize)%n%n", cardinality,
          queryMs);
      _indexSegment.destroy();
    }
  }

  private void buildSegment(int cardinality)
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    Random rng = new Random(20260620L);
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue(KEY_COL, rng.nextInt(cardinality));
      row.putValue(INT_VAL, rng.nextInt(1_000_000) - 500_000);
      row.putValue(LONG_VAL, (long) (rng.nextInt(2_000_000) - 1_000_000));
      row.putValue(DOUBLE_VAL, rng.nextDouble() * 1000.0 - 500.0);
      rows.add(row);
    }
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(KEY_COL, DataType.INT)
        .addMetric(INT_VAL, DataType.INT)
        .addMetric(LONG_VAL, DataType.LONG)
        .addMetric(DOUBLE_VAL, DataType.DOUBLE)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(rows));
    driver.build();
    _indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegments = Collections.singletonList(_indexSegment);
  }

  @Nullable
  private static String resolveDevLibPath() {
    String os = System.getProperty("os.name", "").toLowerCase();
    String libFile;
    if (os.contains("mac") || os.contains("darwin")) {
      libFile = "libpinot_native.dylib";
    } else if (os.contains("windows")) {
      libFile = "pinot_native.dll";
    } else {
      libFile = "libpinot_native.so";
    }
    Path candidate = Paths.get("..", "pinot-native", "native", "target", "release", libFile).toAbsolutePath();
    return Files.exists(candidate) ? candidate.toString() : null;
  }
}
