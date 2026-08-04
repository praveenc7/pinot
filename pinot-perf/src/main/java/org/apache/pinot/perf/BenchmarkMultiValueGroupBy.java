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
package org.apache.pinot.perf;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.MvIntArrayBuffer;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.plan.ProjectPlanNode;
import org.apache.pinot.core.query.aggregation.groupby.DictionaryBasedGroupKeyGenerator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.queries.BaseQueriesTest;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/**
 * Benchmark for group-by on a multi-value dictionary encoded column (OA-1821).
 *
 * <p>Historically a dictionary-encoded MV column was materialised as one {@code int[]} per document. For exactly one
 * MV group-by expression, the group-key generator reused those arrays in place, which avoided another copy but made
 * it impossible to replace only the fetch side with a flat buffer without adding memory traffic. The optimized path
 * carries one flat {@code values + offsets} representation through dictionary-id fetching, group-id mapping, and
 * aggregation.
 *
 * <p>Run with the GC profiler so that the allocation rate is reported next to the latency, since bytes allocated per
 * operation is the headline metric for this change:
 *
 * <pre>{@code
 * org.openjdk.jmh.Main BenchmarkMultiValueGroupBy -prof gc
 * }</pre>
 *
 * <p>{@link #groupByQuery} measures the end-to-end flat path. {@link #generateFlatGroupKeys} and
 * {@link #fetchFlatDictIds} isolate flat group-key generation and dictionary-id fetching; their nested counterparts
 * remain as controls.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@State(Scope.Benchmark)
public class BenchmarkMultiValueGroupBy extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkMultiValueGroupBy");
  private static final String TABLE_NAME = "MyTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String MV_COL = "MV_COL";
  private static final String MV_COL_2 = "MV_COL_2";
  private static final String METRIC_COL = "METRIC_COL";
  private static final long RANDOM_SEED = 42L;
  private static final int NUM_VALUES_SECOND_MV_COL = 16;

  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName(TABLE_NAME)
      .addMultiValueDimension(MV_COL, FieldSpec.DataType.INT)
      .addMultiValueDimension(MV_COL_2, FieldSpec.DataType.INT)
      .addMetric(METRIC_COL, FieldSpec.DataType.INT)
      .build();

  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();

  /**
   * Mirrors the shape of the reported {@code audienceCount} query: group by a high cardinality multi-value dimension
   * and sum a metric.
   */
  private static final String GROUP_BY_QUERY =
      "SELECT MV_COL, SUM(METRIC_COL) FROM MyTable GROUP BY MV_COL LIMIT 10000";

  private static final ExpressionContext[] GROUP_BY_EXPRESSIONS = {ExpressionContext.forIdentifier(MV_COL)};

  /**
   * Two multi-value columns, so that the group keys of a document are the cross product of the dictionary ids of both
   * columns. The wide column is the last expression, which is the one expanded first, so the intermediate results of
   * the expansion are the ones that dominate.
   */
  private static final ExpressionContext[] MULTI_COLUMN_GROUP_BY_EXPRESSIONS =
      {ExpressionContext.forIdentifier(MV_COL_2), ExpressionContext.forIdentifier(MV_COL)};

  private static final String MULTI_COLUMN_GROUP_BY_QUERY =
      "SELECT MV_COL_2, MV_COL, SUM(METRIC_COL) FROM MyTable GROUP BY MV_COL_2, MV_COL LIMIT 10000";

  @Param("1000000")
  private int _numRows;

  /**
   * Average number of values per multi-value entry. The reported query averaged roughly 270 values per document, so
   * the allocation curve is sampled around that.
   */
  @Param({"4", "32", "256"})
  private int _numValuesPerEntry;

  /**
   * Cardinality of the multi-value dimension. {@code 1000} stays under the array based group key threshold, {@code
   * 100000} forces the map based holder that a real high cardinality dimension would use.
   */
  @Param({"1000", "100000"})
  private int _cardinality;

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;
  private int[] _svGroupKeyBuffer;
  private int[][] _mvGroupKeyBuffer;
  private MvIntArrayBuffer _flatDictIdBuffer;

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkMultiValueGroupBy.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  @Setup
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    buildSegment();
    _indexSegment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), new IndexLoadingConfig(TABLE_CONFIG, SCHEMA));
    _indexSegments = List.of(_indexSegment);
    _svGroupKeyBuffer = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    _mvGroupKeyBuffer = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    _flatDictIdBuffer = new MvIntArrayBuffer();
  }

  @TearDown
  public void tearDown() {
    if (_indexSegment != null) {
      _indexSegment.destroy();
    }
    FileUtils.deleteQuietly(INDEX_DIR);
    EXECUTOR_SERVICE.shutdownNow();
  }

  /**
   * End to end {@code SELECT MV_COL, SUM(METRIC_COL) ... GROUP BY MV_COL}.
   */
  @Benchmark
  public BrokerResponseNative groupByQuery() {
    return getBrokerResponse(GROUP_BY_QUERY);
  }

  /**
   * Fetch plus group key generation over the whole segment, which is the part of the query this change targets.
   */
  @Benchmark
  public void generateGroupKeys(Blackhole blackhole) {
    BaseProjectOperator<?> projectOperator = createProjectOperator();
    DictionaryBasedGroupKeyGenerator groupKeyGenerator =
        new DictionaryBasedGroupKeyGenerator(projectOperator, GROUP_BY_EXPRESSIONS,
            Server.DEFAULT_QUERY_EXECUTOR_NUM_GROUPS_LIMIT,
            Server.DEFAULT_QUERY_EXECUTOR_MAX_INITIAL_RESULT_HOLDER_CAPACITY, null);
    ValueBlock valueBlock;
    while ((valueBlock = projectOperator.nextBlock()) != null) {
      groupKeyGenerator.generateKeysForBlock(valueBlock, _mvGroupKeyBuffer);
      // Consume the group keys the way an aggregation function would
      int numDocs = valueBlock.getNumDocs();
      long sum = 0;
      for (int i = 0; i < numDocs; i++) {
        for (int groupKey : _mvGroupKeyBuffer[i]) {
          sum += groupKey;
        }
      }
      blackhole.consume(sum);
    }
    blackhole.consume(_svGroupKeyBuffer);
  }

  /**
   * Flat fetch plus in-place group-id mapping over the whole segment.
   */
  @Benchmark
  public void generateFlatGroupKeys(Blackhole blackhole) {
    BaseProjectOperator<?> projectOperator = createProjectOperator();
    DictionaryBasedGroupKeyGenerator groupKeyGenerator =
        new DictionaryBasedGroupKeyGenerator(projectOperator, GROUP_BY_EXPRESSIONS,
            Server.DEFAULT_QUERY_EXECUTOR_NUM_GROUPS_LIMIT,
            Server.DEFAULT_QUERY_EXECUTOR_MAX_INITIAL_RESULT_HOLDER_CAPACITY, null);
    ValueBlock valueBlock;
    while ((valueBlock = projectOperator.nextBlock()) != null) {
      MvIntArrayBuffer groupKeys = groupKeyGenerator.generateFlatKeysForBlock(valueBlock);
      int[] values = groupKeys.getValues();
      long sum = 0;
      for (int i = 0, numValues = groupKeys.getNumValues(); i < numValues; i++) {
        sum += values[i];
      }
      blackhole.consume(sum);
    }
  }

  /**
   * The fetch path alone: materialising the multi-value dictionary ids for every document of the segment.
   */
  @Benchmark
  public void fetchDictIds(Blackhole blackhole) {
    BaseProjectOperator<?> projectOperator = createProjectOperator();
    ValueBlock valueBlock;
    while ((valueBlock = projectOperator.nextBlock()) != null) {
      BlockValSet blockValSet = valueBlock.getBlockValueSet(MV_COL);
      int[][] dictIds = blockValSet.getDictionaryIdsMV();
      long sum = 0;
      for (int i = 0, numDocs = valueBlock.getNumDocs(); i < numDocs; i++) {
        for (int dictId : dictIds[i]) {
          sum += dictId;
        }
      }
      blackhole.consume(sum);
    }
  }

  /**
   * The flat fetch path alone.
   */
  @Benchmark
  public void fetchFlatDictIds(Blackhole blackhole) {
    BaseProjectOperator<?> projectOperator = createProjectOperator();
    ValueBlock valueBlock;
    while ((valueBlock = projectOperator.nextBlock()) != null) {
      BlockValSet blockValSet = valueBlock.getBlockValueSet(MV_COL);
      blockValSet.getDictionaryIdsMV(valueBlock.getNumDocs(), _flatDictIdBuffer);
      int[] dictIds = _flatDictIdBuffer.getValues();
      long sum = 0;
      for (int i = 0, numValues = _flatDictIdBuffer.getNumValues(); i < numValues; i++) {
        sum += dictIds[i];
      }
      blackhole.consume(sum);
    }
  }

  /**
   * Group key generation over two multi-value columns, which is the path that expands the cross product of the
   * dictionary ids of every group-by expression.
   */
  @Benchmark
  public void generateGroupKeysMultiColumn(Blackhole blackhole) {
    BaseProjectOperator<?> projectOperator =
        createProjectOperator(MULTI_COLUMN_GROUP_BY_QUERY, MULTI_COLUMN_GROUP_BY_EXPRESSIONS);
    DictionaryBasedGroupKeyGenerator groupKeyGenerator =
        new DictionaryBasedGroupKeyGenerator(projectOperator, MULTI_COLUMN_GROUP_BY_EXPRESSIONS,
            Server.DEFAULT_QUERY_EXECUTOR_NUM_GROUPS_LIMIT,
            Server.DEFAULT_QUERY_EXECUTOR_MAX_INITIAL_RESULT_HOLDER_CAPACITY, null);
    ValueBlock valueBlock;
    while ((valueBlock = projectOperator.nextBlock()) != null) {
      groupKeyGenerator.generateKeysForBlock(valueBlock, _mvGroupKeyBuffer);
      int numDocs = valueBlock.getNumDocs();
      long sum = 0;
      for (int i = 0; i < numDocs; i++) {
        for (int groupKey : _mvGroupKeyBuffer[i]) {
          sum += groupKey;
        }
      }
      blackhole.consume(sum);
    }
  }

  private BaseProjectOperator<?> createProjectOperator() {
    return createProjectOperator(GROUP_BY_QUERY, GROUP_BY_EXPRESSIONS);
  }

  private BaseProjectOperator<?> createProjectOperator(String query, ExpressionContext[] expressions) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    return new ProjectPlanNode(new SegmentContext(_indexSegment), queryContext, Arrays.asList(expressions),
        DocIdSetPlanNode.MAX_DOC_PER_CALL).run();
  }

  private void buildSegment()
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GeneratedDataRecordReader(createData())) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  private LazyDataGenerator createData() {
    int numRows = _numRows;
    int numValuesPerEntry = _numValuesPerEntry;
    int cardinality = _cardinality;
    return new LazyDataGenerator() {
      private final Random _random = new Random(RANDOM_SEED);

      @Override
      public int size() {
        return numRows;
      }

      @Override
      public GenericRow next(GenericRow row, int index) {
        // Vary the number of values around the average so that the group key arrays have mixed lengths, which is the
        // adversarial case for any pooling of the per document arrays
        int numValues = Math.max(1, numValuesPerEntry / 2 + _random.nextInt(numValuesPerEntry));
        Integer[] values = new Integer[numValues];
        for (int i = 0; i < numValues; i++) {
          values[i] = _random.nextInt(cardinality);
        }
        row.putValue(MV_COL, values);
        int numValues2 = 1 + _random.nextInt(3);
        Integer[] values2 = new Integer[numValues2];
        for (int i = 0; i < numValues2; i++) {
          values2[i] = _random.nextInt(NUM_VALUES_SECOND_MV_COL);
        }
        row.putValue(MV_COL_2, values2);
        row.putValue(METRIC_COL, _random.nextInt(1000));
        return row;
      }

      @Override
      public void rewind() {
        _random.setSeed(RANDOM_SEED);
      }
    };
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
}
