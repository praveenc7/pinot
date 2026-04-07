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

import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.pinot.common.utils.request.QueryFingerprintUtils;
import org.apache.pinot.spi.trace.QueryFingerprint;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/**
 * Benchmark to measure the performance overhead of query fingerprinting on single-stage queries.
 *
 * Measures:
 * 1. parseOnly (baseline) - CalciteSqlParser.compileToSqlNodeAndOptions only
 * 2. parseAndFingerprint (end-to-end) - parse + fingerprint visitor + FarmHash
 *
 * The delta between parseAndFingerprint and parseOnly isolates the fingerprint overhead.
 *
 * Query types tested (all single-stage):
 * - simple: basic SELECT with 2 WHERE filters (~2 literals to replace)
 * - average: GROUP BY with aggregations, HAVING, ORDER BY, LIMIT (~6 literals)
 * - heavy_filter: IN clause with 1000 values (tests IN-squashing, 1 replacement)
 * - many_filters: 200 individual AND filters (no squashing, 200 replacements)
 * - wide_aggregation: 168 SUM columns with minimal literals (large AST, few mutations)
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(3)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@State(Scope.Benchmark)
public class BenchmarkQueryFingerprint {

  @Param({"simple", "average", "heavy_filter", "many_filters", "wide_aggregation"})
  private String _queryType;

  private String _sql;

  @Setup
  public void setup() {
    _sql = buildQuery(_queryType);
  }

  /**
   * Baseline: measure Calcite parse cost only (no fingerprinting).
   */
  @Benchmark
  public SqlNodeAndOptions parseOnly() {
    return CalciteSqlParser.compileToSqlNodeAndOptions(_sql);
  }

  /**
   * End-to-end: parse + fingerprint visitor + FarmHash.
   * The delta vs parseOnly isolates the fingerprint overhead.
   */
  @Benchmark
  public QueryFingerprint parseAndFingerprint()
      throws Exception {
    SqlNodeAndOptions parsed = CalciteSqlParser.compileToSqlNodeAndOptions(_sql);
    return QueryFingerprintUtils.generateFingerprint(parsed);
  }

  private static String buildQuery(String queryType) {
    switch (queryType) {
      case "simple":
        // Basic point lookup: 2 literals to replace
        return "SELECT col1, col2 FROM myTable WHERE col3 = 100 AND col4 > 50";

      case "average":
        // Typical analytics query: GROUP BY, multiple aggregations, HAVING, ORDER BY, LIMIT
        // ~6 literals to replace
        return "SELECT col1, COUNT(*), SUM(col2), AVG(col3), MIN(col4), MAX(col5) "
            + "FROM myTable WHERE col6 > 100 AND col7 = 'active' "
            + "GROUP BY col1 HAVING COUNT(*) > 10 ORDER BY SUM(col2) DESC LIMIT 100";

      case "heavy_filter":
        // Large IN list with 1000 values: stress-tests the IN-squashing logic
        // All literals are squashed to a single ? so visitor does minimal mutation
        String inValues = IntStream.rangeClosed(1, 1000)
            .mapToObj(Integer::toString)
            .collect(Collectors.joining(", "));
        return "SELECT col1, col2, col3 FROM myTable "
            + "WHERE col4 IN (" + inValues + ") "
            + "AND col5 > 100 AND col6 = 'active' AND col7 BETWEEN 10 AND 1000";

      case "many_filters":
        // 200 individual AND filters: each literal must be replaced one-by-one (no squashing)
        // Contrast with heavy_filter where IN-squashing avoids per-literal replacement
        String filters = IntStream.rangeClosed(1, 200)
            .mapToObj(i -> "col_" + i + " = " + (i * 100))
            .collect(Collectors.joining(" AND "));
        return "SELECT col1, col2 FROM myTable WHERE " + filters;

      case "wide_aggregation":
        // 168 SUM aggregations over synthetic columns, large AST but only 2 string literals
        // Tests visitor traversal overhead on wide queries with minimal mutation
        String daySums = IntStream.rangeClosed(0, 97)
            .mapToObj(i -> "SUM(day_" + i + ")")
            .collect(Collectors.joining(", "));
        String weekSums = IntStream.rangeClosed(0, 13)
            .mapToObj(w -> IntStream.rangeClosed(3, 7)
                .mapToObj(f -> "SUM(week_" + w + "_fcap_" + f + "_7day)")
                .collect(Collectors.joining(", ")))
            .collect(Collectors.joining(", "));
        return "SELECT " + daySums + ", " + weekSums
            + " FROM myWideTable"
            + " WHERE dim_lang IN ('en') AND dim_geo IN ('12345')";

      default:
        throw new IllegalArgumentException("Unknown query type: " + queryType);
    }
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt =
        new OptionsBuilder().include(BenchmarkQueryFingerprint.class.getSimpleName());
    new Runner(opt.build()).run();
  }
}
