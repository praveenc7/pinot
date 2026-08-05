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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.broker.routing.instanceselector.SegmentInstanceCandidate;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;


/**
 * Benchmark comparing stream-based vs loop-based server selection in
 * {@link org.apache.pinot.broker.routing.instanceselector.ReplicaGroupInstanceSelector}.
 *
 * Simulates the per-query inner loop of selectServersUsingAdaptiveServerSelector with
 * 8000 segments, 70 total server instances, and 3 replicas per segment.
 *
 * <pre>
 * Run:
 *   mvn clean package -pl pinot-perf -DskipTests
 *   java -jar pinot-perf/target/benchmarks.jar BenchmarkAdaptiveServerSelection
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 2)
@State(Scope.Benchmark)
public class BenchmarkAdaptiveServerSelection {

  @Param({"8000"})
  private int _numSegments;

  @Param({"70"})
  private int _numServers;

  @Param({"3"})
  private int _numReplicas;

  /** Per-segment candidate lists (each has _numReplicas entries drawn from the server pool). */
  private List<List<SegmentInstanceCandidate>> _candidateLists;

  /** Server rank map: server instance name -> rank (0 = best). */
  private Map<String, Integer> _serverRankMap;

  private int _requestId;

  @Setup(Level.Trial)
  public void setup() {
    Random rng = new Random(42);
    String[] serverNames = new String[_numServers];
    for (int i = 0; i < _numServers; i++) {
      serverNames[i] = "Server_Broker_" + i;
    }

    // Build a global rank for every server (shuffle of 0..numServers-1).
    _serverRankMap = new HashMap<>(_numServers * 2);
    int[] ranks = new int[_numServers];
    for (int i = 0; i < _numServers; i++) {
      ranks[i] = i;
    }
    // Fisher-Yates shuffle
    for (int i = _numServers - 1; i > 0; i--) {
      int j = rng.nextInt(i + 1);
      int tmp = ranks[i];
      ranks[i] = ranks[j];
      ranks[j] = tmp;
    }
    for (int i = 0; i < _numServers; i++) {
      _serverRankMap.put(serverNames[i], ranks[i]);
    }

    // Build per-segment candidate lists.  Each segment picks _numReplicas distinct servers.
    _candidateLists = new ArrayList<>(_numSegments);
    for (int s = 0; s < _numSegments; s++) {
      List<SegmentInstanceCandidate> candidates = new ArrayList<>(_numReplicas);
      // Simple deterministic assignment: consecutive servers per segment, wrapping around.
      int base = (s * _numReplicas) % _numServers;
      for (int r = 0; r < _numReplicas; r++) {
        int serverIdx = (base + r) % _numServers;
        candidates.add(new SegmentInstanceCandidate(serverNames[serverIdx], true, r));
      }
      _candidateLists.add(candidates);
    }

    _requestId = rng.nextInt(1_000_000);
  }

  // ---- Old code (stream-based) ----

  @Benchmark
  public Map<String, String> streamBased() {
    Map<String, String> result = new HashMap<>(_numSegments * 2);
    for (int s = 0; s < _numSegments; s++) {
      List<SegmentInstanceCandidate> candidates = _candidateLists.get(s);
      int roundRobinIdx = _requestId % candidates.size();
      SegmentInstanceCandidate selectedInstance = candidates.get(roundRobinIdx);

      if (!_serverRankMap.isEmpty()) {
        selectedInstance = candidates.stream()
            .anyMatch(candidate -> !_serverRankMap.containsKey(candidate.getInstance()))
            ? candidates.get(roundRobinIdx)
            : candidates.stream()
                .min(Comparator.comparingInt(candidate -> _serverRankMap.get(candidate.getInstance())))
                .orElse(candidates.get(roundRobinIdx));
      }
      result.put("segment_" + s, selectedInstance.getInstance());
    }
    return result;
  }

  // ---- New code (loop-based) ----

  @Benchmark
  public Map<String, String> loopBased() {
    Map<String, String> result = new HashMap<>(_numSegments * 2);
    for (int s = 0; s < _numSegments; s++) {
      List<SegmentInstanceCandidate> candidates = _candidateLists.get(s);
      int roundRobinIdx = _requestId % candidates.size();
      SegmentInstanceCandidate selectedInstance = candidates.get(roundRobinIdx);

      if (!_serverRankMap.isEmpty()) {
        int bestRank = Integer.MAX_VALUE;
        SegmentInstanceCandidate bestCandidate = null;
        for (int i = 0; i < candidates.size(); i++) {
          SegmentInstanceCandidate candidate = candidates.get(i);
          Integer rank = _serverRankMap.get(candidate.getInstance());
          if (rank == null) {
            bestCandidate = null;
            break;
          }
          if (rank < bestRank) {
            bestRank = rank;
            bestCandidate = candidate;
          }
        }
        if (bestCandidate != null) {
          selectedInstance = bestCandidate;
        }
      }
      result.put("segment_" + s, selectedInstance.getInstance());
    }
    return result;
  }

  public static void main(String[] args)
      throws Exception {
    // When running via JMH shaded jar, use the JMH runner:
    // java -jar pinot-perf/target/benchmarks.jar BenchmarkAdaptiveServerSelection
    //
    // For quick standalone execution (no JMH annotation processing needed), use the manual harness below.
    runManualBenchmark();
  }

  /**
   * Simple standalone benchmark harness that doesn't require JMH annotation processing.
   * Runs warmup + measurement iterations and reports average time per invocation.
   */
  private static void runManualBenchmark() {
    BenchmarkAdaptiveServerSelection bench = new BenchmarkAdaptiveServerSelection();
    bench._numSegments = 8000;
    bench._numServers = 70;
    bench._numReplicas = 3;
    bench.setup();

    int warmupIterations = 200;
    int measureIterations = 100;

    System.out.println("=== Benchmark: Adaptive Server Selection (8000 segments, 70 servers, 3 replicas) ===\n");

    // --- Warmup stream-based ---
    System.out.print("Warming up streamBased (" + warmupIterations + " iterations)...");
    for (int i = 0; i < warmupIterations; i++) {
      bench.streamBased();
    }
    System.out.println(" done.");

    // --- Measure stream-based ---
    long[] streamTimes = new long[measureIterations];
    for (int i = 0; i < measureIterations; i++) {
      long start = System.nanoTime();
      bench.streamBased();
      streamTimes[i] = System.nanoTime() - start;
    }

    // --- Warmup loop-based ---
    System.out.print("Warming up loopBased  (" + warmupIterations + " iterations)...");
    for (int i = 0; i < warmupIterations; i++) {
      bench.loopBased();
    }
    System.out.println(" done.\n");

    // --- Measure loop-based ---
    long[] loopTimes = new long[measureIterations];
    for (int i = 0; i < measureIterations; i++) {
      long start = System.nanoTime();
      bench.loopBased();
      loopTimes[i] = System.nanoTime() - start;
    }

    // --- Report ---
    double streamAvgUs = avg(streamTimes) / 1000.0;
    double streamP50Us = percentile(streamTimes, 50) / 1000.0;
    double streamP99Us = percentile(streamTimes, 99) / 1000.0;

    double loopAvgUs = avg(loopTimes) / 1000.0;
    double loopP50Us = percentile(loopTimes, 50) / 1000.0;
    double loopP99Us = percentile(loopTimes, 99) / 1000.0;

    System.out.printf("%-15s %12s %12s %12s%n", "Method", "Avg (us)", "P50 (us)", "P99 (us)");
    System.out.printf("%-15s %12.1f %12.1f %12.1f%n", "streamBased", streamAvgUs, streamP50Us, streamP99Us);
    System.out.printf("%-15s %12.1f %12.1f %12.1f%n", "loopBased", loopAvgUs, loopP50Us, loopP99Us);
    System.out.printf("%n%-15s %11.1fx %11.1fx %11.1fx%n", "Speedup", streamAvgUs / loopAvgUs,
        streamP50Us / loopP50Us, streamP99Us / loopP99Us);
  }

  private static double avg(long[] values) {
    long sum = 0;
    for (long v : values) {
      sum += v;
    }
    return (double) sum / values.length;
  }

  private static double percentile(long[] values, int pct) {
    long[] sorted = values.clone();
    java.util.Arrays.sort(sorted);
    int idx = (int) Math.ceil(pct / 100.0 * sorted.length) - 1;
    return sorted[Math.max(0, idx)];
  }
}
