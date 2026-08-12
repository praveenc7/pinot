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
package org.apache.pinot.core.transport.server.routing.stats;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class ServerRoutingStatsManagerTest {
  private BrokerMetrics _brokerMetrics;

  @BeforeTest
  public void initBrokerMetrics() {
    // Set up metric registry and broker metrics
    PinotConfiguration brokerConfig = new PinotConfiguration();
    PinotMetricsRegistry metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry(
        brokerConfig.subset(CommonConstants.Broker.METRICS_CONFIG_PREFIX));
    _brokerMetrics = new BrokerMetrics(
        brokerConfig.getProperty(
            CommonConstants.Broker.CONFIG_OF_METRICS_NAME_PREFIX,
            CommonConstants.Broker.DEFAULT_METRICS_NAME_PREFIX),
        metricsRegistry,
        brokerConfig.getProperty(
            CommonConstants.Broker.CONFIG_OF_ENABLE_TABLE_LEVEL_METRICS,
            CommonConstants.Broker.DEFAULT_ENABLE_TABLE_LEVEL_METRICS),
        brokerConfig.getProperty(
            CommonConstants.Broker.CONFIG_OF_ALLOWED_TABLES_FOR_EMITTING_METRICS,
            Collections.emptyList()));
    _brokerMetrics.initializeGlobalMeters();
    BrokerMetrics.register(_brokerMetrics);
  }

  @Test
  public void testInitAndShutDown() {
    Map<String, Object> properties = new HashMap<>();

    // Test 1: Test disabled.
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION, false);
    ServerRoutingStatsManager manager = new ServerRoutingStatsManager(new PinotConfiguration(properties),
        _brokerMetrics);
    assertFalse(manager.isEnabled());
    assertNull(manager.getServerRoutingStats());
    manager.init();
    assertFalse(manager.isEnabled());
    assertNull(manager.getServerRoutingStats());

    // Test 2: Test enabled.
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION, true);
    manager = new ServerRoutingStatsManager(new PinotConfiguration(properties), _brokerMetrics);
    assertFalse(manager.isEnabled());
    assertNull(manager.getServerRoutingStats());
    manager.init();
    assertTrue(manager.isEnabled());
    assertNotNull(manager.getServerRoutingStats());

    // Test 3: Shutdown and then init.
    manager.shutDown();
    assertFalse(manager.isEnabled());

    manager.init();
    assertTrue(manager.isEnabled());
  }

  @Test
  public void testEmptyStats() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION, true);
    ServerRoutingStatsManager manager = new ServerRoutingStatsManager(new PinotConfiguration(properties),
        _brokerMetrics);
    manager.init();
    assertNotNull(manager.getServerRoutingStats());

    List<Pair<String, Integer>> numInFlightReqList = manager.fetchNumInFlightRequestsForAllServers();
    assertTrue(numInFlightReqList.isEmpty());
    Integer numInFlightReq = manager.fetchNumInFlightRequestsForServer("testServer");
    assertNull(numInFlightReq);

    List<Pair<String, Double>> latencyList = manager.fetchEMALatencyForAllServers();
    assertTrue(latencyList.isEmpty());

    Double latency = manager.fetchEMALatencyForServer("testServer");
    assertNull(latency);

    List<Pair<String, Double>> scoreList = manager.fetchHybridScoreForAllServers();
    assertTrue(scoreList.isEmpty());

    Double score = manager.fetchHybridScoreForServer("testServer");
    assertNull(score);
  }

  @Test
  public void testQuerySubmitAndCompletionStats() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION, true);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_EWMA_ALPHA, 1.0);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_AUTODECAY_WINDOW_MS, -1);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_WARMUP_DURATION_MS, 0);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_AVG_INITIALIZATION_VAL, 0.0);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_HYBRID_SCORE_EXPONENT, 3);
    ServerRoutingStatsManager manager = new ServerRoutingStatsManager(new PinotConfiguration(properties),
        _brokerMetrics);
    manager.init();

    int requestId = 0;

    // Submit stats for server1.
    manager.recordStatsForQuerySubmission(requestId++, "server1");
    waitForStatsUpdate(manager, requestId);

    List<Pair<String, Integer>> numInFlightReqList = manager.fetchNumInFlightRequestsForAllServers();
    assertEquals(numInFlightReqList.get(0).getLeft(), "server1");
    assertEquals(numInFlightReqList.get(0).getRight().intValue(), 1);

    Integer numInFlightReq = manager.fetchNumInFlightRequestsForServer("server1");
    assertEquals(numInFlightReq.intValue(), 1);

    List<Pair<String, Double>> latencyList = manager.fetchEMALatencyForAllServers();
    assertEquals(latencyList.get(0).getLeft(), "server1");
    assertEquals(latencyList.get(0).getRight().doubleValue(), 0.0);

    Double latency = manager.fetchEMALatencyForServer("server1");
    assertEquals(latency, 0.0);

    List<Pair<String, Double>> scoreList = manager.fetchHybridScoreForAllServers();
    assertEquals(scoreList.get(0).getLeft(), "server1");
    assertEquals(scoreList.get(0).getRight().doubleValue(), 0.0);

    Double score = manager.fetchHybridScoreForServer("server1");
    assertEquals(score, 0.0);

    // Submit more stats for server 1.
    manager.recordStatsForQuerySubmission(requestId++, "server1");
    waitForStatsUpdate(manager, requestId);

    numInFlightReqList = manager.fetchNumInFlightRequestsForAllServers();
    assertEquals(numInFlightReqList.get(0).getLeft(), "server1");
    assertEquals(numInFlightReqList.get(0).getRight().intValue(), 2);

    numInFlightReq = manager.fetchNumInFlightRequestsForServer("server1");
    assertEquals(numInFlightReq.intValue(), 2);

    latencyList = manager.fetchEMALatencyForAllServers();
    assertEquals(latencyList.get(0).getLeft(), "server1");
    assertEquals(latencyList.get(0).getRight().doubleValue(), 0.0);

    latency = manager.fetchEMALatencyForServer("server1");
    assertEquals(latency, 0.0);

    scoreList = manager.fetchHybridScoreForAllServers();
    assertEquals(scoreList.get(0).getLeft(), "server1");
    assertEquals(scoreList.get(0).getRight().doubleValue(), 0.0);

    score = manager.fetchHybridScoreForServer("server1");
    assertEquals(score, 0.0);

    // Add a new server server2.
    manager.recordStatsForQuerySubmission(requestId++, "server2");
    waitForStatsUpdate(manager, requestId);


    numInFlightReqList = manager.fetchNumInFlightRequestsForAllServers();
    int server2Index = numInFlightReqList.get(0).getLeft().equals("server2") ? 0 : 1;
    int server1Index = 1 - server2Index;
    assertEquals(numInFlightReqList.get(server2Index).getLeft(), "server2");
    assertEquals(numInFlightReqList.get(server2Index).getRight().intValue(), 1);
    assertEquals(numInFlightReqList.get(server1Index).getLeft(), "server1");
    assertEquals(numInFlightReqList.get(server1Index).getRight().intValue(), 2);

    numInFlightReq = manager.fetchNumInFlightRequestsForServer("server2");
    assertEquals(numInFlightReq.intValue(), 1);
    numInFlightReq = manager.fetchNumInFlightRequestsForServer("server1");
    assertEquals(numInFlightReq.intValue(), 2);

    latencyList = manager.fetchEMALatencyForAllServers();
    server2Index = latencyList.get(0).getLeft().equals("server2") ? 0 : 1;
    server1Index = 1 - server2Index;
    assertEquals(latencyList.get(server2Index).getLeft(), "server2");
    assertEquals(latencyList.get(server2Index).getRight().doubleValue(), 0.0);
    assertEquals(latencyList.get(server1Index).getLeft(), "server1");
    assertEquals(latencyList.get(server1Index).getRight().doubleValue(), 0.0);

    latency = manager.fetchEMALatencyForServer("server2");
    assertEquals(latency, 0.0);
    latency = manager.fetchEMALatencyForServer("server1");
    assertEquals(latency, 0.0);

    scoreList = manager.fetchHybridScoreForAllServers();
    server2Index = scoreList.get(0).getLeft().equals("server2") ? 0 : 1;
    server1Index = 1 - server2Index;
    assertEquals(scoreList.get(server2Index).getLeft(), "server2");
    assertEquals(scoreList.get(server2Index).getRight().doubleValue(), 0.0);
    assertEquals(scoreList.get(server1Index).getLeft(), "server1");
    assertEquals(scoreList.get(server1Index).getRight().doubleValue(), 0.0);

    score = manager.fetchHybridScoreForServer("server2");
    assertEquals(score, 0.0);
    score = manager.fetchHybridScoreForServer("server1");
    assertEquals(score, 0.0);

    // Record completion stats for server1
    manager.recordStatsUponResponseArrival(requestId++, "server1", 2);
    waitForStatsUpdate(manager, requestId);

    numInFlightReqList = manager.fetchNumInFlightRequestsForAllServers();
    server2Index = numInFlightReqList.get(0).getLeft().equals("server2") ? 0 : 1;
    server1Index = 1 - server2Index;
    assertEquals(numInFlightReqList.get(server2Index).getLeft(), "server2");
    assertEquals(numInFlightReqList.get(server2Index).getRight().intValue(), 1);
    assertEquals(numInFlightReqList.get(server1Index).getLeft(), "server1");
    assertEquals(numInFlightReqList.get(server1Index).getRight().intValue(), 1);

    numInFlightReq = manager.fetchNumInFlightRequestsForServer("server2");
    assertEquals(numInFlightReq.intValue(), 1);
    numInFlightReq = manager.fetchNumInFlightRequestsForServer("server1");
    assertEquals(numInFlightReq.intValue(), 1);

    latencyList = manager.fetchEMALatencyForAllServers();
    server2Index = latencyList.get(0).getLeft().equals("server2") ? 0 : 1;
    server1Index = 1 - server2Index;
    assertEquals(latencyList.get(server2Index).getLeft(), "server2");
    assertEquals(latencyList.get(server2Index).getRight().doubleValue(), 0.0);
    assertEquals(latencyList.get(server1Index).getLeft(), "server1");
    assertEquals(latencyList.get(server1Index).getRight().doubleValue(), 2.0);

    latency = manager.fetchEMALatencyForServer("server2");
    assertEquals(latency, 0.0);
    latency = manager.fetchEMALatencyForServer("server1");
    assertEquals(latency, 2.0);

    scoreList = manager.fetchHybridScoreForAllServers();
    server2Index = scoreList.get(0).getLeft().equals("server2") ? 0 : 1;
    server1Index = 1 - server2Index;
    assertEquals(scoreList.get(server2Index).getLeft(), "server2");
    assertEquals(scoreList.get(server2Index).getRight().doubleValue(), 0.0);
    assertEquals(scoreList.get(server1Index).getLeft(), "server1");
    assertEquals(scoreList.get(server1Index).getRight().doubleValue(), 54.0);

    score = manager.fetchHybridScoreForServer("server2");
    assertEquals(score, 0.0);
    score = manager.fetchHybridScoreForServer("server1");
    assertEquals(score, 54.0);

    // Record completion stats for server2
    manager.recordStatsUponResponseArrival(requestId++, "server2", 10);
    waitForStatsUpdate(manager, requestId);

    numInFlightReqList = manager.fetchNumInFlightRequestsForAllServers();
    server2Index = numInFlightReqList.get(0).getLeft().equals("server2") ? 0 : 1;
    server1Index = 1 - server2Index;
    assertEquals(numInFlightReqList.get(server2Index).getLeft(), "server2");
    assertEquals(numInFlightReqList.get(server2Index).getRight().intValue(), 0);
    assertEquals(numInFlightReqList.get(server1Index).getLeft(), "server1");
    assertEquals(numInFlightReqList.get(server1Index).getRight().intValue(), 1);

    numInFlightReq = manager.fetchNumInFlightRequestsForServer("server2");
    assertEquals(numInFlightReq.intValue(), 0);
    numInFlightReq = manager.fetchNumInFlightRequestsForServer("server1");
    assertEquals(numInFlightReq.intValue(), 1);

    latencyList = manager.fetchEMALatencyForAllServers();
    server2Index = latencyList.get(0).getLeft().equals("server2") ? 0 : 1;
    server1Index = 1 - server2Index;
    assertEquals(latencyList.get(server2Index).getLeft(), "server2");
    assertEquals(latencyList.get(server2Index).getRight().doubleValue(), 10.0);
    assertEquals(latencyList.get(server1Index).getLeft(), "server1");
    assertEquals(latencyList.get(server1Index).getRight().doubleValue(), 2.0);

    latency = manager.fetchEMALatencyForServer("server2");
    assertEquals(latency, 10.0);
    latency = manager.fetchEMALatencyForServer("server1");
    assertEquals(latency, 2.0);

    scoreList = manager.fetchHybridScoreForAllServers();
    server2Index = scoreList.get(0).getLeft().equals("server2") ? 0 : 1;
    server1Index = 1 - server2Index;
    assertEquals(scoreList.get(server2Index).getLeft(), "server2");
    assertEquals(scoreList.get(server2Index).getRight().doubleValue(), 10.0, manager.getServerRoutingStatsStr());
    assertEquals(scoreList.get(server1Index).getLeft(), "server1");
    assertEquals(scoreList.get(server1Index).getRight().doubleValue(), 54.0, manager.getServerRoutingStatsStr());

    score = manager.fetchHybridScoreForServer("server2");
    assertEquals(score, 10.0);
    score = manager.fetchHybridScoreForServer("server1");
    assertEquals(score, 54.0);
  }

  private void waitForStatsUpdate(ServerRoutingStatsManager serverRoutingStatsManager, long taskCount) {
    TestUtils.waitForCondition(aVoid -> {
      return (serverRoutingStatsManager.getCompletedTaskCount() == taskCount);
    }, 10L, 5000, "Failed to record stats for AdaptiveServerSelectorTest");
  }

  /**
   * Sub-millisecond queries record a latency of zero. A zero baseline would decay latency EMAs to zero and
   * reintroduce the exact defect this change exists to prevent, because the queue term is multiplied by latency.
   */
  @Test
  public void testCollapsingLatencyIsClampedAsAFloor() {
    ServerRoutingStatsManager manager = newManagerWithLatencyFloor(true, 60_000);
    long requestId = 0;
    for (int i = 0; i < 5; i++) {
      manager.recordStatsUponResponseArrival(requestId, "server" + i, 0);
      waitForStatsUpdate(manager, ++requestId);
    }

    manager.recomputeLatencyDecayFloor();
    // The observed latencies collapse towards zero, but the floor must never follow them below the initialization
    // value, otherwise the queue term is annihilated again exactly as it was during the incident.
    double target = manager.getLatencyDecayTarget();
    assertTrue(target >= 1.0, "A collapsing baseline must be clamped, but the floor was " + target);
    manager.shutDown();
  }

  /**
   * @param autoDecayWindowMs doubles as the freshness window for the latency floor, which is deliberately not
   *                          separately configurable. Pass a very large value when the test wants neither decay nor
   *                          ageing out to occur, so that the test is not timing dependent.
   */
  /**
   * The baseline must self-disable for selectors other than HYBRID. LATENCY and its variants rank servers directly
   * on the latency EMA and pick the smallest, so raising an idle server's EMA to the fleet maximum would make it
   * permanently the worst candidate: it could never be selected, so it could never earn the real observation that
   * would bring it back down. That is a genuine absorbing state, unlike HYBRID where the queue term is zero for an
   * idle server and its score is zero regardless of the baseline.
   */
  @Test
  public void testLatencyFloorSelfDisablesForNonHybridSelectors() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION, true);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_TYPE,
        CommonConstants.Broker.AdaptiveServerSelector.Type.LATENCY.name());
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_EWMA_ALPHA, 0.666);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_AUTODECAY_WINDOW_MS, 60_000);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_WARMUP_DURATION_MS, 0);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_AVG_INITIALIZATION_VAL, 1.0);
    // Explicitly opted in, and must still be ignored because the selector is not HYBRID.
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_LATENCY_DECAY_FLOOR_ENABLED, true);

    ServerRoutingStatsManager manager =
        new ServerRoutingStatsManager(new PinotConfiguration(properties), _brokerMetrics);
    manager.init();

    long requestId = 0;
    for (int i = 0; i < 3; i++) {
      manager.recordStatsUponResponseArrival(requestId, "peer" + i, 100);
      waitForStatsUpdate(manager, ++requestId);
    }
    manager.recomputeLatencyDecayFloor();

    assertEquals(manager.getLatencyDecayTarget(), 0.0, 1e-9,
        "Non-HYBRID selectors must retain the legacy decay-to-zero behaviour.");
    manager.shutDown();
  }

  /**
   * Samples observed during the warm-up window must not set the baseline. The EMA swallows them without moving
   * _average, so the entry is still holding avgInitializationVal; counting it as a real observation would let the
   * initialization value masquerade as a measurement and pin the baseline to it.
   */
  @Test
  public void testWarmupSamplesDoNotEstablishTheFloor() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION, true);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_TYPE,
        CommonConstants.Broker.AdaptiveServerSelector.Type.HYBRID.name());
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_EWMA_ALPHA, 0.666);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_AUTODECAY_WINDOW_MS, 60_000);
    // Long enough that every sample this test records falls inside the warm-up window.
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_WARMUP_DURATION_MS, 60_000);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_AVG_INITIALIZATION_VAL, 1.0);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_HYBRID_SCORE_EXPONENT, 3);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_LATENCY_DECAY_FLOOR_ENABLED, true);

    ServerRoutingStatsManager manager =
        new ServerRoutingStatsManager(new PinotConfiguration(properties), _brokerMetrics);
    manager.init();

    long requestId = 0;
    for (int i = 0; i < 3; i++) {
      manager.recordStatsUponResponseArrival(requestId, "peer" + i, 100);
      waitForStatsUpdate(manager, ++requestId);
    }
    manager.recomputeLatencyDecayFloor();

    assertTrue(Double.isNaN(manager.getLatencyDecayTarget()),
        "Warm-up samples must not count as fresh observations, so no baseline should exist yet.");
    manager.shutDown();
  }

  private ServerRoutingStatsManager newManagerWithLatencyFloor(boolean floorEnabled, long autoDecayWindowMs) {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION, true);
    // The floor is only supported for HYBRID, which ranks on a score that keeps an idle server selectable.
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_TYPE,
        CommonConstants.Broker.AdaptiveServerSelector.Type.HYBRID.name());
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_EWMA_ALPHA, 0.666);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_AUTODECAY_WINDOW_MS, autoDecayWindowMs);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_WARMUP_DURATION_MS, 0);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_AVG_INITIALIZATION_VAL, 1.0);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_HYBRID_SCORE_EXPONENT, 3);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_LATENCY_DECAY_FLOOR_ENABLED, floorEnabled);

    ServerRoutingStatsManager manager =
        new ServerRoutingStatsManager(new PinotConfiguration(properties), _brokerMetrics);
    manager.init();
    return manager;
  }

  /**
   * When the floor is disabled the decay target must remain 0.0, preserving the original behaviour exactly.
   */
  @Test
  public void testLatencyDecayTargetDisabledByDefault() {
    ServerRoutingStatsManager manager = newManagerWithLatencyFloor(false, 60_000);
    assertEquals(manager.getLatencyDecayTarget(), 0.0);
    manager.shutDown();
  }

  /**
   * With the floor enabled but no observations yet, the decay target must be NaN so that decay is skipped rather
   * than driving latency EMAs towards an arbitrary value.
   */
  @Test
  public void testLatencyDecayTargetIsNaNBeforeAnyObservation() {
    ServerRoutingStatsManager manager = newManagerWithLatencyFloor(true, 60_000);
    assertTrue(Double.isNaN(manager.getLatencyDecayTarget()));
    manager.shutDown();
  }

  /**
   * The floor must be the slowest of the servers with fresh, real latency observations, so that a server returning
   * from an outage is priced like the slowest peer and ramps back up conservatively.
   */
  @Test
  public void testLatencyDecayFloorIsMaxOfFreshObservations() {
    ServerRoutingStatsManager manager = newManagerWithLatencyFloor(true, 60_000);
    long requestId = 0;
    double[] latencies = {10, 20, 30, 40, 50};
    for (int i = 0; i < latencies.length; i++) {
      manager.recordStatsUponResponseArrival(requestId, "server" + i, (long) latencies[i]);
      waitForStatsUpdate(manager, ++requestId);
    }

    manager.recomputeLatencyDecayFloor();
    // Each EMA is seeded at 1.0, so a single observation of L yields L*0.666 + 1.0*0.334. The slowest server
    // observed 50ms, and must set the baseline even though the median observation is 30ms.
    double expectedMax = 50 * 0.666 + 1.0 * 0.334;
    assertEquals(manager.getLatencyDecayTarget(), expectedMax, 1e-9);
    manager.shutDown();
  }

  /**
   * Regression test for the self-fulfilling fixed point: entries are never removed, so idle entries that have
   * already been decayed to the floor must not be counted when recomputing it. If they were, a stale majority
   * would pin the floor at whatever value they hold, regardless of what the active servers are actually doing.
   */
  @Test
  public void testStaleEntriesDoNotPinTheLatencyFloor() {
    // The decay window doubles as the freshness window, so a short one lets the stale majority age out quickly.
    ServerRoutingStatsManager manager = newManagerWithLatencyFloor(true, 200);
    long requestId = 0;

    // A stale majority reporting an implausibly low latency.
    for (int i = 0; i < 7; i++) {
      manager.recordStatsUponResponseArrival(requestId, "stale" + i, 1);
      waitForStatsUpdate(manager, ++requestId);
    }
    // Let the stale entries age out of the freshness window.
    try {
      Thread.sleep(1500);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // A fresh minority reporting the real latency.
    for (int i = 0; i < 3; i++) {
      manager.recordStatsUponResponseArrival(requestId, "fresh" + i, 100);
      waitForStatsUpdate(manager, ++requestId);
    }

    manager.recomputeLatencyDecayFloor();
    double floorMs = manager.getLatencyDecayTarget();
    // Deliberately a range rather than an exact value. Once the first fresh server establishes a baseline, the
    // remaining entries are seeded from it rather than from avgInitializationVal, so the exact result depends on
    // whether the periodic recompute fired in between. The invariant under test is only that the baseline tracks
    // the fresh minority at 100ms and not the stale majority sitting at ~1ms. The lower bound is the EMA of a
    // single 100ms observation seeded at avgInitializationVal; the upper bound is 100ms, which no EMA can exceed
    // since it is a convex combination of values that are themselves at most 100.
    assertTrue(floorMs >= 100 * 0.666 + 1.0 * 0.334 - 1e-9 && floorMs <= 100.0,
        "The floor must track the fresh minority at 100ms, but was " + floorMs);
    manager.shutDown();
  }

  /**
   * The central behaviour of the change: an idle server's latency EMA must converge towards the baseline instead of
   * towards zero. Without this, the queue term in the hybrid score is annihilated and the server becomes an
   * unbounded traffic magnet the moment it returns to routing.
   */
  @Test
  public void testIdleLatencyDecaysTowardsFloorInsteadOfZero()
      throws Exception {
    ServerRoutingStatsManager manager = newManagerWithLatencyFloor(true, 1000);
    long requestId = 0;

    // Three peers observing 100ms establish the baseline.
    for (int i = 0; i < 3; i++) {
      manager.recordStatsUponResponseArrival(requestId, "peer" + i, 100);
      waitForStatsUpdate(manager, ++requestId);
    }
    manager.recomputeLatencyDecayFloor();
    double floor = manager.getLatencyDecayTarget();
    assertTrue(floor > 1.0, "Expected a real baseline, got " + floor);

    // A server that observed a very fast query and then went idle. Its EMA must rise towards the baseline rather
    // than collapsing towards zero.
    manager.recordStatsUponResponseArrival(requestId, "idle", 1);
    waitForStatsUpdate(manager, ++requestId);
    double before = manager.fetchEMALatencyForServer("idle");
    assertTrue(before < floor, "Precondition: the idle server must start below the baseline.");

    TestUtils.waitForCondition(aVoid -> manager.fetchEMALatencyForServer("idle") > before, 50L, 5000L,
        "Latency EMA of the idle server never rose towards the baseline.");

    // It must converge to the baseline, and in particular must never approach zero.
    double after = manager.fetchEMALatencyForServer("idle");
    assertTrue(after > before, "Expected the idle EMA to rise, but it went from " + before + " to " + after);
    assertTrue(after <= floor + 1e-9, "The idle EMA must not overshoot the baseline, got " + after);
    manager.shutDown();
  }

  /**
   * A brand new entry must be seeded from the baseline rather than from avgInitializationVal, which is 1ms and
   * would make every server this broker has not queried yet an immediate traffic magnet.
   */
  @Test
  public void testNewEntryIsSeededFromTheFloor() {
    ServerRoutingStatsManager manager = newManagerWithLatencyFloor(true, 60_000);
    long requestId = 0;
    for (int i = 0; i < 3; i++) {
      manager.recordStatsUponResponseArrival(requestId, "peer" + i, 100);
      waitForStatsUpdate(manager, ++requestId);
    }
    manager.recomputeLatencyDecayFloor();
    double floor = manager.getLatencyDecayTarget();

    // First contact with a previously unseen server.
    manager.recordStatsForQuerySubmission(requestId, "newServer");
    waitForStatsUpdate(manager, ++requestId);

    assertEquals(manager.fetchEMALatencyForServer("newServer"), floor, 1e-9,
        "A new entry must be seeded from the baseline, not from avgInitializationVal.");
    manager.shutDown();
  }

  /**
   * Once a baseline exists it must survive a window in which no server reports a fresh observation, rather than
   * being reset. Resetting would leave idle servers decaying towards nothing.
   */
  @Test
  public void testEstablishedFloorIsRetainedWhenSamplesAgeOut()
      throws Exception {
    ServerRoutingStatsManager manager = newManagerWithLatencyFloor(true, 1000);
    long requestId = 0;
    for (int i = 0; i < 3; i++) {
      manager.recordStatsUponResponseArrival(requestId, "server" + i, 100);
      waitForStatsUpdate(manager, ++requestId);
    }
    manager.recomputeLatencyDecayFloor();
    double established = manager.getLatencyDecayTarget();
    assertTrue(established > 1.0, "Expected a real baseline, got " + established);

    // Let every sample age out of the freshness window, then recompute with no fresh samples at all.
    Thread.sleep(1500);
    manager.recomputeLatencyDecayFloor();

    assertEquals(manager.getLatencyDecayTarget(), established, 1e-9,
        "An established baseline must be retained when no fresh samples remain.");
    manager.shutDown();
  }

  /**
   * A single fresh observation is enough to establish the baseline. Because the baseline is a maximum, requiring
   * more reporters could only raise it, so a larger minimum buys no outlier protection while risking a permanently
   * frozen baseline on a broker with small fan-out or sparse traffic.
   */
  @Test
  public void testSingleFreshSampleEstablishesTheFloor() {
    ServerRoutingStatsManager manager = newManagerWithLatencyFloor(true, 60_000);
    long requestId = 0;
    manager.recordStatsUponResponseArrival(requestId, "server0", 50);
    waitForStatsUpdate(manager, ++requestId);

    manager.recomputeLatencyDecayFloor();
    // avgInitializationVal is 1.0, so the first observation of 50ms moves the EMA to 0.666 * 50 + 0.334 * 1.
    assertEquals(manager.getLatencyDecayTarget(), 50 * 0.666 + 1.0 * 0.334, 1e-9);
    manager.shutDown();
  }

  /**
   * The dangerous form of the negative in-flight counter, and the reason the clamp is required by the decay floor
   * rather than being an unrelated fix.
   *
   * <p>An idle server's in-flight EMA decays towards zero while the raw counter does not decay at all, so a single
   * out-of-order response leaves the sum at roughly -1. Raised to an odd exponent, that yields a negative score,
   * which sorts ahead of every healthy server. This is a pre-existing bug that is already live: latency decays
   * towards zero geometrically but stays strictly positive, so the product is already a real negative number.
   * Decaying latency to a baseline only increases its magnitude. Hence the clamp is not gated on the feature flag.
   */
  @Test
  public void testDecayedIdleServerWithNegativeCounterDoesNotBecomeAMagnet()
      throws Exception {
    ServerRoutingStatsManager manager = newManagerWithLatencyFloor(true, 1000);
    long requestId = 0;

    // Establish a baseline and initialize the in-flight EMAs so that they are eligible to decay.
    for (int i = 0; i < 3; i++) {
      manager.recordStatsForQuerySubmission(requestId, "server" + i);
      waitForStatsUpdate(manager, ++requestId);
      manager.recordStatsUponResponseArrival(requestId, "server" + i, 100);
      waitForStatsUpdate(manager, ++requestId);
    }
    manager.recomputeLatencyDecayFloor();
    assertTrue(manager.getLatencyDecayTarget() > 1.0, "Expected a real baseline to have been established.");

    // Let the servers go idle so their in-flight EMAs decay towards zero. Must exceed autoDecayWindowMs, which is
    // the precondition for decay to fire at all.
    Thread.sleep(2000);

    // An out-of-order response now drives the raw counter negative while the EMA is already near zero.
    manager.recordStatsUponResponseArrival(requestId, "server0", 100);
    waitForStatsUpdate(manager, ++requestId);

    assertEquals(manager.fetchNumInFlightRequestsForServer("server0").intValue(), -1);
    double score = manager.fetchHybridScoreForServer("server0");
    assertTrue(score >= 0.0, "An idle server with a negative counter must not score below zero, but scored " + score);
    manager.shutDown();
  }

  /**
   * A response processed before its corresponding submission drives the counter negative. That transient must be
   * left free to compensate, so the counter does not leak a permanent phantom in-flight request that would starve a
   * healthy server. This deliberately asserts nothing about the score: with the in-flight EMA still at its
   * initialization value the sum is exactly zero, so a score assertion here would pass with or without the clamp
   * and prove nothing. The clamp is covered by testDecayedIdleServerWithNegativeCounterDoesNotBecomeAMagnet, where
   * the EMA has decayed and the sum is genuinely negative.
   */
  @Test
  public void testNegativeInFlightCounterIsAllowedToCompensate() {
    ServerRoutingStatsManager manager = newManagerWithLatencyFloor(true, 60_000);
    long requestId = 0;

    // Response arrives first, driving the counter to -1.
    manager.recordStatsUponResponseArrival(requestId, "server1", 100);
    waitForStatsUpdate(manager, ++requestId);
    assertEquals(manager.fetchNumInFlightRequestsForServer("server1").intValue(), -1);

    // The late submission compensates, restoring the counter to zero rather than leaking a phantom request.
    manager.recordStatsForQuerySubmission(requestId, "server1");
    waitForStatsUpdate(manager, ++requestId);
    assertEquals(manager.fetchNumInFlightRequestsForServer("server1").intValue(), 0);
    manager.shutDown();
  }
}
