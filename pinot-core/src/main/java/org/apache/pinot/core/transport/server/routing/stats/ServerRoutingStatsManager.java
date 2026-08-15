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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker.AdaptiveServerSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 *  {@code ServerRoutingStatsManager} manages the query routing stats for each server and used by the Adaptive
 *  Server Selection feature (when enabled). The stats are maintained at the broker and are updated when a query is
 *  submitted to a server and when a server responds after processing a query.
 */
public class ServerRoutingStatsManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerRoutingStatsManager.class);

  private final PinotConfiguration _config;
  private final BrokerMetrics _brokerMetrics;
  private final AdaptiveServerSelector.Type _adaptiveSelectorType;
  private volatile boolean _isEnabled;
  private ConcurrentHashMap<String, ServerRoutingStatsEntry> _serverQueryStatsMap;

  // Main executor service for collecting and aggregating stats for all servers.
  private ExecutorService _executorService;

  // ScheduledExecutorServer for processing periodic tasks like decay.
  private ScheduledExecutorService _periodicTaskExecutor;

  private double _alpha;
  private long _autoDecayWindowMs;
  private long _warmupDurationMs;
  private double _avgInitializationVal;
  private int _hybridScoreExponent;

  // Absolute lower bound for the baseline, in milliseconds. Deliberately independent of avgInitializationVal, which
  // is permitted to be zero: a zero baseline would decay latency EMAs to zero and reintroduce the very defect this
  // guards against, because the hybrid score multiplies the queue term by latency.
  private static final double MIN_LATENCY_DECAY_FLOOR_MS = 1.0;

  private volatile boolean _latencyDecayFloorEnabled;
  private volatile double _latencyDecayFloorMs = Double.NaN;

  public ServerRoutingStatsManager(PinotConfiguration pinotConfig, BrokerMetrics brokerMetrics) {
    _config = pinotConfig;
    _brokerMetrics = brokerMetrics;
    String typeString =
        _config.getProperty(AdaptiveServerSelector.CONFIG_OF_TYPE, AdaptiveServerSelector.DEFAULT_TYPE);
    AdaptiveServerSelector.Type adaptiveSelectorType;
    try {
      adaptiveSelectorType = AdaptiveServerSelector.Type.valueOf(typeString.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException | NullPointerException e) {
      throw new IllegalArgumentException("Illegal adaptive server selector type: " + typeString, e);
    }
    _adaptiveSelectorType = adaptiveSelectorType;
  }

  public void init() {
    _isEnabled = _config.getProperty(AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION,
        AdaptiveServerSelector.DEFAULT_ENABLE_STATS_COLLECTION);
    if (!_isEnabled) {
      LOGGER.info("Server stats collection for Adaptive Server Selection is not enabled.");
      return;
    }

    LOGGER.info("Initializing ServerRoutingStatsManager for Adaptive Server Selection.");

    _alpha = _config.getProperty(AdaptiveServerSelector.CONFIG_OF_EWMA_ALPHA,
        AdaptiveServerSelector.DEFAULT_EWMA_ALPHA);
    _autoDecayWindowMs = _config.getProperty(AdaptiveServerSelector.CONFIG_OF_AUTODECAY_WINDOW_MS,
        AdaptiveServerSelector.DEFAULT_AUTODECAY_WINDOW_MS);
    _warmupDurationMs = _config.getProperty(AdaptiveServerSelector.CONFIG_OF_WARMUP_DURATION_MS,
        AdaptiveServerSelector.DEFAULT_WARMUP_DURATION_MS);
    _avgInitializationVal = _config.getProperty(AdaptiveServerSelector.CONFIG_OF_AVG_INITIALIZATION_VAL,
        AdaptiveServerSelector.DEFAULT_AVG_INITIALIZATION_VAL);
    _hybridScoreExponent = _config.getProperty(AdaptiveServerSelector.CONFIG_OF_HYBRID_SCORE_EXPONENT,
        AdaptiveServerSelector.DEFAULT_HYBRID_SCORE_EXPONENT);

    int threadPoolSize = _config.getProperty(AdaptiveServerSelector.CONFIG_OF_STATS_MANAGER_THREADPOOL_SIZE,
        AdaptiveServerSelector.DEFAULT_STATS_MANAGER_THREADPOOL_SIZE);
    _executorService = Executors.newFixedThreadPool(threadPoolSize);

    _periodicTaskExecutor = Executors.newSingleThreadScheduledExecutor();

    _latencyDecayFloorEnabled = _config.getProperty(AdaptiveServerSelector.CONFIG_OF_LATENCY_DECAY_FLOOR_ENABLED,
        AdaptiveServerSelector.DEFAULT_LATENCY_DECAY_FLOOR_ENABLED);
    // A previous incarnation's baseline must not leak into a re-initialized manager, whose stats map is empty.
    _latencyDecayFloorMs = Double.NaN;

    // Entries in this map are never deleted unless the broker process restarts. This is okay for now because the
    // number of servers will be finite and should not cause memory bloat.
    _serverQueryStatsMap = new ConcurrentHashMap<>();

    if (_latencyDecayFloorEnabled && _autoDecayWindowMs <= 0) {
      // The decay window doubles as the freshness window, so decay must be enabled for the floor to mean anything.
      // Rather than failing broker startup, fall back to the original decay-to-zero behaviour and make it loud.
      LOGGER.error("Disabling the latency decay floor because autoDecayWindowMs={} is not positive.",
          _autoDecayWindowMs);
      _latencyDecayFloorEnabled = false;
    }

    if (_latencyDecayFloorEnabled && !isHybridSelector()) {
      // The baseline raises the latency EMA of an idle server towards the slowest observed peer. Under HYBRID that
      // is safe, because the score multiplies latency by a queue term that decays to zero, so an idle server still
      // scores zero and is always selectable. LATENCY and NUM_INFLIGHT_REQ rank on a single statistic, so under
      // LATENCY a raised baseline would make an idle server the least attractive candidate, and it could never earn
      // the real observation needed to lower it again. That is an absorbing state, so the floor is not applied.
      // NO_OP is the default and is an explicitly supported "collect stats, decide later" configuration, so it is
      // not an operator error and must not be logged as one.
      LOGGER.warn("Disabling the latency decay floor because it is only supported for the HYBRID selector. type={}",
          _config.getProperty(AdaptiveServerSelector.CONFIG_OF_TYPE, AdaptiveServerSelector.DEFAULT_TYPE));
      _latencyDecayFloorEnabled = false;
    }

    if (_latencyDecayFloorEnabled) {
      LOGGER.info("Latency decay floor enabled. freshnessWindowMs(=autoDecayWindowMs)={}, minFloorMs={}",
          _autoDecayWindowMs, MIN_LATENCY_DECAY_FLOOR_MS);
      _periodicTaskExecutor.scheduleAtFixedRate(() -> {
        // An uncaught exception would silently cancel all future executions of this task, freezing the floor.
        try {
          recomputeLatencyDecayFloor();
        } catch (Throwable t) {
          LOGGER.error("Caught exception while recomputing the latency decay floor.", t);
        }
      }, 0, _autoDecayWindowMs, TimeUnit.MILLISECONDS);
    }
  }

  public boolean isEnabled() {
    return _isEnabled;
  }

  public void shutDown() {
    // As the stats are not persistent, shutdown need not wait for task termination.
    if (!_isEnabled) {
      return;
    }

    LOGGER.info("Shutting down ServerRoutingStatsManager.");
    _isEnabled = false;
    _executorService.shutdownNow();
    if (_periodicTaskExecutor != null) {
      _periodicTaskExecutor.shutdownNow();
    }
  }

  /**
   * Recomputes the fleet-wide latency baseline that idle servers' latency EMAs decay towards.
   *
   * <p>The baseline is the <b>slowest</b> current latency EMA among servers that have received a real response
   * recently, rather than a typical one. A server returning from an outage has no recent evidence about its own
   * performance and may legitimately be slower than its peers for a while, for example with cold JVM and JIT state,
   * or a cold page cache after a host restart. Pricing it at the slowest observed peer makes it ramp back up
   * conservatively instead of at peer parity.
   *
   * <p>Only entries whose latency EMA was last moved by an actual server response, rather than by decay, are
   * counted. This exclusion is what keeps the baseline from becoming an input to itself. Decay moves an EMA
   * <i>towards</i> the baseline, so a decayed entry always sits at or above it: an entry that has fully decayed sits
   * exactly at the baseline, and one still decaying downwards from a high value sits above it. Since the baseline is
   * a maximum, admitting any such entry would make it impossible for the baseline to ever decrease, so it would
   * ratchet monotonically upwards and latch at the worst value ever observed.
   *
   * <p>For example, one server times out. Timeouts are recorded as a latency observation equal to the query timeout,
   * say 10000ms, so the baseline becomes 10000ms and every idle server begins decaying towards it. On the next
   * recompute, were decayed entries counted, one of them would report 10000ms, so the maximum would again be 10000ms.
   * The baseline would stay there permanently, sustained purely by entries that were decayed to it, long after the
   * offending server recovered to 20ms and the whole fleet was fast.
   *
   * <p>The companion condition, that the entry has ever received a real response at all, excludes servers still
   * holding the initialization value. Those cannot raise a maximum, but they would otherwise make an entry that has
   * never been queried look like evidence that a baseline can be computed.
   *
   * <p>Caveat: timeouts and server exceptions are recorded as ordinary latency observations equal to the query
   * timeout, so a single timing-out server can set the baseline for every idle server on this broker. That is
   * deliberately accepted as the conservative direction, and its impact is bounded: it slows how fast an idle
   * server ramps back up, but can never stop a fully idle server being selected: its in-flight EMA decays to zero
   * and the score is pow(queueTerm, N) * latency, so its score is zero regardless of how large this baseline is.
   * Note the "fully idle" qualifier is load-bearing. Once such a server has taken its first query its queue term is
   * about 1, so its score is about the baseline, and an inflated baseline can rank it behind a healthy peer that is
   * already carrying several in-flight requests. That resolves on its next real response, and erring in this
   * direction is the intended conservative bias, but it does mean an unrelated server's errors are imported.
   */
  @VisibleForTesting
  void recomputeLatencyDecayFloor() {
    long nowMs = System.currentTimeMillis();
    double maxLatencyMs = 0.0;
    int numFreshSamples = 0;

    for (ServerRoutingStatsEntry entry : _serverQueryStatsMap.values()) {
      long lastRealUpdateMs = entry.getLastRealLatencyUpdateMs();
      // _autoDecayWindowMs doubles as the freshness window and is deliberately not separately configurable. Decay
      // only fires once an EMA has gone untouched for that long, so an entry whose last real update is more recent
      // provably has not been decayed since, which is what guarantees every sample counted here is an undecayed
      // observation. A larger window would admit already-decayed entries and reintroduce the self-fulfilling fixed
      // point; a smaller one would only discard valid samples.
      if (lastRealUpdateMs > 0 && (nowMs - lastRealUpdateMs) <= _autoDecayWindowMs) {
        // getLatencyEMA() is a volatile read; the server lock is deliberately not taken here to keep this scan off
        // the query path's lock.
        double latencyMs = entry.getLatencyEMA();
        if (!Double.isNaN(latencyMs)) {
          numFreshSamples++;
          maxLatencyMs = Math.max(maxLatencyMs, latencyMs);
        }
      }
    }

    if (numFreshSamples == 0) {
      // Nothing observed the fleet recently, so there is no evidence to recompute from. Retain the previous
      // baseline rather than falling through to the lower bound, which would drop the baseline to a near-zero
      // latency and recreate the magnet this exists to prevent. If no baseline has ever been computed,
      // _latencyDecayFloorMs stays NaN and latency decay is skipped entirely.
      //
      // Deliberately no larger minimum than one. Because the baseline is a maximum, requiring more servers to
      // report can only ever raise it, so a larger minimum buys no protection against an outlier; the freshness
      // filter above already guarantees every counted value is a genuine, undecayed observation. It would, however,
      // freeze the baseline on any broker whose fan-out is small or whose traffic is sparse enough that fewer than
      // that many servers respond within a window.
      return;
    }

    // A near-zero baseline would decay latency EMAs to near zero and reintroduce the very defect this guards
    // against, because the queue term is multiplied by latency and so a vanishing latency annihilates all
    // backpressure. This is reachable: latency is recorded in whole milliseconds, so a fleet of consistently
    // sub-millisecond servers records zeros and drives the EMA asymptotically towards zero.
    _latencyDecayFloorMs = Math.max(maxLatencyMs, MIN_LATENCY_DECAY_FLOOR_MS);
  }

  /**
   * Returns the value that latency EMAs decay towards. This is a production code path, wired as the decay target
   * supplier for every entry's latency EMA; it is also read directly by tests.
   *
   * <p>Returns 0.0 when the floor is disabled, which preserves the original decay-to-zero behaviour, and NaN when
   * enabled but no valid baseline is available yet, which causes decay to be skipped.
   */
  double getLatencyDecayTarget() {
    if (!_latencyDecayFloorEnabled) {
      return 0.0;
    }
    return _latencyDecayFloorMs;
  }

  /**
   * Returns the value a newly created entry's latency EMA is seeded with: the current fleet-wide baseline when one
   * is available, otherwise the configured initialization value.
   */
  private double getLatencyInitializationVal() {
    if (!_latencyDecayFloorEnabled) {
      // Exactly the legacy behaviour.
      return _avgInitializationVal;
    }
    double floorMs = _latencyDecayFloorMs;
    if (Double.isFinite(floorMs) && floorMs > 0.0) {
      return floorMs;
    }
    // No baseline yet. avgInitializationVal is documented as the seed for the in-flight *count* EMA and is permitted
    // to be zero, which would seed a new entry at 0ms latency and make it an immediate traffic magnet, so it gets
    // the same lower bound the baseline itself does.
    return Math.max(_avgInitializationVal, MIN_LATENCY_DECAY_FLOOR_MS);
  }

  private boolean isHybridSelector() {
    return _adaptiveSelectorType == AdaptiveServerSelector.Type.HYBRID;
  }

  public int getQueueSize() {
    if (!_isEnabled) {
      return 0;
    }

    ThreadPoolExecutor tpe = (ThreadPoolExecutor) _executorService;
    return tpe.getQueue().size();
  }

  public long getCompletedTaskCount() {
    if (!_isEnabled) {
      return 0;
    }

    ThreadPoolExecutor tpe = (ThreadPoolExecutor) _executorService;
    return tpe.getCompletedTaskCount();
  }

  /**
   * Called just before submitting a query to a server. Updates stats corresponding to query submission.
   */
  public void recordStatsForQuerySubmission(long requestId, String serverInstanceId) {
    if (!_isEnabled) {
      return;
    }

    _executorService.execute(() -> {
      try {
        recordQueueSizeMetrics();
        updateStatsAfterQuerySubmission(serverInstanceId);
      } catch (Exception e) {
        LOGGER.error("Exception caught while updating stats. requestId={}, exception={}", requestId, e);
      }
    });
  }

  private void updateStatsAfterQuerySubmission(String serverInstanceId) {
    ServerRoutingStatsEntry stats = _serverQueryStatsMap.computeIfAbsent(serverInstanceId,
        k -> new ServerRoutingStatsEntry(serverInstanceId, _alpha, _autoDecayWindowMs, _warmupDurationMs,
            _avgInitializationVal, getLatencyInitializationVal(), _hybridScoreExponent, _periodicTaskExecutor,
            this::getLatencyDecayTarget));

    try {
      stats.getServerWriteLock().lock();
      stats.updateNumInFlightRequestsForQuerySubmission();
    } finally {
      stats.getServerWriteLock().unlock();
    }
  }

  /**
   * Called when a query response is received from the server. Updates stats related to query completion.
   */
  public void recordStatsUponResponseArrival(long requestId, String serverInstanceId, long latency) {
    if (!_isEnabled) {
      return;
    }

    _executorService.execute(() -> {
      try {
        updateStatsUponResponseArrival(serverInstanceId, latency);
      } catch (Exception e) {
        LOGGER.error("Exception caught while updating stats. requestId={}, exception={}", requestId, e);
      }
    });
  }

  private void updateStatsUponResponseArrival(String serverInstanceId, long latencyMs) {
    ServerRoutingStatsEntry stats = _serverQueryStatsMap.computeIfAbsent(serverInstanceId,
        k -> new ServerRoutingStatsEntry(serverInstanceId, _alpha, _autoDecayWindowMs, _warmupDurationMs,
            _avgInitializationVal, getLatencyInitializationVal(), _hybridScoreExponent, _periodicTaskExecutor,
            this::getLatencyDecayTarget));

    try {
      stats.getServerWriteLock().lock();
      stats.updateNumInFlightRequestsForResponseArrival();
      if (latencyMs >= 0.0) {
        stats.updateLatency(latencyMs);
      }
    } finally {
      stats.getServerWriteLock().unlock();
    }
  }

  public Map<String, ServerRoutingStatsEntry> getServerRoutingStats() {
    return _serverQueryStatsMap;
  }

  /**
   * Returns ServerRoutingStatsStr for debugging/logging.
   */
  public String getServerRoutingStatsStr() {
    if (!_isEnabled) {
      return "";
    }

    StringBuilder stringBuilder =
        new StringBuilder("(Server=NumInFlightRequests,NumInFlightRequestsEMA,LatencyEMA," + "Score)");

    for (Map.Entry<String, ServerRoutingStatsEntry> entry : _serverQueryStatsMap.entrySet()) {
      String server = entry.getKey();
      Preconditions.checkState(entry.getValue() != null, "Server stats is null");
      ServerRoutingStatsEntry stats = entry.getValue();

      stats.getServerReadLock().lock();
      Integer numInFlightRequests = stats.getNumInFlightRequests();
      Double numInFlightRequestsEMA = stats.getInFlightRequestsEMA();
      Double latencyEMA = stats.getLatencyEMA();
      Double score = stats.computeHybridScore();
      stats.getServerReadLock().unlock();

      stringBuilder.append(";").append(server).append("=").append(numInFlightRequests.toString()).append(",")
          .append(numInFlightRequestsEMA.toString()).append(",").append(latencyEMA.toString()).append(",")
          .append(score.toString());
    }

    return stringBuilder.toString();
  }

  /*
   * ===================================================================================================================
   * The helper functions required by various AdaptiveSelectors are defined below.
   *
   * 1. NumInFlightReqSelector - fetchNumInFlightRequestsForAllServers(), fetchNumInFlightRequestsForServer()
   * 2. LatencySelector - fetchEMALatencyForAllServers(), fetchEMALatencyForServer()
   * 3. HybridSelector - fetchScoreForAllServers(), fetchScoreForServer()
   *
   * We avoid returning all the stats to each selector to keep the critical section (under locks) as small as
   * possible). ServerRoutingStatsManager does not sort the servers in any particular order while accumulating stats
   * as it is not aware of what sorting strategy to use. This logic is contained in the various
   * AdaptiveServerSelectors.
   *
   * TODO: Explore if reads to the _serverQueryStatsMap can be done without locking.
   * ===================================================================================================================
   */

  /**
   * Returns a list containing each server and the corresponding number of in-flight requests active on the server.
   */
  public List<Pair<String, Integer>> fetchNumInFlightRequestsForAllServers() {
    List<Pair<String, Integer>> response = new ArrayList<>();
    if (!_isEnabled) {
      return response;
    }

    for (Map.Entry<String, ServerRoutingStatsEntry> entry : _serverQueryStatsMap.entrySet()) {
      String server = entry.getKey();
      Preconditions.checkState(entry.getValue() != null, "Server stats is null");
      ServerRoutingStatsEntry stats = entry.getValue();

      stats.getServerReadLock().lock();
      int numInFlightRequests = stats.getNumInFlightRequests();
      stats.getServerReadLock().unlock();

      response.add(new ImmutablePair<>(server, numInFlightRequests));
    }

    return response;
  }

  /**
   * Same as above but returns the number of inflight requests for the input server.
   */
  public Integer fetchNumInFlightRequestsForServer(String server) {
    if (!_isEnabled) {
      return null;
    }

    ServerRoutingStatsEntry stats = _serverQueryStatsMap.get(server);
    if (stats == null) {
      return null;
    }

    try {
      stats.getServerReadLock().lock();
      return stats.getNumInFlightRequests();
    } finally {
      stats.getServerReadLock().unlock();
    }
  }

  /**
   * Returns a list containing each server and the corresponding EMA latency seen for queries on the server.
   */
  public List<Pair<String, Double>> fetchEMALatencyForAllServers() {
    List<Pair<String, Double>> response = new ArrayList<>();
    if (!_isEnabled) {
      return response;
    }

    for (Map.Entry<String, ServerRoutingStatsEntry> entry : _serverQueryStatsMap.entrySet()) {
      String server = entry.getKey();
      Preconditions.checkState(entry.getValue() != null, "Server stats is null");
      ServerRoutingStatsEntry stats = entry.getValue();

      stats.getServerReadLock().lock();
      double latency = stats.getLatencyEMA();
      stats.getServerReadLock().unlock();

      response.add(new ImmutablePair<>(server, latency));
    }

    return response;
  }

  /**
   * Same as above but returns the EMA latency for the input server.
   */
  public Double fetchEMALatencyForServer(String server) {
    if (!_isEnabled) {
      return null;
    }

    ServerRoutingStatsEntry stats = _serverQueryStatsMap.get(server);
    if (stats == null) {
      return null;
    }

    try {
      stats.getServerReadLock().lock();
      return stats.getLatencyEMA();
    } finally {
      stats.getServerReadLock().unlock();
    }
  }

  /**
   * Returns a list containing each server and the corresponding Hybrid score for each server. The Hybrid score is
   * calculated based on https://www.usenix.org/system/files/conference/nsdi15/nsdi15-paper-suresh.pdf.
   */
  public List<Pair<String, Double>> fetchHybridScoreForAllServers() {
    List<Pair<String, Double>> response = new ArrayList<>();
    if (!_isEnabled) {
      return response;
    }

    for (Map.Entry<String, ServerRoutingStatsEntry> entry : _serverQueryStatsMap.entrySet()) {
      String server = entry.getKey();
      Preconditions.checkState(entry.getValue() != null, "Server stats is null");
      ServerRoutingStatsEntry stats = entry.getValue();

      stats.getServerReadLock().lock();
      double score = stats.computeHybridScore();
      stats.getServerReadLock().unlock();

      response.add(new ImmutablePair<>(server, score));
    }

    return response;
  }

  /**
   * Same as above but returns the score for a single server.
   */
  public Double fetchHybridScoreForServer(String server) {
    if (!_isEnabled) {
      return null;
    }

    ServerRoutingStatsEntry stats = _serverQueryStatsMap.get(server);
    if (stats == null) {
      return null;
    }

    try {
      stats.getServerReadLock().lock();
      return stats.computeHybridScore();
    } finally {
      stats.getServerReadLock().unlock();
    }
  }

  /**
   * Returns the score used by the configured adaptive selector for one server.
   */
  public Double fetchConfiguredScoreForServer(String server) {
    switch (_adaptiveSelectorType) {
      case NUM_INFLIGHT_REQ:
        Integer numInFlightRequests = fetchNumInFlightRequestsForServer(server);
        return numInFlightRequests != null ? numInFlightRequests.doubleValue() : null;
      case LATENCY:
        return fetchEMALatencyForServer(server);
      case HYBRID:
        return fetchHybridScoreForServer(server);
      case NO_OP:
      default:
        return null;
    }
  }

  private void recordQueueSizeMetrics() {
    int queueSize = getQueueSize();
    _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.ROUTING_STATS_MANAGER_QUEUE_SIZE, queueSize);
  }
}
