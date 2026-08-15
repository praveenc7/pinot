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
package org.apache.pinot.core.transport;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.config.NettyConfig;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerTimer;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.transport.AsyncQueryResponse.ResponseOutcome;
import org.apache.pinot.core.transport.HedgeBudgetManager.AdmissionResult;
import org.apache.pinot.core.transport.QueryRequestPlan.RequestGroup;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code QueryRouter} class provides methods to route the query based on the routing table, and returns a
 * {@link AsyncQueryResponse} so that caller can handle the query response asynchronously.
 * <p>It works on {@link ServerChannels} which maintains only a single connection between the broker and each server.
 */
@ThreadSafe
public class QueryRouter {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryRouter.class);

  // Backoff for channel lock timeout retries: 100ms initial delay, 2x scale factor (with jitter).
  private static final long SEND_REQUEST_INITIAL_DELAY_MS = 100L;
  private static final double SEND_REQUEST_DELAY_SCALE_FACTOR = 2.0;
  private static final Consumer<ServerRoutingInstance> NO_OP_SERVER_FAILURE_CONSUMER = ignored -> { };

  private final String _brokerId;
  private final ServerChannels _serverChannels;
  private final ServerChannels _serverChannelsTls;
  private final ServerRoutingStatsManager _serverRoutingStatsManager;
  private final int _sendRequestMaxAttempts;
  private final HedgingConfig _hedgingConfig;
  private final HedgeBudgetManager _hedgeBudgetManager;
  private final ScheduledExecutorService _hedgeScheduler;
  private final Executor _hedgeRequestExecutor;
  private final LongSupplier _currentTimeMillis;
  private final Consumer<ServerRoutingInstance> _serverFailureConsumer;

  private final BrokerMetrics _brokerMetrics = BrokerMetrics.get();
  private final ConcurrentHashMap<Long, AsyncQueryResponse> _asyncQueryResponseMap = new ConcurrentHashMap<>();

  /**
   * Creates a query router with TLS config.
   *
   * @param brokerId broker id
   * @param nettyConfig configurations for netty library
   * @param tlsConfig TLS config
   */
  public QueryRouter(String brokerId, @Nullable NettyConfig nettyConfig, @Nullable TlsConfig tlsConfig,
      ServerRoutingStatsManager serverRoutingStatsManager, ThreadAccountant threadAccountant,
      int sendRequestMaxAttempts) {
    this(brokerId, nettyConfig, tlsConfig, serverRoutingStatsManager, threadAccountant, sendRequestMaxAttempts,
        HedgingConfig.disabled(), NO_OP_SERVER_FAILURE_CONSUMER);
  }

  public QueryRouter(String brokerId, @Nullable NettyConfig nettyConfig, @Nullable TlsConfig tlsConfig,
      ServerRoutingStatsManager serverRoutingStatsManager, ThreadAccountant threadAccountant,
      int sendRequestMaxAttempts, HedgingConfig hedgingConfig) {
    this(brokerId, nettyConfig, tlsConfig, serverRoutingStatsManager, threadAccountant, sendRequestMaxAttempts,
        hedgingConfig, NO_OP_SERVER_FAILURE_CONSUMER);
  }

  public QueryRouter(String brokerId, @Nullable NettyConfig nettyConfig, @Nullable TlsConfig tlsConfig,
      ServerRoutingStatsManager serverRoutingStatsManager, ThreadAccountant threadAccountant,
      int sendRequestMaxAttempts, HedgingConfig hedgingConfig,
      Consumer<ServerRoutingInstance> serverFailureConsumer) {
    this(brokerId, nettyConfig, tlsConfig, serverRoutingStatsManager, threadAccountant, sendRequestMaxAttempts,
        hedgingConfig, new HedgeBudgetManager(hedgingConfig), createHedgeScheduler(hedgingConfig),
        createHedgeRequestExecutor(hedgingConfig), System::currentTimeMillis, serverFailureConsumer);
  }

  @VisibleForTesting
  QueryRouter(String brokerId, @Nullable NettyConfig nettyConfig, @Nullable TlsConfig tlsConfig,
      ServerRoutingStatsManager serverRoutingStatsManager, ThreadAccountant threadAccountant,
      int sendRequestMaxAttempts, HedgingConfig hedgingConfig, HedgeBudgetManager hedgeBudgetManager,
      @Nullable ScheduledExecutorService hedgeScheduler, LongSupplier currentTimeMillis) {
    this(brokerId, nettyConfig, tlsConfig, serverRoutingStatsManager, threadAccountant, sendRequestMaxAttempts,
        hedgingConfig, hedgeBudgetManager, hedgeScheduler, Runnable::run, currentTimeMillis,
        NO_OP_SERVER_FAILURE_CONSUMER);
  }

  @VisibleForTesting
  QueryRouter(String brokerId, @Nullable NettyConfig nettyConfig, @Nullable TlsConfig tlsConfig,
      ServerRoutingStatsManager serverRoutingStatsManager, ThreadAccountant threadAccountant,
      int sendRequestMaxAttempts, HedgingConfig hedgingConfig, HedgeBudgetManager hedgeBudgetManager,
      @Nullable ScheduledExecutorService hedgeScheduler, LongSupplier currentTimeMillis,
      Consumer<ServerRoutingInstance> serverFailureConsumer) {
    this(brokerId, nettyConfig, tlsConfig, serverRoutingStatsManager, threadAccountant, sendRequestMaxAttempts,
        hedgingConfig, hedgeBudgetManager, hedgeScheduler, Runnable::run, currentTimeMillis, serverFailureConsumer);
  }

  @VisibleForTesting
  QueryRouter(String brokerId, @Nullable NettyConfig nettyConfig, @Nullable TlsConfig tlsConfig,
      ServerRoutingStatsManager serverRoutingStatsManager, ThreadAccountant threadAccountant,
      int sendRequestMaxAttempts, HedgingConfig hedgingConfig, HedgeBudgetManager hedgeBudgetManager,
      @Nullable ScheduledExecutorService hedgeScheduler, Executor hedgeRequestExecutor, LongSupplier currentTimeMillis,
      Consumer<ServerRoutingInstance> serverFailureConsumer) {
    _brokerId = brokerId;
    _serverChannels = new ServerChannels(this, nettyConfig, null, threadAccountant);
    _serverChannelsTls = tlsConfig != null ? new ServerChannels(this, nettyConfig, tlsConfig, threadAccountant) : null;
    _serverRoutingStatsManager = serverRoutingStatsManager;
    _sendRequestMaxAttempts = sendRequestMaxAttempts;
    _hedgingConfig = hedgingConfig;
    _hedgeBudgetManager = hedgeBudgetManager;
    _hedgeScheduler = hedgeScheduler;
    _hedgeRequestExecutor = hedgeRequestExecutor;
    _currentTimeMillis = currentTimeMillis;
    _serverFailureConsumer = serverFailureConsumer;
    if (_hedgingConfig.isEnabled() && _brokerMetrics != null) {
      _brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.ACTIVE_HEDGE_REQUESTS,
          () -> (long) _hedgeBudgetManager.getActiveHedges());
      _brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.HEDGE_PRIMARY_REQUESTS_LAST_WINDOW,
          _hedgeBudgetManager::getPrimaryRequestsInWindow);
      _brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.HEDGE_REQUESTS_LAST_WINDOW,
          _hedgeBudgetManager::getHedgeRequestsInWindow);
    }
  }

  @Nullable
  private static ScheduledExecutorService createHedgeScheduler(HedgingConfig hedgingConfig) {
    if (!hedgingConfig.isEnabled()) {
      return null;
    }
    ScheduledThreadPoolExecutor executor =
        new ScheduledThreadPoolExecutor(2, new NamedThreadFactory("pinot-query-hedger"));
    executor.setRemoveOnCancelPolicy(true);
    return executor;
  }

  private static Executor createHedgeRequestExecutor(HedgingConfig hedgingConfig) {
    if (!hedgingConfig.isEnabled()) {
      return Runnable::run;
    }
    return new ThreadPoolExecutor(0, hedgingConfig.getMaxConcurrentRequests(), 60L, TimeUnit.SECONDS,
        new SynchronousQueue<>(), new NamedThreadFactory("pinot-query-hedge-sender"));
  }

  public AsyncQueryResponse submitQuery(long requestId, String rawTableName,
      @Nullable BrokerRequest offlineBrokerRequest,
      @Nullable Map<ServerInstance, ServerRouteInfo> offlineRoutingTable,
      @Nullable BrokerRequest realtimeBrokerRequest,
      @Nullable Map<ServerInstance, ServerRouteInfo> realtimeRoutingTable, long timeoutMs) {
    TableRouteInfo tableRouteInfo = new ImplicitHybridTableRouteInfo(offlineBrokerRequest, realtimeBrokerRequest,
        offlineRoutingTable, realtimeRoutingTable);

    return submitQuery(requestId, rawTableName, tableRouteInfo, timeoutMs);
  }

  public AsyncQueryResponse submitQuery(long requestId, String rawTableName, TableRouteInfo route, long timeoutMs) {
    BrokerRequest offlineBrokerRequest = route.getOfflineBrokerRequest();
    BrokerRequest realtimeBrokerRequest = route.getRealtimeBrokerRequest();

    assert offlineBrokerRequest != null || realtimeBrokerRequest != null;

    // can prefer but not require TLS until all servers guaranteed to be on TLS
    boolean preferTls = _serverChannelsTls != null;

    // skip unavailable servers if the query option is set
    boolean skipUnavailableServers = isSkipUnavailableServers(offlineBrokerRequest, realtimeBrokerRequest);

    boolean hedgingEligible = isHedgingEligible(route, offlineBrokerRequest, realtimeBrokerRequest);
    QueryRequestPlan queryRequestPlan = hedgingEligible
        ? route.getQueryRequestPlan(requestId, _brokerId, preferTls)
        : QueryRequestPlan.primaryOnly(route.getRequestMap(requestId, _brokerId, preferTls));
    Map<ServerRoutingInstance, InstanceRequest> requestMap = queryRequestPlan.getPrimaryRequestMap();

    // Create the asynchronous query response with the request map
    AsyncQueryResponse asyncQueryResponse =
        new AsyncQueryResponse(this, requestId, rawTableName, queryRequestPlan, _currentTimeMillis.getAsLong(),
            timeoutMs, _serverRoutingStatsManager);
    _asyncQueryResponseMap.put(requestId, asyncQueryResponse);
    for (Map.Entry<ServerRoutingInstance, InstanceRequest> entry : requestMap.entrySet()) {
      ServerRoutingInstance serverRoutingInstance = entry.getKey();
      ServerChannels serverChannels = serverRoutingInstance.isTlsEnabled() ? _serverChannelsTls : _serverChannels;
      try {
        sendRequestWithRetry(serverChannels, rawTableName, asyncQueryResponse, serverRoutingInstance, entry.getValue(),
            timeoutMs);
        asyncQueryResponse.markRequestSubmitted(serverRoutingInstance);
        if (_hedgingConfig.isEnabled()) {
          _hedgeBudgetManager.recordPrimaryRequest();
        }
      } catch (Exception e) {
        if (e instanceof TimeoutException
            && ServerChannels.CHANNEL_LOCK_TIMEOUT_MSG.equals(e.getMessage())) {
          _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.REQUEST_CHANNEL_LOCK_TIMEOUT_EXCEPTIONS, 1);
        } else {
          _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.REQUEST_SEND_EXCEPTIONS, 1);
        }
        if (skipUnavailableServers) {
          asyncQueryResponse.skipServerResponse(serverRoutingInstance);
        } else {
          markQueryFailed(requestId, serverRoutingInstance, asyncQueryResponse, e);
          break;
        }
      }
    }

    if (hedgingEligible) {
      scheduleHedge(rawTableName, asyncQueryResponse);
    }
    return asyncQueryResponse;
  }

  private boolean isHedgingEligible(TableRouteInfo route, @Nullable BrokerRequest offlineBrokerRequest,
      @Nullable BrokerRequest realtimeBrokerRequest) {
    if (!_hedgingConfig.isEnabled()) {
      return false;
    }
    BrokerRequest brokerRequest = offlineBrokerRequest != null ? offlineBrokerRequest : realtimeBrokerRequest;
    return brokerRequest != null && isEligibleForHedging(route, brokerRequest.getPinotQuery());
  }

  public static boolean isEligibleForHedging(TableRouteInfo route, PinotQuery pinotQuery) {
    if (!(route instanceof ImplicitHybridTableRouteInfo) || pinotQuery.isExplain()) {
      return false;
    }
    Map<String, String> queryOptions = pinotQuery.getQueryOptions();
    return queryOptions == null || !QueryOptionsUtils.isSecondaryWorkload(queryOptions);
  }

  private void scheduleHedge(String rawTableName, AsyncQueryResponse asyncQueryResponse) {
    if (!_hedgingConfig.isEnabled() || _hedgeScheduler == null
        || asyncQueryResponse.getStatus() != QueryResponse.Status.IN_PROGRESS) {
      return;
    }
    if (asyncQueryResponse.getOutstandingHedgeCandidates().isEmpty()) {
      addHedgeMeter(rawTableName, BrokerMeter.HEDGE_SKIPPED_NO_ALTERNATE);
      return;
    }

    long remainingMs = asyncQueryResponse.getMaxEndTimeMs() - _currentTimeMillis.getAsLong();
    if (remainingMs <= 0) {
      addHedgeMeter(rawTableName, BrokerMeter.HEDGE_SKIPPED_DEADLINE);
      return;
    }
    long delayMs = Math.round(remainingMs * _hedgingConfig.getDelayRatio());
    delayMs = Math.max(_hedgingConfig.getMinDelayMs(), Math.min(delayMs, _hedgingConfig.getMaxDelayMs()));
    if (delayMs >= remainingMs) {
      addHedgeMeter(rawTableName, BrokerMeter.HEDGE_SKIPPED_DEADLINE);
      return;
    }

    try {
      asyncQueryResponse.markHedgeDecisionScheduled(_hedgeScheduler.schedule(() -> {
        try {
          _hedgeRequestExecutor.execute(() -> {
            try {
              sendHedge(rawTableName, asyncQueryResponse);
            } catch (Throwable t) {
              LOGGER.error("Caught exception while evaluating hedge for request {}",
                  asyncQueryResponse.getRequestId(), t);
            }
          });
        } catch (RejectedExecutionException e) {
          addHedgeMeter(rawTableName, BrokerMeter.HEDGE_SKIPPED_CONCURRENCY_LIMIT);
        }
      }, delayMs, TimeUnit.MILLISECONDS));
      addHedgeMeter(rawTableName, BrokerMeter.HEDGE_DECISIONS_SCHEDULED);
    } catch (RejectedExecutionException e) {
      LOGGER.warn("Failed to schedule hedge for request: {}", asyncQueryResponse.getRequestId(), e);
    }
  }

  private void sendHedge(String rawTableName, AsyncQueryResponse asyncQueryResponse) {
    long remainingMs = asyncQueryResponse.getMaxEndTimeMs() - _currentTimeMillis.getAsLong();
    if (remainingMs <= 0) {
      addHedgeMeter(rawTableName, BrokerMeter.HEDGE_SKIPPED_DEADLINE);
      return;
    }
    if (asyncQueryResponse.getStatus() != QueryResponse.Status.IN_PROGRESS) {
      addHedgeMeter(rawTableName, BrokerMeter.HEDGE_SKIPPED_NO_OUTSTANDING_REQUEST);
      return;
    }

    List<RequestGroup> candidates = asyncQueryResponse.getOutstandingHedgeCandidates();
    if (candidates.isEmpty()) {
      addHedgeMeter(rawTableName, BrokerMeter.HEDGE_SKIPPED_NO_OUTSTANDING_REQUEST);
      return;
    }
    Map<String, Double> configuredScores = new HashMap<>(candidates.size());
    Map<String, Double> latencyScores = new HashMap<>(candidates.size());
    for (RequestGroup candidate : candidates) {
      String instanceId = candidate.getPrimaryServer().getInstanceId();
      configuredScores.put(instanceId, _serverRoutingStatsManager.fetchConfiguredScoreForServer(instanceId));
      latencyScores.put(instanceId, _serverRoutingStatsManager.fetchEMALatencyForServer(instanceId));
    }
    candidates.sort((left, right) -> compareHedgeCandidates(left, right, configuredScores, latencyScores));

    AdmissionResult admissionResult = _hedgeBudgetManager.tryAcquire();
    if (admissionResult == AdmissionResult.RATIO_LIMIT) {
      addHedgeMeter(rawTableName, BrokerMeter.HEDGE_SKIPPED_RATIO_BUDGET);
      return;
    }
    if (admissionResult == AdmissionResult.CONCURRENCY_LIMIT) {
      addHedgeMeter(rawTableName, BrokerMeter.HEDGE_SKIPPED_CONCURRENCY_LIMIT);
      return;
    }

    boolean admissionOwnedByRouter = true;
    try {
      RequestGroup selectedGroup = null;
      for (RequestGroup candidate : candidates) {
        if (asyncQueryResponse.tryRegisterHedge(candidate.getPrimaryServer())) {
          selectedGroup = candidate;
          break;
        }
      }
      if (selectedGroup == null) {
        addHedgeMeter(rawTableName, BrokerMeter.HEDGE_SKIPPED_NO_OUTSTANDING_REQUEST);
        return;
      }

      admissionOwnedByRouter = false;
      ServerRoutingInstance hedgeServer = selectedGroup.getAlternateServer();
      InstanceRequest hedgeRequest = selectedGroup.getAlternateRequest();
      assert hedgeServer != null;
      assert hedgeRequest != null;
      try {
        long cleanupDelayMs =
            Math.max(0L, asyncQueryResponse.getMaxEndTimeMs() - _currentTimeMillis.getAsLong());
        asyncQueryResponse.markDeadlineCleanupScheduled(
            _hedgeScheduler.schedule(asyncQueryResponse::expireAtDeadline, cleanupDelayMs, TimeUnit.MILLISECONDS));
        ServerChannels serverChannels = hedgeServer.isTlsEnabled() ? _serverChannelsTls : _serverChannels;
        long hedgeRemainingMs = asyncQueryResponse.getMaxEndTimeMs() - _currentTimeMillis.getAsLong();
        if (hedgeRemainingMs <= 0) {
          throw new TimeoutException("Query deadline reached before sending hedge request");
        }
        if (asyncQueryResponse.isHedgingCancelled()) {
          throw new TimeoutException("Query cancelled before sending hedge request");
        }
        // Mark the attempt submitted before sending so that a fast response cannot be timed against an unset
        // submission timestamp.
        asyncQueryResponse.markRequestSubmitted(hedgeServer);
        sendRequestWithRetry(serverChannels, rawTableName, asyncQueryResponse, hedgeServer,
            boundTimeoutToRemainingTime(hedgeRequest, hedgeRemainingMs), hedgeRemainingMs);
        asyncQueryResponse.markHedgeRequestDispatched(hedgeServer);
        addHedgeMeter(rawTableName, BrokerMeter.HEDGE_REQUESTS_SENT);
      } catch (Exception e) {
        addHedgeMeter(rawTableName, BrokerMeter.HEDGE_SEND_EXCEPTIONS);
        asyncQueryResponse.markHedgeSendFailed(hedgeServer, e);
      }
    } finally {
      if (admissionOwnedByRouter) {
        _hedgeBudgetManager.release();
      }
    }
  }

  /**
   * Returns a copy of the alternate request whose server-side timeout is bounded by the time remaining before the
   * original broker deadline. Because a hedge is dispatched after a delay, reusing the request as-is would let the
   * alternate keep working past the deadline the broker itself honors.
   */
  private static InstanceRequest boundTimeoutToRemainingTime(InstanceRequest hedgeRequest, long remainingTimeMs) {
    BrokerRequest brokerRequest = hedgeRequest.getQuery();
    if (brokerRequest == null || brokerRequest.getPinotQuery() == null) {
      return hedgeRequest;
    }
    InstanceRequest boundedRequest = hedgeRequest.deepCopy();
    PinotQuery pinotQuery = boundedRequest.getQuery().getPinotQuery();
    long timeoutMs = remainingTimeMs;
    Map<String, String> queryOptions = pinotQuery.getQueryOptions();
    if (queryOptions != null) {
      String configuredTimeoutMs = queryOptions.get(QueryOptionKey.TIMEOUT_MS);
      if (configuredTimeoutMs != null) {
        try {
          timeoutMs = Math.min(Long.parseLong(configuredTimeoutMs), remainingTimeMs);
        } catch (NumberFormatException e) {
          // Keep the remaining time when the configured timeout cannot be parsed.
        }
      }
    }
    pinotQuery.putToQueryOptions(QueryOptionKey.TIMEOUT_MS, Long.toString(timeoutMs));
    return boundedRequest;
  }

  private static int compareHedgeCandidates(RequestGroup left, RequestGroup right,
      Map<String, Double> configuredScores, Map<String, Double> latencyScores) {
    String leftInstanceId = left.getPrimaryServer().getInstanceId();
    String rightInstanceId = right.getPrimaryServer().getInstanceId();
    int comparison = compareNullableDescendingIfBothPresent(configuredScores.get(leftInstanceId),
        configuredScores.get(rightInstanceId));
    if (comparison != 0) {
      return comparison;
    }
    comparison =
        compareNullableDescendingIfBothPresent(latencyScores.get(leftInstanceId), latencyScores.get(rightInstanceId));
    if (comparison != 0) {
      return comparison;
    }
    return leftInstanceId.compareTo(rightInstanceId);
  }

  private static int compareNullableDescendingIfBothPresent(@Nullable Double left, @Nullable Double right) {
    if (left == null || !Double.isFinite(left) || right == null || !Double.isFinite(right)) {
      return 0;
    }
    return Double.compare(right, left);
  }

  /**
   * Sends a request to a server with retry on channel lock timeout using exponential backoff.
   * Only channel lock timeouts are retried; other exceptions propagate immediately.
   */
  private void sendRequestWithRetry(ServerChannels serverChannels, String rawTableName,
      AsyncQueryResponse asyncQueryResponse, ServerRoutingInstance serverRoutingInstance,
      InstanceRequest instanceRequest, long timeoutMs)
      throws Exception {
    long deadlineMs = System.currentTimeMillis() + timeoutMs;
    try {
      RetryPolicies.exponentialBackoffRetryPolicy(_sendRequestMaxAttempts, SEND_REQUEST_INITIAL_DELAY_MS,
          SEND_REQUEST_DELAY_SCALE_FACTOR).attempt(() -> {
        long remainingMs = deadlineMs - System.currentTimeMillis();
        if (remainingMs <= 0) {
          return false;
        }
        try {
          serverChannels.sendRequest(rawTableName, asyncQueryResponse, serverRoutingInstance, instanceRequest,
              remainingMs);
          return true;
        } catch (TimeoutException e) {
          if (ServerChannels.CHANNEL_LOCK_TIMEOUT_MSG.equals(e.getMessage())) {
            return false; // channel lock contention — retry with backoff
          }
          throw e; // other timeout — abort
        }
      });
    } catch (AttemptsExceededException e) {
      throw new TimeoutException(ServerChannels.CHANNEL_LOCK_TIMEOUT_MSG);
    } catch (RetriableOperationException e) {
      throw (Exception) e.getCause();
    }
  }

  private boolean isSkipUnavailableServers(@Nullable BrokerRequest offlineBrokerRequest,
      @Nullable BrokerRequest realtimeBrokerRequest) {
    if (offlineBrokerRequest != null && QueryOptionsUtils.isSkipUnavailableServers(
        offlineBrokerRequest.getPinotQuery().getQueryOptions())) {
      return true;
    }
    return realtimeBrokerRequest != null && QueryOptionsUtils.isSkipUnavailableServers(
        realtimeBrokerRequest.getPinotQuery().getQueryOptions());
  }

  private void markQueryFailed(long requestId, ServerRoutingInstance serverRoutingInstance,
      AsyncQueryResponse asyncQueryResponse, Exception e) {
    LOGGER.error("Caught exception while sending request {} to server: {}, marking query failed", requestId,
        serverRoutingInstance, e);
    asyncQueryResponse.markQueryFailed(serverRoutingInstance, e);
  }

  public boolean hasChannel(ServerInstance serverInstance) {
    if (_serverChannelsTls != null) {
      return _serverChannelsTls.hasChannel(serverInstance.toServerRoutingInstance(TableType.OFFLINE, true));
    } else {
      return _serverChannels.hasChannel(serverInstance.toServerRoutingInstance(TableType.OFFLINE, false));
    }
  }

  /**
   * Connects to the given server, returns {@code true} if the server is successfully connected.
   */
  public boolean connect(ServerInstance serverInstance) {
    try {
      if (_serverChannelsTls != null) {
        _serverChannelsTls.connect(serverInstance.toServerRoutingInstance(TableType.OFFLINE, true));
      } else {
        _serverChannels.connect(serverInstance.toServerRoutingInstance(TableType.OFFLINE, false));
      }
      return true;
    } catch (Exception e) {
      LOGGER.debug("Failed to connect to server: {}", serverInstance, e);
      return false;
    }
  }

  public void shutDown() {
    _serverChannels.shutDown();
    if (_hedgeScheduler != null) {
      _hedgeScheduler.shutdownNow();
    }
    if (_hedgeRequestExecutor instanceof ExecutorService) {
      ((ExecutorService) _hedgeRequestExecutor).shutdownNow();
    }
  }

  public void cancelHedging(long requestId) {
    AsyncQueryResponse asyncQueryResponse = _asyncQueryResponseMap.get(requestId);
    if (asyncQueryResponse != null) {
      asyncQueryResponse.cancelHedging();
    }
  }

  void receiveDataTable(ServerRoutingInstance serverRoutingInstance, DataTable dataTable, int responseSize,
      int deserializationTimeMs) {
    long requestId = Long.parseLong(dataTable.getMetadata().get(MetadataKey.REQUEST_ID.getName()));
    AsyncQueryResponse asyncQueryResponse = _asyncQueryResponseMap.get(requestId);

    // Query future might be null if the query is already done (maybe due to failure)
    if (asyncQueryResponse != null) {
      ResponseOutcome outcome =
          asyncQueryResponse.receiveDataTable(serverRoutingInstance, dataTable, responseSize, deserializationTimeMs);
      switch (outcome) {
        case HEDGE_WIN:
          addHedgeMeter(asyncQueryResponse.getRawTableName(), BrokerMeter.HEDGE_WINS);
          break;
        case PRIMARY_WIN:
          addHedgeMeter(asyncQueryResponse.getRawTableName(), BrokerMeter.PRIMARY_WINS_AFTER_HEDGE);
          break;
        case ALL_ATTEMPTS_FAILED:
          addHedgeMeter(asyncQueryResponse.getRawTableName(), BrokerMeter.HEDGE_ALL_ATTEMPTS_FAILED);
          break;
        case LOSER:
          addHedgeMeter(asyncQueryResponse.getRawTableName(), BrokerMeter.HEDGE_LOSER_RESPONSES_IGNORED);
          break;
        case DUPLICATE:
          addHedgeMeter(asyncQueryResponse.getRawTableName(), BrokerMeter.HEDGE_DUPLICATE_RESPONSES_IGNORED);
          break;
        default:
          break;
      }
    }
  }

  void onHedgeAttemptTerminal(String rawTableName, ServerResponse hedgeResponse) {
    _hedgeBudgetManager.release();
    int responseDelayMs = hedgeResponse.getResponseDelayMs();
    if (_brokerMetrics != null && responseDelayMs >= 0) {
      _brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.HEDGE_RESPONSE_LATENCY_MS, responseDelayMs,
          TimeUnit.MILLISECONDS);
      _brokerMetrics.addTimedValue(BrokerTimer.HEDGE_RESPONSE_LATENCY_MS, responseDelayMs, TimeUnit.MILLISECONDS);
    }
  }

  void onAllHedgeAttemptsFailed(String rawTableName) {
    addHedgeMeter(rawTableName, BrokerMeter.HEDGE_ALL_ATTEMPTS_FAILED);
  }

  private void addHedgeMeter(String rawTableName, BrokerMeter meter) {
    if (_brokerMetrics != null) {
      _brokerMetrics.addMeteredTableValue(rawTableName, meter, 1);
      _brokerMetrics.addMeteredGlobalValue(meter, 1);
    }
  }

  void markServerDown(ServerRoutingInstance serverRoutingInstance, Exception exception) {
    boolean requestAffected = false;
    for (AsyncQueryResponse asyncQueryResponse : _asyncQueryResponseMap.values()) {
      requestAffected |= asyncQueryResponse.markServerDown(serverRoutingInstance, exception);
    }
    if (requestAffected) {
      _serverFailureConsumer.accept(serverRoutingInstance);
    }
  }

  void markQueryDone(long requestId) {
    _asyncQueryResponseMap.remove(requestId);
  }
}
