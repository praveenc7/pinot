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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.routing.AlternateServerRouteInfo;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.transport.HedgeBudgetManager.AdmissionResult;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for QueryRouter retry and skip behavior on send failures.
 */
public class QueryRouterSendRetryTest {
  private static final ServerInstance SERVER_INSTANCE = new ServerInstance("localhost", 12345);
  private static final ServerRoutingInstance OFFLINE_ROUTING_INSTANCE =
      SERVER_INSTANCE.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
  private static final BrokerRequest BROKER_REQUEST =
      CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM testTable");
  private static final Map<ServerInstance, ServerRouteInfo> ROUTING_TABLE =
      Collections.singletonMap(SERVER_INSTANCE,
          new ServerRouteInfo(Collections.emptyList(), Collections.emptyList()));

  private QueryRouter _queryRouter;
  private ServerChannels _mockServerChannels;

  @BeforeMethod
  public void setUp()
      throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION, true);
    PinotConfiguration cfg = new PinotConfiguration(properties);
    ServerRoutingStatsManager statsManager = new ServerRoutingStatsManager(cfg, mock(BrokerMetrics.class));
    statsManager.init();

    _queryRouter = new QueryRouter("testBroker", null, null, statsManager,
        ThreadAccountantUtils.getNoOpAccountant(), 3);

    // Replace _serverChannels with a mock via reflection
    _mockServerChannels = mock(ServerChannels.class);
    Field serverChannelsField = QueryRouter.class.getDeclaredField("_serverChannels");
    serverChannelsField.setAccessible(true);
    serverChannelsField.set(_queryRouter, _mockServerChannels);
  }

  @Test
  public void testRetryOnChannelLockTimeout()
      throws Exception {
    // First call throws channel lock timeout, second call succeeds
    doThrow(new TimeoutException(ServerChannels.CHANNEL_LOCK_TIMEOUT_MSG))
        .doNothing()
        .when(_mockServerChannels)
        .sendRequest(anyString(), any(), any(), any(InstanceRequest.class), anyLong());

    BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM testTable");
    AsyncQueryResponse response =
        _queryRouter.submitQuery(123L, "testTable", brokerRequest, ROUTING_TABLE, null, null, 10_000L);

    // Should have retried and succeeded — verify sendRequest was called twice
    verify(_mockServerChannels, times(2))
        .sendRequest(eq("testTable"), any(), eq(OFFLINE_ROUTING_INSTANCE), any(InstanceRequest.class), anyLong());

    // Query should not be failed
    assertEquals(response.getStatus(), QueryResponse.Status.IN_PROGRESS);
  }

  @Test
  public void testRetryExhaustedOnChannelLockTimeout()
      throws Exception {
    // All 3 attempts throw channel lock timeout
    doThrow(new TimeoutException(ServerChannels.CHANNEL_LOCK_TIMEOUT_MSG))
        .when(_mockServerChannels)
        .sendRequest(anyString(), any(), any(), any(InstanceRequest.class), anyLong());

    BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM testTable");
    AsyncQueryResponse response =
        _queryRouter.submitQuery(123L, "testTable", brokerRequest, ROUTING_TABLE, null, null, 10_000L);

    // All 3 retry attempts exhausted, query should be marked failed
    verify(_mockServerChannels, times(3))
        .sendRequest(eq("testTable"), any(), eq(OFFLINE_ROUTING_INSTANCE), any(InstanceRequest.class), anyLong());
    assertEquals(response.getStatus(), QueryResponse.Status.FAILED);
  }

  @Test
  public void testChannelLockTimeoutSkippedWithSkipUnavailableServers()
      throws Exception {
    // All attempts throw channel lock timeout
    doThrow(new TimeoutException(ServerChannels.CHANNEL_LOCK_TIMEOUT_MSG))
        .when(_mockServerChannels)
        .sendRequest(anyString(), any(), any(), any(InstanceRequest.class), anyLong());

    BrokerRequest brokerRequest =
        CalciteSqlCompiler.compileToBrokerRequest("SET skipUnavailableServers=true; SELECT * FROM testTable");
    AsyncQueryResponse response =
        _queryRouter.submitQuery(123L, "testTable", brokerRequest, ROUTING_TABLE, null, null, 10_000L);

    // With skipUnavailableServers, the server should be skipped instead of failing the query
    Map<ServerRoutingInstance, ServerResponse> responses = response.getFinalResponses();
    assertEquals(responses.size(), 1);
    assertNull(responses.get(OFFLINE_ROUTING_INSTANCE).getDataTable());
    // Query should complete (skipped), not fail
    assertEquals(response.getStatus(), QueryResponse.Status.COMPLETED);
  }

  @Test
  public void testNoRetryOnNonTimeoutException()
      throws Exception {
    // Non-timeout exception should not be retried
    doThrow(new RuntimeException("Connection refused"))
        .when(_mockServerChannels)
        .sendRequest(anyString(), any(), any(), any(InstanceRequest.class), anyLong());

    BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM testTable");
    AsyncQueryResponse response =
        _queryRouter.submitQuery(123L, "testTable", brokerRequest, ROUTING_TABLE, null, null, 10_000L);

    // Should NOT retry — only 1 call
    verify(_mockServerChannels, times(1))
        .sendRequest(eq("testTable"), any(), eq(OFFLINE_ROUTING_INSTANCE), any(InstanceRequest.class), anyLong());
    assertEquals(response.getStatus(), QueryResponse.Status.FAILED);
  }

  @Test
  public void testNonTimeoutExceptionSkippedWithSkipUnavailableServers()
      throws Exception {
    // Non-timeout exception with skipUnavailableServers should skip
    doThrow(new RuntimeException("Connection refused"))
        .when(_mockServerChannels)
        .sendRequest(anyString(), any(), any(), any(InstanceRequest.class), anyLong());

    BrokerRequest brokerRequest =
        CalciteSqlCompiler.compileToBrokerRequest("SET skipUnavailableServers=true; SELECT * FROM testTable");
    AsyncQueryResponse response =
        _queryRouter.submitQuery(123L, "testTable", brokerRequest, ROUTING_TABLE, null, null, 10_000L);

    Map<ServerRoutingInstance, ServerResponse> responses = response.getFinalResponses();
    assertEquals(responses.size(), 1);
    assertNull(responses.get(OFFLINE_ROUTING_INSTANCE).getDataTable());
    assertEquals(response.getStatus(), QueryResponse.Status.COMPLETED);
  }

  @Test
  public void testHedgeRetryIsBoundedByRemainingOriginalDeadline()
      throws Exception {
    ServerInstance hedgeServerInstance = new ServerInstance("localhost", 12346);
    ServerRoutingInstance hedgeRoutingInstance =
        hedgeServerInstance.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
    FakeScheduledExecutorService scheduler = new FakeScheduledExecutorService();
    HedgeBudgetManager hedgeBudgetManager = mock(HedgeBudgetManager.class);
    when(hedgeBudgetManager.tryAcquire()).thenReturn(AdmissionResult.ACQUIRED);
    ServerRoutingStatsManager statsManager = mock(ServerRoutingStatsManager.class);
    QueryRouter queryRouter = new QueryRouter("testBroker", null, null, statsManager,
        ThreadAccountantUtils.getNoOpAccountant(), 2, createHedgingConfig(0L, 0L), hedgeBudgetManager, scheduler,
        () -> 1_000L);
    ServerChannels serverChannels = mock(ServerChannels.class);
    setServerChannels(queryRouter, serverChannels);

    List<Long> hedgeTimeouts = new ArrayList<>();
    doAnswer(invocation -> {
      ServerRoutingInstance serverRoutingInstance = invocation.getArgument(2);
      long timeoutMs = invocation.getArgument(4);
      if (serverRoutingInstance.equals(hedgeRoutingInstance)) {
        hedgeTimeouts.add(timeoutMs);
        if (hedgeTimeouts.size() == 1) {
          throw new TimeoutException(ServerChannels.CHANNEL_LOCK_TIMEOUT_MSG);
        }
      }
      return null;
    }).when(serverChannels).sendRequest(anyString(), any(), any(), any(InstanceRequest.class), anyLong());

    AsyncQueryResponse response = queryRouter.submitQuery(9L, "testTable",
        createOfflineRoute(SERVER_INSTANCE, hedgeServerInstance), 350L);
    scheduler.runNext();

    assertEquals(hedgeTimeouts.size(), 2);
    assertTrue(hedgeTimeouts.get(0) <= 350L && hedgeTimeouts.get(0) > 0L);
    assertTrue(hedgeTimeouts.get(1) <= 350L && hedgeTimeouts.get(1) > 0L);
    assertTrue(hedgeTimeouts.get(1) < hedgeTimeouts.get(0));
    response.expireAtDeadline();
  }

  private void setServerChannels(QueryRouter queryRouter, ServerChannels serverChannels)
      throws Exception {
    Field serverChannelsField = QueryRouter.class.getDeclaredField("_serverChannels");
    serverChannelsField.setAccessible(true);
    serverChannelsField.set(queryRouter, serverChannels);
  }

  private static HedgingConfig createHedgingConfig(long minDelayMs, long maxDelayMs) {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_ENABLED, true);
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_RATIO, 0.5d);
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_MIN_MS, minDelayMs);
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_MAX_MS, maxDelayMs);
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_MAX_EXTRA_REQUEST_RATIO, 1.0d);
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_BUDGET_WINDOW_MS, 60_000L);
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_MAX_CONCURRENT_REQUESTS, 8);
    return HedgingConfig.fromPinotConfiguration(new PinotConfiguration(properties));
  }

  private static ImplicitHybridTableRouteInfo createOfflineRoute(ServerInstance primaryServer,
      ServerInstance hedgeServer) {
    Map<ServerInstance, ServerRouteInfo> routingTable =
        Collections.singletonMap(primaryServer, new ServerRouteInfo(Collections.emptyList(), Collections.emptyList()));
    ImplicitHybridTableRouteInfo route = new ImplicitHybridTableRouteInfo(BROKER_REQUEST, null, routingTable, null);
    route.setOfflineAlternateRoutes(Collections.singletonMap(primaryServer,
        Collections.singletonList(new AlternateServerRouteInfo(hedgeServer,
            new ServerRouteInfo(Collections.emptyList(), Collections.emptyList())))));
    return route;
  }

  private static final class FakeScheduledExecutorService extends AbstractExecutorService
      implements ScheduledExecutorService {
    private final List<FakeScheduledFuture> _tasks = new ArrayList<>();
    private boolean _shutdown;

    @Override
    public void shutdown() {
      _shutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
      _shutdown = true;
      return List.of();
    }

    @Override
    public boolean isShutdown() {
      return _shutdown;
    }

    @Override
    public boolean isTerminated() {
      return _shutdown;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return true;
    }

    @Override
    public void execute(Runnable command) {
      command.run();
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
      FakeScheduledFuture future = new FakeScheduledFuture(command);
      _tasks.add(future);
      return future;
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
      throw new UnsupportedOperationException();
    }

    private void runNext() {
      _tasks.remove(0).run();
    }
  }

  private static final class FakeScheduledFuture implements ScheduledFuture<Object> {
    private final Runnable _command;
    private boolean _cancelled;
    private boolean _done;

    private FakeScheduledFuture(Runnable command) {
      _command = command;
    }

    private void run() {
      if (!_cancelled) {
        _command.run();
        _done = true;
      }
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return 0;
    }

    @Override
    public int compareTo(Delayed other) {
      return 0;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      _cancelled = true;
      return true;
    }

    @Override
    public boolean isCancelled() {
      return _cancelled;
    }

    @Override
    public boolean isDone() {
      return _done || _cancelled;
    }

    @Override
    public Object get() {
      return null;
    }

    @Override
    public Object get(long timeout, TimeUnit unit) {
      return null;
    }
  }
}
