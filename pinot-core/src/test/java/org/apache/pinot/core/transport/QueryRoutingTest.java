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

import com.google.common.util.concurrent.Futures;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.pinot.core.routing.AlternateServerRouteInfo;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.transport.HedgeBudgetManager.AdmissionResult;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.server.access.AccessControl;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.util.TestUtils;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class QueryRoutingTest {
  private static final int TEST_PORT = 12345;
  private static final ServerInstance SERVER_INSTANCE = new ServerInstance("localhost", TEST_PORT);
  private static final ServerRoutingInstance OFFLINE_SERVER_ROUTING_INSTANCE =
      SERVER_INSTANCE.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
  private static final ServerRoutingInstance REALTIME_SERVER_ROUTING_INSTANCE =
      SERVER_INSTANCE.toServerRoutingInstance(TableType.REALTIME, ServerInstance.RoutingType.NETTY);
  private static final BrokerRequest BROKER_REQUEST =
      CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM testTable");
  private static final Map<ServerInstance, ServerRouteInfo> ROUTING_TABLE =
      Collections.singletonMap(SERVER_INSTANCE,
          new ServerRouteInfo(Collections.emptyList(), Collections.emptyList()));

  private QueryRouter _queryRouter;
  private ServerRoutingStatsManager _serverRoutingStatsManager;
  int _requestCount;
  private QueryServer _queryServer;

  @BeforeClass
  public void setUp() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION, true);
    PinotConfiguration cfg = new PinotConfiguration(properties);
    _serverRoutingStatsManager = new ServerRoutingStatsManager(cfg, mock(BrokerMetrics.class));
    _serverRoutingStatsManager.init();
    _queryRouter = new QueryRouter("testBroker", null, null, _serverRoutingStatsManager,
        ThreadAccountantUtils.getNoOpAccountant(), 1);
    _requestCount = 0;
  }

  @AfterMethod
  void shutdownServer() {
    if (_queryServer != null) {
      _queryServer.shutDown();
      _queryServer = null;
    }
  }

  @AfterMethod
  void deregisterServerMetrics() {
    ServerMetrics.deregister();
  }

  private QueryServer getQueryServer(int responseDelayMs, byte[] responseBytes) {
    return getQueryServer(responseDelayMs, responseBytes, TEST_PORT);
  }

  private QueryServer getQueryServer(int responseDelayMs, byte[] responseBytes, int port) {
    InstanceRequestHandler handler = new InstanceRequestHandler("server01", new PinotConfiguration(),
        mockQueryScheduler(responseDelayMs, responseBytes), mock(AccessControl.class),
        ThreadAccountantUtils.getNoOpAccountant());
    return new QueryServer(port, null, handler);
  }

  private QueryScheduler mockQueryScheduler(int responseDelayMs, byte[] responseBytes) {
    QueryScheduler queryScheduler = mock(QueryScheduler.class);
    when(queryScheduler.submit(any())).thenAnswer(invocation -> {
      Thread.sleep(responseDelayMs);
      return Futures.immediateFuture(responseBytes);
    });
    return queryScheduler;
  }

  @Test
  public void testValidResponse()
      throws Exception {
    long requestId = 123;
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    byte[] responseBytes = dataTable.toBytes();
    String serverId = SERVER_INSTANCE.getInstanceId();

    // Start the server
    _queryServer = getQueryServer(0, responseBytes);
    _queryServer.start();

    // OFFLINE only
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, 600_000L);
    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(OFFLINE_SERVER_ROUTING_INSTANCE));
    ServerResponse serverResponse = response.get(OFFLINE_SERVER_ROUTING_INSTANCE);
    assertNotNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseSize(), responseBytes.length);
    // 2 requests - query submit and query response.
    _requestCount += 2;
    waitForStatsUpdate(_requestCount);
    assertEquals(_serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverId).intValue(), 0);

    // REALTIME only
    asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", null, null, BROKER_REQUEST, ROUTING_TABLE, 1_000L);
    response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(REALTIME_SERVER_ROUTING_INSTANCE));
    serverResponse = response.get(REALTIME_SERVER_ROUTING_INSTANCE);
    assertNotNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseSize(), responseBytes.length);
    _requestCount += 2;
    waitForStatsUpdate(_requestCount);
    assertEquals(_serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverId).intValue(), 0);

    // Hybrid
    asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, ROUTING_TABLE, BROKER_REQUEST, ROUTING_TABLE,
            1_000L);
    response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 2);
    assertTrue(response.containsKey(OFFLINE_SERVER_ROUTING_INSTANCE));
    serverResponse = response.get(OFFLINE_SERVER_ROUTING_INSTANCE);
    assertNotNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseSize(), responseBytes.length);
    assertTrue(response.containsKey(REALTIME_SERVER_ROUTING_INSTANCE));
    serverResponse = response.get(REALTIME_SERVER_ROUTING_INSTANCE);
    assertNotNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseSize(), responseBytes.length);
    _requestCount += 4;
    waitForStatsUpdate(_requestCount);
    assertEquals(_serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverId).intValue(), 0);
  }

  @Test
  public void testInvalidResponse()
      throws Exception {
    long requestId = 123;
    String serverId = SERVER_INSTANCE.getInstanceId();

    // Start the server
    _queryServer = getQueryServer(0, new byte[0]);
    _queryServer.start();

    long startTimeMs = System.currentTimeMillis();
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, 1_000L);
    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(OFFLINE_SERVER_ROUTING_INSTANCE));
    ServerResponse serverResponse = response.get(OFFLINE_SERVER_ROUTING_INSTANCE);
    assertNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseDelayMs(), -1);
    assertEquals(serverResponse.getResponseSize(), 0);
    assertEquals(serverResponse.getDeserializationTimeMs(), 0);
    // Query should time out
    assertTrue(System.currentTimeMillis() - startTimeMs >= 1000);
    _requestCount += 2;
    waitForStatsUpdate(_requestCount);
    assertEquals(_serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverId).intValue(), 0);
  }

  @Test
  public void testLatencyForQueryServerException()
      throws Exception {
    long requestId = 123;
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    dataTable.addException(QueryErrorCode.SERVER_TABLE_MISSING, "Test error message");
    byte[] responseBytes = dataTable.toBytes();
    String serverId = SERVER_INSTANCE.getInstanceId();
    // Start the server
    _queryServer = getQueryServer(0, responseBytes);
    _queryServer.start();

    // Send a query with ServerSide exception and check if the latency is set to timeout value.
    Double latencyBefore = _serverRoutingStatsManager.fetchEMALatencyForServer(serverId);
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, 1_000L);
    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(OFFLINE_SERVER_ROUTING_INSTANCE));

    _requestCount += 2;
    waitForStatsUpdate(_requestCount);
    Double latencyAfter = _serverRoutingStatsManager.fetchEMALatencyForServer(serverId);

    if (latencyBefore == null) {
      // This means that no queries were run before this test. So we can just make sure that latencyAfter is equal to
      //666.334.
      // This corresponds to the EWMA value when a latency timeout value of 1000 is set. Latency set to timeout value
      //when server side exception occurs.
      double serverEWMALatency = 666.334;
      // Leaving an error budget of 2%
      double delta = 13.32;
      assertEquals(latencyAfter, serverEWMALatency, delta);
    } else {
      assertTrue(latencyAfter > latencyBefore, latencyAfter + " should be greater than " + latencyBefore);
    }
  }

  @Test
  public void testLatencyForClientException()
      throws Exception {
    long requestId = 123;
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    dataTable.addException(QueryErrorCode.QUERY_CANCELLATION, "Test error message");
    byte[] responseBytes = dataTable.toBytes();
    String serverId = SERVER_INSTANCE.getInstanceId();
    // Start the server
    _queryServer = getQueryServer(0, responseBytes);
    _queryServer.start();

    // Send a query with client side errors.
    Double latencyBefore = _serverRoutingStatsManager.fetchEMALatencyForServer(serverId);

    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, 1_000L);
    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(OFFLINE_SERVER_ROUTING_INSTANCE));
    ServerResponse serverResponse = response.get(OFFLINE_SERVER_ROUTING_INSTANCE);

    _requestCount += 2;
    waitForStatsUpdate(_requestCount);

    Double latencyAfter = _serverRoutingStatsManager.fetchEMALatencyForServer(serverId);

    if (latencyBefore == null) {
      // Latency for the server with client side exception is assigned as serverResponse.getResponseDelayMs() and the
      //calculated
      // EWMLatency for the server will be less than serverResponse.getResponseDelayMs()
      assertTrue(latencyAfter <= serverResponse.getResponseDelayMs());
    } else {
      assertTrue(latencyAfter < latencyBefore, latencyAfter + " should be lesser than " + latencyBefore);
    }
  }

  @Test
  public void testLatencyForMultipleExceptions()
      throws Exception {
    long requestId = 123;
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    dataTable.addException(QueryErrorCode.QUERY_CANCELLATION, "Test cancellation error message");
    dataTable.addException(QueryErrorCode.SERVER_TABLE_MISSING, "Test table missing error message");
    byte[] responseBytes = dataTable.toBytes();
    String serverId = SERVER_INSTANCE.getInstanceId();
    // Start the server
    _queryServer = getQueryServer(0, responseBytes);
    _queryServer.start();

    // Send a query with multiple exceptions. Make sure that the latency is set to timeout value even if a single
    //server-side exception is seen.
    Double latencyBefore = _serverRoutingStatsManager.fetchEMALatencyForServer(serverId);
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, 1_000L);
    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(OFFLINE_SERVER_ROUTING_INSTANCE));

    _requestCount += 2;
    waitForStatsUpdate(_requestCount);
    Double latencyAfter = _serverRoutingStatsManager.fetchEMALatencyForServer(serverId);

    if (latencyBefore == null) {
      // This means that no queries where run before this test. So we can just make sure that latencyAfter is equal
      //to 666.334.
      // This corresponds to the EWMA value when a latency timeout value of 1000 is set.
      double serverEWMALatency = 666.334;
      // Leaving an error budget of 2%
      double delta = 13.32;
      assertEquals(latencyAfter, serverEWMALatency, delta);
    } else {
      assertTrue(latencyAfter > latencyBefore, latencyAfter + " should be greater than " + latencyBefore);
    }
  }

  @Test
  public void testLatencyForNoException()
      throws Exception {
    long requestId = 123;
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    byte[] responseBytes = dataTable.toBytes();
    String serverId = SERVER_INSTANCE.getInstanceId();
    // Start the server
    _queryServer = getQueryServer(0, responseBytes);
    _queryServer.start();

    // Send a valid query and get latency
    Double latencyBefore = _serverRoutingStatsManager.fetchEMALatencyForServer(serverId);
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, 1_000L);
    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(OFFLINE_SERVER_ROUTING_INSTANCE));
    ServerResponse serverResponse = response.get(OFFLINE_SERVER_ROUTING_INSTANCE);

    _requestCount += 2;
    waitForStatsUpdate(_requestCount);
    Double latencyAfter = _serverRoutingStatsManager.fetchEMALatencyForServer(serverId);

    if (latencyBefore == null) {
      // Latency for the server with no exceptions is assigned as serverResponse.getResponseDelayMs() and the calculated
      // EWMLatency for the server will be less than serverResponse.getResponseDelayMs()
      assertTrue(latencyAfter <= serverResponse.getResponseDelayMs());
    } else {
      assertTrue(latencyAfter < latencyBefore, latencyAfter + " should be lesser than " + latencyBefore);
    }
  }

  @Test
  public void testNonMatchingRequestId()
      throws Exception {
    long requestId = 123;
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    byte[] responseBytes = dataTable.toBytes();
    String serverId = SERVER_INSTANCE.getInstanceId();

    // Start the server
    _queryServer = getQueryServer(0, responseBytes);
    _queryServer.start();

    long startTimeMs = System.currentTimeMillis();
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId + 1, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, 1_000L);
    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 1);
    assertTrue(response.containsKey(OFFLINE_SERVER_ROUTING_INSTANCE));
    ServerResponse serverResponse = response.get(OFFLINE_SERVER_ROUTING_INSTANCE);
    assertNull(serverResponse.getDataTable());
    assertEquals(serverResponse.getResponseDelayMs(), -1);
    assertEquals(serverResponse.getResponseSize(), 0);
    assertEquals(serverResponse.getDeserializationTimeMs(), 0);
    // Query should time out
    assertTrue(System.currentTimeMillis() - startTimeMs >= 1000);
    _requestCount += 2;
    waitForStatsUpdate(_requestCount);
    assertEquals(_serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverId).intValue(), 0);
  }

  @Test
  public void testServerDown()
      throws Exception {
    long requestId = 123;
    // To avoid flakyness, set timeoutMs to 2000 msec. For some test runs, it can take up to
    // 1400 msec to mark request as failed.
    long timeoutMs = 2000L;
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    byte[] responseBytes = dataTable.toBytes();
    String serverId = SERVER_INSTANCE.getInstanceId();

    // Start the server
    _queryServer = getQueryServer(500, responseBytes);
    _queryServer.start();

    long startTimeMs = System.currentTimeMillis();
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId + 1, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, timeoutMs);

    // Shut down the server before getting the response
    _queryServer.shutDown();

    try {
      assertFalse(_queryServer.getChannel().isOpen());
      assertFalse(_queryServer.getChannel().isActive());

      Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
      assertEquals(response.size(), 1);
      assertTrue(response.containsKey(OFFLINE_SERVER_ROUTING_INSTANCE));
      ServerResponse serverResponse = response.get(OFFLINE_SERVER_ROUTING_INSTANCE);
      assertNull(serverResponse.getDataTable());
      assertEquals(serverResponse.getResponseDelayMs(), -1);
      assertEquals(serverResponse.getResponseSize(), 0);
      assertEquals(serverResponse.getDeserializationTimeMs(), 0);
      // Query should early terminate
      assertTrue(System.currentTimeMillis() - startTimeMs < timeoutMs);
      _requestCount += 2;
      waitForStatsUpdate(_requestCount);
      assertEquals(_serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverId).intValue(), 0);

      // Submit query after server is down
      startTimeMs = System.currentTimeMillis();
      asyncQueryResponse =
          _queryRouter.submitQuery(requestId + 1, "testTable", BROKER_REQUEST, ROUTING_TABLE, null, null, timeoutMs);
      response = asyncQueryResponse.getFinalResponses();
      assertEquals(response.size(), 1);
      assertTrue(response.containsKey(OFFLINE_SERVER_ROUTING_INSTANCE));
      serverResponse = response.get(OFFLINE_SERVER_ROUTING_INSTANCE);
      assertNull(serverResponse.getDataTable());
      assertEquals(serverResponse.getSubmitDelayMs(), -1);
      assertEquals(serverResponse.getResponseDelayMs(), -1);
      assertEquals(serverResponse.getResponseSize(), 0);
      assertEquals(serverResponse.getDeserializationTimeMs(), 0);
      // Query should early terminate
      assertTrue(System.currentTimeMillis() - startTimeMs < timeoutMs);
      _requestCount += 2;
      waitForStatsUpdate(_requestCount);
      assertEquals(_serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverId).intValue(), 0);
    } finally {
      // To be sure we don't close it again on the @AfterMethod method
      _queryServer = null;
    }
  }

  @Test
  public void testSkipUnavailableServer()
      throws IOException, InterruptedException {
    // Using a different port is a hack to avoid resource conflict with other tests, ideally _queryServer.shutdown()
    // should ensure there is no possibility of resource conflict.
    int port = 12346;
    ServerInstance serverInstance1 = new ServerInstance("localhost", port);
    ServerInstance serverInstance2 = new ServerInstance("localhost", port + 1);
    ServerRoutingInstance serverRoutingInstance1 =
        serverInstance1.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
    ServerRoutingInstance serverRoutingInstance2 =
        serverInstance2.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
    Map<ServerInstance, ServerRouteInfo> routingTable =
        Map.of(serverInstance1, new ServerRouteInfo(Collections.emptyList(), Collections.emptyList()),
            serverInstance2, new ServerRouteInfo(Collections.emptyList(), Collections.emptyList()));

    long requestId = 123;
    DataSchema dataSchema =
        new DataSchema(new String[]{"column1"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING});
    DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    builder.startRow();
    builder.setColumn(0, "value1");
    builder.finishRow();
    DataTable dataTableSuccess = builder.build();
    Map<String, String> dataTableMetadata = dataTableSuccess.getMetadata();
    dataTableMetadata.put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    byte[] successResponseBytes = dataTableSuccess.toBytes();

    // Only start a single QueryServer, on port from serverInstance1
    _queryServer = getQueryServer(500, successResponseBytes, port);
    _queryServer.start();

    // Submit the query with skipUnavailableServers=true, the single started server should return a valid response
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.compileToBrokerRequest("SET skipUnavailableServers=true; SELECT * FROM testTable");
    long startTime = System.currentTimeMillis();
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", brokerRequest, routingTable, null, null, 10_000L);
    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 2);
    assertTrue(response.containsKey(serverRoutingInstance1));
    assertTrue(response.containsKey(serverRoutingInstance2));

    ServerResponse serverResponse1 = response.get(serverRoutingInstance1);
    ServerResponse serverResponse2 = response.get(serverRoutingInstance2);
    assertNotNull(serverResponse1.getDataTable());
    assertNull(serverResponse2.getDataTable());
    assertTrue(serverResponse1.getResponseDelayMs() > 500);   // > response delay set by getQueryServer
    assertTrue(serverResponse2.getResponseDelayMs() < 100);   // connection refused, no delay
    assertTrue(System.currentTimeMillis() - startTime > 500); // > response delay set by getQueryServer
    _requestCount += 4;
    waitForStatsUpdate(_requestCount);
    assertEquals(
        _serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverInstance1.getInstanceId()).intValue(), 0);
    assertEquals(
        _serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverInstance2.getInstanceId()).intValue(), 0);

    // Submit the same query without skipUnavailableServers, the servers should not return any response
    brokerRequest = CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM testTable");
    startTime = System.currentTimeMillis();
    asyncQueryResponse =
        _queryRouter.submitQuery(requestId, "testTable", brokerRequest, routingTable, null, null, 10_000L);
    response = asyncQueryResponse.getFinalResponses();
    assertEquals(response.size(), 2);
    assertTrue(response.containsKey(serverRoutingInstance1));
    assertTrue(response.containsKey(serverRoutingInstance2));

    serverResponse1 = response.get(serverRoutingInstance1);
    serverResponse2 = response.get(serverRoutingInstance2);
    assertNull(serverResponse1.getDataTable());
    assertNull(serverResponse2.getDataTable());
    assertTrue(serverResponse1.getResponseDelayMs() < 100);
    assertTrue(serverResponse2.getResponseDelayMs() < 100);
    assertTrue(System.currentTimeMillis() - startTime < 100);
    _requestCount += 4;
    waitForStatsUpdate(_requestCount);
    assertEquals(
        _serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverInstance1.getInstanceId()).intValue(), 0);
    assertEquals(
        _serverRoutingStatsManager.fetchNumInFlightRequestsForServer(serverInstance2.getInstanceId()).intValue(), 0);
  }

  @Test
  public void testNoHedgeSentWhenDisabled()
      throws Exception {
    FakeScheduledExecutorService scheduler = new FakeScheduledExecutorService();
    MutableClock clock = new MutableClock(1_000L);
    ServerRoutingStatsManager statsManager = mock(ServerRoutingStatsManager.class);
    HedgeBudgetManager hedgeBudgetManager = mock(HedgeBudgetManager.class);
    QueryRouter queryRouter =
        createQueryRouter(createHedgingConfig(false, 25L, 500L), hedgeBudgetManager, scheduler, clock, statsManager);
    ServerChannels serverChannels = mock(ServerChannels.class);
    setServerChannels(queryRouter, serverChannels);

    AsyncQueryResponse asyncQueryResponse = queryRouter.submitQuery(1L, "testTable",
        createOfflineRoute(Map.of(new ServerInstance("primary-disabled", 14000), new ServerInstance("hedge-disabled",
            14001))), 1_000L);

    assertEquals(scheduler.getScheduledTaskCount(), 0);
    verify(serverChannels, times(1)).sendRequest(any(), any(), any(), any(), anyLong());
    asyncQueryResponse.expireAtDeadline();
  }

  @Test
  public void testHedgingEligibilityExcludesUnsupportedQueriesAndRoutes() {
    ImplicitHybridTableRouteInfo route = createOfflineRoute(Collections.emptyMap());
    PinotQuery eligibleQuery = BROKER_REQUEST.getPinotQuery().deepCopy();
    assertTrue(QueryRouter.isEligibleForHedging(route, eligibleQuery));

    PinotQuery explainQuery = eligibleQuery.deepCopy();
    explainQuery.setExplain(true);
    assertFalse(QueryRouter.isEligibleForHedging(route, explainQuery));

    PinotQuery secondaryQuery = eligibleQuery.deepCopy();
    secondaryQuery.putToQueryOptions(
        CommonConstants.Broker.Request.QueryOptionKey.IS_SECONDARY_WORKLOAD, Boolean.TRUE.toString());
    assertFalse(QueryRouter.isEligibleForHedging(route, secondaryQuery));
    assertFalse(QueryRouter.isEligibleForHedging(mock(TableRouteInfo.class), eligibleQuery));
  }

  @Test
  public void testNoHedgeSentWhenNoAlternateExists()
      throws Exception {
    FakeScheduledExecutorService scheduler = new FakeScheduledExecutorService();
    MutableClock clock = new MutableClock(1_000L);
    ServerRoutingStatsManager statsManager = mock(ServerRoutingStatsManager.class);
    HedgeBudgetManager hedgeBudgetManager = mock(HedgeBudgetManager.class);
    QueryRouter queryRouter =
        createQueryRouter(createHedgingConfig(true, 25L, 500L), hedgeBudgetManager, scheduler, clock, statsManager);
    ServerChannels serverChannels = mock(ServerChannels.class);
    setServerChannels(queryRouter, serverChannels);
    Map<ServerInstance, ServerInstance> primaryOnlyRoute = new HashMap<>();
    primaryOnlyRoute.put(new ServerInstance("primary-only", 14010), null);

    AsyncQueryResponse asyncQueryResponse =
        queryRouter.submitQuery(2L, "testTable", createOfflineRoute(primaryOnlyRoute), 1_000L);

    assertEquals(scheduler.getScheduledTaskCount(), 0);
    verify(serverChannels, times(1)).sendRequest(any(), any(), any(), any(), anyLong());
    asyncQueryResponse.expireAtDeadline();
  }

  @Test
  public void testHedgeSchedulerRejectionDoesNotFailPrimary()
      throws Exception {
    long requestId = 11L;
    ServerInstance primaryServer = new ServerInstance("primary-scheduler-rejected", 14015);
    ServerInstance hedgeServer = new ServerInstance("hedge-scheduler-rejected", 14016);
    ServerRoutingInstance primaryRoutingInstance =
        primaryServer.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    when(scheduler.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
        .thenThrow(new RejectedExecutionException("scheduler stopped"));
    MutableClock clock = new MutableClock(1_000L);
    ServerRoutingStatsManager statsManager = mock(ServerRoutingStatsManager.class);
    HedgeBudgetManager hedgeBudgetManager = mock(HedgeBudgetManager.class);
    QueryRouter queryRouter =
        createQueryRouter(createHedgingConfig(true, 25L, 500L), hedgeBudgetManager, scheduler, clock, statsManager);
    ServerChannels serverChannels = mock(ServerChannels.class);
    setServerChannels(queryRouter, serverChannels);

    AsyncQueryResponse asyncQueryResponse = queryRouter.submitQuery(requestId, "testTable",
        createOfflineRoute(Map.of(primaryServer, hedgeServer)), 1_000L);
    asyncQueryResponse.receiveDataTable(primaryRoutingInstance, createDataTable(requestId), 17, 2);

    Map<ServerRoutingInstance, ServerResponse> finalResponses = asyncQueryResponse.getFinalResponses();
    assertEquals(asyncQueryResponse.getStatus(), QueryResponse.Status.COMPLETED);
    assertTrue(finalResponses.containsKey(primaryRoutingInstance));
    verify(serverChannels, times(1)).sendRequest(any(), any(), any(), any(), anyLong());
    verify(hedgeBudgetManager, never()).tryAcquire();
  }

  @Test
  public void testHedgeSenderRejectionDoesNotFailPrimary()
      throws Exception {
    long requestId = 14L;
    ServerInstance primaryServer = new ServerInstance("primary-sender-rejected", 14017);
    ServerInstance hedgeServer = new ServerInstance("hedge-sender-rejected", 14018);
    ServerRoutingInstance primaryRoutingInstance =
        primaryServer.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
    FakeScheduledExecutorService scheduler = new FakeScheduledExecutorService();
    MutableClock clock = new MutableClock(1_000L);
    ServerRoutingStatsManager statsManager = mock(ServerRoutingStatsManager.class);
    HedgeBudgetManager hedgeBudgetManager = mock(HedgeBudgetManager.class);
    QueryRouter queryRouter = new QueryRouter("testBroker", null, null, statsManager,
        ThreadAccountantUtils.getNoOpAccountant(), 2, createHedgingConfig(true, 25L, 500L), hedgeBudgetManager,
        scheduler, command -> {
          throw new RejectedExecutionException("sender saturated");
        }, clock, ignored -> {
        });
    ServerChannels serverChannels = mock(ServerChannels.class);
    setServerChannels(queryRouter, serverChannels);

    AsyncQueryResponse asyncQueryResponse = queryRouter.submitQuery(requestId, "testTable",
        createOfflineRoute(Map.of(primaryServer, hedgeServer)), 1_000L);
    scheduler.runNext();
    asyncQueryResponse.receiveDataTable(primaryRoutingInstance, createDataTable(requestId), 17, 2);

    Map<ServerRoutingInstance, ServerResponse> finalResponses = asyncQueryResponse.getFinalResponses();
    assertEquals(asyncQueryResponse.getStatus(), QueryResponse.Status.COMPLETED);
    assertTrue(finalResponses.containsKey(primaryRoutingInstance));
    verify(serverChannels, times(1)).sendRequest(any(), any(), any(), any(), anyLong());
    verify(hedgeBudgetManager, never()).tryAcquire();
  }

  @Test
  public void testNoHedgeSentWhenNoOutstandingPrimaryAtDecisionTime()
      throws Exception {
    ServerInstance primaryServer = new ServerInstance("primary-finished", 14020);
    ServerInstance hedgeServer = new ServerInstance("hedge-finished", 14021);
    ServerRoutingInstance primaryRoutingInstance =
        primaryServer.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
    FakeScheduledExecutorService scheduler = new FakeScheduledExecutorService();
    MutableClock clock = new MutableClock(1_000L);
    ServerRoutingStatsManager statsManager = mock(ServerRoutingStatsManager.class);
    HedgeBudgetManager hedgeBudgetManager = mock(HedgeBudgetManager.class);
    QueryRouter queryRouter =
        createQueryRouter(createHedgingConfig(true, 25L, 500L), hedgeBudgetManager, scheduler, clock, statsManager);
    ServerChannels serverChannels = mock(ServerChannels.class);
    setServerChannels(queryRouter, serverChannels);

    AsyncQueryResponse asyncQueryResponse =
        queryRouter.submitQuery(3L, "testTable", createOfflineRoute(Map.of(primaryServer, hedgeServer)), 1_000L);
    asyncQueryResponse.receiveDataTable(primaryRoutingInstance, createDataTable(3L), 17, 2);

    assertEquals(scheduler.getScheduledTaskCount(), 1);
    scheduler.runNext();

    verify(hedgeBudgetManager, never()).tryAcquire();
    verify(serverChannels, times(1)).sendRequest(any(), any(), any(), any(), anyLong());
    asyncQueryResponse.getFinalResponses();
  }

  @Test
  public void testQueryCancellationSuppressesPendingHedge()
      throws Exception {
    long requestId = 13L;
    FakeScheduledExecutorService scheduler = new FakeScheduledExecutorService();
    MutableClock clock = new MutableClock(1_000L);
    ServerRoutingStatsManager statsManager = mock(ServerRoutingStatsManager.class);
    HedgeBudgetManager hedgeBudgetManager = mock(HedgeBudgetManager.class);
    QueryRouter queryRouter =
        createQueryRouter(createHedgingConfig(true, 25L, 500L), hedgeBudgetManager, scheduler, clock, statsManager);
    ServerChannels serverChannels = mock(ServerChannels.class);
    setServerChannels(queryRouter, serverChannels);

    AsyncQueryResponse asyncQueryResponse = queryRouter.submitQuery(requestId, "testTable",
        createOfflineRoute(Map.of(new ServerInstance("primary-cancelled", 14025),
            new ServerInstance("hedge-cancelled", 14026))), 1_000L);
    queryRouter.cancelHedging(requestId);
    scheduler.runNext();

    verify(hedgeBudgetManager, never()).tryAcquire();
    verify(serverChannels, times(1)).sendRequest(any(), any(), any(), any(), anyLong());
    asyncQueryResponse.expireAtDeadline();
  }

  @Test
  public void testNoHedgeSentWhenRatioBudgetBlocksIt()
      throws Exception {
    assertBudgetAdmissionSkipsHedge(AdmissionResult.RATIO_LIMIT, 4L, "primary-ratio", "hedge-ratio", 14030);
  }

  @Test
  public void testNoHedgeSentWhenConcurrencyBudgetBlocksIt()
      throws Exception {
    assertBudgetAdmissionSkipsHedge(AdmissionResult.CONCURRENCY_LIMIT, 5L, "primary-concurrency",
        "hedge-concurrency", 14040);
  }

  @Test
  public void testHedgeDelayUsesRemainingTimeAndClamps()
      throws Exception {
    assertScheduledHedgeDelay(1_000L, 500L);
    assertScheduledHedgeDelay(40L, 25L);
    assertScheduledHedgeDelay(2_000L, 500L);
  }

  @Test
  public void testOneHedgeMaximumSelectsWorstScoreThenLatency()
      throws Exception {
    ServerInstance primaryA = new ServerInstance("primary-a", 14100);
    ServerInstance primaryB = new ServerInstance("primary-b", 14110);
    ServerInstance primaryC = new ServerInstance("primary-c", 14120);
    ServerInstance hedgeA = new ServerInstance("hedge-a", 14101);
    ServerInstance hedgeB = new ServerInstance("hedge-b", 14111);
    ServerInstance hedgeC = new ServerInstance("hedge-c", 14121);
    ServerRoutingInstance hedgeBRouting =
        hedgeB.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
    FakeScheduledExecutorService scheduler = new FakeScheduledExecutorService();
    MutableClock clock = new MutableClock(1_000L);
    ServerRoutingStatsManager statsManager = mock(ServerRoutingStatsManager.class);
    when(statsManager.fetchConfiguredScoreForServer(primaryA.getInstanceId())).thenReturn(10d);
    when(statsManager.fetchConfiguredScoreForServer(primaryB.getInstanceId())).thenReturn(10d);
    when(statsManager.fetchConfiguredScoreForServer(primaryC.getInstanceId())).thenReturn(9d);
    when(statsManager.fetchEMALatencyForServer(primaryA.getInstanceId())).thenReturn(20d);
    when(statsManager.fetchEMALatencyForServer(primaryB.getInstanceId())).thenReturn(30d);
    when(statsManager.fetchEMALatencyForServer(primaryC.getInstanceId())).thenReturn(999d);
    HedgeBudgetManager hedgeBudgetManager = mock(HedgeBudgetManager.class);
    when(hedgeBudgetManager.tryAcquire()).thenReturn(AdmissionResult.ACQUIRED);
    QueryRouter queryRouter =
        createQueryRouter(createHedgingConfig(true, 25L, 500L), hedgeBudgetManager, scheduler, clock, statsManager);
    ServerChannels serverChannels = mock(ServerChannels.class);
    setServerChannels(queryRouter, serverChannels);

    AsyncQueryResponse asyncQueryResponse = queryRouter.submitQuery(6L, "testTable", createOfflineRoute(
        Map.of(primaryA, hedgeA, primaryB, hedgeB, primaryC, hedgeC)), 1_000L);
    scheduler.runNext();

    verify(serverChannels, times(4)).sendRequest(any(), any(), any(), any(), anyLong());
    verify(serverChannels).sendRequest(eq("testTable"), any(), eq(hedgeBRouting), any(), anyLong());
    asyncQueryResponse.expireAtDeadline();
  }

  @Test
  public void testOneHedgeMaximumUsesStableIdentityWhenScoreAndLatencyTie()
      throws Exception {
    ServerInstance primaryA = new ServerInstance("identity-a", 14130);
    ServerInstance primaryB = new ServerInstance("identity-b", 14140);
    ServerInstance hedgeA = new ServerInstance("identity-hedge-a", 14131);
    ServerInstance hedgeB = new ServerInstance("identity-hedge-b", 14141);
    ServerRoutingInstance hedgeARouting =
        hedgeA.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
    FakeScheduledExecutorService scheduler = new FakeScheduledExecutorService();
    MutableClock clock = new MutableClock(1_000L);
    ServerRoutingStatsManager statsManager = mock(ServerRoutingStatsManager.class);
    when(statsManager.fetchConfiguredScoreForServer(primaryA.getInstanceId())).thenReturn(10d);
    when(statsManager.fetchConfiguredScoreForServer(primaryB.getInstanceId())).thenReturn(10d);
    when(statsManager.fetchEMALatencyForServer(primaryA.getInstanceId())).thenReturn(30d);
    when(statsManager.fetchEMALatencyForServer(primaryB.getInstanceId())).thenReturn(30d);
    HedgeBudgetManager hedgeBudgetManager = mock(HedgeBudgetManager.class);
    when(hedgeBudgetManager.tryAcquire()).thenReturn(AdmissionResult.ACQUIRED);
    QueryRouter queryRouter =
        createQueryRouter(createHedgingConfig(true, 25L, 500L), hedgeBudgetManager, scheduler, clock, statsManager);
    ServerChannels serverChannels = mock(ServerChannels.class);
    setServerChannels(queryRouter, serverChannels);

    AsyncQueryResponse asyncQueryResponse = queryRouter.submitQuery(7L, "testTable",
        createOfflineRoute(Map.of(primaryA, hedgeA, primaryB, hedgeB)), 1_000L);
    scheduler.runNext();

    verify(serverChannels, times(3)).sendRequest(any(), any(), any(), any(), anyLong());
    verify(serverChannels).sendRequest(eq("testTable"), any(), eq(hedgeARouting), any(), anyLong());
    asyncQueryResponse.expireAtDeadline();
  }

  @Test
  public void testHedgeSendFailureDoesNotFailOutstandingPrimaryAndReleasesConcurrency()
      throws Exception {
    ServerInstance primaryServer = new ServerInstance("primary-send-failure", 14150);
    ServerInstance hedgeServer = new ServerInstance("hedge-send-failure", 14151);
    ServerRoutingInstance primaryRoutingInstance =
        primaryServer.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
    ServerRoutingInstance hedgeRoutingInstance =
        hedgeServer.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
    FakeScheduledExecutorService scheduler = new FakeScheduledExecutorService();
    MutableClock clock = new MutableClock(1_000L);
    ServerRoutingStatsManager statsManager = mock(ServerRoutingStatsManager.class);
    HedgeBudgetManager hedgeBudgetManager = mock(HedgeBudgetManager.class);
    when(hedgeBudgetManager.tryAcquire()).thenReturn(AdmissionResult.ACQUIRED);
    QueryRouter queryRouter =
        createQueryRouter(createHedgingConfig(true, 25L, 500L), hedgeBudgetManager, scheduler, clock, statsManager);
    ServerChannels serverChannels = mock(ServerChannels.class);
    doThrow(new RuntimeException("hedge send failed")).when(serverChannels)
        .sendRequest(eq("testTable"), any(), eq(hedgeRoutingInstance), any(), anyLong());
    setServerChannels(queryRouter, serverChannels);

    AsyncQueryResponse asyncQueryResponse =
        queryRouter.submitQuery(8L, "testTable", createOfflineRoute(Map.of(primaryServer, hedgeServer)), 1_000L);
    scheduler.runNext();

    assertEquals(asyncQueryResponse.getStatus(), QueryResponse.Status.IN_PROGRESS);
    verify(hedgeBudgetManager).release();

    asyncQueryResponse.receiveDataTable(primaryRoutingInstance, createDataTable(8L), 17, 2);
    Map<ServerRoutingInstance, ServerResponse> finalResponses = asyncQueryResponse.getFinalResponses();

    assertEquals(asyncQueryResponse.getStatus(), QueryResponse.Status.COMPLETED);
    assertEquals(finalResponses.size(), 1);
    assertTrue(finalResponses.containsKey(primaryRoutingInstance));
    assertNull(asyncQueryResponse.getFailedServer());
  }

  @Test
  public void testHedgeRegistrationFailureReleasesConcurrency()
      throws Exception {
    long requestId = 9L;
    ServerInstance primaryServer = new ServerInstance("primary-registration-failure", 14160);
    ServerInstance hedgeServer = new ServerInstance("hedge-registration-failure", 14161);
    ServerRoutingInstance primaryRoutingInstance =
        primaryServer.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
    FakeScheduledExecutorService scheduler = new FakeScheduledExecutorService();
    MutableClock clock = new MutableClock(1_000L);
    ServerRoutingStatsManager statsManager = mock(ServerRoutingStatsManager.class);
    doThrow(new IllegalStateException("stats registration failed")).when(statsManager)
        .recordStatsForQuerySubmission(requestId, hedgeServer.getInstanceId());
    HedgeBudgetManager hedgeBudgetManager = mock(HedgeBudgetManager.class);
    when(hedgeBudgetManager.tryAcquire()).thenReturn(AdmissionResult.ACQUIRED);
    QueryRouter queryRouter =
        createQueryRouter(createHedgingConfig(true, 25L, 500L), hedgeBudgetManager, scheduler, clock, statsManager);
    ServerChannels serverChannels = mock(ServerChannels.class);
    setServerChannels(queryRouter, serverChannels);

    AsyncQueryResponse asyncQueryResponse = queryRouter.submitQuery(requestId, "testTable",
        createOfflineRoute(Map.of(primaryServer, hedgeServer)), 1_000L);
    scheduler.runNext();

    verify(hedgeBudgetManager).release();
    verify(serverChannels, times(1)).sendRequest(any(), any(), any(), any(), anyLong());
    assertEquals(asyncQueryResponse.getStatus(), QueryResponse.Status.IN_PROGRESS);

    asyncQueryResponse.receiveDataTable(primaryRoutingInstance, createDataTable(requestId), 17, 2);
    Map<ServerRoutingInstance, ServerResponse> finalResponses = asyncQueryResponse.getFinalResponses();

    assertEquals(asyncQueryResponse.getStatus(), QueryResponse.Status.COMPLETED);
    assertTrue(finalResponses.containsKey(primaryRoutingInstance));
  }

  @Test
  public void testDeadlineCleanupIsRetainedForLoserAndCancelledAfterAccounting()
      throws Exception {
    long requestId = 10L;
    ServerInstance primaryServer = new ServerInstance("primary-cleanup", 14170);
    ServerInstance hedgeServer = new ServerInstance("hedge-cleanup", 14171);
    ServerRoutingInstance primaryRoutingInstance =
        primaryServer.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
    ServerRoutingInstance hedgeRoutingInstance =
        hedgeServer.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
    FakeScheduledExecutorService scheduler = new FakeScheduledExecutorService();
    MutableClock clock = new MutableClock(1_000L);
    ServerRoutingStatsManager statsManager = mock(ServerRoutingStatsManager.class);
    HedgeBudgetManager hedgeBudgetManager = mock(HedgeBudgetManager.class);
    when(hedgeBudgetManager.tryAcquire()).thenReturn(AdmissionResult.ACQUIRED);
    QueryRouter queryRouter =
        createQueryRouter(createHedgingConfig(true, 25L, 500L), hedgeBudgetManager, scheduler, clock, statsManager);
    ServerChannels serverChannels = mock(ServerChannels.class);
    setServerChannels(queryRouter, serverChannels);

    AsyncQueryResponse asyncQueryResponse = queryRouter.submitQuery(requestId, "testTable",
        createOfflineRoute(Map.of(primaryServer, hedgeServer)), 1_000L);
    scheduler.runNext();

    assertEquals(scheduler.getScheduledTaskCount(), 1);
    FakeScheduledFuture deadlineCleanup = scheduler.getOnlyTask();
    asyncQueryResponse.receiveDataTable(hedgeRoutingInstance, createDataTable(requestId), 17, 2);
    asyncQueryResponse.getFinalResponses();

    assertFalse(deadlineCleanup.isCancelled());

    asyncQueryResponse.receiveDataTable(primaryRoutingInstance, createDataTable(requestId), 19, 2);

    assertTrue(deadlineCleanup.isCancelled());
    verify(statsManager, times(1)).recordStatsUponResponseArrival(eq(requestId),
        eq(primaryServer.getInstanceId()), anyLong());
  }

  @Test
  public void testLatePrimaryChannelFailureAfterHedgeWinNotifiesFailureDetector()
      throws Exception {
    long requestId = 12L;
    ServerInstance primaryServer = new ServerInstance("primary-late-channel-failure", 14180);
    ServerInstance hedgeServer = new ServerInstance("hedge-late-channel-failure", 14181);
    ServerRoutingInstance primaryRoutingInstance =
        primaryServer.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
    ServerRoutingInstance hedgeRoutingInstance =
        hedgeServer.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
    FakeScheduledExecutorService scheduler = new FakeScheduledExecutorService();
    MutableClock clock = new MutableClock(1_000L);
    ServerRoutingStatsManager statsManager = mock(ServerRoutingStatsManager.class);
    HedgeBudgetManager hedgeBudgetManager = mock(HedgeBudgetManager.class);
    when(hedgeBudgetManager.tryAcquire()).thenReturn(AdmissionResult.ACQUIRED);
    List<ServerRoutingInstance> failedServers = new ArrayList<>();
    QueryRouter queryRouter = new QueryRouter("testBroker", null, null, statsManager,
        ThreadAccountantUtils.getNoOpAccountant(), 2, createHedgingConfig(true, 25L, 500L), hedgeBudgetManager,
        scheduler, clock, failedServers::add);
    ServerChannels serverChannels = mock(ServerChannels.class);
    setServerChannels(queryRouter, serverChannels);

    AsyncQueryResponse asyncQueryResponse = queryRouter.submitQuery(requestId, "testTable",
        createOfflineRoute(Map.of(primaryServer, hedgeServer)), 1_000L);
    scheduler.runNext();
    asyncQueryResponse.receiveDataTable(hedgeRoutingInstance, createDataTable(requestId), 17, 2);
    asyncQueryResponse.getFinalResponses();

    queryRouter.markServerDown(primaryRoutingInstance, new IllegalStateException("primary channel down"));

    assertEquals(failedServers, List.of(primaryRoutingInstance));
  }

  @Test
  public void testHedgeRequestTimeoutIsBoundedByRemainingDeadline()
      throws Exception {
    ServerInstance primaryServer = new ServerInstance("primary-hedge-timeout", 14190);
    ServerInstance hedgeServer = new ServerInstance("hedge-hedge-timeout", 14191);
    ServerRoutingInstance hedgeRoutingInstance =
        hedgeServer.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
    FakeScheduledExecutorService scheduler = new FakeScheduledExecutorService();
    MutableClock clock = new MutableClock(1_000L);
    ServerRoutingStatsManager statsManager = mock(ServerRoutingStatsManager.class);
    HedgeBudgetManager hedgeBudgetManager = mock(HedgeBudgetManager.class);
    when(hedgeBudgetManager.tryAcquire()).thenReturn(AdmissionResult.ACQUIRED);
    QueryRouter queryRouter =
        createQueryRouter(createHedgingConfig(true, 25L, 500L), hedgeBudgetManager, scheduler, clock, statsManager);
    ServerChannels serverChannels = mock(ServerChannels.class);
    setServerChannels(queryRouter, serverChannels);
    BrokerRequest brokerRequest = BROKER_REQUEST.deepCopy();
    brokerRequest.getPinotQuery().putToQueryOptions(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS, "1000");

    AsyncQueryResponse asyncQueryResponse = queryRouter.submitQuery(15L, "testTable",
        createOfflineRoute(brokerRequest, Map.of(primaryServer, hedgeServer)), 1_000L);
    clock._currentTimeMs += 400L;
    scheduler.runNext();

    ArgumentCaptor<InstanceRequest> hedgeRequestCaptor = ArgumentCaptor.forClass(InstanceRequest.class);
    verify(serverChannels).sendRequest(eq("testTable"), any(), eq(hedgeRoutingInstance),
        hedgeRequestCaptor.capture(), anyLong());
    assertEquals(hedgeRequestCaptor.getValue().getQuery().getPinotQuery().getQueryOptions()
        .get(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS), "600");
    assertEquals(brokerRequest.getPinotQuery().getQueryOptions()
        .get(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS), "1000");
    asyncQueryResponse.expireAtDeadline();
  }

  private void assertBudgetAdmissionSkipsHedge(AdmissionResult admissionResult, long requestId, String primaryHost,
      String hedgeHost, int basePort)
      throws Exception {
    ServerInstance primaryServer = new ServerInstance(primaryHost, basePort);
    ServerInstance hedgeServer = new ServerInstance(hedgeHost, basePort + 1);
    FakeScheduledExecutorService scheduler = new FakeScheduledExecutorService();
    MutableClock clock = new MutableClock(1_000L);
    ServerRoutingStatsManager statsManager = mock(ServerRoutingStatsManager.class);
    HedgeBudgetManager hedgeBudgetManager = mock(HedgeBudgetManager.class);
    when(hedgeBudgetManager.tryAcquire()).thenReturn(admissionResult);
    QueryRouter queryRouter =
        createQueryRouter(createHedgingConfig(true, 25L, 500L), hedgeBudgetManager, scheduler, clock, statsManager);
    ServerChannels serverChannels = mock(ServerChannels.class);
    setServerChannels(queryRouter, serverChannels);

    AsyncQueryResponse asyncQueryResponse =
        queryRouter.submitQuery(requestId, "testTable", createOfflineRoute(Map.of(primaryServer, hedgeServer)),
            1_000L);
    scheduler.runNext();

    verify(serverChannels, times(1)).sendRequest(any(), any(), any(), any(), anyLong());
    asyncQueryResponse.expireAtDeadline();
  }

  private void assertScheduledHedgeDelay(long timeoutMs, long expectedDelayMs)
      throws Exception {
    FakeScheduledExecutorService scheduler = new FakeScheduledExecutorService();
    MutableClock clock = new MutableClock(10_000L);
    ServerRoutingStatsManager statsManager = mock(ServerRoutingStatsManager.class);
    HedgeBudgetManager hedgeBudgetManager = mock(HedgeBudgetManager.class);
    QueryRouter queryRouter =
        createQueryRouter(createHedgingConfig(true, 25L, 500L), hedgeBudgetManager, scheduler, clock, statsManager);
    ServerChannels serverChannels = mock(ServerChannels.class);
    setServerChannels(queryRouter, serverChannels);

    AsyncQueryResponse asyncQueryResponse = queryRouter.submitQuery(timeoutMs, "testTable",
        createOfflineRoute(Map.of(new ServerInstance("primary-delay-" + timeoutMs, 14200 + (int) timeoutMs),
            new ServerInstance("hedge-delay-" + timeoutMs, 14300 + (int) timeoutMs))), timeoutMs);

    assertEquals(scheduler.getScheduledTaskCount(), 1);
    assertEquals(scheduler.getOnlyTaskDelayMs(), expectedDelayMs);
    asyncQueryResponse.expireAtDeadline();
  }

  private QueryRouter createQueryRouter(HedgingConfig hedgingConfig, HedgeBudgetManager hedgeBudgetManager,
      ScheduledExecutorService scheduler, LongSupplier currentTimeMillis, ServerRoutingStatsManager statsManager) {
    return new QueryRouter("testBroker", null, null, statsManager, ThreadAccountantUtils.getNoOpAccountant(), 2,
        hedgingConfig, hedgeBudgetManager, scheduler, currentTimeMillis);
  }

  private void setServerChannels(QueryRouter queryRouter, ServerChannels serverChannels)
      throws Exception {
    Field serverChannelsField = QueryRouter.class.getDeclaredField("_serverChannels");
    serverChannelsField.setAccessible(true);
    serverChannelsField.set(queryRouter, serverChannels);
  }

  private static HedgingConfig createHedgingConfig(boolean enabled, long minDelayMs, long maxDelayMs) {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_ENABLED, enabled);
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_RATIO, 0.5d);
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_MIN_MS, minDelayMs);
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_MAX_MS, maxDelayMs);
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_MAX_EXTRA_REQUEST_RATIO, 1.0d);
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_BUDGET_WINDOW_MS, 60_000L);
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_MAX_CONCURRENT_REQUESTS, 8);
    return HedgingConfig.fromPinotConfiguration(new PinotConfiguration(properties));
  }

  private static ImplicitHybridTableRouteInfo createOfflineRoute(
      Map<ServerInstance, ServerInstance> primaryToAlternate) {
    return createOfflineRoute(BROKER_REQUEST, primaryToAlternate);
  }

  private static ImplicitHybridTableRouteInfo createOfflineRoute(BrokerRequest brokerRequest,
      Map<ServerInstance, ServerInstance> primaryToAlternate) {
    Map<ServerInstance, ServerRouteInfo> routingTable = new HashMap<>();
    Map<ServerInstance, List<AlternateServerRouteInfo>> alternateRoutes = new HashMap<>();
    for (Map.Entry<ServerInstance, ServerInstance> entry : primaryToAlternate.entrySet()) {
      routingTable.put(entry.getKey(), new ServerRouteInfo(Collections.emptyList(), Collections.emptyList()));
      if (entry.getValue() != null) {
        alternateRoutes.put(entry.getKey(), List.of(new AlternateServerRouteInfo(entry.getValue(),
            new ServerRouteInfo(Collections.emptyList(), Collections.emptyList()))));
      }
    }
    ImplicitHybridTableRouteInfo route = new ImplicitHybridTableRouteInfo(brokerRequest, null, routingTable, null);
    if (!alternateRoutes.isEmpty()) {
      route.setOfflineAlternateRoutes(alternateRoutes);
    }
    return route;
  }

  private static DataTable createDataTable(long requestId)
      throws Exception {
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    dataTable.getMetadata().put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
    return dataTable;
  }

  private void waitForStatsUpdate(long taskCount) {
    TestUtils.waitForCondition(aVoid -> {
      return (_serverRoutingStatsManager.getCompletedTaskCount() == taskCount);
    }, 5L, 5000, "Failed to record stats for AdaptiveServerSelectorTest");
  }

  private static final class MutableClock implements LongSupplier {
    private long _currentTimeMs;

    private MutableClock(long currentTimeMs) {
      _currentTimeMs = currentTimeMs;
    }

    @Override
    public long getAsLong() {
      return _currentTimeMs;
    }
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
      FakeScheduledFuture future = new FakeScheduledFuture(command, unit.toMillis(delay));
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

    private int getScheduledTaskCount() {
      return _tasks.size();
    }

    private long getOnlyTaskDelayMs() {
      return _tasks.get(0)._delayMs;
    }

    private FakeScheduledFuture getOnlyTask() {
      return _tasks.get(0);
    }

    private void runNext() {
      _tasks.remove(0).run();
    }
  }

  private static final class FakeScheduledFuture implements ScheduledFuture<Object> {
    private final Runnable _command;
    private final long _delayMs;
    private boolean _cancelled;
    private boolean _done;

    private FakeScheduledFuture(Runnable command, long delayMs) {
      _command = command;
      _delayMs = delayMs;
    }

    private void run() {
      if (!_cancelled) {
        _command.run();
        _done = true;
      }
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(_delayMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
      return Long.compare(getDelay(TimeUnit.MILLISECONDS), other.getDelay(TimeUnit.MILLISECONDS));
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
