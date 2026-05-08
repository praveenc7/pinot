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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.routing.ServerRouteInfo;
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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/**
 * Tests for QueryRouter retry and skip behavior on send failures.
 */
public class QueryRouterSendRetryTest {
  private static final ServerInstance SERVER_INSTANCE = new ServerInstance("localhost", 12345);
  private static final ServerRoutingInstance OFFLINE_ROUTING_INSTANCE =
      SERVER_INSTANCE.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY);
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
}
