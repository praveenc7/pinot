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
package org.apache.pinot.broker.requesthandler;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.grpc.ServerGrpcQueryClient;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class GrpcBrokerRequestHandlerTest {

  @BeforeClass
  public void setUp() {
    BrokerMetrics.register(mock(BrokerMetrics.class));
    BrokerQueryEventListenerFactory.init(new PinotConfiguration());
  }

  // ========================
  // PinotServerStreamingQueryClient.cleanupStaleClients tests
  // ========================

  @Test
  public void testCleanupRemovesDecommissionedServer() {
    GrpcBrokerRequestHandler.PinotServerStreamingQueryClient queryClient = createQueryClient();

    ServerGrpcQueryClient mockClient1 = mock(ServerGrpcQueryClient.class);
    ServerGrpcQueryClient mockClient2 = mock(ServerGrpcQueryClient.class);
    ServerGrpcQueryClient mockClient3 = mock(ServerGrpcQueryClient.class);

    Map<String, ServerGrpcQueryClient> clientMap = queryClient.getGrpcQueryClientMap();
    clientMap.put("host1_8090", mockClient1);
    clientMap.put("host2_8090", mockClient2);
    clientMap.put("host3_8090", mockClient3);

    // Only host1 and host3 are still enabled
    Map<String, ServerInstance> enabledServers = new HashMap<>();
    enabledServers.put("Server_host1_8099", mockServerInstance("host1", 8090));
    enabledServers.put("Server_host3_8099", mockServerInstance("host3", 8090));

    queryClient.cleanupStaleClients(enabledServers);

    verify(mockClient2).close();
    verify(mockClient1, never()).close();
    verify(mockClient3, never()).close();
    assertEquals(clientMap.size(), 2);
    assertTrue(clientMap.containsKey("host1_8090"));
    assertFalse(clientMap.containsKey("host2_8090"));
    assertTrue(clientMap.containsKey("host3_8090"));
  }

  @Test
  public void testCleanupRemovesMultipleDecommissionedServers() {
    GrpcBrokerRequestHandler.PinotServerStreamingQueryClient queryClient = createQueryClient();

    ServerGrpcQueryClient mockClient1 = mock(ServerGrpcQueryClient.class);
    ServerGrpcQueryClient mockClient2 = mock(ServerGrpcQueryClient.class);
    ServerGrpcQueryClient mockClient3 = mock(ServerGrpcQueryClient.class);
    ServerGrpcQueryClient mockClient4 = mock(ServerGrpcQueryClient.class);

    Map<String, ServerGrpcQueryClient> clientMap = queryClient.getGrpcQueryClientMap();
    clientMap.put("host1_8090", mockClient1);
    clientMap.put("host2_8090", mockClient2);
    clientMap.put("host3_8090", mockClient3);
    clientMap.put("host4_8090", mockClient4);

    // Only host1 remains; host2, host3, host4 all decommissioned
    Map<String, ServerInstance> enabledServers = new HashMap<>();
    enabledServers.put("Server_host1_8099", mockServerInstance("host1", 8090));

    queryClient.cleanupStaleClients(enabledServers);

    verify(mockClient1, never()).close();
    verify(mockClient2).close();
    verify(mockClient3).close();
    verify(mockClient4).close();
    assertEquals(clientMap.size(), 1);
    assertTrue(clientMap.containsKey("host1_8090"));
  }

  @Test
  public void testCleanupDistinguishesSameHostDifferentPorts() {
    GrpcBrokerRequestHandler.PinotServerStreamingQueryClient queryClient = createQueryClient();

    ServerGrpcQueryClient clientPort8090 = mock(ServerGrpcQueryClient.class);
    ServerGrpcQueryClient clientPort9090 = mock(ServerGrpcQueryClient.class);

    Map<String, ServerGrpcQueryClient> clientMap = queryClient.getGrpcQueryClientMap();
    clientMap.put("host1_8090", clientPort8090);
    clientMap.put("host1_9090", clientPort9090);

    // Only port 8090 is still enabled
    Map<String, ServerInstance> enabledServers = new HashMap<>();
    enabledServers.put("Server_host1_8099", mockServerInstance("host1", 8090));

    queryClient.cleanupStaleClients(enabledServers);

    verify(clientPort8090, never()).close();
    verify(clientPort9090).close();
    assertEquals(clientMap.size(), 1);
    assertTrue(clientMap.containsKey("host1_8090"));
    assertFalse(clientMap.containsKey("host1_9090"));
  }

  @Test
  public void testCleanupWithEmptyEnabledServersRemovesAll() {
    GrpcBrokerRequestHandler.PinotServerStreamingQueryClient queryClient = createQueryClient();

    ServerGrpcQueryClient mockClient1 = mock(ServerGrpcQueryClient.class);
    ServerGrpcQueryClient mockClient2 = mock(ServerGrpcQueryClient.class);

    Map<String, ServerGrpcQueryClient> clientMap = queryClient.getGrpcQueryClientMap();
    clientMap.put("host1_8090", mockClient1);
    clientMap.put("host2_8090", mockClient2);

    queryClient.cleanupStaleClients(Collections.emptyMap());

    verify(mockClient1).close();
    verify(mockClient2).close();
    assertTrue(clientMap.isEmpty());
  }

  @Test
  public void testCleanupNoOpWhenAllServersActive() {
    GrpcBrokerRequestHandler.PinotServerStreamingQueryClient queryClient = createQueryClient();

    ServerGrpcQueryClient mockClient1 = mock(ServerGrpcQueryClient.class);

    Map<String, ServerGrpcQueryClient> clientMap = queryClient.getGrpcQueryClientMap();
    clientMap.put("host1_8090", mockClient1);

    Map<String, ServerInstance> enabledServers = new HashMap<>();
    enabledServers.put("Server_host1_8099", mockServerInstance("host1", 8090));

    queryClient.cleanupStaleClients(enabledServers);

    verify(mockClient1, never()).close();
    assertEquals(clientMap.size(), 1);
  }

  @Test
  public void testCleanupNoOpWhenClientMapEmpty() {
    GrpcBrokerRequestHandler.PinotServerStreamingQueryClient queryClient = createQueryClient();

    Map<String, ServerInstance> enabledServers = new HashMap<>();
    enabledServers.put("Server_host1_8099", mockServerInstance("host1", 8090));

    queryClient.cleanupStaleClients(enabledServers);

    assertTrue(queryClient.getGrpcQueryClientMap().isEmpty());
  }

  @Test
  public void testCleanupIsIdempotent() {
    GrpcBrokerRequestHandler.PinotServerStreamingQueryClient queryClient = createQueryClient();

    ServerGrpcQueryClient mockClient1 = mock(ServerGrpcQueryClient.class);
    ServerGrpcQueryClient mockClient2 = mock(ServerGrpcQueryClient.class);

    Map<String, ServerGrpcQueryClient> clientMap = queryClient.getGrpcQueryClientMap();
    clientMap.put("host1_8090", mockClient1);
    clientMap.put("host2_8090", mockClient2);

    Map<String, ServerInstance> enabledServers = new HashMap<>();
    enabledServers.put("Server_host1_8099", mockServerInstance("host1", 8090));

    queryClient.cleanupStaleClients(enabledServers);
    assertEquals(clientMap.size(), 1);
    verify(mockClient2).close();

    // Second call should be a no-op — host2 is already gone
    queryClient.cleanupStaleClients(enabledServers);
    assertEquals(clientMap.size(), 1);
    assertTrue(clientMap.containsKey("host1_8090"));
  }

  @Test
  public void testCleanupContinuesWhenCloseThrows() {
    GrpcBrokerRequestHandler.PinotServerStreamingQueryClient queryClient = createQueryClient();

    ServerGrpcQueryClient failingClient = mock(ServerGrpcQueryClient.class);
    doThrow(new RuntimeException("channel close failed")).when(failingClient).close();
    ServerGrpcQueryClient normalClient = mock(ServerGrpcQueryClient.class);

    Map<String, ServerGrpcQueryClient> clientMap = queryClient.getGrpcQueryClientMap();
    clientMap.put("host1_8090", failingClient);
    clientMap.put("host2_8090", normalClient);

    // All servers decommissioned — both should be removed even if one close() throws
    queryClient.cleanupStaleClients(Collections.emptyMap());

    verify(failingClient).close();
    verify(normalClient).close();
    assertTrue(clientMap.isEmpty());
  }

  // ========================
  // retryUnhealthyServer integration tests
  // ========================

  @Test
  public void testRetryUnhealthyServerCleansUpStaleClients() {
    // Capture the retrier function registered with the failure detector
    FailureDetector failureDetector = mock(FailureDetector.class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Function<String, FailureDetector.ServerState>> retrierCaptor =
        ArgumentCaptor.forClass(Function.class);

    RoutingManager routingManager = mock(RoutingManager.class);
    Map<String, ServerInstance> enabledServers = new HashMap<>();
    enabledServers.put("Server_host1_8099", mockServerInstance("host1", 8090));
    when(routingManager.getEnabledServerInstanceMap()).thenReturn(enabledServers);

    GrpcBrokerRequestHandler handler = createHandler(routingManager, failureDetector);
    verify(failureDetector).registerUnhealthyServerRetrier(retrierCaptor.capture());
    Function<String, FailureDetector.ServerState> retrier = retrierCaptor.getValue();

    // Inject a stale client for a decommissioned server
    Map<String, ServerGrpcQueryClient> clientMap = handler.getStreamingQueryClient().getGrpcQueryClientMap();
    ServerGrpcQueryClient staleClient = mock(ServerGrpcQueryClient.class);
    clientMap.put("host2_8090", staleClient);

    // Retrying the decommissioned server should clean up its client and return UNHEALTHY
    FailureDetector.ServerState result = retrier.apply("Server_host2_8099");

    assertEquals(result, FailureDetector.ServerState.UNHEALTHY);
    verify(staleClient).close();
    assertFalse(clientMap.containsKey("host2_8090"));
  }

  @Test
  public void testRetryCleanupAndHealthyReturnInSameCall() {
    FailureDetector failureDetector = mock(FailureDetector.class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Function<String, FailureDetector.ServerState>> retrierCaptor =
        ArgumentCaptor.forClass(Function.class);

    RoutingManager routingManager = mock(RoutingManager.class);
    Map<String, ServerInstance> enabledServers = new HashMap<>();
    enabledServers.put("Server_host1_8099", mockServerInstance("host1", 8090));
    when(routingManager.getEnabledServerInstanceMap()).thenReturn(enabledServers);

    GrpcBrokerRequestHandler handler = createHandler(routingManager, failureDetector);
    verify(failureDetector).registerUnhealthyServerRetrier(retrierCaptor.capture());
    Function<String, FailureDetector.ServerState> retrier = retrierCaptor.getValue();

    Map<String, ServerGrpcQueryClient> clientMap = handler.getStreamingQueryClient().getGrpcQueryClientMap();

    // Inject a stale client for a decommissioned server
    ServerGrpcQueryClient staleClient = mock(ServerGrpcQueryClient.class);
    clientMap.put("host2_8090", staleClient);

    // Inject a healthy client for the server being retried
    ServerGrpcQueryClient healthyClient = mock(ServerGrpcQueryClient.class);
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    when(healthyClient.getChannel()).thenReturn(mockChannel);
    when(mockChannel.getState(true)).thenReturn(ConnectivityState.READY);
    clientMap.put("host1_8090", healthyClient);

    // Retrying host1: should clean up host2's stale client AND return HEALTHY for host1
    FailureDetector.ServerState result = retrier.apply("Server_host1_8099");

    assertEquals(result, FailureDetector.ServerState.HEALTHY);
    verify(staleClient).close();
    assertFalse(clientMap.containsKey("host2_8090"));
    // host1's client should still be there, not closed
    verify(healthyClient, never()).close();
    assertTrue(clientMap.containsKey("host1_8090"));
  }

  @Test
  public void testRetryReturnsUnhealthyWhenServerNotInRouting() {
    FailureDetector failureDetector = mock(FailureDetector.class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Function<String, FailureDetector.ServerState>> retrierCaptor =
        ArgumentCaptor.forClass(Function.class);

    RoutingManager routingManager = mock(RoutingManager.class);
    when(routingManager.getEnabledServerInstanceMap()).thenReturn(Collections.emptyMap());

    createHandler(routingManager, failureDetector);
    verify(failureDetector).registerUnhealthyServerRetrier(retrierCaptor.capture());
    Function<String, FailureDetector.ServerState> retrier = retrierCaptor.getValue();

    assertEquals(retrier.apply("Server_host1_8099"), FailureDetector.ServerState.UNHEALTHY);
  }

  @Test
  public void testRetryReturnsUnknownWhenNoGrpcClient() {
    FailureDetector failureDetector = mock(FailureDetector.class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Function<String, FailureDetector.ServerState>> retrierCaptor =
        ArgumentCaptor.forClass(Function.class);

    RoutingManager routingManager = mock(RoutingManager.class);
    Map<String, ServerInstance> enabledServers = new HashMap<>();
    enabledServers.put("Server_host1_8099", mockServerInstance("host1", 8090));
    when(routingManager.getEnabledServerInstanceMap()).thenReturn(enabledServers);

    createHandler(routingManager, failureDetector);
    verify(failureDetector).registerUnhealthyServerRetrier(retrierCaptor.capture());
    Function<String, FailureDetector.ServerState> retrier = retrierCaptor.getValue();

    // Server is in routing but no gRPC client has been created for it
    assertEquals(retrier.apply("Server_host1_8099"), FailureDetector.ServerState.UNKNOWN);
  }

  @Test
  public void testRetryReturnsHealthyWhenChannelReady() {
    FailureDetector failureDetector = mock(FailureDetector.class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Function<String, FailureDetector.ServerState>> retrierCaptor =
        ArgumentCaptor.forClass(Function.class);

    RoutingManager routingManager = mock(RoutingManager.class);
    Map<String, ServerInstance> enabledServers = new HashMap<>();
    enabledServers.put("Server_host1_8099", mockServerInstance("host1", 8090));
    when(routingManager.getEnabledServerInstanceMap()).thenReturn(enabledServers);

    GrpcBrokerRequestHandler handler = createHandler(routingManager, failureDetector);
    verify(failureDetector).registerUnhealthyServerRetrier(retrierCaptor.capture());
    Function<String, FailureDetector.ServerState> retrier = retrierCaptor.getValue();

    // Inject a client with a READY channel
    ServerGrpcQueryClient mockClient = mock(ServerGrpcQueryClient.class);
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    when(mockClient.getChannel()).thenReturn(mockChannel);
    when(mockChannel.getState(true)).thenReturn(ConnectivityState.READY);
    handler.getStreamingQueryClient().getGrpcQueryClientMap().put("host1_8090", mockClient);

    assertEquals(retrier.apply("Server_host1_8099"), FailureDetector.ServerState.HEALTHY);
  }

  @Test
  public void testRetryReturnsUnhealthyWhenChannelNotReady() {
    FailureDetector failureDetector = mock(FailureDetector.class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Function<String, FailureDetector.ServerState>> retrierCaptor =
        ArgumentCaptor.forClass(Function.class);

    RoutingManager routingManager = mock(RoutingManager.class);
    Map<String, ServerInstance> enabledServers = new HashMap<>();
    enabledServers.put("Server_host1_8099", mockServerInstance("host1", 8090));
    when(routingManager.getEnabledServerInstanceMap()).thenReturn(enabledServers);

    GrpcBrokerRequestHandler handler = createHandler(routingManager, failureDetector);
    verify(failureDetector).registerUnhealthyServerRetrier(retrierCaptor.capture());
    Function<String, FailureDetector.ServerState> retrier = retrierCaptor.getValue();

    // Inject a client with a TRANSIENT_FAILURE channel
    ServerGrpcQueryClient mockClient = mock(ServerGrpcQueryClient.class);
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    when(mockClient.getChannel()).thenReturn(mockChannel);
    when(mockChannel.getState(true)).thenReturn(ConnectivityState.TRANSIENT_FAILURE);
    handler.getStreamingQueryClient().getGrpcQueryClientMap().put("host1_8090", mockClient);

    assertEquals(retrier.apply("Server_host1_8099"), FailureDetector.ServerState.UNHEALTHY);
  }

  // ========================
  // Helpers
  // ========================

  private static GrpcBrokerRequestHandler.PinotServerStreamingQueryClient createQueryClient() {
    return new GrpcBrokerRequestHandler.PinotServerStreamingQueryClient(new GrpcConfig(Collections.emptyMap()));
  }

  private static ServerInstance mockServerInstance(String hostname, int grpcPort) {
    ServerInstance si = mock(ServerInstance.class);
    when(si.getHostname()).thenReturn(hostname);
    when(si.getGrpcPort()).thenReturn(grpcPort);
    return si;
  }

  private static GrpcBrokerRequestHandler createHandler(RoutingManager routingManager,
      FailureDetector failureDetector) {
    PinotConfiguration config = new PinotConfiguration();
    return new GrpcBrokerRequestHandler(config, "testBrokerId", new BrokerRequestIdGenerator(), routingManager,
        new AllowAllAccessControlFactory(), mock(QueryQuotaManager.class), mock(
        org.apache.pinot.common.config.provider.TableCache.class), failureDetector,
        ThreadAccountantUtils.getNoOpAccountant());
  }
}
