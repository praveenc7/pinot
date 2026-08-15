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
package org.apache.pinot.query.routing.table;

import java.util.Map;
import java.util.Set;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.transport.ImplicitHybridTableRouteInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.transport.TableRouteInfo;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class LogicalTableRouteProviderCalculateRouteTest extends BaseTableRouteTest {

  private void assertTableRoute(String tableName, String logicalTableName,
      Map<String, Set<String>> expectedOfflineRoutingTable, Map<String, Set<String>> expectedRealtimeRoutingTable,
      boolean isOfflineExpected, boolean isRealtimeExpected) {
    TableRouteInfo routeInfo = getLogicalTableRouteInfo(tableName, logicalTableName);
    BrokerRequestPair brokerRequestPair =
        getBrokerRequestPair(logicalTableName, routeInfo.hasOffline(), routeInfo.hasRealtime(),
            routeInfo.getOfflineTableName(), routeInfo.getRealtimeTableName());

    _logicalTableRouteProvider.calculateRoutes(routeInfo, _routingManager, brokerRequestPair._offlineBrokerRequest,
        brokerRequestPair._realtimeBrokerRequest, 0);
    LogicalTableRouteInfo logicalTableRouteInfo = (LogicalTableRouteInfo) routeInfo;

    if (isOfflineExpected) {
      assertNotNull(logicalTableRouteInfo.getOfflineTables());
      assertEquals(logicalTableRouteInfo.getOfflineTables().size(), 1);
      Map<ServerInstance, ServerRouteInfo> offlineRoutingTable =
          logicalTableRouteInfo.getOfflineTables().get(0).getOfflineRoutingTable();
      assertNotNull(offlineRoutingTable);
      assertRoutingTableEqual(offlineRoutingTable, expectedOfflineRoutingTable);
    } else {
      assertTrue(logicalTableRouteInfo.getOfflineExecutionServers().isEmpty());
    }

    if (isRealtimeExpected) {
      assertNotNull(logicalTableRouteInfo.getRealtimeTables());
      assertEquals(logicalTableRouteInfo.getRealtimeTables().size(), 1);
      Map<ServerInstance, ServerRouteInfo> realtimeRoutingTable =
          logicalTableRouteInfo.getRealtimeTables().get(0).getRealtimeRoutingTable();
      assertNotNull(realtimeRoutingTable);
      assertRoutingTableEqual(realtimeRoutingTable, expectedRealtimeRoutingTable);
    } else {
      assertTrue(logicalTableRouteInfo.getRealtimeExecutionServers().isEmpty());
    }

    if (!isOfflineExpected && !isRealtimeExpected) {
      assertTrue(routeInfo.getOfflineBrokerRequest() == null && routeInfo.getRealtimeBrokerRequest() == null);
    } else {
      assertFalse(routeInfo.getOfflineBrokerRequest() == null && routeInfo.getRealtimeBrokerRequest() == null);
      // Check requestMap
      Map<ServerRoutingInstance, InstanceRequest> requestMap = routeInfo.getRequestMap(0, "broker", false);
      assertNotNull(requestMap);
      assertFalse(requestMap.isEmpty());
    }

    if (routeInfo.isHybrid()) {
      assertNotNull(routeInfo.getTimeBoundaryInfo(), "Time boundary info should not be null for hybrid table");
    } else {
      assertNull(routeInfo.getTimeBoundaryInfo(), "Time boundary info should be null for non-hybrid table");
    }
  }

  @Test(dataProvider = "offlineTableAndRouteProvider")
  void testOfflineTableRoute(String tableName, Map<String, Set<String>> expectedRoutingTable) {
    assertTableRoute(tableName, "offlineTableAndRouteProvider", expectedRoutingTable, null, true, false);
  }

  @Test(dataProvider = "realtimeTableAndRouteProvider")
  void testRealtimeTableRoute(String tableName, Map<String, Set<String>> expectedRoutingTable) {
    assertTableRoute(tableName, "realtimeTableAndRouteProvider", null, expectedRoutingTable, false, true);
  }

  @Test(dataProvider = "hybridTableAndRouteProvider")
  void testHybridTableRoute(String tableName, Map<String, Set<String>> expectedOfflineRoutingTable,
      Map<String, Set<String>> expectedRealtimeRoutingTable) {
    assertTableRoute(tableName, "hybridTableAndRouteProvider", expectedOfflineRoutingTable,
        expectedRealtimeRoutingTable, expectedOfflineRoutingTable != null, expectedRealtimeRoutingTable != null);
  }

  @Test(dataProvider = "routeNotExistsProvider")
  void testRouteNotExists(String tableName) {
    assertTableRoute(tableName, "routeNotExistsProvider", null, null, false, false);
  }

  @Test(dataProvider = "partiallyDisabledTableAndRouteProvider")
  void testPartiallyDisabledTable(String tableName, Map<String, Set<String>> expectedOfflineRoutingTable,
      Map<String, Set<String>> expectedRealtimeRoutingTable) {
    assertTableRoute(tableName, "partiallyDisabledTableAndRouteProvider", expectedOfflineRoutingTable,
        expectedRealtimeRoutingTable, expectedOfflineRoutingTable != null, expectedRealtimeRoutingTable != null);
  }

  @Test(dataProvider = "disabledTableProvider")
  void testDisabledTable(String tableName) {
    assertTableRoute(tableName, "disabledTableProvider", null, null, false, false);
  }

  @Test
  void testLogicalTableRoutingUsesPrimaryOnlyPhysicalOverloads() {
    RoutingManager routingManager = mock(RoutingManager.class);
    BrokerRequest offlineBrokerRequest = mockBrokerRequest();
    BrokerRequest realtimeBrokerRequest = mockBrokerRequest();
    LogicalTableRouteInfo routeInfo = new LogicalTableRouteInfo();
    routeInfo.setLogicalTableName("logicalTable");
    ImplicitHybridTableRouteInfo offlinePhysicalTable = new ImplicitHybridTableRouteInfo();
    offlinePhysicalTable.setOfflineTableName("physicalTable_OFFLINE");
    offlinePhysicalTable.setOfflineRouteExists(true);
    ImplicitHybridTableRouteInfo realtimePhysicalTable = new ImplicitHybridTableRouteInfo();
    realtimePhysicalTable.setRealtimeTableName("physicalTable_REALTIME");
    realtimePhysicalTable.setRealtimeRouteExists(true);
    routeInfo.setOfflineTables(java.util.List.of(offlinePhysicalTable));
    routeInfo.setRealtimeTables(java.util.List.of(realtimePhysicalTable));
    ServerInstance offlinePrimary = createServerInstance(1300);
    ServerInstance realtimePrimary = createServerInstance(1301);
    when(routingManager.getRoutingTable(offlineBrokerRequest, "physicalTable_OFFLINE", 13L)).thenReturn(
        new RoutingTable(Map.of(offlinePrimary,
            new ServerRouteInfo(java.util.List.of("offlineSegment"), java.util.List.of())), java.util.List.of(), 0));
    when(routingManager.getRoutingTable(realtimeBrokerRequest, "physicalTable_REALTIME", 13L)).thenReturn(
        new RoutingTable(Map.of(realtimePrimary,
            new ServerRouteInfo(java.util.List.of("realtimeSegment"), java.util.List.of())), java.util.List.of(), 0));

    _logicalTableRouteProvider.calculateRoutes(routeInfo, routingManager, offlineBrokerRequest, realtimeBrokerRequest,
        13L);

    verify(routingManager).getRoutingTable(offlineBrokerRequest, "physicalTable_OFFLINE", 13L);
    verify(routingManager).getRoutingTable(realtimeBrokerRequest, "physicalTable_REALTIME", 13L);
    verify(routingManager, never()).getRoutingTable(offlineBrokerRequest, "physicalTable_OFFLINE", 13L, true);
    verify(routingManager, never()).getRoutingTable(realtimeBrokerRequest, "physicalTable_REALTIME", 13L, true);
    assertNotNull(routeInfo.getRequestMap(13L, "broker", false));
  }

  private static BrokerRequest mockBrokerRequest() {
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    PinotQuery pinotQuery = mock(PinotQuery.class);
    when(brokerRequest.getPinotQuery()).thenReturn(pinotQuery);
    when(pinotQuery.getQueryOptions()).thenReturn(null);
    return brokerRequest;
  }

  private static ServerInstance createServerInstance(int port) {
    return new ServerInstance(InstanceConfig.toInstanceConfig("Server_localhost_" + port));
  }
}
