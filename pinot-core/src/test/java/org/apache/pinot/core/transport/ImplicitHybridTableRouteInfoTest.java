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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.routing.AlternateServerRouteInfo;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.spi.config.table.TableType;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class ImplicitHybridTableRouteInfoTest {
  @Test
  public void testGetQueryRequestPlanMatchesPrimaryOnlyRequestsWithoutAlternates() {
    ServerInstance offlinePrimary = new ServerInstance("localhost", 1000);
    ServerInstance realtimePrimary = new ServerInstance("localhost", 1001);
    ImplicitHybridTableRouteInfo routeInfo = new ImplicitHybridTableRouteInfo(
        mockBrokerRequest(), mockBrokerRequest(),
        Map.of(offlinePrimary, serverRoute(List.of("offlineSegment"), List.of())),
        Map.of(realtimePrimary, serverRoute(List.of("realtimeSegment"), List.of())));

    QueryRequestPlan requestPlan = routeInfo.getQueryRequestPlan(7L, "broker", false);

    Map<ServerRoutingInstance, InstanceRequest> primaryRequests = requestPlan.getPrimaryRequestMap();
    Map<ServerRoutingInstance, InstanceRequest> legacyRequests =
        routeInfo.getRequestMap(7L, "broker", false);
    assertEquals(primaryRequests.keySet(), legacyRequests.keySet());
    for (ServerRoutingInstance server : primaryRequests.keySet()) {
      assertEquals(primaryRequests.get(server).getSearchSegments(), legacyRequests.get(server).getSearchSegments());
      assertEquals(primaryRequests.get(server).getOptionalSegments(), legacyRequests.get(server).getOptionalSegments());
    }
    for (QueryRequestPlan.RequestGroup requestGroup : requestPlan.getRequestGroups().values()) {
      assertNull(requestGroup.getAlternateServer());
      assertNull(requestGroup.getAlternateRequest());
      assertNull(requestGroup.getAlternateServerInstance());
    }
    assertTrue(requestPlan.getPotentialHedgeServers().isEmpty());
  }

  @Test
  public void testGetQueryRequestPlanBuildsExactAlternateRequests() {
    ServerInstance primary = new ServerInstance("localhost", 1000);
    ServerInstance alternate = new ServerInstance("localhost", 1001);
    ImplicitHybridTableRouteInfo routeInfo = new ImplicitHybridTableRouteInfo(
        mockBrokerRequest(), null, Map.of(primary, serverRoute(List.of("segment0", "segment1"),
            List.of("optional0", "optional1"))), null);
    routeInfo.setOfflineAlternateRoutes(Map.of(primary, List.of(new AlternateServerRouteInfo(alternate,
        serverRoute(List.of("segment0", "segment1"), List.of("optional0"))))));

    QueryRequestPlan.RequestGroup requestGroup =
        routeInfo.getQueryRequestPlan(8L, "broker", false).getRequestGroups()
            .get(primary.toServerRoutingInstance(TableType.OFFLINE, false));

    assertEquals(requestGroup.getPrimaryRequest().getSearchSegments(), List.of("segment0", "segment1"));
    assertEquals(requestGroup.getPrimaryRequest().getOptionalSegments(), List.of("optional0", "optional1"));
    assertEquals(requestGroup.getAlternateServerInstance(), alternate);
    assertEquals(requestGroup.getAlternateRequest().getSearchSegments(), List.of("segment0", "segment1"));
    assertEquals(requestGroup.getAlternateRequest().getOptionalSegments(), List.of("optional0"));
  }

  @Test
  public void testGetQueryRequestPlanSkipsPrimaryTargetsAcrossOfflineAndRealtime() {
    ServerInstance offlinePrimary = new ServerInstance("localhost", 1000);
    ServerInstance realtimePrimary = new ServerInstance("localhost", 1001);
    ServerInstance offlineSafeAlternate = new ServerInstance("localhost", 1002);
    ServerInstance realtimeSafeAlternate = new ServerInstance("localhost", 1003);
    ImplicitHybridTableRouteInfo routeInfo = new ImplicitHybridTableRouteInfo(
        mockBrokerRequest(), mockBrokerRequest(),
        Map.of(offlinePrimary, serverRoute(List.of("offlineSegment"), List.of())),
        Map.of(realtimePrimary, serverRoute(List.of("realtimeSegment"), List.of())));
    routeInfo.setOfflineAlternateRoutes(Map.of(offlinePrimary, List.of(
        new AlternateServerRouteInfo(realtimePrimary, serverRoute(List.of("offlineSegment"), List.of())),
        new AlternateServerRouteInfo(offlineSafeAlternate, serverRoute(List.of("offlineSegment"), List.of())))));
    routeInfo.setRealtimeAlternateRoutes(Map.of(realtimePrimary, List.of(
        new AlternateServerRouteInfo(offlinePrimary, serverRoute(List.of("realtimeSegment"), List.of())),
        new AlternateServerRouteInfo(realtimeSafeAlternate, serverRoute(List.of("realtimeSegment"), List.of())))));

    Map<ServerRoutingInstance, QueryRequestPlan.RequestGroup> requestGroups =
        routeInfo.getQueryRequestPlan(9L, "broker", false).getRequestGroups();

    assertEquals(requestGroups.get(offlinePrimary.toServerRoutingInstance(TableType.OFFLINE, false))
        .getAlternateServerInstance(), offlineSafeAlternate);
    assertEquals(requestGroups.get(realtimePrimary.toServerRoutingInstance(TableType.REALTIME, false))
        .getAlternateServerInstance(), realtimeSafeAlternate);
  }

  @Test
  public void testGetPotentialHedgeServersUsesOnlySafeAlternates() {
    ServerInstance offlinePrimary = new ServerInstance("localhost", 1000);
    ServerInstance realtimePrimary = new ServerInstance("localhost", 1001);
    ServerInstance offlineSafeAlternate = new ServerInstance("localhost", 1002);
    ServerInstance realtimeSafeAlternate = new ServerInstance("localhost", 1003);
    ImplicitHybridTableRouteInfo routeInfo = new ImplicitHybridTableRouteInfo(
        mockBrokerRequest(), mockBrokerRequest(),
        Map.of(offlinePrimary, serverRoute(List.of("offlineSegment"), List.of())),
        Map.of(realtimePrimary, serverRoute(List.of("realtimeSegment"), List.of())));
    routeInfo.setOfflineAlternateRoutes(Map.of(offlinePrimary, List.of(
        new AlternateServerRouteInfo(realtimePrimary, serverRoute(List.of("offlineSegment"), List.of())),
        new AlternateServerRouteInfo(offlineSafeAlternate, serverRoute(List.of("offlineSegment"), List.of())))));
    routeInfo.setRealtimeAlternateRoutes(Map.of(realtimePrimary, List.of(
        new AlternateServerRouteInfo(offlinePrimary, serverRoute(List.of("realtimeSegment"), List.of())),
        new AlternateServerRouteInfo(realtimeSafeAlternate, serverRoute(List.of("realtimeSegment"), List.of())))));

    assertEquals(routeInfo.getPotentialHedgeServers(), Set.of(offlineSafeAlternate, realtimeSafeAlternate));
  }

  private static BrokerRequest mockBrokerRequest() {
    BrokerRequest brokerRequest = Mockito.mock(BrokerRequest.class);
    PinotQuery pinotQuery = Mockito.mock(PinotQuery.class);
    Mockito.when(brokerRequest.getPinotQuery()).thenReturn(pinotQuery);
    Mockito.when(pinotQuery.getQueryOptions()).thenReturn(null);
    return brokerRequest;
  }

  private static ServerRouteInfo serverRoute(List<String> segments, List<String> optionalSegments) {
    return new ServerRouteInfo(new ArrayList<>(segments), new ArrayList<>(optionalSegments));
  }
}
