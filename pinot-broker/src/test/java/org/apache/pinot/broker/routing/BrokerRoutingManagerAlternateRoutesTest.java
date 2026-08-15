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
package org.apache.pinot.broker.routing;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.broker.routing.instanceselector.InstanceSelector;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.core.routing.AlternateServerRouteInfo;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class BrokerRoutingManagerAlternateRoutesTest {
  private static final String TABLE_NAME = "testTable_OFFLINE";

  @Test
  @SuppressWarnings("unchecked")
  public void testGetAlternateServerRoutesBuildsOnlyExactSafeMirrors()
      throws Exception {
    BrokerRoutingManager routingManager = new BrokerRoutingManager(mock(BrokerMetrics.class), null,
        new PinotConfiguration());
    ServerInstance primaryA = createServerInstance(1000);
    ServerInstance primaryB = createServerInstance(1001);
    ServerInstance alternateFullMirror = createServerInstance(1002);
    ServerInstance alternatePartialMirror = createServerInstance(1003);
    addEnabledServers(routingManager, primaryA, primaryB, alternateFullMirror, alternatePartialMirror);

    String requiredSegment0 = "segment0";
    String requiredSegment1 = "segment1";
    String optionalSegmentOnline = "segment2";
    String optionalSegmentOffline = "segment3";
    String requiredSegmentOnOtherPrimary = "segment4";

    Map<ServerInstance, ServerRouteInfo> primaryRoutes = Map.of(
        primaryA, new ServerRouteInfo(new ArrayList<>(List.of(requiredSegment0, requiredSegment1)),
            new ArrayList<>(List.of(optionalSegmentOnline, optionalSegmentOffline))),
        primaryB, new ServerRouteInfo(new ArrayList<>(List.of(requiredSegmentOnOtherPrimary)), new ArrayList<>()));
    Map<String, List<String>> alternateCandidates = Map.of(
        requiredSegment0, List.of(alternatePartialMirror.getInstanceId(), primaryB.getInstanceId(),
            alternateFullMirror.getInstanceId()),
        requiredSegment1, List.of(primaryB.getInstanceId(), alternateFullMirror.getInstanceId()),
        optionalSegmentOnline, List.of(alternateFullMirror.getInstanceId(), alternatePartialMirror.getInstanceId()),
        optionalSegmentOffline, List.of(alternatePartialMirror.getInstanceId()),
        requiredSegmentOnOtherPrimary, List.of(primaryA.getInstanceId()));
    InstanceSelector.SelectionResult selectionResult =
        new InstanceSelector.SelectionResult(Pair.of(Collections.emptyMap(), Collections.emptyMap()),
            alternateCandidates, List.of(), 0);

    Method method = BrokerRoutingManager.class.getDeclaredMethod("getAlternateServerRoutes", String.class,
        InstanceSelector.SelectionResult.class, Map.class);
    method.setAccessible(true);
    Map<ServerInstance, List<AlternateServerRouteInfo>> alternateRoutes =
        (Map<ServerInstance, List<AlternateServerRouteInfo>>) method.invoke(routingManager, TABLE_NAME,
            selectionResult, primaryRoutes);

    assertEquals(alternateRoutes.size(), 1);
    assertFalse(alternateRoutes.containsKey(primaryB));
    List<AlternateServerRouteInfo> routesForPrimaryA = alternateRoutes.get(primaryA);
    assertEquals(routesForPrimaryA.size(), 1);
    AlternateServerRouteInfo alternateRoute = routesForPrimaryA.get(0);
    assertEquals(alternateRoute.getServerInstance(), alternateFullMirror);
    assertEquals(alternateRoute.getServerRouteInfo().getSegments(), List.of(requiredSegment0, requiredSegment1));
    assertEquals(alternateRoute.getServerRouteInfo().getOptionalSegments(), List.of(optionalSegmentOnline));
    assertTrue(routesForPrimaryA.stream()
        .noneMatch(route -> route.getServerInstance().equals(alternatePartialMirror)));
  }

  @SuppressWarnings("unchecked")
  private static void addEnabledServers(BrokerRoutingManager routingManager, ServerInstance... serverInstances)
      throws Exception {
    Field field = BrokerRoutingManager.class.getDeclaredField("_enabledServerInstanceMap");
    field.setAccessible(true);
    Map<String, ServerInstance> enabledServerMap = (Map<String, ServerInstance>) field.get(routingManager);
    for (ServerInstance serverInstance : serverInstances) {
      enabledServerMap.put(serverInstance.getInstanceId(), serverInstance);
    }
  }

  private static ServerInstance createServerInstance(int port) {
    InstanceConfig instanceConfig = InstanceConfig.toInstanceConfig("Server_localhost_" + port);
    return new ServerInstance(instanceConfig);
  }
}
