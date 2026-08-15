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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.routing.AlternateServerRouteInfo;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;


public class ImplicitHybridTableRouteInfo extends BaseTableRouteInfo {
  private String _offlineTableName = null;
  private boolean _isOfflineRouteExists;
  private TableConfig _offlineTableConfig;
  private boolean _isOfflineTableDisabled;

  private String _realtimeTableName = null;
  private boolean _isRealtimeRouteExists;
  private TableConfig _realtimeTableConfig;
  private boolean _isRealtimeTableDisabled;

  private TimeBoundaryInfo _timeBoundaryInfo;

  private List<String> _unavailableSegments;
  private int _numPrunedSegmentsTotal;

  private BrokerRequest _offlineBrokerRequest;
  private BrokerRequest _realtimeBrokerRequest;
  private Map<ServerInstance, ServerRouteInfo> _offlineRoutingTable;
  private Map<ServerInstance, ServerRouteInfo> _realtimeRoutingTable;
  private Map<ServerInstance, List<AlternateServerRouteInfo>> _offlineAlternateRoutes;
  private Map<ServerInstance, List<AlternateServerRouteInfo>> _realtimeAlternateRoutes;

  public ImplicitHybridTableRouteInfo() {
  }

  public ImplicitHybridTableRouteInfo(@Nullable BrokerRequest offlineBrokerRequest,
      @Nullable BrokerRequest realtimeBrokerRequest,
      @Nullable Map<ServerInstance, ServerRouteInfo> offlineRoutingTable,
      @Nullable Map<ServerInstance, ServerRouteInfo> realtimeRoutingTable) {
    _offlineBrokerRequest = offlineBrokerRequest;
    _realtimeBrokerRequest = realtimeBrokerRequest;
    _offlineRoutingTable = offlineRoutingTable;
    _realtimeRoutingTable = realtimeRoutingTable;
  }

  @Nullable
  @Override
  public String getOfflineTableName() {
    return _offlineTableName;
  }

  public void setOfflineTableName(String offlineTableName) {
    _offlineTableName = offlineTableName;
  }

  @Nullable
  @Override
  public String getRealtimeTableName() {
    return _realtimeTableName;
  }

  public void setRealtimeTableName(String realtimeTableName) {
    _realtimeTableName = realtimeTableName;
  }

  @Nullable
  @Override
  public TableConfig getOfflineTableConfig() {
    return _offlineTableConfig;
  }

  public void setOfflineBrokerRequest(BrokerRequest offlineBrokerRequest) {
    _offlineBrokerRequest = offlineBrokerRequest;
  }

  public void setOfflineTableConfig(TableConfig offlineTableConfig) {
    _offlineTableConfig = offlineTableConfig;
  }

  public void setRealtimeBrokerRequest(BrokerRequest realtimeBrokerRequest) {
    _realtimeBrokerRequest = realtimeBrokerRequest;
  }

  @Nullable
  @Override
  public TableConfig getRealtimeTableConfig() {
    return _realtimeTableConfig;
  }

  @Nullable
  @Override
  public QueryConfig getOfflineTableQueryConfig() {
    return _offlineTableConfig != null ? _offlineTableConfig.getQueryConfig() : null;
  }

  @Nullable
  @Override
  public QueryConfig getRealtimeTableQueryConfig() {
    return _realtimeTableConfig != null ? _realtimeTableConfig.getQueryConfig() : null;
  }

  public void setRealtimeTableConfig(TableConfig realtimeTableConfig) {
    _realtimeTableConfig = realtimeTableConfig;
  }

  @Nullable
  @Override
  public BrokerRequest getOfflineBrokerRequest() {
    return _offlineBrokerRequest;
  }

  @Nullable
  @Override
  public BrokerRequest getRealtimeBrokerRequest() {
    return _realtimeBrokerRequest;
  }

  @Nullable
  @Override
  public Set<ServerInstance> getOfflineExecutionServers() {
    return _offlineRoutingTable != null ? _offlineRoutingTable.keySet() : null;
  }

  @Nullable
  @Override
  public Set<ServerInstance> getRealtimeExecutionServers() {
    return _realtimeRoutingTable != null ? _realtimeRoutingTable.keySet() : null;
  }

  @Nullable
  @Override
  public Map<ServerInstance, ServerRouteInfo> getOfflineRoutingTable() {
    return _offlineRoutingTable;
  }

  public void setOfflineRoutingTable(Map<ServerInstance, ServerRouteInfo> offlineRoutingTable) {
    _offlineRoutingTable = offlineRoutingTable;
  }

  public void setOfflineAlternateRoutes(
      Map<ServerInstance, List<AlternateServerRouteInfo>> offlineAlternateRoutes) {
    _offlineAlternateRoutes = offlineAlternateRoutes;
  }

  @Nullable
  @Override
  public Map<ServerInstance, ServerRouteInfo> getRealtimeRoutingTable() {
    return _realtimeRoutingTable;
  }

  public void setRealtimeRoutingTable(Map<ServerInstance, ServerRouteInfo> realtimeRoutingTable) {
    _realtimeRoutingTable = realtimeRoutingTable;
  }

  public void setRealtimeAlternateRoutes(
      Map<ServerInstance, List<AlternateServerRouteInfo>> realtimeAlternateRoutes) {
    _realtimeAlternateRoutes = realtimeAlternateRoutes;
  }

  /**
   * Offline if offline table config is present.
   * @return true if there is an OFFLINE table, false otherwise
   */
  @Override
  public boolean hasOffline() {
    return _offlineTableConfig != null;
  }

  /**
   * Realtime if realtime table config is present.
   * @return true if there is a REALTIME table, false otherwise
   */
  @Override
  public boolean hasRealtime() {
    return _realtimeTableConfig != null;
  }

  @Override
  public boolean isOfflineRouteExists() {
    return _isOfflineRouteExists;
  }

  public void setOfflineRouteExists(boolean offlineRouteExists) {
    _isOfflineRouteExists = offlineRouteExists;
  }

  @Override
  public boolean isRealtimeRouteExists() {
    return _isRealtimeRouteExists;
  }

  public void setRealtimeRouteExists(boolean realtimeRouteExists) {
    _isRealtimeRouteExists = realtimeRouteExists;
  }

  @Override
  public boolean isOfflineTableDisabled() {
    return _isOfflineTableDisabled;
  }

  public void setOfflineTableDisabled(boolean offlineTableDisabled) {
    _isOfflineTableDisabled = offlineTableDisabled;
  }

  @Override
  public boolean isRealtimeTableDisabled() {
    return _isRealtimeTableDisabled;
  }

  public void setRealtimeTableDisabled(boolean realtimeTableDisabled) {
    _isRealtimeTableDisabled = realtimeTableDisabled;
  }

  @Nullable
  @Override
  public List<String> getDisabledTableNames() {
    if (isOffline() && isOfflineTableDisabled()) {
      return List.of(_offlineTableName);
    } else if (isRealtime() && isRealtimeTableDisabled()) {
      return List.of(_realtimeTableName);
    } else if (isOfflineTableDisabled() && isRealtimeTableDisabled()) {
      return List.of(_offlineTableName, _realtimeTableName);
    } else if (isOfflineTableDisabled()) {
      return List.of(_offlineTableName);
    } else if (isRealtimeTableDisabled()) {
      return List.of(_realtimeTableName);
    }
    return null;
  }

  @Nullable
  @Override
  public TimeBoundaryInfo getTimeBoundaryInfo() {
    return _timeBoundaryInfo;
  }

  public void setTimeBoundaryInfo(TimeBoundaryInfo timeBoundaryInfo) {
    _timeBoundaryInfo = timeBoundaryInfo;
  }

  @Override
  public List<String> getUnavailableSegments() {
    return _unavailableSegments;
  }

  public void setUnavailableSegments(List<String> unavailableSegments) {
    _unavailableSegments = unavailableSegments;
  }

  public int getNumPrunedSegmentsTotal() {
    return _numPrunedSegmentsTotal;
  }

  public void setNumPrunedSegmentsTotal(int numPrunedSegmentsTotal) {
    _numPrunedSegmentsTotal = numPrunedSegmentsTotal;
  }

  protected static Map<ServerRoutingInstance, InstanceRequest> getRequestMapFromRoutingTable(TableType tableType,
      Map<ServerInstance, ServerRouteInfo> routingTable, BrokerRequest brokerRequest, long requestId, String brokerId,
      boolean preferTls) {
    Map<ServerRoutingInstance, InstanceRequest> requestMap = new HashMap<>();
    for (Map.Entry<ServerInstance, ServerRouteInfo> entry : routingTable.entrySet()) {
      ServerRoutingInstance serverRoutingInstance = entry.getKey().toServerRoutingInstance(tableType, preferTls);
      InstanceRequest instanceRequest = getInstanceRequest(requestId, brokerId, brokerRequest, entry.getValue());
      requestMap.put(serverRoutingInstance, instanceRequest);
    }
    return requestMap;
  }

  protected static InstanceRequest getInstanceRequest(long requestId, String brokerId, BrokerRequest brokerRequest,
      ServerRouteInfo segments) {
    InstanceRequest instanceRequest = TableRouteInfo.createInstanceRequest(brokerRequest, brokerId, requestId);
    instanceRequest.setSearchSegments(segments.getSegments());
    if (CollectionUtils.isNotEmpty(segments.getOptionalSegments())) {
      // Don't set this field, i.e. leave it as null, if there is no optional segment at all, to be more backward
      // compatible, as there are places like in multi-stage query engine where this field is not set today when
      // creating the InstanceRequest.
      instanceRequest.setOptionalSegments(segments.getOptionalSegments());
    }
    return instanceRequest;
  }

  @Override
  public Map<ServerRoutingInstance, InstanceRequest> getRequestMap(long requestId, String brokerId, boolean preferTls) {
    Map<ServerRoutingInstance, InstanceRequest> requestMap = null;
    Map<ServerRoutingInstance, InstanceRequest> offlineRequestMap = null;
    Map<ServerRoutingInstance, InstanceRequest> realtimeRequestMap = null;

    if (_offlineRoutingTable != null && _offlineBrokerRequest != null) {
      offlineRequestMap = getRequestMapFromRoutingTable(TableType.OFFLINE, _offlineRoutingTable,
              _offlineBrokerRequest, requestId, brokerId, preferTls);
    }

    if (_realtimeRoutingTable != null && _realtimeBrokerRequest != null) {
      realtimeRequestMap = getRequestMapFromRoutingTable(TableType.REALTIME, _realtimeRoutingTable,
              _realtimeBrokerRequest, requestId, brokerId, preferTls);
    }

    if (offlineRequestMap != null) {
      requestMap = offlineRequestMap;
    }
    if (realtimeRequestMap != null) {
      if (requestMap == null) {
        requestMap = realtimeRequestMap;
      } else {
        requestMap.putAll(realtimeRequestMap);
      }
    }

    return requestMap;
  }

  @Override
  public QueryRequestPlan getQueryRequestPlan(long requestId, String brokerId, boolean preferTls) {
    Set<String> primaryInstanceIds = getPrimaryInstanceIds();
    Map<ServerRoutingInstance, QueryRequestPlan.RequestGroup> requestGroups = new HashMap<>();
    if (_offlineRoutingTable != null && _offlineBrokerRequest != null) {
      addRequestGroups(requestGroups, TableType.OFFLINE, _offlineRoutingTable, _offlineAlternateRoutes,
          _offlineBrokerRequest, primaryInstanceIds, requestId, brokerId, preferTls);
    }
    if (_realtimeRoutingTable != null && _realtimeBrokerRequest != null) {
      addRequestGroups(requestGroups, TableType.REALTIME, _realtimeRoutingTable, _realtimeAlternateRoutes,
          _realtimeBrokerRequest, primaryInstanceIds, requestId, brokerId, preferTls);
    }
    return new QueryRequestPlan(requestGroups);
  }

  @Override
  public Set<ServerInstance> getPotentialHedgeServers() {
    Set<String> primaryInstanceIds = getPrimaryInstanceIds();
    Set<ServerInstance> potentialHedgeServers = new HashSet<>();
    addPotentialHedgeServers(potentialHedgeServers, _offlineRoutingTable, _offlineAlternateRoutes, primaryInstanceIds);
    addPotentialHedgeServers(potentialHedgeServers, _realtimeRoutingTable, _realtimeAlternateRoutes,
        primaryInstanceIds);
    return potentialHedgeServers;
  }

  private Set<String> getPrimaryInstanceIds() {
    Set<String> primaryInstanceIds = new HashSet<>();
    if (_offlineRoutingTable != null) {
      for (ServerInstance serverInstance : _offlineRoutingTable.keySet()) {
        primaryInstanceIds.add(serverInstance.getInstanceId());
      }
    }
    if (_realtimeRoutingTable != null) {
      for (ServerInstance serverInstance : _realtimeRoutingTable.keySet()) {
        primaryInstanceIds.add(serverInstance.getInstanceId());
      }
    }
    return primaryInstanceIds;
  }

  private static void addRequestGroups(
      Map<ServerRoutingInstance, QueryRequestPlan.RequestGroup> requestGroups, TableType tableType,
      Map<ServerInstance, ServerRouteInfo> routingTable,
      @Nullable Map<ServerInstance, List<AlternateServerRouteInfo>> alternateRoutes, BrokerRequest brokerRequest,
      Set<String> primaryInstanceIds, long requestId, String brokerId, boolean preferTls) {
    for (Map.Entry<ServerInstance, ServerRouteInfo> entry : routingTable.entrySet()) {
      ServerRoutingInstance primaryServer = entry.getKey().toServerRoutingInstance(tableType, preferTls);
      InstanceRequest primaryRequest = getInstanceRequest(requestId, brokerId, brokerRequest, entry.getValue());
      AlternateServerRouteInfo alternateRoute =
          getFirstSafeAlternate(entry.getKey(), alternateRoutes, primaryInstanceIds);
      if (alternateRoute == null) {
        requestGroups.put(primaryServer,
            new QueryRequestPlan.RequestGroup(primaryServer, primaryRequest, null, null, null));
      } else {
        ServerInstance alternateServerInstance = alternateRoute.getServerInstance();
        ServerRoutingInstance alternateServer =
            alternateServerInstance.toServerRoutingInstance(tableType, preferTls);
        InstanceRequest alternateRequest =
            getInstanceRequest(requestId, brokerId, brokerRequest, alternateRoute.getServerRouteInfo());
        requestGroups.put(primaryServer, new QueryRequestPlan.RequestGroup(primaryServer, primaryRequest,
            alternateServer, alternateRequest, alternateServerInstance));
      }
    }
  }

  private static void addPotentialHedgeServers(Set<ServerInstance> potentialHedgeServers,
      @Nullable Map<ServerInstance, ServerRouteInfo> routingTable,
      @Nullable Map<ServerInstance, List<AlternateServerRouteInfo>> alternateRoutes,
      Set<String> primaryInstanceIds) {
    if (routingTable == null) {
      return;
    }
    for (ServerInstance primaryServer : routingTable.keySet()) {
      AlternateServerRouteInfo alternateRoute =
          getFirstSafeAlternate(primaryServer, alternateRoutes, primaryInstanceIds);
      if (alternateRoute != null) {
        potentialHedgeServers.add(alternateRoute.getServerInstance());
      }
    }
  }

  @Nullable
  private static AlternateServerRouteInfo getFirstSafeAlternate(ServerInstance primaryServer,
      @Nullable Map<ServerInstance, List<AlternateServerRouteInfo>> alternateRoutes,
      Set<String> primaryInstanceIds) {
    if (alternateRoutes == null) {
      return null;
    }
    List<AlternateServerRouteInfo> routes = alternateRoutes.get(primaryServer);
    if (routes == null) {
      return null;
    }
    for (AlternateServerRouteInfo route : routes) {
      if (!primaryInstanceIds.contains(route.getServerInstance().getInstanceId())) {
        return route;
      }
    }
    return null;
  }
}
