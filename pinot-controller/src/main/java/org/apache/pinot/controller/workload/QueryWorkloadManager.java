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
package org.apache.pinot.controller.workload;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.ws.rs.NotFoundException;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.messages.QueryWorkloadRefreshMessage;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.workload.splitter.CostSplitter;
import org.apache.pinot.controller.workload.splitter.InstancesInfo;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.workload.InstanceCost;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationScheme;
import org.apache.pinot.spi.config.workload.QueryWorkloadConfig;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The PropagationManager class is responsible for propagating the query workload refresh message to the relevant
 * instances based on the node configurations.
 */
public class QueryWorkloadManager {
  public static final Logger LOGGER = LoggerFactory. getLogger(QueryWorkloadManager.class);

  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final CostSplitter _costSplitter;

  // Mapping of table name to tenants for each node type, since a table can have multiple tenants,
  // and we may want to propagate the workload to only a subset of tenants
  private Map<String, Map<NodeConfig.Type, Set<String>>> _tableToTenants;
  // Provide instances associated with each tenant
  private Map<String, Set<String>> _tenantToInstances;
  // Provide mapping of instance to a tenant tags
  private Map<String, Set<String>> _instanceToTenants;
  private Map<String, Set<QueryWorkloadConfig>> _tenantToWorkloadConfigs;


  public QueryWorkloadManager(PinotHelixResourceManager pinotHelixResourceManager, CostSplitter costSplitter) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _costSplitter = costSplitter;
  }

  public void propagateWorkload(QueryWorkloadConfig queryWorkloadConfig) {
    Map<NodeConfig.Type, NodeConfig> nodeConfigs = queryWorkloadConfig.getNodeConfigs();
    String queryWorkloadName = queryWorkloadConfig.getQueryWorkloadName();
    refreshTenantInstanceMapping();
    refreshTableToTenantsMapping();
    nodeConfigs.forEach((nodeType, nodeConfig) -> {
      Set<String> instances = resolveInstances(nodeType, nodeConfig);
      if (instances.isEmpty()) {
        String errorMsg = String.format("No instances found for Workload: %s nodeType: %s, nodeConfig: %s",
            queryWorkloadName, nodeType.getJsonValue(), nodeConfig);
        LOGGER.error(errorMsg);
        throw new NotFoundException(errorMsg);
      }
      Map<String, InstanceCost> instanceCostMap = _costSplitter.getInstanceCostMap(nodeConfig,
          new InstancesInfo(instances));
      Map<String, QueryWorkloadRefreshMessage> instanceToRefreshMessageMap = instanceCostMap.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey,
              entry -> new QueryWorkloadRefreshMessage(queryWorkloadName, entry.getValue())));
      _pinotHelixResourceManager.sendQueryWorkloadRefreshMessage(instanceToRefreshMessageMap);
    });
  }

  public void propagateWorkloadFor(String tableName, NodeConfig.Type nodeType) {
    try {
      refreshTenantInstanceMapping();
      refreshTenantToWorkloadConfigs();
      // Get the tenants associated with the table for the given node type
      Set<String> tenantNames = _tableToTenants.get(tableName).get(nodeType);
      for (String tenantName : tenantNames) {
        Set<QueryWorkloadConfig> queryWorkloadConfigs = _tenantToWorkloadConfigs.get(tenantName);
        if (queryWorkloadConfigs == null) {
          continue;
        }
        for (QueryWorkloadConfig queryWorkloadConfig : queryWorkloadConfigs) {
          Set<String> instances = _tenantToInstances.get(tenantName);
          NodeConfig nodeConfig = queryWorkloadConfig.getNodeConfigs().get(nodeType);
          if (instances == null || nodeConfig == null) {
            continue;
          }
          Map<String, InstanceCost> instanceCostMap = _costSplitter.getInstanceCostMap(nodeConfig,
              new InstancesInfo(instances));
          Map<String, QueryWorkloadRefreshMessage> instanceToRefreshMessageMap = instanceCostMap.entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey,
                  entry -> new QueryWorkloadRefreshMessage(queryWorkloadConfig.getQueryWorkloadName(), entry.getValue())));
          _pinotHelixResourceManager.sendQueryWorkloadRefreshMessage(instanceToRefreshMessageMap);
        }
      }
    } catch (Exception e) {
      String errorMsg = String.format("Failed to propagate workload for table: %s", tableName);
      LOGGER.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
  }

  public Map<String, InstanceCost> getWorkloadToInstanceCostFor(String instanceName, NodeConfig.Type nodeType) {
    try {
      refreshTenantToWorkloadConfigs();
      refreshTenantInstanceMapping();
      Map<String, InstanceCost> workloadToInstanceCostMap = new HashMap<>();
      Set<String> tenantNames  = _instanceToTenants.get(instanceName);
      for (String tenantName : tenantNames) {
        Set<QueryWorkloadConfig> queryWorkloadConfigs = _tenantToWorkloadConfigs.get(tenantName);
        if (queryWorkloadConfigs == null) {
          continue;
        }
        for (QueryWorkloadConfig queryWorkloadConfig : queryWorkloadConfigs) {
          workloadToInstanceCostMap.computeIfAbsent(queryWorkloadConfig.getQueryWorkloadName(), k -> {
            Set<String> instances = _tenantToInstances.get(tenantName);
            NodeConfig nodeConfig = queryWorkloadConfig.getNodeConfigs().get(nodeType);
            return _costSplitter.getInstanceCost(nodeConfig, new InstancesInfo(instances), instanceName);
          });
        }
      }
      return workloadToInstanceCostMap;
    } catch (Exception e) {
      String errorMsg = String.format("Failed to get workload to instance cost map for instance: %s, nodeType: %s",
          instanceName, nodeType.getJsonValue());
      LOGGER.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
  }

  private Set<String> resolveInstances(NodeConfig.Type nodeType, NodeConfig nodeConfig) {
    PropagationScheme.Type propagationType = nodeConfig.getPropagationScheme().getPropagationType();
    Set<String> instances;
    switch (propagationType) {
      case TABLE:
        instances = resolveForTablePropagation(nodeType, nodeConfig);
        break;
      case TENANT:
        instances = resolveForTenantPropagation(nodeType, nodeConfig);
        break;
      default:
        instances = resolveForDefaultPropagation(nodeType);
        break;
    }
    return instances;
  }

  private Set<String> resolveForTablePropagation(NodeConfig.Type nodeType, NodeConfig nodeConfig) {
    Set<String> instances = new HashSet<>();
    List<String> tableNames = nodeConfig.getPropagationScheme().getValues();
    for (String tableName : tableNames) {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
      List<String> tablesWithType = new ArrayList<>();
      if (tableType == null) {
        // If table name does not have type suffix, get both offline and realtime table names
        tablesWithType.add(TableNameBuilder.OFFLINE.tableNameWithType(tableName));
        tablesWithType.add(TableNameBuilder.REALTIME.tableNameWithType(tableName));
      } else {
        tablesWithType.add(tableName);
      }
      for (String tableWithType : tablesWithType) {
        Map<NodeConfig.Type, Set<String>> tenants = _tableToTenants.get(tableWithType);
        if (tenants != null) {
          Set<String> tenantNames = tenants.get(nodeType);
          if (tenantNames != null) {
            for (String tenantName : tenantNames) {
              Set<String> tenantInstances = _tenantToInstances.get(tenantName);
              if (tenantInstances != null) {
                instances.addAll(tenantInstances);
              }
            }
          }
        }
      }
    }
    return instances;
  }

  private Set<String> resolveForTenantPropagation(NodeConfig.Type nodeType, NodeConfig nodeConfig) {
    Set<String> instances = new HashSet<>();
    List<String> tenantNames = nodeConfig.getPropagationScheme().getValues();
    for (String tenantName : tenantNames) {
      List<String> tenantsWithType = new ArrayList<>();
      if (nodeType == NodeConfig.Type.NON_LEAF_NODE) {
        tenantsWithType.add(TagNameUtils.getBrokerTagForTenant(tenantName));
      } else if (nodeType == NodeConfig.Type.LEAF_NODE) {
        if (TagNameUtils.isOfflineServerTag(tenantName) || TagNameUtils.isRealtimeServerTag(tenantName)) {
          tenantsWithType.add(tenantName);
        } else {
          tenantsWithType.add(TagNameUtils.getOfflineTagForTenant(tenantName));
          tenantsWithType.add(TagNameUtils.getRealtimeTagForTenant(tenantName));
        }
      }
      for (String tenantWithType : tenantsWithType) {
        Set<String> tenantInstances = _tenantToInstances.get(tenantWithType);
        if (tenantInstances != null) {
          instances.addAll(tenantInstances);
        }
      }
    }
    return instances;
  }

  private Set<String> resolveForDefaultPropagation(NodeConfig.Type nodeType) {
    Set<String> instances;
    switch (nodeType) {
      case NON_LEAF_NODE:
        instances = new HashSet<>(_pinotHelixResourceManager.getAllBrokerInstances());
        break;
      case LEAF_NODE:
        instances = new HashSet<>(_pinotHelixResourceManager.getAllServerInstances());
        break;
      default:
        throw new IllegalArgumentException("Invalid node type: " + nodeType);
    }
    return instances;
  }


  public void updateTenantInstanceMappings() {
    List<InstanceConfig> instanceConfigs = _pinotHelixResourceManager.getAllHelixInstanceConfigs();
    Map<String, Set<String>> tenantToInstances = new HashMap<>();
    Map<String, Set<String>> instanceToTenant = new HashMap<>();
    for (InstanceConfig instanceConfig : instanceConfigs) {
      String instanceName = instanceConfig.getInstanceName();
      org.apache.pinot.spi.config.instance.InstanceType instanceType = InstanceTypeUtils.getInstanceType(instanceName);
      List<String> tags = instanceConfig.getTags();
      for (String tag : tags) {
        if (instanceType == InstanceType.SERVER) {
          tenantToInstances.computeIfAbsent(tag, k -> new HashSet<>()).add(instanceName);
        } else if (instanceType == InstanceType.BROKER) {
          tenantToInstances.computeIfAbsent(tag, k -> new HashSet<>()).add(instanceName);
        }
      }
      instanceToTenant.computeIfAbsent(instanceName, k -> new HashSet<>()).addAll(tags);
    }
    _instanceToTenants = instanceToTenant;
    _tenantToInstances = tenantToInstances;
  }

  private Map<String, Map<NodeConfig.Type, Set<String>>> getTableToTenants() {
    List<TableConfig> tableConfigs = _pinotHelixResourceManager.getAllTableConfigs();
    Map<String, Map<NodeConfig.Type, Set<String>>> tableToTenants = new HashMap<>();
    for (TableConfig tableConfig : tableConfigs) {
      String tableName = tableConfig.getTableName();
      TenantConfig tenantConfig = tableConfig.getTenantConfig();
      Map<NodeConfig.Type, Set<String>> tenantMap = tableToTenants.computeIfAbsent(tableName, k -> new HashMap<>());
      tenantMap.computeIfAbsent(NodeConfig.Type.NON_LEAF_NODE, k -> new HashSet<>()).add(TagNameUtils.getBrokerTagForTenant(tenantConfig.getBroker()));
      String serverTag = null;
      if (tableConfig.getTableType() == TableType.OFFLINE) {
        serverTag = TagNameUtils.getOfflineTagForTenant(tenantConfig.getServer());
      } else if (tableConfig.getTableType() == TableType.REALTIME) {
        serverTag = TagNameUtils.getRealtimeTagForTenant(tenantConfig.getServer());
      }
      tenantMap.computeIfAbsent(NodeConfig.Type.LEAF_NODE, k -> new HashSet<>()).add(serverTag);
      TagOverrideConfig tagOverrideConfig = tenantConfig.getTagOverrideConfig();
      if (tagOverrideConfig != null) {
        Set<String> leafNodeTenants = tenantMap.computeIfAbsent(NodeConfig.Type.LEAF_NODE, k -> new HashSet<>());
        Optional.ofNullable(tagOverrideConfig.getRealtimeCompleted()).ifPresent(leafNodeTenants::add);
        Optional.ofNullable(tagOverrideConfig.getRealtimeConsuming()).ifPresent(leafNodeTenants::add);
      }
    }
    return tableToTenants;
  }

  private Map<String, Set<QueryWorkloadConfig>> getTenantToWorkloadConfigs() {
    try {
      Map<String, Set<QueryWorkloadConfig>> tenantToWorkloadConfigs = new HashMap<>();
      List<QueryWorkloadConfig> queryWorkloadConfigs = _pinotHelixResourceManager.getQueryWorkloadConfigs();
      if (queryWorkloadConfigs == null) {
        return tenantToWorkloadConfigs;
      }
      _tableToTenants = getTableToTenants();
      for (QueryWorkloadConfig queryWorkloadConfig : queryWorkloadConfigs) {
        Map<NodeConfig.Type, NodeConfig> nodeConfigs = queryWorkloadConfig.getNodeConfigs();
        nodeConfigs.forEach((nodeType, nodeConfig) -> {
          PropagationScheme.Type propagationType = nodeConfig.getPropagationScheme().getPropagationType();
          if (propagationType == PropagationScheme.Type.TENANT) {
            List<String> tenantNames = nodeConfig.getPropagationScheme().getValues();
            for (String tenantName : tenantNames) {
              if (nodeType == NodeConfig.Type.LEAF_NODE) {
                if (TagNameUtils.isOfflineServerTag(tenantName) || TagNameUtils.isRealtimeServerTag(tenantName)) {
                  tenantToWorkloadConfigs.computeIfAbsent(tenantName, k -> new HashSet<>()).add(queryWorkloadConfig);
                } else {
                  tenantToWorkloadConfigs.computeIfAbsent(TagNameUtils.getOfflineTagForTenant(tenantName), k -> new HashSet<>()).add(queryWorkloadConfig);
                  tenantToWorkloadConfigs.computeIfAbsent(TagNameUtils.getRealtimeTagForTenant(tenantName), k -> new HashSet<>()).add(queryWorkloadConfig);
                }
              } else if (nodeType == NodeConfig.Type.NON_LEAF_NODE) {
                tenantName = TagNameUtils.getBrokerTagForTenant(tenantName);
                tenantToWorkloadConfigs.computeIfAbsent(tenantName, k -> new HashSet<>()).add(queryWorkloadConfig);
              }
            }
          } else if (propagationType == PropagationScheme.Type.TABLE) {
            List<String> tableNames = nodeConfig.getPropagationScheme().getValues();
            for (String tableName : tableNames) {
              TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
              List<String> tablesWithType = new ArrayList<>();
              if (tableType == null) {
                // If table name does not have type suffix, get both offline and realtime table names
                tablesWithType.add(TableNameBuilder.OFFLINE.tableNameWithType(tableName));
                tablesWithType.add(TableNameBuilder.REALTIME.tableNameWithType(tableName));
              } else {
                tablesWithType.add(tableName);
              }
              for (String tableWithType : tablesWithType) {
                Map<NodeConfig.Type, Set<String>> tenants = _tableToTenants.get(tableWithType);
                if (tenants != null) {
                  Set<String> tenantNames = tenants.get(nodeType);
                  if (tenantNames != null) {
                    for (String tenantName : tenantNames) {
                      tenantToWorkloadConfigs.computeIfAbsent(tenantName, k -> new HashSet<>())
                          .add(queryWorkloadConfig);
                    }
                  }
                }
              }
            }
          }
        });
      }
      return tenantToWorkloadConfigs;
    } catch (Exception e) {
      String errorMsg = "Failed to get tenant to workload configs";
      LOGGER.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
  }

  private void refreshTableToTenantsMapping() {
    _tableToTenants = getTableToTenants();
  }

  private void refreshTenantInstanceMapping() {
    updateTenantInstanceMappings();
  }

  private void refreshTenantToWorkloadConfigs() {
    _tenantToWorkloadConfigs = getTenantToWorkloadConfigs();
  }

}
