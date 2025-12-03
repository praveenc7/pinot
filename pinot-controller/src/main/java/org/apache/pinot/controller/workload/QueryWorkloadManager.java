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

import com.google.common.util.concurrent.RateLimiter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.helix.ExtraInstanceConfig;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ControllerTimer;
import org.apache.pinot.common.utils.config.QueryWorkloadConfigUtils;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.workload.scheme.PropagationScheme;
import org.apache.pinot.controller.workload.scheme.PropagationSchemeProvider;
import org.apache.pinot.controller.workload.scheme.PropagationUtils;
import org.apache.pinot.controller.workload.splitter.CostSplitter;
import org.apache.pinot.controller.workload.splitter.DefaultCostSplitter;
import org.apache.pinot.spi.config.workload.InstanceCost;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationEntity;
import org.apache.pinot.spi.config.workload.PropagationEntityOverrides;
import org.apache.pinot.spi.config.workload.QueryWorkloadConfig;
import org.apache.pinot.spi.config.workload.QueryWorkloadRefreshRequest;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code QueryWorkloadManager} is responsible for managing query workload configurations
 * in a Pinot Helix cluster.
 *
 * <p>
 * It propagates and computes workload costs to be enforced by relevant instances based on
 * the configured propagation scheme. This ensures that workloads can be isolated and resource
 * budgets (CPU and memory) can be enforced consistently across brokers and servers.
 * </p>
 *
 * <p><strong>Responsibilities include:</strong></p>
 * <ul>
 *   <li>Resolving instances based on node type and propagation scheme.</li>
 *   <li>Computing instance costs using a cost split strategy.</li>
 *   <li>Sending HTTP refresh requests to instances with their assigned costs.</li>
 *   <li>Handling workload deletions by propagating delete requests.</li>
 *   <li>Providing lookup APIs for workload costs per instance.</li>
 * </ul>
 */
public class QueryWorkloadManager {
  public static final Logger LOGGER = LoggerFactory.getLogger(QueryWorkloadManager.class);
  private static final int PROPAGATION_TIMEOUT_SECONDS = 300;
  // Instance fields
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final PropagationSchemeProvider _propagationSchemeProvider;
  private final CostSplitter _costSplitter;
  // Rate limiter to control QPS (queries per second) of HTTP requests during workload propagation
  private final RateLimiter _rateLimiter;
  // Endpoint configs (protocol + port) discovered at startup, can be null if discovery failed
  private Pair<String, Integer> _serverEndpoint;
  private Pair<String, Integer> _brokerEndpoint;
  // TODO: Remove this check once we have fully rolled out query workload configs
  private boolean _enableTableChangePropagation;

  public QueryWorkloadManager(PinotHelixResourceManager pinotHelixResourceManager,
                              ControllerConf controllerConf) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _propagationSchemeProvider = new PropagationSchemeProvider(pinotHelixResourceManager);
    // TODO: To make this configurable once we have multiple cost splitters implementations
    _costSplitter = new DefaultCostSplitter();
    // Initialize rate limiter to control QPS of propagation requests
    double requestsPerSecond = controllerConf.getControllerWorkloadPropagationRequestsPerSecond();
    _rateLimiter = RateLimiter.create(requestsPerSecond);
    _enableTableChangePropagation = controllerConf.enableTableChangePropagation();
    // Discover broker and server endpoint configs (protocol scheme and port) at startup
    Map<NodeConfig.Type, Pair<String, Integer>> nodeEndpointConfigs = discoverNodeEndpointConfigs(null);
    _serverEndpoint = nodeEndpointConfigs.getOrDefault(NodeConfig.Type.SERVER_NODE, null);
    _brokerEndpoint = nodeEndpointConfigs.getOrDefault(NodeConfig.Type.BROKER_NODE, null);
    LOGGER.info("Initialized QueryWorkloadManager with rate limit: {} requests/second", requestsPerSecond);
  }

  /**
   * Propagates an upsert of a workload's cost configuration to all relevant instances.
   *
   * <p>
   * For each {@link NodeConfig} in the supplied {@link QueryWorkloadConfig}, this method:
   * </p>
   * <ol>
   *   <li>Resolves the {@link PropagationScheme} from the node's configured scheme type.</li>
   *   <li>Computes the per-instance {@link InstanceCost} map using the configured
   *       {@link CostSplitter}.</li>
   *   <li>Sends HTTP refresh requests to each instance with its computed cost.</li>
   * </ol>
   *
   * <p>
   * This call is idempotent from the manager's perspective: the same inputs will result in the
   * same set of messages being sent. Instances are expected to apply the new costs immediately.
   * </p>
   *
   * <p>
   *  This call is atomic to the extent possible: if any error occurs during estimating the target instances
   *  and their cost. The entire propagation is aborted and no partial updates are sent to any instances.
   * </p>
   *
   * @param queryWorkloadConfig The workload definition (name, node types, budgets, and propagation
   *                            scheme) to propagate.
   */
  public void propagateWorkloadUpdateMessage(QueryWorkloadConfig queryWorkloadConfig) {
    String queryWorkloadName = queryWorkloadConfig.getQueryWorkloadName();
    LOGGER.info("Propagating workload update for: {}", queryWorkloadName);
    long startTime = System.currentTimeMillis();
    ControllerMetrics metrics = ControllerMetrics.get();

    // Track propagation call
    metrics.addMeteredTableValue(queryWorkloadName, ControllerMeter.QUERY_WORKLOAD_PROPAGATION_COUNT, 1L);
    metrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_PROPAGATION_COUNT, 1L);

    Map<String, QueryWorkloadRefreshRequest> instanceToRefreshRequestMap = new HashMap<>();
    try {
      Map<String, InstanceCost> workloadInstanceCostMap = new HashMap<>();
      for (NodeConfig nodeConfig: queryWorkloadConfig.getNodeConfigs()) {
        resolveInstanceCostMap(nodeConfig, workloadInstanceCostMap);
      }
      // Create request for each instance with single workload
      Map<String, QueryWorkloadRefreshRequest> nodeToRefreshRequestMap = workloadInstanceCostMap.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, entry -> new QueryWorkloadRefreshRequest(
            queryWorkloadName, entry.getValue(), QueryWorkloadRefreshRequest.OperationType.REFRESH_QUERY_WORKLOAD)));

      instanceToRefreshRequestMap.putAll(nodeToRefreshRequestMap);
      // Sends the message only after all nodeConfigs are processed successfully
      // TODO: See if we also need to send a delete message message to entities that were previously targeted but
      //  are no longer targeted by the updated workload config.
      sendQueryWorkloadRefreshMessage(instanceToRefreshRequestMap);
      metrics.addMeteredTableValue(queryWorkloadName, ControllerMeter.QUERY_WORKLOAD_PROPAGATION_SUCCESS, 1L);
      metrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_PROPAGATION_SUCCESS, 1L);
      LOGGER.info("Successfully propagated workload update for: {} to {} instances", queryWorkloadName,
          instanceToRefreshRequestMap.size());
    } catch (Exception e) {
      // Track failure
      metrics.addMeteredTableValue(queryWorkloadName, ControllerMeter.QUERY_WORKLOAD_PROPAGATION_FAILURE, 1L);
      metrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_PROPAGATION_FAILURE, 1L);
      String errorMsg = String.format("Failed to propagate workload update for: %s", queryWorkloadName);
      LOGGER.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    } finally {
      // Track propagation time
      long duration = System.currentTimeMillis() - startTime;
      metrics.addTimedTableValue(queryWorkloadName, ControllerTimer.QUERY_WORKLOAD_PROPAGATE_TIME_MS,
          duration, TimeUnit.MILLISECONDS);
      metrics.addTimedValue(ControllerTimer.QUERY_WORKLOAD_PROPAGATE_TIME_MS, duration, TimeUnit.MILLISECONDS);
    }
  }

  private void resolveInstanceCostMap(NodeConfig nodeConfig, Map<String, InstanceCost> instanceCostMap) {
    PropagationScheme propagationScheme = _propagationSchemeProvider.getPropagationScheme(
        nodeConfig.getPropagationScheme().getPropagationType());
    for (PropagationEntity entity : nodeConfig.getPropagationScheme().getPropagationEntities()) {
      if (entity.getOverrides() != null && propagationScheme.isOverrideSupported(entity)) {
        List<PropagationEntityOverrides> overrides = entity.getOverrides();
        // Apply each override separately and aggregate the instance costs
        for (PropagationEntityOverrides override : overrides) {
          resolveAndAggregateInstanceCosts(propagationScheme, entity, override, nodeConfig.getNodeType(),
              instanceCostMap);
        }
      } else {
        resolveAndAggregateInstanceCosts(propagationScheme, entity, null, nodeConfig.getNodeType(),
            instanceCostMap);
      }
    }
  }

  private void resolveAndAggregateInstanceCosts(PropagationScheme propagationScheme,
                                                PropagationEntity entity, PropagationEntityOverrides override,
                                                NodeConfig.Type nodeType,
                                                Map<String, InstanceCost> workloadInstanceCostMap) {
    Set<String> instances = propagationScheme.resolveInstances(entity, nodeType, override);
    Map<String, InstanceCost> entityInstanceCostMap = _costSplitter.computeInstanceCostMap(entity.getCpuCostNs(),
        entity.getMemoryCostBytes(), instances);
    PropagationUtils.mergeCosts(workloadInstanceCostMap, entityInstanceCostMap);
  }

  /**
   * Propagates a delete for the given workload to all relevant instances.
   *
   * <p>
   * The method resolves the target instances for each {@link NodeConfig} and sends HTTP
   * delete requests to instruct instances to remove local state associated with the workload
   * and stop enforcing costs for it.
   * </p>
   *
   * @param queryWorkloadConfig The workload to delete (only the name and node scoping are used).
   */
  public void propagateDeleteWorkloadMessage(QueryWorkloadConfig queryWorkloadConfig) {
    String queryWorkloadName = queryWorkloadConfig.getQueryWorkloadName();
    LOGGER.info("Propagating workload delete for: {}", queryWorkloadName);
    long startTime = System.currentTimeMillis();
    ControllerMetrics metrics = ControllerMetrics.get();

    metrics.addMeteredTableValue(queryWorkloadName, ControllerMeter.QUERY_WORKLOAD_PROPAGATION_COUNT, 1L);
    metrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_PROPAGATION_COUNT, 1L);
    Map<String, QueryWorkloadRefreshRequest> instanceToDeleteRequestMap = new HashMap<>();
    try {
      for (NodeConfig nodeConfig : queryWorkloadConfig.getNodeConfigs()) {
        if (nodeConfig == null) {
          LOGGER.warn("Skipping null NodeConfig for workload delete: {}", queryWorkloadName);
          continue;
        }
        Set<String> instances = resolveInstances(nodeConfig);
        if (instances.isEmpty()) {
          LOGGER.warn("No instances found for workload delete: {} with nodeConfig: {}", queryWorkloadName, nodeConfig);
          continue;
        }
        QueryWorkloadRefreshRequest deleteRequest = new QueryWorkloadRefreshRequest(
            queryWorkloadName, null, QueryWorkloadRefreshRequest.OperationType.DELETE_QUERY_WORKLOAD);
        instanceToDeleteRequestMap.putAll(instances.stream()
            .collect(Collectors.toMap(instance -> instance, instance -> deleteRequest)));
      }
      sendQueryWorkloadRefreshMessage(instanceToDeleteRequestMap);
      metrics.addMeteredTableValue(queryWorkloadName, ControllerMeter.QUERY_WORKLOAD_PROPAGATION_SUCCESS, 1L);
      metrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_PROPAGATION_SUCCESS, 1L);
      LOGGER.info("Successfully propagated workload delete for: {} to {} instances", queryWorkloadName,
          instanceToDeleteRequestMap.size());
    } catch (Exception e) {
      metrics.addMeteredTableValue(queryWorkloadName, ControllerMeter.QUERY_WORKLOAD_PROPAGATION_FAILURE, 1L);
      metrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_PROPAGATION_FAILURE, 1L);
      String errorMsg = String.format("Failed to propagate workload delete for: %s", queryWorkloadName);
      LOGGER.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    } finally {
      long duration = System.currentTimeMillis() - startTime;
      metrics.addTimedTableValue(queryWorkloadName, ControllerTimer.QUERY_WORKLOAD_PROPAGATE_TIME_MS,
          duration, TimeUnit.MILLISECONDS);
      metrics.addTimedValue(ControllerTimer.QUERY_WORKLOAD_PROPAGATE_TIME_MS, duration, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Propagates workload configurations for tables that were added or removed.
   * Optimized to deduplicate workloads across multiple tables.
   *
   * @param tablesAdded List of tables that were added
   * @param tablesRemoved List of tables that were removed
   * @param nodeType The node type (BROKER_NODE or SERVER_NODE)
   */
  public void propagateWorkloadForTables(List<String> tablesAdded, List<String> tablesRemoved,
                                         NodeConfig.Type nodeType) {
    Set<String> affectedTables = new HashSet<>();
    if (tablesAdded != null) {
      affectedTables.addAll(tablesAdded);
    }
    if (tablesRemoved != null) {
      affectedTables.addAll(tablesRemoved);
    }
    if (affectedTables.isEmpty()) {
      return;
    }
    // Use the optimized batch method
    propagateWorkloadForTables(new ArrayList<>(affectedTables), nodeType);
  }

  /**
   * Propagates workload configurations for multiple tables efficiently.
   * Deduplicates workloads that apply to multiple tables to avoid redundant propagation.
   *
   * @param tableNames List of table names
   * @param nodeType The node type (BROKER_NODE, SERVER_NODE, or null for both)
   */
  public void propagateWorkloadForTables(List<String> tableNames, @Nullable NodeConfig.Type nodeType) {
    if (tableNames == null || tableNames.isEmpty() || !_enableTableChangePropagation) {
      return;
    }
    if (nodeType == null) {
      // Propagate to both broker and server nodes
      propagateWorkloadForTables(tableNames, NodeConfig.Type.BROKER_NODE);
      propagateWorkloadForTables(tableNames, NodeConfig.Type.SERVER_NODE);
      return;
    }
    // Collect all unique helix tags from all tables
    Set<String> allHelixTags = new HashSet<>();
    for (String tableName : tableNames) {
      try {
        Set<String> helixTags = PropagationUtils.getHelixTagsForTable(_pinotHelixResourceManager, tableName, nodeType);
        allHelixTags.addAll(helixTags);
      } catch (Exception e) {
        LOGGER.error("Failed to get helix tags for table: {}", tableName, e);
      }
    }
    if (allHelixTags.isEmpty()) {
      LOGGER.info("No helix tags found for tables: {}", tableNames);
      return;
    }
    // Propagate once for all unique tags - this deduplicates workloads
    String entityName = String.format("tables: %s", tableNames);
    propagateWorkloadForHelixTags(allHelixTags, entityName);
  }

  /**
   * Common helper method to propagate workload configurations based on Helix tags.
   * Batches multiple workloads going to the same instance into a single message.
   *
   * @param helixTags Set of Helix tags to filter workload configs
   * @param entityName Name of the entity for logging
   */
  private void propagateWorkloadForHelixTags(Set<String> helixTags, String entityName) {
    List<QueryWorkloadConfig> queryWorkloadConfigs = _pinotHelixResourceManager.getAllQueryWorkloadConfigs();
    if (queryWorkloadConfigs.isEmpty()) {
      return;
    }
    // Find all workloads associated with the helix tags
    Set<QueryWorkloadConfig> queryWorkloadConfigsForTags =
        PropagationUtils.getQueryWorkloadConfigsForTags(_pinotHelixResourceManager, helixTags, queryWorkloadConfigs);

    if (queryWorkloadConfigsForTags.isEmpty()) {
      LOGGER.info("No workload configs match {}, no propagation needed", entityName);
      return;
    }
    // Build a map of instance -> (workloadName -> instanceCost) to batch workloads per instance
    Map<String, Map<String, InstanceCost>> instanceToWorkloadCostMap = new HashMap<>();
    int successCount = 0;
    for (QueryWorkloadConfig queryWorkloadConfig : queryWorkloadConfigsForTags) {
      try {
        List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(queryWorkloadConfig);
        if (!errors.isEmpty()) {
          LOGGER.error("Invalid QueryWorkloadConfig: {}: {}, errors: {}", queryWorkloadConfig, entityName, errors);
          continue;
        }
        String queryWorkloadName = queryWorkloadConfig.getQueryWorkloadName();
        Map<String, InstanceCost> workloadInstanceCostMap = new HashMap<>();
        for (NodeConfig nodeConfig: queryWorkloadConfig.getNodeConfigs()) {
          resolveInstanceCostMap(nodeConfig, workloadInstanceCostMap);
        }
        // Group by instance
        for (Map.Entry<String, InstanceCost> entry : workloadInstanceCostMap.entrySet()) {
          String instanceName = entry.getKey();
          instanceToWorkloadCostMap.computeIfAbsent(instanceName, k -> new HashMap<>())
              .put(queryWorkloadName, entry.getValue());
        }
        successCount++;
      } catch (Exception e) {
        LOGGER.error("Error processing workload config: {} for {}", queryWorkloadConfig.getQueryWorkloadName(),
            entityName, e);
      }
    }

    // Convert to refresh requests and send
    Map<String, QueryWorkloadRefreshRequest> instanceToRefreshRequestMap =
        instanceToWorkloadCostMap.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> new QueryWorkloadRefreshRequest(
                entry.getValue(), QueryWorkloadRefreshRequest.OperationType.REFRESH_QUERY_WORKLOAD)));
    // Send all requests
    if (!instanceToRefreshRequestMap.isEmpty()) {
      sendQueryWorkloadRefreshMessage(instanceToRefreshRequestMap);
      LOGGER.info("Successfully propagated {} workloads for {} to {} instances", successCount, entityName,
          instanceToRefreshRequestMap.size());
    } else {
      LOGGER.info("No instances to propagate workloads for {}", entityName);
    }
  }

  /**
   * Computes the workload-to-cost mapping for a specific instance.
   *
   * <p>
   * This method iterates through all {@link QueryWorkloadConfig}s stored in Zookeeper and
   * determines which workloads apply to the given instance. For each applicable workload, it
   * computes the {@link InstanceCost} (CPU and memory budgets) assigned to that instance. The
   * computation is based on the workload's {@link PropagationScheme} and the manager's
   * {@link CostSplitter}.
   * </p>
   *
   * <p>
   * If the instance is not a recognized Pinot broker or server, or if its Helix configuration
   * cannot be found, an empty map is returned and a warning is logged.
   * </p>
   *
   * @param instanceName The Helix instance name (e.g., {@code Server_foo_8001} or
   *                     {@code Broker_bar_8099}).
   * @return A map from workload name to {@link InstanceCost} representing the budgets that apply
   *         to the given instance for its role.
   */
  public Map<String, InstanceCost> getWorkloadToInstanceCostFor(String instanceName) {
    LOGGER.debug("Computing workload costs for instance: {}", instanceName);

    Map<String, InstanceCost> workloadToInstanceCostMap = new HashMap<>();

    try {
      List<QueryWorkloadConfig> queryWorkloadConfigs = _pinotHelixResourceManager.getAllQueryWorkloadConfigs();
      if (queryWorkloadConfigs.isEmpty()) {
        LOGGER.warn("No query workload configs found in zookeeper");
        return workloadToInstanceCostMap;
      }

      // Determine node type from instance name
      NodeConfig.Type nodeType;
      if (InstanceTypeUtils.isServer(instanceName)) {
        nodeType = NodeConfig.Type.SERVER_NODE;
      } else if (InstanceTypeUtils.isBroker(instanceName)) {
        nodeType = NodeConfig.Type.BROKER_NODE;
      } else {
        LOGGER.warn("Instance {} is neither a server nor a broker", instanceName);
        return workloadToInstanceCostMap;
      }

      // Iterate through all workload configs and compute costs for this instance
      for (QueryWorkloadConfig queryWorkloadConfig : queryWorkloadConfigs) {
        try {
          List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(queryWorkloadConfig);
          if (!errors.isEmpty()) {
            LOGGER.error("Invalid QueryWorkloadConfig: {}, errors: {}", queryWorkloadConfig, errors);
            continue;
          }
          String queryWorkloadName = queryWorkloadConfig.getQueryWorkloadName();
          for (NodeConfig nodeConfig : queryWorkloadConfig.getNodeConfigs()) {
            if (nodeConfig.getNodeType() != nodeType) {
              // Skip node configs that don't match this instance's type
              continue;
            }
            Map<String, InstanceCost> instanceCostMap = new HashMap<>();
            resolveInstanceCostMap(nodeConfig, instanceCostMap);
            InstanceCost instanceCost = instanceCostMap.get(instanceName);
            if (instanceCost != null) {
              workloadToInstanceCostMap.put(queryWorkloadName, instanceCost);
              break; // Found cost for this workload, move to next workload
            }
          }
        } catch (Exception e) {
          LOGGER.error("Error computing cost for workload: {}", queryWorkloadConfig.getQueryWorkloadName(), e);
          // Continue with other workloads instead of failing completely
        }
      }
      LOGGER.info("Computed {} workload costs for instance: {}", workloadToInstanceCostMap.size(), instanceName);
      return workloadToInstanceCostMap;
    } catch (Exception e) {
      String errorMsg = String.format("Failed to compute workload costs for instance: %s", instanceName);
      LOGGER.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
  }

  /**
   * Propagates workload configurations for a specific tenant.
   *
   * <p>
   * This method identifies all workload configurations that are associated with the specified
   * tenant and propagates them to the relevant instances. The tenant name is resolved to its
   * corresponding Helix tags (broker, offline server, and realtime server tags), and all
   * workloads whose propagation scope matches these tags are propagated.
   * </p>
   *
   * <p>
   * The propagation process:
   * </p>
   * <ol>
   *   <li>Resolves the Helix tags associated with the tenant (broker, offline, realtime).</li>
   *   <li>Filters the workload configs to those whose scope matches the tenant's tags.</li>
   *   <li>Invokes {@link #propagateWorkloadUpdateMessage(QueryWorkloadConfig)} for each match.</li>
   * </ol>
   *
   * <p>
   * If no workloads are configured, the method returns immediately. Any exception encountered is
   * logged but does not cause the method to fail completely.
   * </p>
   *
   * @param tenantName The tenant name (e.g., {@code DefaultTenant}).
   */
  public void propagateWorkloadForTenant(String tenantName) {
    NodeConfig.Type nodeType = null;
    if (TagNameUtils.isBrokerTag(tenantName)) {
      nodeType = NodeConfig.Type.BROKER_NODE;
    } else if (TagNameUtils.isServerTag(tenantName)) {
      nodeType = NodeConfig.Type.SERVER_NODE;
    }
    Set<String> helixTags = PropagationUtils.getHelixTagsForTenant(tenantName, nodeType);
    propagateWorkloadForHelixTags(helixTags, tenantName);
  }

  private Set<String> resolveInstances(NodeConfig nodeConfig) {
    PropagationScheme propagationScheme =
        _propagationSchemeProvider.getPropagationScheme(nodeConfig.getPropagationScheme().getPropagationType());
    Set<String> instances = new HashSet<>();
    for (PropagationEntity entity : nodeConfig.getPropagationScheme().getPropagationEntities()) {
      if (entity.getOverrides() != null && propagationScheme.isOverrideSupported(entity)) {
        List<PropagationEntityOverrides> overrides = entity.getOverrides();
        // Apply each override separately and aggregate the instances
        for (PropagationEntityOverrides override : overrides) {
          instances.addAll(propagationScheme.resolveInstances(entity, nodeConfig.getNodeType(), override));
        }
      } else {
        instances.addAll(propagationScheme.resolveInstances(entity, nodeConfig.getNodeType(), null));
      }
    }
    return instances;
  }

  /**
   * Sends workload refresh messages to instances via HTTP in parallel.
   * Uses RateLimiter to control the QPS (requests per second) of propagation requests
   * to prevent overwhelming servers/brokers with too many concurrent requests.
   */
  public void sendQueryWorkloadRefreshMessage(Map<String, QueryWorkloadRefreshRequest> instanceToRefreshRequestMap) {
    long startTime = System.currentTimeMillis();
    int totalInstances = instanceToRefreshRequestMap.size();
    ControllerMetrics metrics = ControllerMetrics.get();

    LOGGER.info("Starting parallel workload refresh to {} instances", totalInstances);
    metrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_MESSAGES_ENQUEUED, totalInstances);

    // Create async requests for all instances (executed in parallel)
    List<CompletableFuture<Boolean>> futures = new ArrayList<>(totalInstances);
    for (Map.Entry<String, QueryWorkloadRefreshRequest> entry : instanceToRefreshRequestMap.entrySet()) {
      String instance = entry.getKey();
      try {
        String baseUrl = InstanceTypeUtils.isBroker(instance) ? getBrokerUrl(instance) : getServerUrl(instance);
        String url = baseUrl + "/queryWorkloadConfigs/refresh";
        // Acquire rate limit permit before initiating async request
        _rateLimiter.acquire();
        futures.add(QueryWorkloadConfigUtils.sendWorkloadRefreshRequestWithRetry(url, entry.getValue(), instance));
      } catch (Exception e) {
        LOGGER.error("Error creating request for instance: {}", instance, e);
        futures.add(CompletableFuture.completedFuture(false));
      }
    }

    // Wait for all requests to complete with timeout
    int successCount = 0;
    int failureCount = 0;
    try {
      // Global timeout for all requests to complete
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
          .get(PROPAGATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      // Count successes
      for (CompletableFuture<Boolean> future : futures) {
        if (future.isDone() && !future.isCompletedExceptionally() && future.join()) {
          successCount++;
        } else {
          failureCount++;
        }
      }
    } catch (TimeoutException e) {
      // Count completed vs incomplete
      for (CompletableFuture<Boolean> future : futures) {
        if (future.isDone() && !future.isCompletedExceptionally() && future.join()) {
          successCount++;
        } else {
          failureCount++;
        }
      }
      LOGGER.warn("Query workload refresh timed out after {}s: {}/{} successful", PROPAGATION_TIMEOUT_SECONDS,
          successCount, totalInstances);
    } catch (Exception e) {
      LOGGER.error("Error waiting for query workload refresh completion", e);
      failureCount = totalInstances;
    }

    // Emit message metrics
    if (successCount > 0) {
      metrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_MESSAGES_SENT, successCount);
    }
    if (failureCount > 0) {
      metrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_MESSAGES_FAILED, failureCount);
    }
    metrics.addTimedValue(ControllerTimer.QUERY_WORKLOAD_SEND_MESSAGE_TIME_MS,
        System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);

    if (failureCount > 0) {
      LOGGER.warn("Query workload refresh completed with failures: {}/{} successful", successCount, totalInstances);
    } else {
      LOGGER.info("Query workload refresh completed successfully: {}/{} instances", successCount, totalInstances);
    }
  }

  private String getServerUrl(String instanceName) {
    if (_serverEndpoint == null) {
      _serverEndpoint = discoverNodeEndpointConfigs(NodeConfig.Type.SERVER_NODE)
          .getOrDefault(NodeConfig.Type.SERVER_NODE, null);
    }
    String hostname = instanceName.split("_")[1];
    return _serverEndpoint.getLeft() + "://" + hostname + ":" + _serverEndpoint.getRight();
  }

  private String getBrokerUrl(String instanceName) {
    if (_brokerEndpoint == null) {
      _brokerEndpoint = discoverNodeEndpointConfigs(NodeConfig.Type.BROKER_NODE)
          .getOrDefault(NodeConfig.Type.BROKER_NODE, null);
    }
    String hostname = instanceName.split("_")[1];
    return _brokerEndpoint.getLeft() + "://" + hostname + ":" + _brokerEndpoint.getRight();
  }

  /**
   * Discovers endpoint configurations (protocol scheme and port) for node types.
   * @param targetType Specific node type to discover, or null to discover both broker and server
   * @return Map of node type to endpoint config (scheme, port)
   */
  private Map<NodeConfig.Type, Pair<String, Integer>> discoverNodeEndpointConfigs(
      @Nullable NodeConfig.Type targetType) {
    List<NodeConfig.Type> typesToDiscover = targetType == null
        ? List.of(NodeConfig.Type.BROKER_NODE, NodeConfig.Type.SERVER_NODE)
        : List.of(targetType);
    Map<NodeConfig.Type, Pair<String, Integer>> discoveredConfigs = new HashMap<>();
    try {
      for (String instanceName : _pinotHelixResourceManager.getAllInstances()) {
        if (discoveredConfigs.size() == typesToDiscover.size()) {
          break;
        }
        // Find matching undiscovered node type for this instance
        NodeConfig.Type matchedType = null;
        for (NodeConfig.Type type : typesToDiscover) {
          if (!discoveredConfigs.containsKey(type)
              && ((type == NodeConfig.Type.BROKER_NODE && InstanceTypeUtils.isBroker(instanceName))
              || (type == NodeConfig.Type.SERVER_NODE && InstanceTypeUtils.isServer(instanceName)))) {
            matchedType = type;
            break;
          }
        }
        if (matchedType == null) {
          continue;
        }
        // Extract endpoint config from instance
        InstanceConfig config = _pinotHelixResourceManager.getHelixInstanceConfig(instanceName);
        if (config != null) {
          Pair<String, Integer> endpointConfig = extractEndpointConfig(config);
          if (endpointConfig != null) {
            discoveredConfigs.put(matchedType, endpointConfig);
            LOGGER.info("Discovered {} endpoint config: {}:{}", matchedType, endpointConfig.getLeft(),
                endpointConfig.getRight());
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("Endpoint config discovery error: {}", e.getMessage());
    }
    return discoveredConfigs;
  }

  /**
   * Extracts endpoint configuration (scheme and port) from an instance config.
   */
  private Pair<String, Integer> extractEndpointConfig(InstanceConfig config) {
    try {
      Map<String, String> fields = config.getRecord().getSimpleFields();
      String scheme = "http";
      String port = fields.getOrDefault(CommonConstants.Helix.Instance.ADMIN_PORT_KEY, config.getPort());
      // Check for HTTPS configuration
      if (fields.containsKey(CommonConstants.Helix.Instance.ADMIN_HTTPS_PORT_KEY)) {
        scheme = "https";
        port = fields.get(CommonConstants.Helix.Instance.ADMIN_HTTPS_PORT_KEY);
      } else if (fields.containsKey(ExtraInstanceConfig.PinotInstanceConfigProperty.PINOT_TLS_PORT.toString())) {
        scheme = "https";
        port = fields.get(ExtraInstanceConfig.PinotInstanceConfigProperty.PINOT_TLS_PORT.toString());
      }
      return Pair.of(scheme, Integer.parseInt(port));
    } catch (Exception e) {
      return null;
    }
  }
}
