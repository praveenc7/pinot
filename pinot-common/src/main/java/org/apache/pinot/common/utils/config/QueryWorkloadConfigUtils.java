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
package org.apache.pinot.common.utils.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.HttpVersion;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.helix.ExtraInstanceConfig;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.common.utils.http.HttpClientConfig;
import org.apache.pinot.common.utils.tls.TlsUtils;
import org.apache.pinot.spi.accounting.WorkloadBudgetManager;
import org.apache.pinot.spi.accounting.WorkloadBudgetManagerFactory;
import org.apache.pinot.spi.config.workload.EnforcementProfile;
import org.apache.pinot.spi.config.workload.InstanceCost;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationEntity;
import org.apache.pinot.spi.config.workload.PropagationEntityOverrides;
import org.apache.pinot.spi.config.workload.PropagationScheme;
import org.apache.pinot.spi.config.workload.QueryWorkloadConfig;
import org.apache.pinot.spi.config.workload.QueryWorkloadRefreshRequest;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.slf4j.Logger;


public class QueryWorkloadConfigUtils {
  private QueryWorkloadConfigUtils() {
  }

  private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(QueryWorkloadConfigUtils.class);

  private static final HttpClient HTTP_CLIENT = new HttpClient(HttpClientConfig.DEFAULT_HTTP_CLIENT_CONFIG,
      TlsUtils.getSslContext());
  private static final Random RANDOM = new Random();
  private static final int WORKLOAD_PROPAGATION_MAX_RETRIES = 3;
  private static final long RETRY_INITIAL_DELAY_MS = 3000L;
  private static final double RETRY_BACKOFF_MULTIPLIER = 2.0;

  /**
   * Converts a ZNRecord into a QueryWorkloadConfig object by extracting mapFields.
   *
   * @param znRecord The ZNRecord containing workload config data.
   * @return A QueryWorkloadConfig object.
   */
  public static QueryWorkloadConfig fromZNRecord(ZNRecord znRecord) {
    Preconditions.checkNotNull(znRecord, "ZNRecord cannot be null");
    String queryWorkloadName = znRecord.getSimpleField(QueryWorkloadConfig.QUERY_WORKLOAD_NAME);
    Preconditions.checkNotNull(queryWorkloadName, "queryWorkloadName cannot be null");
    String nodeConfigsJson = znRecord.getSimpleField(QueryWorkloadConfig.NODE_CONFIGS);
    Preconditions.checkNotNull(nodeConfigsJson, "nodeConfigs cannot be null");
    try {
      List<NodeConfig> nodeConfigs = JsonUtils.stringToObject(nodeConfigsJson, new TypeReference<>() { });
      return new QueryWorkloadConfig(queryWorkloadName, nodeConfigs);
    } catch (Exception e) {
      String errorMessage = String.format("Failed to convert ZNRecord : %s to QueryWorkloadConfig", znRecord);
      throw new RuntimeException(errorMessage, e);
    }
  }

  /**
   * Updates a ZNRecord with the fields from a WorkloadConfig object.
   *
   * @param queryWorkloadConfig The QueryWorkloadConfig object to convert.
   * @param znRecord The ZNRecord to update.
   */
  public static void updateZNRecordWithWorkloadConfig(ZNRecord znRecord, QueryWorkloadConfig queryWorkloadConfig) {
    znRecord.setSimpleField(QueryWorkloadConfig.QUERY_WORKLOAD_NAME, queryWorkloadConfig.getQueryWorkloadName());
    try {
      znRecord.setSimpleField(QueryWorkloadConfig.NODE_CONFIGS,
          JsonUtils.objectToString(queryWorkloadConfig.getNodeConfigs()));
    } catch (Exception e) {
      String errorMessage = String.format("Failed to convert QueryWorkloadConfig : %s to ZNRecord",
          queryWorkloadConfig);
      throw new RuntimeException(errorMessage, e);
    }
  }


  /**
   * Gets a random controller URL by dynamically discovering all live controller instances from Helix.
   * This distributes load across all available controllers instead of always hitting the lead controller.
   *
   * @param helixManager The Helix manager to use for dynamic discovery
   * @return controller URL or null if not available
   */
  public static String getControllerUrl(HelixManager helixManager) {
    try {
      HelixDataAccessor helixDataAccessor = helixManager.getHelixDataAccessor();
      PropertyKey.Builder keyBuilder = helixDataAccessor.keyBuilder();

      // Get all live instances first (this is a single ZK call)
      List<String> liveInstances = helixDataAccessor.getChildNames(keyBuilder.liveInstances());
      if (liveInstances == null || liveInstances.isEmpty()) {
        LOGGER.warn("No live instances found in Helix");
        return null;
      }

      // Filter for live controller instances only
      List<String> liveControllerInstances = new ArrayList<>();
      for (String instanceName : liveInstances) {
        if (InstanceTypeUtils.isController(instanceName)) {
          liveControllerInstances.add(instanceName);
        }
      }

      if (liveControllerInstances.isEmpty()) {
        LOGGER.warn("No live controller instances found in Helix");
        return null;
      }

      String selectedInstance = liveControllerInstances.get(RANDOM.nextInt(liveControllerInstances.size()));
      ExtraInstanceConfig extraInstanceConfig = new ExtraInstanceConfig(
          helixDataAccessor.getProperty(keyBuilder.instanceConfig(selectedInstance)));
      String baseUrl = extraInstanceConfig.getComponentUrl();
      if (baseUrl == null) {
        LOGGER.warn("Unable to extract the base URL from controller instance config: {}", selectedInstance);
        return null;
      }
      LOGGER.info("Dynamically discovered controller URL from Helix (randomly selected from {} controllers): {}",
          liveControllerInstances.size(), baseUrl);
      return baseUrl;
    } catch (Exception e) {
      LOGGER.warn("Failed to dynamically discover controller URL from Helix", e);
      return null;
    }
  }

  /**
   * Fetches and updates the query workload budgets that this instance should use.
   * This method is called by the instance at startup, it fetches the workload budgets from the controller
   * and updates the WorkloadBudgetManager accordingly.
   *
   * @param instanceId The ID of the instance to fetch configs for.
   * @param helixManager The Helix manager to use for dynamic controller discovery (can be null).
   */
  public static void getAndUpdateWorkloadBudgets(String instanceId, @Nullable HelixManager helixManager) {
    try {
      WorkloadBudgetManager workloadBudgetManager = WorkloadBudgetManagerFactory.get();
      if (workloadBudgetManager == null) {
        LOGGER.info("WorkloadBudgetManager not initialized for instance: {}. Skipping fetching workload budgets.",
            instanceId);
        return;
      }
      String controllerUrl = getControllerUrl(helixManager);
      if (controllerUrl == null) {
        return;
      }
      URI queryWorkloadURI = new URI(controllerUrl + "/queryWorkloadConfigs/instance/" + instanceId);
      ClassicHttpRequest request = ClassicRequestBuilder.get(queryWorkloadURI)
          .setVersion(HttpVersion.HTTP_1_1)
          .setHeader(HttpHeaders.CONTENT_TYPE, HttpClient.JSON_CONTENT_TYPE)
          .build();
      AtomicReference<Map<String, InstanceCost>> workloadToInstanceCost = new AtomicReference<>(null);
      RetryPolicy retryPolicy = RetryPolicies.exponentialBackoffRetryPolicy(3, 3000L, 1.2f);
      retryPolicy.attempt(() -> {
        try {
          SimpleHttpResponse response = HttpClient.wrapAndThrowHttpException(
              HTTP_CLIENT.sendRequest(request, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS));
          if (response.getStatusCode() == HttpStatus.SC_OK) {
            workloadToInstanceCost.set(JsonUtils.stringToObject(response.getResponse(), new TypeReference<>() { }));
            LOGGER.info("Successfully fetched query workload configs from controller: {}, Instance: {}",
                    controllerUrl, instanceId);
            return true;
          }
          return false;
        } catch (Exception e) {
          if (e instanceof HttpErrorStatusException) {
            HttpErrorStatusException httpErrorStatusException = (HttpErrorStatusException) e;
            // Non-retriable errors
            if (httpErrorStatusException.getStatusCode() == HttpStatus.SC_BAD_REQUEST
                || httpErrorStatusException.getStatusCode() == HttpStatus.SC_FORBIDDEN
                || httpErrorStatusException.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
              LOGGER.info("Non-retriable error while fetching query workload configs from controller: {}, Instance: {},"
                      + " status code: {}",
                  controllerUrl, instanceId, httpErrorStatusException.getStatusCode());
              return true;
            }
          }
          LOGGER.warn("Failed to fetch query workload configs from controller: {}, Instance: {}", controllerUrl,
              instanceId, e);
          return false;
        }
      });
      Map<String, InstanceCost> instanceCostMap = workloadToInstanceCost.get();
      if (instanceCostMap != null) {
        instanceCostMap.forEach((workloadName, instanceCost) ->
            workloadBudgetManager.addOrUpdateWorkload(workloadName, instanceCost.getCpuCostNs(),
                instanceCost.getMemoryCostBytes()));
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to fetch query workload configs for instance: {}", instanceId, e);
    }
  }

  /**
   * Validates the given QueryWorkloadConfig and returns a list of validation error messages.
   *
   * @param config the QueryWorkloadConfig to validate
   * @return a list of validation errors; empty if config is valid
   */
  public static List<String> validateQueryWorkloadConfig(QueryWorkloadConfig config) {
    List<String> errors = new ArrayList<>();
    if (config == null) {
      errors.add("QueryWorkloadConfig cannot be null");
      return errors;
    }
    String name = config.getQueryWorkloadName();
    if (name == null || name.trim().isEmpty()) {
      errors.add("queryWorkloadName cannot be null or empty");
    }
    List<NodeConfig> nodeConfigs = config.getNodeConfigs();
    if (nodeConfigs == null || nodeConfigs.isEmpty()) {
      errors.add("nodeConfigs cannot be null or empty");
    } else {
      for (int i = 0; i < nodeConfigs.size(); i++) {
        NodeConfig nodeConfig = nodeConfigs.get(i);
        String prefix = "nodeConfigs[" + i + "]";
        if (nodeConfig == null) {
          errors.add(prefix + " cannot be null");
          continue;
        }
        if (nodeConfig.getNodeType() == null) {
          errors.add(prefix + ".type cannot be null");
        }
        // Validate EnforcementProfile
        EnforcementProfile enforcementProfile = nodeConfig.getEnforcementProfile();
        if (enforcementProfile == null) {
          errors.add(prefix + "enforcementProfile cannot be null");
        } else {
          long enforcementCpu = enforcementProfile.getCpuCostNs();
          long enforcementMem = enforcementProfile.getMemoryCostBytes();
           if (enforcementCpu <= 0) {
             errors.add(prefix + ".enforcementProfile.cpuCostNs has to positive");
           }
           if (enforcementMem <= 0) {
             errors.add(prefix + ".enforcementProfile.memoryCostBytes has to positive");
           }
          // Validate PropagationScheme
          PropagationScheme propagationScheme = nodeConfig.getPropagationScheme();
          if (propagationScheme == null) {
            errors.add(prefix + ".propagationScheme cannot be null");
          } else {
            PropagationScheme.Type propagationType = propagationScheme.getPropagationType();
            if (propagationType == null) {
              errors.add(prefix + ".propagationScheme.type cannot be null");
            }
            // Validate PropagationEntities
            validateEntityList(propagationScheme.getPropagationEntities(),
                prefix + ".propagationScheme.propagationEntities", errors,
                enforcementProfile.getCpuCostNs(), enforcementProfile.getMemoryCostBytes());
          }
        }
      }
    }
    return errors;
  }

  /**
   * Validates a list of PropagationEntity objects.
   * <p>
   * This method performs comprehensive validation including:
   * <ul>
   *   <li>Ensures the list is non-null and non-empty</li>
   *   <li>Checks for duplicate propagationEntity IDs</li>
   *   <li>Validates cpuCostNs and memoryCostBytes for non-null/positive values</li>
   *   <li>Ensures consistency in cost definitions across all entities (either all or none define costs)</li>
   *   <li>Validates that total costs do not exceed provided limits (if any)</li>
   *   <li>Validates any overrides within each entity for the same cost rules</li>
   *   <li>Rewrites empty costs to evenly distribute parent limits if all entities have empty costs</li>
   * </ul>
   *
   */
  private static void validateEntityList(List<PropagationEntity> entities, String prefix,
                                         List<String> errors, Long limitCpu, Long limitMem) {
    if (entities == null || entities.isEmpty()) {
      errors.add(prefix + " cannot be null or empty");
      return;
    }
    Set<String> seenIds = new HashSet<>();
    // Accumulate total CPU/memory costs to ensure they don't exceed enforcementProfile limits
    long totalCpu = 0;
    long totalMem = 0;
    // Track whether costs are defined or empty to ensure consistency across all entities
    int definedCount = 0;
    int emptyCount = 0;
    for (int i = 0; i < entities.size(); i++) {
      PropagationEntity entity = entities.get(i);
      String entityPrefix = prefix + "[" + i + "]";
      if (entity == null) {
        errors.add(entityPrefix + " cannot be null");
        continue;
      }
      validateDuplicateEntity(entity.getEntity(), entityPrefix, seenIds, errors);
      Long currentCpu = entity.getCpuCostNs();
      Long currentMem = entity.getMemoryCostBytes();
      // Both costs must be defined or both null
      // If both are defined, add to totalCpu and totalMem for limit validation
      // If both are null, do nothing
      if (currentCpu != null && currentMem != null) {
        totalCpu += costOrZero(entityPrefix, "cpuCostNs", currentCpu, errors);
        totalMem += costOrZero(entityPrefix, "memoryCostBytes", currentMem, errors);
        definedCount++;
      } else if (currentCpu == null && currentMem == null) {
        emptyCount++;
      } else {
        errors.add(entityPrefix + " must have both cpuCostNs and memoryCostBytes defined or both null");
        break;
      }
      if (definedCount > 0 && emptyCount > 0) {
        errors.add(prefix + " must have either all or none of the propagationEntities define costs");
        break;
      }
      List<PropagationEntityOverrides> overrides = entity.getOverrides();
      if (overrides != null && !overrides.isEmpty()) {
        validateOverrides(overrides, entityPrefix, errors, currentCpu, currentMem);
      }
    }
    validateLimits(totalCpu, totalMem, limitCpu, limitMem, prefix, errors);
    // If no errors and all entities have empty costs, rewrite to evenly distribute enforcementProfile costs
    if (errors.isEmpty() && definedCount == 0) {
      rewriteEmptyCosts(entities, limitCpu, limitMem);
    }
  }

  private static void validateOverrides(List<PropagationEntityOverrides> overrides, String prefix,
                                        List<String> errors, Long limitCpu, Long limitMem) {
    long totalCpu = 0;
    long totalMem = 0;
    for (int i = 0; i < overrides.size(); i++) {
      PropagationEntityOverrides override = overrides.get(i);
      String overridePrefix = prefix + ".overrides[" + i + "]";
      if (override == null) {
        errors.add(overridePrefix + " cannot be null");
        continue;
      }
      Set<String> seenIds = new HashSet<>();
      validateDuplicateEntity(override.getEntity(), overridePrefix, seenIds, errors);
      // For overrides, costs must be defined for each entry
      totalCpu += costOrZero(overridePrefix, "cpuCostNs", override.getCpuCostNs(), errors);
      totalMem += costOrZero(overridePrefix, "memoryCostBytes", override.getMemoryCostBytes(), errors);
    }
    validateLimits(totalCpu, totalMem, limitCpu, limitMem, prefix, errors);
  }

  private static void validateLimits(Long totalCpu, Long totalMem, Long limitCpu, Long limitMem,
      String prefix, List<String> errors) {
    if (limitCpu != null && totalCpu > limitCpu) {
      errors.add(prefix + " total CPU cost (" + totalCpu + " ns) exceeds parent/limit (" + limitCpu + " ns)");
    }
    if (limitMem != null && totalMem > limitMem) {
      errors.add(prefix + " total memory cost (" + totalMem + " bytes) exceeds parent/limit (" + limitMem + " bytes)");
    }
  }

  private static void validateDuplicateEntity(String entityId, String prefix, Set<String> entityIds,
      List<String> errors) {
    if (entityId == null || entityId.trim().isEmpty()) {
      errors.add(prefix + ".propagationEntity cannot be null or empty");
    } else {
      // Check for duplicate propagationEntity IDs
      if (entityIds.contains(entityId)) {
        errors.add(prefix + ".propagationEntity '" + entityId + "' is duplicated");
      } else {
        entityIds.add(entityId);
      }
    }
  }

  private static void rewriteEmptyCosts(List<PropagationEntity> entities, long totalCpu, long totalMem) {
    int numEntities = entities.size();
    long shareCpuCostNs = totalCpu / numEntities;
    long shareMemoryCostBytes = totalMem / numEntities;
    for (PropagationEntity entity : entities) {
      if (entity.getCpuCostNs() == null && entity.getMemoryCostBytes() == null) {
        entity.setCpuCostNs(shareCpuCostNs);
        entity.setMemoryCostBytes(shareMemoryCostBytes);
      }
    }
  }

  /** Validate non-null/non-negative and return the positive value (else 0) for accumulation. */
  private static long costOrZero(String prefix, String field, Long value, List<String> errors) {
    if (value == null) {
      errors.add(prefix + "." + field + " cannot be null");
      return 0L;
    }
    if (value <= 0) {
      errors.add(prefix + "." + field + " has to positive, got: " + value);
      return 0L;
    }
    return value;
  }

  /**
   * Handles a workload refresh request for a server or broker instance.
   *
   * @param requestString The JSON request string containing workload refresh data
   * @param instanceId The instance ID for logging and error messages
   * @param logger The logger to use for logging
   * @return JAX-RS Response with appropriate status code and message
   */
  public static Response handleRefreshRequest(String requestString, String instanceId, Logger logger) {
    try {
      QueryWorkloadRefreshRequest request = JsonUtils.stringToObject(requestString, QueryWorkloadRefreshRequest.class);
      WorkloadBudgetManager budgetManager = WorkloadBudgetManagerFactory.get();
      if (budgetManager == null) {
        String errorMsg = "WorkloadBudgetManager not initialized for instance: " + instanceId;
        logger.warn(errorMsg);
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(errorMsg).build();
      }
      logger.info("Processing {} workloads, operationType: {} on instance: {}",
          request.getWorkloadToCostMap().size(), request.getOperationType(), instanceId);
      int successCount = 0;
      int failureCount = 0;
      StringBuilder resultMessage = new StringBuilder();

      for (Map.Entry<String, InstanceCost> entry : request.getWorkloadToCostMap().entrySet()) {
        String workloadName = entry.getKey();
        InstanceCost instanceCost = entry.getValue();
        try {
          if (request.isDelete()) {
            budgetManager.deleteWorkload(workloadName);
            logger.info("Deleted workload: {} on instance: {}", workloadName, instanceId);
            resultMessage.append(String.format("Deleted: %s; ", workloadName));
            successCount++;
          } else if (request.isRefresh()) {
            if (instanceCost == null) {
              logger.error("InstanceCost is null for workload: {}", workloadName);
              resultMessage.append(String.format("Failed %s: null cost; ", workloadName));
              failureCount++;
              continue;
            }
            budgetManager.addOrUpdateWorkload(workloadName, instanceCost.getCpuCostNs(),
                instanceCost.getMemoryCostBytes());
            logger.info("Updated workload: {} on instance: {}", workloadName, instanceId);
            resultMessage.append(String.format("Updated: %s; ", workloadName));
            successCount++;
          }
        } catch (Exception e) {
          logger.error("Error processing workload: {}", workloadName, e);
          resultMessage.append(String.format("Failed %s: %s; ", workloadName, e.getMessage()));
          failureCount++;
        }
      }
      String message = String.format("Processed %d workloads: %d succeeded, %d failed. %s",
          request.getWorkloadToCostMap().size(), successCount, failureCount, resultMessage);

      if (failureCount > 0 && successCount == 0) {
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(message).build();
      } else if (failureCount > 0) {
        // Indicate partial success with 202 Accepted
        return Response.status(Response.Status.ACCEPTED).entity(message).build();
      } else {
        return Response.ok(message).build();
      }
    } catch (Exception e) {
      String errorMsg = "Error processing workload refresh request: " + e.getMessage();
      logger.error(errorMsg, e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(errorMsg).build();
    }
  }

  /**
   * Sends a workload refresh request with automatic retries and exponential backoff.
   * Uses async HTTP client for non-blocking I/O.
   *
   * @param url The target URL for the refresh endpoint
   * @param request The workload refresh request to send
   * @param instanceId The instance ID for logging
   * @return CompletableFuture that completes with true if successful, false otherwise
   */
  public static CompletableFuture<Boolean> sendWorkloadRefreshRequestWithRetry(String url,
      QueryWorkloadRefreshRequest request, String instanceId) {
    return sendWorkloadRefreshRequestWithRetryInternal(url, request, instanceId, 0);
  }

  private static CompletableFuture<Boolean> sendWorkloadRefreshRequestWithRetryInternal(String url,
      QueryWorkloadRefreshRequest request, String instanceId, int attemptNumber) {
    return sendWorkloadRefreshRequestAsync(request, url, instanceId)
        .thenCompose(success -> {
          if (success || attemptNumber >= WORKLOAD_PROPAGATION_MAX_RETRIES - 1) {
            return CompletableFuture.completedFuture(success);
          }
          // Exponential backoff before retry
          long delayMs = (long) (RETRY_INITIAL_DELAY_MS * Math.pow(RETRY_BACKOFF_MULTIPLIER, attemptNumber));
          LOGGER.debug("Retrying workload refresh for instance {} after {}ms delay (attempt {}/{})",
              instanceId, delayMs, attemptNumber + 1, WORKLOAD_PROPAGATION_MAX_RETRIES);
          CompletableFuture<Boolean> delayed = new CompletableFuture<>();
          CompletableFuture.delayedExecutor(delayMs, TimeUnit.MILLISECONDS)
              .execute(() -> {
                sendWorkloadRefreshRequestWithRetryInternal(url, request, instanceId, attemptNumber + 1)
                    .whenComplete((result, error) -> {
                      if (error != null) {
                        delayed.completeExceptionally(error);
                      } else {
                        delayed.complete(result);
                      }
                    });
              });
          return delayed;
        });
  }

  /**
   * Sends a workload refresh request to an instance asynchronously via HTTP POST using true non-blocking I/O.
   * Supports both HTTP and HTTPS using the TLS-configured HTTP_CLIENT.
   * This method does NOT block any threads - it uses Apache HttpAsyncClient for true async execution.
   *
   */
  public static CompletableFuture<Boolean> sendWorkloadRefreshRequestAsync(QueryWorkloadRefreshRequest request,
                                                                           String uri, String instanceId) {
    try {
      String requestBody = JsonUtils.objectToString(request);
      SimpleHttpRequest httpRequest = SimpleRequestBuilder.post(uri)
          .setHeader(HttpHeaders.CONTENT_TYPE, HttpClient.JSON_CONTENT_TYPE)
          .setBody(requestBody, ContentType.APPLICATION_JSON)
          .build();
      return HTTP_CLIENT.sendSimpleRequestAsync(httpRequest)
          .thenApply(response -> {
            try {
              SimpleHttpResponse wrappedResponse = HttpClient.wrapAndThrowHttpException(response);
              if (wrappedResponse.getStatusCode() == HttpStatus.SC_OK
                  || wrappedResponse.getStatusCode() == HttpStatus.SC_ACCEPTED) {
                LOGGER.info("Successfully sent workload refresh request to instance: {}", instanceId);
                return true;
              } else {
                LOGGER.error("Failed to send workload refresh request to instance: {}, status: {}, response: {}",
                    instanceId, wrappedResponse.getStatusCode(), wrappedResponse.getResponse());
                return false;
              }
            } catch (Exception e) {
              LOGGER.error("Error processing response for instance: {}", instanceId, e);
              return false;
            }
          })
          .exceptionally(ex -> {
            LOGGER.error("Exception sending async workload refresh request to instance: {}", instanceId, ex);
            return false;
          });
    } catch (Exception e) {
      LOGGER.error("Exception creating async workload refresh request for instance: {}", instanceId, e);
      return CompletableFuture.completedFuture(false);
    }
  }
}
