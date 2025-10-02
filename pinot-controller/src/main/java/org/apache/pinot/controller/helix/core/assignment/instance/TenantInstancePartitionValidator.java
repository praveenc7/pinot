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
package org.apache.pinot.controller.helix.core.assignment.instance;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validator Tenant instance partition requests.
 * Enforces strict validation rules for candidate instances.
 */
public class TenantInstancePartitionValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(TenantInstancePartitionValidator.class);

  private final PinotHelixResourceManager _pinotHelixResourceManager;

  public TenantInstancePartitionValidator(PinotHelixResourceManager pinotHelixResourceManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
  }

  /**
   * Validates the instance partition request.
   *
   * @param tenantName The tenant name
   * @param instancePartitionType The instance partition type
   * @param request The request to validate
   * @throws ControllerApplicationException if validation fails
   */
  public void validateRequest(String tenantName, TenantInstancePartitionType instancePartitionType,
      TenantInstancePartitionRequest request) {

    // Basic parameter validation
    if (request.getNumReplicaGroups() <= 0 || request.getNumInstancesPerReplicaGroup() <= 0) {
      throw new ControllerApplicationException(LOGGER,
          "numReplicaGroups and numInstancesPerReplicaGroup must be positive, replica: "
              + request.getNumReplicaGroups() + ", instancesPerReplica: "
              + request.getNumInstancesPerReplicaGroup(),
          Response.Status.BAD_REQUEST);
    }

    // Validate candidate instances if provided
    Set<String> candidateInstances = request.getCandidateInstances();
    if (candidateInstances != null && !candidateInstances.isEmpty()) {
      validateCandidateInstances(tenantName, instancePartitionType, candidateInstances, request);
    }
  }

  /**
   * Validates candidate instances meet all requirements:
   * 1. Count >= numReplicaGroups × numInstancesPerReplicaGroup
   * 2. All instances are tagged to the tenant
   * 3. All instances exist in the cluster
   */
  private void validateCandidateInstances(String tenantName, TenantInstancePartitionType instancePartitionType,
      Set<String> candidateInstances, TenantInstancePartitionRequest request) {
    int requiredInstanceCount = request.getNumReplicaGroups() * request.getNumInstancesPerReplicaGroup();

    // Validate count
    if (candidateInstances.size() < requiredInstanceCount) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Insufficient candidate instances. Required: %d, Provided: %d. "
              + "candidateInstances must contain at least (numReplicaGroups × numInstancesPerReplicaGroup) "
              + "instances.", requiredInstanceCount, candidateInstances.size()),
          Response.Status.BAD_REQUEST);
    }

    // Get the expected tenant tag
    String expectedTenantTag = instancePartitionType.getHelixInstanceTag(tenantName);

    // Validate all instances exist and have correct tenant tag
    List<String> invalidInstances = candidateInstances.stream()
        .filter(instanceName -> !isValidTenantInstance(instanceName, expectedTenantTag))
        .collect(Collectors.toList());

    if (!invalidInstances.isEmpty()) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Invalid candidate instances. The following instances either don't exist "
              + "or are not tagged with tenant '%s': %s",
              expectedTenantTag, invalidInstances),
          Response.Status.BAD_REQUEST);
    }

    LOGGER.info("Validated {} candidate instances for tenant '{}' (type: {})",
        candidateInstances.size(), tenantName, instancePartitionType);
  }

  /**
   * Checks if an instance exists in the cluster and has the expected tenant tag.
   */
  private boolean isValidTenantInstance(String instanceName, String expectedTenantTag) {
    try {
      List<String> tags = _pinotHelixResourceManager.getTagsForInstance(instanceName);
      boolean hasExpectedTag = tags.contains(expectedTenantTag);

      if (!hasExpectedTag) {
        LOGGER.warn("Instance '{}' does not have expected tenant tag '{}'. Current tags: {}",
            instanceName, expectedTenantTag, tags);
      }

      return hasExpectedTag;
    } catch (Exception e) {
      LOGGER.error("Error validating instance '{}': {}", instanceName, e.getMessage());
      return false;
    }
  }
}
