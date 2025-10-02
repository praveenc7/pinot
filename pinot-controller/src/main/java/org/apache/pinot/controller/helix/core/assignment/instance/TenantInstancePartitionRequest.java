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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Request object for creating tenant instance partitions.
 *
 * Note: prioritizeExistingInstances option was removed for API simplicity. This option controlled
 * MZ sorting priority to prefer MZs with existing unused tenant instances. Can be re-added later
 * if needed for minimizing instance churn during operations.
 */
public class TenantInstancePartitionRequest {
  @JsonPropertyDescription("Number of replica groups in the tenant")
  private final int _numReplicaGroups;

  @JsonPropertyDescription("Number of instances per replica group in the tenant")
  private final int _numInstancesPerReplicaGroup;

  @JsonPropertyDescription("Optional pool of candidate instances to use for allocation. "
      + "If not provided, will auto-discover instances with the tenant tag. "
      + "Must contain AT LEAST (numReplicaGroups × numInstancesPerReplicaGroup) instances. "
      + "All instances must be tagged with the tenant.")
  private final Set<String> _candidateInstances;

  // TODO: Check if the following options are needed as parameters here:
  //  1. _relaxMZConstraints - When true, allows relaxing MZ constraints if they cannot be satisfied with available
  //  2. _mzConstraintPercentage - max percentage of instances per MZ per mirror server set
  //  3. prioritizeExistingInstances - This option controlled MZ sorting priority to prefer MZs with existing unused
  //  tenant instances.

  @JsonCreator
  public TenantInstancePartitionRequest(
      @JsonProperty("numReplicaGroups") int numReplicaGroups,
      @JsonProperty("numInstancesPerReplicaGroup") int numInstancesPerReplicaGroup,
      @JsonProperty("candidateInstances") @Nullable Set<String> candidateInstances) {
    _numReplicaGroups = numReplicaGroups;
    _numInstancesPerReplicaGroup = numInstancesPerReplicaGroup;
    _candidateInstances = candidateInstances;
  }

  @JsonProperty("numReplicaGroups")
  public int getNumReplicaGroups() {
    return _numReplicaGroups;
  }

  @JsonProperty("numInstancesPerReplicaGroup")
  public int getNumInstancesPerReplicaGroup() {
    return _numInstancesPerReplicaGroup;
  }

  @JsonProperty("candidateInstances")
  @Nullable
  public Set<String> getCandidateInstances() {
    return _candidateInstances;
  }
}
