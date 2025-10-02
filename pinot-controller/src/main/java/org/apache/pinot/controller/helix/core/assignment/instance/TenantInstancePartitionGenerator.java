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

import javax.annotation.Nullable;
import org.apache.pinot.common.assignment.InstancePartitions;


/**
 * Interface for generating Tenant instance partitions. Provides a pluggable architecture for different Tenant IP
 * strategies.
 */
public interface TenantInstancePartitionGenerator {

  /**
   * Generates instance partitions for the given tenant based on the provided request parameters.
   *
   * @param tenantName                     The tenant name for which to generate instance partitions
   * @param instancePartitionType          The type of instance partition (OFFLINE, CONSUMING, COMPLETED)
   * @param request                        The request containing allocation parameters
   * @param dryRun                         If true, does not persist Tenant InstancePartition to ZK.
   * @param currentTenantInstancePartition The current TenantInstancePartition
   */
  InstancePartitions assignInstances(
      String tenantName,
      TenantInstancePartitionType instancePartitionType,
      TenantInstancePartitionRequest request,
      boolean dryRun,
      @Nullable InstancePartitions currentTenantInstancePartition);
}
