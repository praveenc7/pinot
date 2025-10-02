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

import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;

/**
 * Default implementation of TenantInstancePartitionGenerator that provides
 * simple round-robin assignment of instances to replica groups.
 *
 * When candidate instances are provided, it arranges them in a round-robin
 * fashion across the specified replica groups. When candidate instances are
 * not provided, it auto-discovers instances by looking through tenant tagged
 * instances in Helix ZooKeeper.
 */
public class DefaultTenantInstancePartitionGenerator implements TenantInstancePartitionGenerator {

  private final PinotHelixResourceManager _pinotHelixResourceManager;

  /**
   * Constructor with PinotHelixResourceManager dependency for auto-discovery capabilities.
   *
   * @param pinotHelixResourceManager the Pinot Helix resource manager (can be null)
   */
  public DefaultTenantInstancePartitionGenerator(PinotHelixResourceManager pinotHelixResourceManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
  }

  @Override
  public InstancePartitions assignInstances(String tenantName, TenantInstancePartitionType instancePartitionType,
      TenantInstancePartitionRequest request, boolean dryRun, InstancePartitions currentTenantInstancePartition) {

    // TODO: Implement simple round-robin assignment logic:
    // 1. If candidateInstances are provided, use them directly
    // 2. If not provided, auto-discover them by looking through tenant tagged instances in Helix-ZK
    //    (use _pinotHelixResourceManager.getAllHelixInstanceConfigs() if available)
    // 3. Arrange instances in basic round robin fashion to get the instancePartition
    // 4. Return InstancePartitions object with the assignment

    throw new UnsupportedOperationException("DefaultTenantInstancePartitionGenerator not yet implemented");
  }
}
