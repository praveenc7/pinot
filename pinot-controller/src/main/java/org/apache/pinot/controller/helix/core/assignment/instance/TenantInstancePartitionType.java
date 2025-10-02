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
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enum representing different types of tenant instance partitions.
 * Each type has a specific tenant tag format.
 */
public enum TenantInstancePartitionType {
  OFFLINE("OFFLINE", "_OFFLINE", "_OFFLINE"),
  CONSUMING("CONSUMING", "_REALTIME", "_CONSUMING"),
  COMPLETED("COMPLETED", "Completed_OFFLINE", "Completed_OFFLINE");

  private static final String TENANT_PREFIX = "Tenant-";

  private final String _value;
  private final String _helixInstanceTagSuffix;
  private final String _partitionNameSuffix;


  TenantInstancePartitionType(String value, String helixInstanceTagSuffix, String partitionNameSuffix) {
    _value = value;
    _helixInstanceTagSuffix = helixInstanceTagSuffix;
    _partitionNameSuffix = partitionNameSuffix;
  }

  /**
   * Gets the string value of the enum for serialization.
   *
   * @return the string value
   */
  @JsonValue
  public String getValue() {
    return _value;
  }

  @JsonCreator
  public static TenantInstancePartitionType fromString(String value) {
    for (TenantInstancePartitionType type : values()) {
      if (type._value.equalsIgnoreCase(value)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Invalid TenantInstancePartitionType: " + value
        + ". Valid values are: OFFLINE, CONSUMING, COMPLETED");
  }

  /**
   * Gets the expected tenant tag stamped in Helix participant for each instance partition type.
   *  OFFLINE     -> tenantName + "_OFFLINE"
   *  CONSUMING   -> tenantName + "_REALTIME"
   *  COMPLETED   -> tenantName + "Completed_OFFLINE"
   *
   * @param tenantName the base tenant name
   * @return the full tenant tag string
   */
  public String getHelixInstanceTag(String tenantName) {
    return tenantName + _helixInstanceTagSuffix;
  }

  /**
   * Gets the expected tenant Instance Partition stamped in Helix participant for each instance partition type.
   *   OFFLINE     -> "Tenant-" + tenantName + "_OFFLINE"
   *   CONSUMING   -> "Tenant-" + tenantName + "_CONSUMING"
   *   COMPLETED   -> "Tenant-" + tenantName + "Completed_OFFLINE"
   *
   * @param tenantName the base tenant name
   * @return the full tenant tag string
   */
  public String getTenantInstancePartitionName(String tenantName) {
    return TENANT_PREFIX + tenantName + _partitionNameSuffix;
  }

  @Override
  public String toString() {
    return _value;
  }
}
