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
package org.apache.pinot.spi.config.workload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * Request object for propagating query workload configurations from controller to server/broker instances.
 * <p>
 * This class encapsulates workload budget information that needs to be applied on individual instances.
 * It supports both refresh (add/update) and delete operations, and can handle single or batch workload updates.
 * </p>
 *
 * <p><b>Usage:</b></p>
 * <ul>
 *   <li><b>Refresh Operation:</b> Updates or adds workload budgets with specified CPU and memory costs</li>
 *   <li><b>Delete Operation:</b> Removes workload budgets from instances (cost values can be null)</li>
 *   <li><b>Batch Updates:</b> Multiple workloads can be updated in a single request</li>
 * </ul>
 *
 * <p><b>Example JSON:</b></p>
 * <pre>{@code
 * {
 *   "workloadToCostMap": {
 *     "foo": {"cpuCostNs": 1000000, "memoryCostBytes": 1000000},
 *     "bar": {"cpuCostNs": 500000, "memoryCostBytes": 500000}
 *   },
 *   "operationType": "REFRESH_QUERY_WORKLOAD"
 * }
 * }</pre>
 */
public class QueryWorkloadRefreshRequest {

  /**
   * Type of operation to perform on workload budgets.
   */
  public enum OperationType {
    /** Add or update workload budgets with the specified costs */
    REFRESH_QUERY_WORKLOAD,
    /** Remove workload budgets from the instance */
    DELETE_QUERY_WORKLOAD
  }

  private final Map<String, InstanceCost> _workloadToCostMap;
  private final OperationType _operationType;

  @JsonCreator
  public QueryWorkloadRefreshRequest(
      @JsonProperty("workloadToCostMap") Map<String, InstanceCost> workloadToCostMap,
      @JsonProperty("operationType") OperationType operationType) {
    _workloadToCostMap = workloadToCostMap;
    _operationType = operationType;
  }


  public QueryWorkloadRefreshRequest(String workloadName, @Nullable InstanceCost instanceCost,
      OperationType operationType) {
    Map<String, InstanceCost> workloadToCostMap = new HashMap<>();
    workloadToCostMap.put(workloadName, instanceCost);
    _workloadToCostMap = workloadToCostMap;
    _operationType = operationType;
  }

  @JsonProperty("workloadToCostMap")
  public Map<String, InstanceCost> getWorkloadToCostMap() {
    return _workloadToCostMap;
  }

  @JsonProperty("operationType")
  public OperationType getOperationType() {
    return _operationType;
  }

  /**
   * Checks if this is a refresh operation.
   */
  @JsonIgnore
  public boolean isRefresh() {
    return _operationType == OperationType.REFRESH_QUERY_WORKLOAD;
  }

  /**
   * Checks if this is a delete operation.
   */
  @JsonIgnore
  public boolean isDelete() {
    return _operationType == OperationType.DELETE_QUERY_WORKLOAD;
  }

  @Override
  public String toString() {
    return "QueryWorkloadRefreshRequest{"
        + "workloadToCostMap=" + _workloadToCostMap
        + ", operationType='" + _operationType + '\''
        + '}';
  }
}
