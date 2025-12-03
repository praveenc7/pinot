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
package org.apache.pinot.server.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.utils.config.QueryWorkloadConfigUtils;
import org.apache.pinot.server.api.AdminApiApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REST API resource for managing query workload configurations on server instances.
 * <p>
 * This resource handles workload budget updates sent from the controller via direct HTTP calls,
 * bypassing Helix messaging for better performance and reduced ZooKeeper overhead.
 * </p>
 */
@Api(tags = "QueryWorkload")
@Path("/")
public class QueryWorkloadConfigResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryWorkloadConfigResource.class);

  @Inject
  @Named(AdminApiApplication.SERVER_INSTANCE_ID)
  private String _instanceId;

  /**
   * Refreshes query workload configurations on this instance.
   * <p>
   * This endpoint processes workload budget updates sent from the controller.
   * It supports batch updates of multiple workloads in a single request.
   * </p>
   *
   * <p><b>Supported Operations:</b></p>
   * <ul>
   *   <li><b>REFRESH_QUERY_WORKLOAD:</b> Add or update workload budgets with specified costs</li>
   *   <li><b>DELETE_QUERY_WORKLOAD:</b> Remove workload budgets from this instance</li>
   * </ul>
   *
   * <p><b>Request Body Example (Refresh):</b></p>
   * <pre>{@code
   * {
   *   "workloadToCostMap": {
   *     "foo": {"cpuCostNs": 1000000, "memoryCostBytes": 1000000},
   *     "bar": {"cpuCostNs": 500000, "memoryCostBytes": 500000}
   *   },
   *   "operationType": "REFRESH_QUERY_WORKLOAD"
   * }
   * }</pre>
   *
   * <p><b>Request Body Example (Delete):</b></p>
   * <pre>{@code
   * {
   *   "workloadToCostMap": {
   *     "foo": null
   *   },
   *   "operationType": "DELETE_QUERY_WORKLOAD"
   * }
   * }</pre>
   *
   * @param requestString JSON request containing workload-to-cost map and operation type
   * @return HTTP 200 (success), 202 (partial success), 400 (bad request), or 500 (error)
   */
  @POST
  @Path("/queryWorkloadConfigs/refresh")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Refresh query workload configuration",
      notes = "Updates or deletes workload budget configuration on this instance. Supports batch updates.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully updated workload configuration"),
      @ApiResponse(code = 400, message = "Invalid request body"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public Response refreshQueryWorkloadConfig(String requestString) {
    return QueryWorkloadConfigUtils.handleRefreshRequest(requestString, _instanceId, LOGGER);
  }
}
