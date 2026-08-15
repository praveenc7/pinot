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
package org.apache.pinot.core.transport;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.InstanceRequest;


/**
 * Immutable physical request plan grouped by the primary logical server request.
 */
public class QueryRequestPlan {
  private final Map<ServerRoutingInstance, RequestGroup> _requestGroups;

  public QueryRequestPlan(Map<ServerRoutingInstance, RequestGroup> requestGroups) {
    _requestGroups = Collections.unmodifiableMap(new HashMap<>(requestGroups));
  }

  public static QueryRequestPlan primaryOnly(Map<ServerRoutingInstance, InstanceRequest> primaryRequests) {
    Map<ServerRoutingInstance, RequestGroup> requestGroups = new HashMap<>(primaryRequests.size());
    for (Map.Entry<ServerRoutingInstance, InstanceRequest> entry : primaryRequests.entrySet()) {
      requestGroups.put(entry.getKey(), new RequestGroup(entry.getKey(), entry.getValue(), null, null, null));
    }
    return new QueryRequestPlan(requestGroups);
  }

  public Map<ServerRoutingInstance, RequestGroup> getRequestGroups() {
    return _requestGroups;
  }

  public Map<ServerRoutingInstance, InstanceRequest> getPrimaryRequestMap() {
    Map<ServerRoutingInstance, InstanceRequest> primaryRequests = new HashMap<>(_requestGroups.size());
    for (RequestGroup requestGroup : _requestGroups.values()) {
      primaryRequests.put(requestGroup.getPrimaryServer(), requestGroup.getPrimaryRequest());
    }
    return primaryRequests;
  }

  public Set<ServerInstance> getPotentialHedgeServers() {
    Set<ServerInstance> servers = new HashSet<>();
    for (RequestGroup requestGroup : _requestGroups.values()) {
      if (requestGroup.getAlternateServerInstance() != null) {
        servers.add(requestGroup.getAlternateServerInstance());
      }
    }
    return servers;
  }

  public static class RequestGroup {
    private final ServerRoutingInstance _primaryServer;
    private final InstanceRequest _primaryRequest;
    private final ServerRoutingInstance _alternateServer;
    private final InstanceRequest _alternateRequest;
    private final ServerInstance _alternateServerInstance;

    public RequestGroup(ServerRoutingInstance primaryServer, InstanceRequest primaryRequest,
        @Nullable ServerRoutingInstance alternateServer, @Nullable InstanceRequest alternateRequest,
        @Nullable ServerInstance alternateServerInstance) {
      _primaryServer = primaryServer;
      _primaryRequest = primaryRequest;
      _alternateServer = alternateServer;
      _alternateRequest = alternateRequest;
      _alternateServerInstance = alternateServerInstance;
    }

    public ServerRoutingInstance getPrimaryServer() {
      return _primaryServer;
    }

    public InstanceRequest getPrimaryRequest() {
      return _primaryRequest;
    }

    @Nullable
    public ServerRoutingInstance getAlternateServer() {
      return _alternateServer;
    }

    @Nullable
    public InstanceRequest getAlternateRequest() {
      return _alternateRequest;
    }

    @Nullable
    public ServerInstance getAlternateServerInstance() {
      return _alternateServerInstance;
    }

    void validate() {
      Preconditions.checkNotNull(_primaryRequest, "Primary request must not be null for server: %s", _primaryServer);
      Preconditions.checkState((_alternateServer == null) == (_alternateRequest == null),
          "Alternate server and request must both be set or both be null for primary: %s", _primaryServer);
      Preconditions.checkState((_alternateServer == null) == (_alternateServerInstance == null),
          "Alternate routing and server metadata must both be set or both be null for primary: %s", _primaryServer);
    }
  }
}
