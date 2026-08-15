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
package org.apache.pinot.core.routing;

import org.apache.pinot.core.transport.ServerInstance;


/**
 * Exact alternate route for a primary server request.
 */
public class AlternateServerRouteInfo {
  private final ServerInstance _serverInstance;
  private final ServerRouteInfo _serverRouteInfo;

  public AlternateServerRouteInfo(ServerInstance serverInstance, ServerRouteInfo serverRouteInfo) {
    _serverInstance = serverInstance;
    _serverRouteInfo = serverRouteInfo;
  }

  public ServerInstance getServerInstance() {
    return _serverInstance;
  }

  public ServerRouteInfo getServerRouteInfo() {
    return _serverRouteInfo;
  }
}
