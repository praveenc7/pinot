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
package org.apache.pinot.common.lineage;

/**
 * Enum for represent the state of lineage entry.
 *
 * STAGED is a terminal-but-not-visible state used by the delayed consistent push protocol: new segments are ONLINE in
 * the external view but queries still route to {@code segmentsFrom} (same routing behavior as IN_PROGRESS). An
 * operator-driven bulk transition flips STAGED entries to COMPLETED atomically.
 */
public enum LineageEntryState {
  IN_PROGRESS, STAGED, COMPLETED, REVERTED
}
