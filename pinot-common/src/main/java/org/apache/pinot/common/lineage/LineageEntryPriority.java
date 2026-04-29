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
 * LineageEntryPriority defines some common used priorities of the lineage entry.The larger the value is, the lower
 * priority the entry has. A lineage entry update request with higher priority can force clean up (revert an
 * IN_PROGRESS entry) a lineage entry with lower priority or equal priority.
 */
public final class LineageEntryPriority {
  // Prevent instantiation
  private LineageEntryPriority() {
  }

  // PO is usually used by manual operations, such as request from pinot-tool to force clean up everything.
  public static final int P0 = 0;
  // P1 is usually used by pinot offline push job, we want it go through first if it's conflict with pinot minion tasks
  public static final int P1 = 1;
  // P2 is the default priority for lineage entry, usually used by pinot minion tasks, such as merge-and-roll-up task.
  // We want it to be preempted by pinot offline push job if it's conflict.
  public static final int P2 = 2;
  public static final int DEFAULT_LINEAGE_PRIORITY = P2;
}
