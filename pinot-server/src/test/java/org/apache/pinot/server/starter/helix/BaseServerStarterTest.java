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

package org.apache.pinot.server.starter.helix;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class BaseServerStarterTest {

  private static final long MAX_QUERY_TIME_MS = 15_000L;

  @Test
  public void testRemainingDrainSleepUncapped() {
    // Latest query finishes well before the shutdown deadline -> sleep is full remaining query time.
    long currentTimeMs = 100_000L;
    long latestQueryTimeMs = currentTimeMs - 5_000L; // last query was 5s ago, has 10s left
    long endTimeMs = currentTimeMs + 60_000L;        // plenty of time before shutdown deadline

    long sleepMs = BaseServerStarter.computeRemainingDrainSleepMs(latestQueryTimeMs, MAX_QUERY_TIME_MS, currentTimeMs,
        endTimeMs);

    assertEquals(sleepMs, 10_000L,
        "Should wait the remainder of maxQueryTimeMs when the deadline is not the binding constraint");
  }

  @Test
  public void testRemainingDrainSleepCappedByDeadline() {
    // Regression test for the bug where the post-drain sleep ignored endTimeMs and could overrun the
    // configured shutdown timeout by up to maxQueryTimeMs.
    long currentTimeMs = 100_000L;
    long latestQueryTimeMs = currentTimeMs;          // a query just landed; would normally need 15s
    long endTimeMs = currentTimeMs + 2_000L;         // only 2s left in the shutdown budget

    long sleepMs = BaseServerStarter.computeRemainingDrainSleepMs(latestQueryTimeMs, MAX_QUERY_TIME_MS, currentTimeMs,
        endTimeMs);

    assertEquals(sleepMs, 2_000L, "Sleep must be capped by endTimeMs so shutdown does not overrun its deadline");
    assertTrue(sleepMs <= endTimeMs - currentTimeMs, "Sleep must never push past endTimeMs");
  }

  @Test
  public void testRemainingDrainSleepNoSleepWhenQueriesAlreadyFinished() {
    // Latest query already finished before now -> no sleep needed.
    long currentTimeMs = 100_000L;
    long latestQueryTimeMs = currentTimeMs - 30_000L; // 30s ago, finish time was 15s ago
    long endTimeMs = currentTimeMs + 60_000L;

    long sleepMs = BaseServerStarter.computeRemainingDrainSleepMs(latestQueryTimeMs, MAX_QUERY_TIME_MS, currentTimeMs,
        endTimeMs);

    assertTrue(sleepMs <= 0L, "Should not sleep when in-flight queries should already be finished, got " + sleepMs);
  }

  @Test
  public void testRemainingDrainSleepAtDeadline() {
    // Edge case: deadline equals current time -> no sleep regardless of latest query.
    long currentTimeMs = 100_000L;
    long latestQueryTimeMs = currentTimeMs;
    long endTimeMs = currentTimeMs;

    long sleepMs = BaseServerStarter.computeRemainingDrainSleepMs(latestQueryTimeMs, MAX_QUERY_TIME_MS, currentTimeMs,
        endTimeMs);

    assertEquals(sleepMs, 0L, "No sleep when the deadline is already reached");
  }
}
