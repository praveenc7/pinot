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
package org.apache.pinot.common.utils;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ExponentialMovingAverageTest {
  ScheduledExecutorService _executorService = Executors.newSingleThreadScheduledExecutor();

  @Test
  public void testAvgInitialization() {
    assertEquals(new ExponentialMovingAverage(0.5, -1, 0, 1.0, _executorService).getAverage(), 1.0);
    assertEquals(new ExponentialMovingAverage(0.5, -1, 0, 0.123, _executorService).getAverage(), 0.123);
    assertEquals(new ExponentialMovingAverage(0.5, -1, 0, 88.0, _executorService).getAverage(), 88.0);
    assertEquals(new ExponentialMovingAverage(0.5, -1, 0, 0.0, _executorService).getAverage(), 0.0);
  }

  @Test
  public void testWarmUpDuration()
      throws InterruptedException {
    // Test 1. Set warmupDurationMs to 5 seconds.
    ExponentialMovingAverage average = new ExponentialMovingAverage(0.5, -1, 5000, 0.0, _executorService);
    Random rand = new Random();
    for (int ii = 0; ii < 10; ii++) {
      average.compute(rand.nextDouble());
      assertEquals(average.getAverage(), 0.0, "Iteration=" + ii);
    }
    Thread.sleep(5000);
    average.compute(1.0);
    assertEquals(average.getAverage(), 0.5);

    // Test 2
    average = new ExponentialMovingAverage(0.5, -1, 0, 0.0, _executorService);
    average.compute(1.0);
    assertEquals(average.getAverage(), 0.5);
  }

  @Test
  public void testAverage() {
    // Test 1.
    ExponentialMovingAverage average = new ExponentialMovingAverage(1.0, -1, 0, 0.0, _executorService);
    assertEquals(average.getAverage(), 0.0);
    average.compute(0.5);
    assertEquals(average.getAverage(), 0.5);
    average.compute(0.112);
    assertEquals(average.getAverage(), 0.112);
    average.compute(10.0);
    assertEquals(average.getAverage(), 10.0);

    // Test 2.
    average = new ExponentialMovingAverage(0.5, -1, 0, 0.0, _executorService);
    assertEquals(average.getAverage(), 0.0);
    average.compute(0.1);
    assertEquals(average.getAverage(), 0.05);
    average.compute(0.5);
    assertEquals(average.getAverage(), 0.275);
    average.compute(1.25);
    assertEquals(average.getAverage(), 0.7625);

    // Test 3
    average = new ExponentialMovingAverage(0.3, -1, 0, 0.0, _executorService);
    assertEquals(average.getAverage(), 0.0);
    average.compute(1.0);
    assertEquals(average.getAverage(), 0.3);
    average.compute(1.0);
    assertEquals(average.getAverage(), 0.51);
    average.compute(1.0);
    assertEquals(average.getAverage(), 0.657);
  }

  @Test
  public void testAutoDecay()
      throws InterruptedException {
    // Test 1: Test decay
    ExponentialMovingAverage average = new ExponentialMovingAverage(0.3, 10, 0, 0.0, _executorService);
    average.compute(10.0);
    double currAvg = average.getAverage();

    for (int ii = 0; ii < 10; ii++) {
      Thread.sleep(100);
      assertTrue(average.getAverage() < currAvg);
    }

    // Test 2: Test no decay
    average = new ExponentialMovingAverage(1.0, -1, 0, 0, _executorService);
    average.compute(10.0);
    currAvg = average.getAverage();

    for (int jj = 0; jj < 10; jj++) {
      Thread.sleep(100);
      assertEquals(average.getAverage(), currAvg);
    }
  }

  /**
   * decayIfStale must move the average towards the supplied target rather than towards zero, and must converge to
   * the target rather than overshoot it. This is the primitive the adaptive server selector's latency baseline is
   * built on.
   */
  @Test
  public void testDecayIfStaleMovesTowardsTarget() {
    ExponentialMovingAverage ema = new ExponentialMovingAverage(0.5, 1000, 0, 100.0, _executorService, () -> 10.0);
    // An average that has never been updated is not eligible to decay, so record the initial value as an
    // observation first.
    ema.compute(100.0);
    long now = System.currentTimeMillis();

    // Not stale yet: the decay window has not elapsed since the update.
    ema.decayIfStale(now);
    assertEquals(ema.getAverage(), 100.0);

    // Stale: each call halves the distance to the target.
    ema.decayIfStale(now + 2000);
    assertEquals(ema.getAverage(), 55.0, 1e-9);

    for (int i = 2; i < 60; i++) {
      ema.decayIfStale(now + 2000L * i);
    }
    assertEquals(ema.getAverage(), 10.0, 1e-6, "The average must converge to the target, not to zero.");
  }

  /**
   * A non-finite or negative decay target must be skipped rather than poisoning the average. NaN in particular
   * would propagate into the routing score and corrupt server ranking.
   */
  @Test
  public void testDecayIfStaleSkipsInvalidTargets() {
    long now = System.currentTimeMillis();
    for (double target : new double[]{Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -1.0}) {
      ExponentialMovingAverage ema =
          new ExponentialMovingAverage(0.5, 1000, 0, 100.0, _executorService, () -> target);
      ema.compute(100.0);
      ema.decayIfStale(now + 5000);
      assertEquals(ema.getAverage(), 100.0, "Target " + target + " must have been skipped.");
    }
  }

  /**
   * computeAndReportIfApplied must report false for samples the warm-up period discards, so that callers can tell a
   * genuine measurement apart from one that left the average holding its initialization value.
   */
  @Test
  public void testComputeReportsWhetherSampleWasApplied()
      throws Exception {
    ExponentialMovingAverage warmingUp = new ExponentialMovingAverage(0.5, -1, 30_000, 1.0, _executorService);
    assertTrue(!warmingUp.computeAndReportIfApplied(500.0), "A warm-up sample must not be reported as applied.");
    assertEquals(warmingUp.getAverage(), 1.0, "A warm-up sample must not move the average.");

    ExponentialMovingAverage warm = new ExponentialMovingAverage(0.5, -1, 0, 1.0, _executorService);
    // The warm-up window is measured from construction, so ensure it has strictly elapsed.
    Thread.sleep(5);
    assertTrue(warm.computeAndReportIfApplied(500.0), "A post warm-up sample must be reported as applied.");
    assertEquals(warm.getAverage(), 250.5, 1e-9);
  }
}
