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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.Test;


public class HedgeBudgetManagerTest {

  @Test
  public void testPrimaryRatioAdmissionAtOnePercent() {
    TestClock testClock = new TestClock();
    HedgeBudgetManager hedgeBudgetManager = createManager(testClock, 0.01d, 60_000L, 32);

    hedgeBudgetManager.recordPrimaryRequests(99);
    Assert.assertEquals(hedgeBudgetManager.getPrimaryRequestsInWindow(), 99L);
    Assert.assertEquals(hedgeBudgetManager.tryAcquire(), HedgeBudgetManager.AdmissionResult.RATIO_LIMIT);

    hedgeBudgetManager.recordPrimaryRequest();
    Assert.assertEquals(hedgeBudgetManager.tryAcquire(), HedgeBudgetManager.AdmissionResult.ACQUIRED);

    hedgeBudgetManager.recordPrimaryRequests(100);
    Assert.assertEquals(hedgeBudgetManager.getPrimaryRequestsInWindow(), 200L);
    Assert.assertEquals(hedgeBudgetManager.tryAcquire(), HedgeBudgetManager.AdmissionResult.ACQUIRED);
    Assert.assertEquals(hedgeBudgetManager.getHedgeRequestsInWindow(), 2L);
    Assert.assertEquals(hedgeBudgetManager.getActiveHedges(), 2);

    hedgeBudgetManager.release();
    hedgeBudgetManager.release();
    Assert.assertEquals(hedgeBudgetManager.getActiveHedges(), 0);
  }

  @Test
  public void testAdmissionRetentionAfterRelease() {
    TestClock testClock = new TestClock();
    HedgeBudgetManager hedgeBudgetManager = createManager(testClock, 0.01d, 60_000L, 32);

    hedgeBudgetManager.recordPrimaryRequests(100);
    Assert.assertEquals(hedgeBudgetManager.tryAcquire(), HedgeBudgetManager.AdmissionResult.ACQUIRED);
    hedgeBudgetManager.release();

    Assert.assertEquals(hedgeBudgetManager.getActiveHedges(), 0);
    Assert.assertEquals(hedgeBudgetManager.getHedgeRequestsInWindow(), 1L);
    Assert.assertEquals(hedgeBudgetManager.tryAcquire(), HedgeBudgetManager.AdmissionResult.RATIO_LIMIT);
  }

  @Test
  public void testRollingExpiryDoesNotStoreCredit() {
    TestClock testClock = new TestClock();
    HedgeBudgetManager hedgeBudgetManager = createManager(testClock, 0.5d, 2_000L, 32);

    hedgeBudgetManager.recordPrimaryRequests(10);
    Assert.assertEquals(hedgeBudgetManager.getPrimaryRequestsInWindow(), 10L);

    testClock.setCurrentTimeMillis(2_000L);
    Assert.assertEquals(hedgeBudgetManager.getPrimaryRequestsInWindow(), 0L);
    Assert.assertEquals(hedgeBudgetManager.tryAcquire(), HedgeBudgetManager.AdmissionResult.RATIO_LIMIT);
  }

  @Test
  public void testBucketRollover() {
    TestClock testClock = new TestClock();
    HedgeBudgetManager hedgeBudgetManager = createManager(testClock, 1.0d, 2_500L, 32);

    hedgeBudgetManager.recordPrimaryRequests(3);
    testClock.setCurrentTimeMillis(1_000L);
    hedgeBudgetManager.recordPrimaryRequests(4);
    testClock.setCurrentTimeMillis(2_000L);
    hedgeBudgetManager.recordPrimaryRequests(5);

    Assert.assertEquals(hedgeBudgetManager.getPrimaryRequestsInWindow(), 12L);

    testClock.setCurrentTimeMillis(3_000L);
    Assert.assertEquals(hedgeBudgetManager.getPrimaryRequestsInWindow(), 9L);

    testClock.setCurrentTimeMillis(4_000L);
    Assert.assertEquals(hedgeBudgetManager.getPrimaryRequestsInWindow(), 5L);
  }

  @Test
  public void testConcurrencyLimit32() {
    TestClock testClock = new TestClock();
    HedgeBudgetManager hedgeBudgetManager = createManager(testClock, 1.0d, 60_000L, 32);

    hedgeBudgetManager.recordPrimaryRequests(1_000);
    for (int i = 0; i < 32; i++) {
      Assert.assertEquals(hedgeBudgetManager.tryAcquire(), HedgeBudgetManager.AdmissionResult.ACQUIRED);
    }

    Assert.assertEquals(hedgeBudgetManager.tryAcquire(), HedgeBudgetManager.AdmissionResult.CONCURRENCY_LIMIT);
    Assert.assertEquals(hedgeBudgetManager.getActiveHedges(), 32);
    Assert.assertEquals(hedgeBudgetManager.getHedgeRequestsInWindow(), 32L);

    for (int i = 0; i < 32; i++) {
      hedgeBudgetManager.release();
    }
    Assert.assertEquals(hedgeBudgetManager.getActiveHedges(), 0);
  }

  @Test
  public void testOverReleaseFailsExplicitly() {
    TestClock testClock = new TestClock();
    HedgeBudgetManager hedgeBudgetManager = createManager(testClock, 1.0d, 60_000L, 32);

    hedgeBudgetManager.recordPrimaryRequest();
    Assert.assertEquals(hedgeBudgetManager.tryAcquire(), HedgeBudgetManager.AdmissionResult.ACQUIRED);
    hedgeBudgetManager.release();

    IllegalStateException exception = Assert.expectThrows(IllegalStateException.class, hedgeBudgetManager::release);
    Assert.assertEquals(exception.getMessage(), "Cannot release hedge admission when there are no active hedges.");
    Assert.assertEquals(hedgeBudgetManager.getActiveHedges(), 0);
  }

  @Test
  public void testConcurrentAdmissionSafety()
      throws Exception {
    TestClock testClock = new TestClock();
    HedgeBudgetManager hedgeBudgetManager = createManager(testClock, 0.5d, 60_000L, 100);
    hedgeBudgetManager.recordPrimaryRequests(100);

    int numTasks = 64;
    CountDownLatch readyLatch = new CountDownLatch(numTasks);
    CountDownLatch startLatch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newFixedThreadPool(numTasks);
    try {
      List<Future<HedgeBudgetManager.AdmissionResult>> futures = new ArrayList<>(numTasks);
      for (int i = 0; i < numTasks; i++) {
        futures.add(executorService.submit(() -> {
          readyLatch.countDown();
          startLatch.await();
          return hedgeBudgetManager.tryAcquire();
        }));
      }

      Assert.assertTrue(readyLatch.await(10, TimeUnit.SECONDS));
      startLatch.countDown();

      int acquiredCount = 0;
      for (Future<HedgeBudgetManager.AdmissionResult> future : futures) {
        if (future.get() == HedgeBudgetManager.AdmissionResult.ACQUIRED) {
          acquiredCount++;
        }
      }

      Assert.assertEquals(acquiredCount, 50);
      Assert.assertEquals(hedgeBudgetManager.getActiveHedges(), 50);
      Assert.assertEquals(hedgeBudgetManager.getHedgeRequestsInWindow(), 50L);

      for (int i = 0; i < acquiredCount; i++) {
        hedgeBudgetManager.release();
      }
      Assert.assertEquals(hedgeBudgetManager.getActiveHedges(), 0);
    } finally {
      executorService.shutdownNow();
      Assert.assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static HedgeBudgetManager createManager(TestClock testClock, double maxExtraRequestRatio, long budgetWindowMs,
      int maxConcurrentRequests) {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_ENABLED, true);
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_MAX_EXTRA_REQUEST_RATIO, maxExtraRequestRatio);
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_BUDGET_WINDOW_MS, budgetWindowMs);
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_MAX_CONCURRENT_REQUESTS, maxConcurrentRequests);

    HedgingConfig hedgingConfig = HedgingConfig.fromConfig(new PinotConfiguration(properties));
    return new HedgeBudgetManager(hedgingConfig, testClock);
  }

  private static final class TestClock implements LongSupplier {
    private final AtomicLong _currentTimeMillis = new AtomicLong();

    @Override
    public long getAsLong() {
      return _currentTimeMillis.get();
    }

    private void setCurrentTimeMillis(long currentTimeMillis) {
      _currentTimeMillis.set(currentTimeMillis);
    }
  }
}
