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

import com.google.common.annotations.VisibleForTesting;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;


public class HedgeBudgetManager {
  private static final long BUCKET_DURATION_MS = TimeUnit.SECONDS.toMillis(1);

  public enum AdmissionResult {
    ACQUIRED,
    RATIO_LIMIT,
    CONCURRENCY_LIMIT
  }

  private final BigDecimal _maxExtraRequestRatio;
  private final int _maxConcurrentRequests;
  private final LongSupplier _currentTimeMillis;
  private final WindowCounter _primaryRequests;
  private final WindowCounter _hedgeRequests;
  private final ReentrantLock _admissionLock = new ReentrantLock();

  private volatile int _activeHedges;

  public HedgeBudgetManager(HedgingConfig hedgingConfig) {
    this(hedgingConfig, System::currentTimeMillis);
  }

  @VisibleForTesting
  HedgeBudgetManager(HedgingConfig hedgingConfig, LongSupplier currentTimeMillis) {
    Objects.requireNonNull(hedgingConfig, "hedgingConfig must not be null");
    _currentTimeMillis = Objects.requireNonNull(currentTimeMillis, "currentTimeMillis must not be null");
    _maxExtraRequestRatio = BigDecimal.valueOf(hedgingConfig.getMaxExtraRequestRatio());
    _maxConcurrentRequests = hedgingConfig.getMaxConcurrentRequests();

    int bucketCount = Math.toIntExact(1L + (hedgingConfig.getBudgetWindowMs() - 1L) / BUCKET_DURATION_MS);
    _primaryRequests = new WindowCounter(bucketCount);
    _hedgeRequests = new WindowCounter(bucketCount);
  }

  public AdmissionResult tryAcquire() {
    long currentTimeMillis = _currentTimeMillis.getAsLong();

    _admissionLock.lock();
    try {
      if (_activeHedges >= _maxConcurrentRequests) {
        return AdmissionResult.CONCURRENCY_LIMIT;
      }

      long primaryRequestsInWindow = _primaryRequests.getCountInWindow(currentTimeMillis);
      long hedgeRequestsInWindow = _hedgeRequests.getCountInWindow(currentTimeMillis);
      long allowedHedges = computeAllowedHedges(primaryRequestsInWindow);
      if (hedgeRequestsInWindow + 1L > allowedHedges) {
        return AdmissionResult.RATIO_LIMIT;
      }

      _hedgeRequests.add(currentTimeMillis, 1L);
      _activeHedges++;
      return AdmissionResult.ACQUIRED;
    } finally {
      _admissionLock.unlock();
    }
  }

  public void release() {
    _admissionLock.lock();
    try {
      if (_activeHedges == 0) {
        throw new IllegalStateException("Cannot release hedge admission when there are no active hedges.");
      }
      _activeHedges--;
    } finally {
      _admissionLock.unlock();
    }
  }

  public void recordPrimaryRequest() {
    recordPrimaryRequests(1);
  }

  public void recordPrimaryRequests(int numPrimaryRequests) {
    if (numPrimaryRequests < 0) {
      throw new IllegalArgumentException("numPrimaryRequests must be >= 0.");
    }
    if (numPrimaryRequests == 0) {
      return;
    }
    _primaryRequests.add(_currentTimeMillis.getAsLong(), numPrimaryRequests);
  }

  public int getActiveHedges() {
    return _activeHedges;
  }

  public long getPrimaryRequestsInWindow() {
    return _primaryRequests.getCountInWindow(_currentTimeMillis.getAsLong());
  }

  public long getHedgeRequestsInWindow() {
    return _hedgeRequests.getCountInWindow(_currentTimeMillis.getAsLong());
  }

  private long computeAllowedHedges(long primaryRequestsInWindow) {
    return BigDecimal.valueOf(primaryRequestsInWindow).multiply(_maxExtraRequestRatio)
        .setScale(0, RoundingMode.FLOOR).longValue();
  }

  private static final class WindowCounter {
    private final int _bucketCount;
    private final AtomicReferenceArray<Bucket> _buckets;

    private WindowCounter(int bucketCount) {
      _bucketCount = bucketCount;
      _buckets = new AtomicReferenceArray<>(bucketCount);
      for (int i = 0; i < bucketCount; i++) {
        _buckets.set(i, Bucket.EMPTY);
      }
    }

    private void add(long currentTimeMillis, long count) {
      long epochSecond = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis);
      int bucketIndex = bucketIndex(epochSecond);

      while (true) {
        Bucket currentBucket = _buckets.get(bucketIndex);
        Bucket updatedBucket = currentBucket._epochSecond == epochSecond
            ? new Bucket(epochSecond, currentBucket._count + count)
            : new Bucket(epochSecond, count);
        if (_buckets.compareAndSet(bucketIndex, currentBucket, updatedBucket)) {
          return;
        }
      }
    }

    private long getCountInWindow(long currentTimeMillis) {
      long currentEpochSecond = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis);
      long oldestEpochSecond = currentEpochSecond - _bucketCount + 1L;
      long totalCount = 0L;

      for (int i = 0; i < _bucketCount; i++) {
        Bucket bucket = _buckets.get(i);
        if (bucket._epochSecond >= oldestEpochSecond && bucket._epochSecond <= currentEpochSecond) {
          totalCount += bucket._count;
        }
      }
      return totalCount;
    }

    private int bucketIndex(long epochSecond) {
      return (int) (epochSecond % _bucketCount);
    }
  }

  private static final class Bucket {
    private static final Bucket EMPTY = new Bucket(Long.MIN_VALUE, 0L);

    private final long _epochSecond;
    private final long _count;

    private Bucket(long epochSecond, long count) {
      _epochSecond = epochSecond;
      _count = count;
    }
  }
}
