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

import com.google.common.base.Preconditions;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code ExponentialMovingAverage} is the implementation of the utility Exponential Weighted Moving Average
 * which is a statistical measure used to model time series data. Refer https://en.wikipedia.org/wiki/Moving_average
 * for more details.
 */
@ThreadSafe
public class ExponentialMovingAverage {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExponentialMovingAverage.class);

  private final double _alpha;
  private final long _autoDecayWindowMs;

  private final long _warmUpDurationMs;
  private final long _initializationTimeMs;

  private volatile double _average;
  // Volatile because the periodic decay task reads this outside of the synchronized compute() method.
  private volatile long _lastUpdatedTimeMs;

  // Supplies the value that the average decays towards when no updates are received. Returning NaN indicates that
  // no valid decay target is currently available, in which case decay is skipped and the average is retained.
  private final DoubleSupplier _decayTargetSupplier;

  /**
   * Constructor
   *
   * @param alpha                Determines how much weightage should be given to the new value. Can only take a value
   *                             between 0 and 1.
   * @param autoDecayWindowMs    Time interval to periodically decay the average if no updates are received. For example
   *                             if autoDecayWindowMs = 30s, if average is not updated for a period of 30 seconds, we
   *                             automatically update the average to 0.0 with a weightage of alpha.
   * @param warmUpDurationMs     The initial duration after initialization during which new incoming values are ignored
   *                             in the average calculation.
   * @param avgInitializationVal The default value to initialize for average.
   * @param periodicTaskExecutor Executor to schedule periodic tasks like autoDecay.
   */
  public ExponentialMovingAverage(double alpha, long autoDecayWindowMs, long warmUpDurationMs,
      double avgInitializationVal, @Nullable ScheduledExecutorService periodicTaskExecutor) {
    this(alpha, autoDecayWindowMs, warmUpDurationMs, avgInitializationVal, periodicTaskExecutor, () -> 0.0);
  }

  /**
   * Constructor that allows the decay target to be supplied dynamically.
   *
   * @param decayTargetSupplier Supplies the value the average decays towards when no updates are received. Returning
   *                            any non-finite or negative value (for example {@link Double#NaN}) indicates no valid
   *                            target is available, in which case decay is skipped and the current average is
   *                            retained.
   */
  public ExponentialMovingAverage(double alpha, long autoDecayWindowMs, long warmUpDurationMs,
      double avgInitializationVal, @Nullable ScheduledExecutorService periodicTaskExecutor,
      DoubleSupplier decayTargetSupplier) {
    Preconditions.checkState(alpha >= 0.0 && alpha <= 1.0, "Alpha should be between 0 and 1");
    _alpha = alpha;
    Preconditions.checkState(warmUpDurationMs >= 0, "warmUpDurationMs is negative.");
    _warmUpDurationMs = warmUpDurationMs;
    Preconditions.checkState(avgInitializationVal >= 0.0, "avgInitializationVal is negative.");
    _average = avgInitializationVal;
    _decayTargetSupplier = Preconditions.checkNotNull(decayTargetSupplier, "decayTargetSupplier is null.");

    _initializationTimeMs = System.currentTimeMillis();
    _lastUpdatedTimeMs = 0;
    _autoDecayWindowMs = autoDecayWindowMs;

    if (_autoDecayWindowMs > 0) {
      // Schedule a task to automatically decay the average if updates are not performed in the last _autoDecayWindowMs.
      Preconditions.checkState(periodicTaskExecutor != null);
      periodicTaskExecutor.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          // An uncaught exception here would silently cancel all future executions of this task, permanently
          // disabling decay for this average.
          try {
            decayIfStale(System.currentTimeMillis());
          } catch (Throwable t) {
            LOGGER.error("Caught exception while decaying ExponentialMovingAverage.", t);
          }
        }
      }, 0, _autoDecayWindowMs, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Decays the average towards the configured decay target if no update has been received within
   * autoDecayWindowMs. The staleness check and the decay are performed atomically so that a concurrent real
   * update cannot be immediately decayed away.
   */
  synchronized void decayIfStale(long nowMs) {
    if (_lastUpdatedTimeMs <= 0 || (nowMs - _lastUpdatedTimeMs) <= _autoDecayWindowMs) {
      return;
    }

    double decayTarget = _decayTargetSupplier.getAsDouble();
    if (!Double.isFinite(decayTarget) || decayTarget < 0.0) {
      // No valid decay target is available. Retain the current average rather than decaying it towards an
      // arbitrary value. _lastUpdatedTimeMs is deliberately left untouched so that decay is retried on the next
      // window once a target becomes available.
      return;
    }

    compute(decayTarget);
  }

  /**
   * Returns the exponentially weighted moving average.
   */
  public double getAverage() {
    return _average;
  }

  /**
   * Adds a value to the exponentially weighted moving average. If warmUpDurationMs is not reached yet, this value is
   * ignored.
   * @param value incoming value
   * @return the updated exponentially weighted moving average.
   */
  public synchronized double compute(double value) {
    computeAndReportIfApplied(value);
    return _average;
  }

  /**
   * Same as {@link #compute(double)}, but reports whether the value was actually folded into the average or
   * discarded because the warm-up period has not elapsed. Callers that record metadata about "real" observations
   * must use this, otherwise they would attribute a discarded sample to the average, which still holds the
   * initialization value.
   *
   * @return true if the value was applied to the average.
   */
  public synchronized boolean computeAndReportIfApplied(double value) {
    long currTime = System.currentTimeMillis();
    _lastUpdatedTimeMs = currTime;

    if (_initializationTimeMs + _warmUpDurationMs > currTime) {
      return false;
    }

    _average = value * _alpha + _average * (1 - _alpha);
    return true;
  }
}
