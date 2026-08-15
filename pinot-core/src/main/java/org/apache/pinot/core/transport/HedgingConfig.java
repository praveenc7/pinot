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

import java.util.Objects;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;


public final class HedgingConfig {
  private final boolean _enabled;
  private final double _delayRatio;
  private final long _minDelayMs;
  private final long _maxDelayMs;
  private final int _maxHedgesPerQuery;
  private final double _maxExtraRequestRatio;
  private final long _budgetWindowMs;
  private final int _maxConcurrentRequests;

  private HedgingConfig(boolean enabled, double delayRatio, long minDelayMs, long maxDelayMs, int maxHedgesPerQuery,
      double maxExtraRequestRatio, long budgetWindowMs, int maxConcurrentRequests) {
    _enabled = enabled;
    _delayRatio = delayRatio;
    _minDelayMs = minDelayMs;
    _maxDelayMs = maxDelayMs;
    _maxHedgesPerQuery = maxHedgesPerQuery;
    _maxExtraRequestRatio = maxExtraRequestRatio;
    _budgetWindowMs = budgetWindowMs;
    _maxConcurrentRequests = maxConcurrentRequests;
    validate();
  }

  public static HedgingConfig disabled() {
    return new HedgingConfig(CommonConstants.Broker.Hedging.DEFAULT_ENABLED,
        CommonConstants.Broker.Hedging.DEFAULT_DELAY_RATIO, CommonConstants.Broker.Hedging.DEFAULT_DELAY_MIN_MS,
        CommonConstants.Broker.Hedging.DEFAULT_DELAY_MAX_MS,
        CommonConstants.Broker.Hedging.DEFAULT_MAX_HEDGES_PER_QUERY,
        CommonConstants.Broker.Hedging.DEFAULT_MAX_EXTRA_REQUEST_RATIO,
        CommonConstants.Broker.Hedging.DEFAULT_BUDGET_WINDOW_MS,
        CommonConstants.Broker.Hedging.DEFAULT_MAX_CONCURRENT_REQUESTS);
  }

  public static HedgingConfig fromConfig(PinotConfiguration config) {
    Objects.requireNonNull(config, "config must not be null");

    return new HedgingConfig(
        config.getProperty(CommonConstants.Broker.Hedging.CONFIG_OF_ENABLED,
            CommonConstants.Broker.Hedging.DEFAULT_ENABLED),
        config.getProperty(CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_RATIO,
            CommonConstants.Broker.Hedging.DEFAULT_DELAY_RATIO),
        config.getProperty(CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_MIN_MS,
            CommonConstants.Broker.Hedging.DEFAULT_DELAY_MIN_MS),
        config.getProperty(CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_MAX_MS,
            CommonConstants.Broker.Hedging.DEFAULT_DELAY_MAX_MS),
        config.getProperty(CommonConstants.Broker.Hedging.CONFIG_OF_MAX_HEDGES_PER_QUERY,
            CommonConstants.Broker.Hedging.DEFAULT_MAX_HEDGES_PER_QUERY),
        config.getProperty(CommonConstants.Broker.Hedging.CONFIG_OF_MAX_EXTRA_REQUEST_RATIO,
            CommonConstants.Broker.Hedging.DEFAULT_MAX_EXTRA_REQUEST_RATIO),
        config.getProperty(CommonConstants.Broker.Hedging.CONFIG_OF_BUDGET_WINDOW_MS,
            CommonConstants.Broker.Hedging.DEFAULT_BUDGET_WINDOW_MS),
        config.getProperty(CommonConstants.Broker.Hedging.CONFIG_OF_MAX_CONCURRENT_REQUESTS,
            CommonConstants.Broker.Hedging.DEFAULT_MAX_CONCURRENT_REQUESTS));
  }

  static HedgingConfig fromPinotConfiguration(PinotConfiguration config) {
    return fromConfig(config);
  }

  public boolean isEnabled() {
    return _enabled;
  }

  public double getDelayRatio() {
    return _delayRatio;
  }

  public long getMinDelayMs() {
    return _minDelayMs;
  }

  public long getMaxDelayMs() {
    return _maxDelayMs;
  }

  public int getMaxHedgesPerQuery() {
    return _maxHedgesPerQuery;
  }

  public double getMaxExtraRequestRatio() {
    return _maxExtraRequestRatio;
  }

  public long getBudgetWindowMs() {
    return _budgetWindowMs;
  }

  public int getMaxConcurrentRequests() {
    return _maxConcurrentRequests;
  }

  private void validate() {
    if (!_enabled) {
      return;
    }

    if (!Double.isFinite(_delayRatio) || _delayRatio <= 0d || _delayRatio > 1d) {
      throw new IllegalArgumentException(
          CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_RATIO + " must be in the range (0, 1].");
    }
    if (_minDelayMs < 0L) {
      throw new IllegalArgumentException(CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_MIN_MS + " must be >= 0.");
    }
    if (_maxDelayMs < _minDelayMs) {
      throw new IllegalArgumentException(
          CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_MAX_MS + " must be >= "
              + CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_MIN_MS + ".");
    }
    if (_maxHedgesPerQuery != 1) {
      throw new IllegalArgumentException(
          CommonConstants.Broker.Hedging.CONFIG_OF_MAX_HEDGES_PER_QUERY + " must be 1.");
    }
    if (!Double.isFinite(_maxExtraRequestRatio) || _maxExtraRequestRatio <= 0d
        || _maxExtraRequestRatio > 1d) {
      throw new IllegalArgumentException(
          CommonConstants.Broker.Hedging.CONFIG_OF_MAX_EXTRA_REQUEST_RATIO + " must be in the range (0, 1].");
    }
    if (_budgetWindowMs <= 0L) {
      throw new IllegalArgumentException(CommonConstants.Broker.Hedging.CONFIG_OF_BUDGET_WINDOW_MS + " must be > 0.");
    }
    if (_maxConcurrentRequests <= 0) {
      throw new IllegalArgumentException(
          CommonConstants.Broker.Hedging.CONFIG_OF_MAX_CONCURRENT_REQUESTS + " must be > 0.");
    }
  }
}
