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

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class HedgingConfigTest {

  @Test
  public void testDefaults() {
    HedgingConfig hedgingConfig = HedgingConfig.disabled();

    Assert.assertFalse(hedgingConfig.isEnabled());
    Assert.assertEquals(hedgingConfig.getDelayRatio(), 0.5d);
    Assert.assertEquals(hedgingConfig.getMinDelayMs(), 25L);
    Assert.assertEquals(hedgingConfig.getMaxDelayMs(), 500L);
    Assert.assertEquals(hedgingConfig.getMaxHedgesPerQuery(), 1);
    Assert.assertEquals(hedgingConfig.getMaxExtraRequestRatio(), 0.01d);
    Assert.assertEquals(hedgingConfig.getBudgetWindowMs(), 60_000L);
    Assert.assertEquals(hedgingConfig.getMaxConcurrentRequests(), 32);
  }

  @Test
  public void testDisabledMatchesConfigDefaults() {
    HedgingConfig expected = HedgingConfig.fromConfig(new PinotConfiguration());
    HedgingConfig actual = HedgingConfig.disabled();

    Assert.assertEquals(actual.isEnabled(), expected.isEnabled());
    Assert.assertEquals(actual.getDelayRatio(), expected.getDelayRatio());
    Assert.assertEquals(actual.getMinDelayMs(), expected.getMinDelayMs());
    Assert.assertEquals(actual.getMaxDelayMs(), expected.getMaxDelayMs());
    Assert.assertEquals(actual.getMaxHedgesPerQuery(), expected.getMaxHedgesPerQuery());
    Assert.assertEquals(actual.getMaxExtraRequestRatio(), expected.getMaxExtraRequestRatio());
    Assert.assertEquals(actual.getBudgetWindowMs(), expected.getBudgetWindowMs());
    Assert.assertEquals(actual.getMaxConcurrentRequests(), expected.getMaxConcurrentRequests());
  }

  @Test
  public void testEnabledConfigurationParsing() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_ENABLED, true);
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_RATIO, 0.75d);
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_MIN_MS, 50L);
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_MAX_MS, 250L);
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_MAX_HEDGES_PER_QUERY, 1);
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_MAX_EXTRA_REQUEST_RATIO, 0.2d);
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_BUDGET_WINDOW_MS, 3_000L);
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_MAX_CONCURRENT_REQUESTS, 8);

    HedgingConfig hedgingConfig = HedgingConfig.fromConfig(new PinotConfiguration(properties));

    Assert.assertTrue(hedgingConfig.isEnabled());
    Assert.assertEquals(hedgingConfig.getDelayRatio(), 0.75d);
    Assert.assertEquals(hedgingConfig.getMinDelayMs(), 50L);
    Assert.assertEquals(hedgingConfig.getMaxDelayMs(), 250L);
    Assert.assertEquals(hedgingConfig.getMaxHedgesPerQuery(), 1);
    Assert.assertEquals(hedgingConfig.getMaxExtraRequestRatio(), 0.2d);
    Assert.assertEquals(hedgingConfig.getBudgetWindowMs(), 3_000L);
    Assert.assertEquals(hedgingConfig.getMaxConcurrentRequests(), 8);
  }

  @Test(dataProvider = "invalidEnabledConfigurations")
  public void testInvalidEnabledConfigurations(Map<String, Object> properties, String expectedMessage) {
    IllegalArgumentException exception = Assert.expectThrows(IllegalArgumentException.class,
        () -> HedgingConfig.fromConfig(new PinotConfiguration(properties)));
    Assert.assertEquals(exception.getMessage(), expectedMessage);
  }

  @DataProvider(name = "invalidEnabledConfigurations")
  public Object[][] invalidEnabledConfigurations() {
    return new Object[][]{
        {properties(CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_RATIO, 0d),
            CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_RATIO + " must be in the range (0, 1]."},
        {properties(CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_RATIO, Double.NaN),
            CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_RATIO + " must be in the range (0, 1]."},
        {properties(CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_MIN_MS, -1L),
            CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_MIN_MS + " must be >= 0."},
        {properties(CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_MAX_MS, 24L),
            CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_MAX_MS + " must be >= "
                + CommonConstants.Broker.Hedging.CONFIG_OF_DELAY_MIN_MS + "."},
        {properties(CommonConstants.Broker.Hedging.CONFIG_OF_MAX_HEDGES_PER_QUERY, 2),
            CommonConstants.Broker.Hedging.CONFIG_OF_MAX_HEDGES_PER_QUERY + " must be 1."},
        {properties(CommonConstants.Broker.Hedging.CONFIG_OF_MAX_EXTRA_REQUEST_RATIO, 0d),
            CommonConstants.Broker.Hedging.CONFIG_OF_MAX_EXTRA_REQUEST_RATIO + " must be in the range (0, 1]."},
        {properties(CommonConstants.Broker.Hedging.CONFIG_OF_MAX_EXTRA_REQUEST_RATIO, Double.NaN),
            CommonConstants.Broker.Hedging.CONFIG_OF_MAX_EXTRA_REQUEST_RATIO + " must be in the range (0, 1]."},
        {properties(CommonConstants.Broker.Hedging.CONFIG_OF_BUDGET_WINDOW_MS, 0L),
            CommonConstants.Broker.Hedging.CONFIG_OF_BUDGET_WINDOW_MS + " must be > 0."},
        {properties(CommonConstants.Broker.Hedging.CONFIG_OF_MAX_CONCURRENT_REQUESTS, 0),
            CommonConstants.Broker.Hedging.CONFIG_OF_MAX_CONCURRENT_REQUESTS + " must be > 0."}
    };
  }

  private static Map<String, Object> properties(String key, Object value) {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.Hedging.CONFIG_OF_ENABLED, true);
    properties.put(key, value);
    return properties;
  }
}
