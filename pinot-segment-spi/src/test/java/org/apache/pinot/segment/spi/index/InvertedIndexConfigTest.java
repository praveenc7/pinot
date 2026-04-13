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
package org.apache.pinot.segment.spi.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class InvertedIndexConfigTest {

  @Test
  public void withEmptyConf()
      throws JsonProcessingException {
    InvertedIndexConfig config = JsonUtils.stringToObject("{}", InvertedIndexConfig.class);
    assertFalse(config.isDisabled());
    assertEquals(config.getVersion(), InvertedIndexConfig.DEFAULT_VERSION);
  }

  @Test
  public void withDisabledNull()
      throws JsonProcessingException {
    InvertedIndexConfig config = JsonUtils.stringToObject("{\"disabled\": null}", InvertedIndexConfig.class);
    assertFalse(config.isDisabled());
    assertEquals(config.getVersion(), InvertedIndexConfig.DEFAULT_VERSION);
  }

  @Test
  public void withDisabledFalse()
      throws JsonProcessingException {
    InvertedIndexConfig config = JsonUtils.stringToObject("{\"disabled\": false}", InvertedIndexConfig.class);
    assertFalse(config.isDisabled());
    assertEquals(config.getVersion(), InvertedIndexConfig.DEFAULT_VERSION);
  }

  @Test
  public void withDisabledTrue()
      throws JsonProcessingException {
    InvertedIndexConfig config = JsonUtils.stringToObject("{\"disabled\": true}", InvertedIndexConfig.class);
    assertTrue(config.isDisabled());
    assertEquals(config.getVersion(), InvertedIndexConfig.DEFAULT_VERSION);
  }

  @Test
  public void withExplicitVersion()
      throws JsonProcessingException {
    InvertedIndexConfig config = JsonUtils.stringToObject("{\"version\": 1}", InvertedIndexConfig.class);
    assertFalse(config.isDisabled());
    assertEquals(config.getVersion(), 1);
  }

  @Test
  public void withVersionNull()
      throws JsonProcessingException {
    InvertedIndexConfig config = JsonUtils.stringToObject("{\"version\": null}", InvertedIndexConfig.class);
    assertFalse(config.isDisabled());
    assertEquals(config.getVersion(), InvertedIndexConfig.DEFAULT_VERSION);
  }

  @Test
  public void defaultConstantsAreEnabled() {
    assertFalse(InvertedIndexConfig.ENABLED.isDisabled());
    assertTrue(InvertedIndexConfig.DISABLED.isDisabled());
    assertEquals(InvertedIndexConfig.ENABLED.getVersion(), InvertedIndexConfig.DEFAULT_VERSION);
  }

  @Test
  public void withExplicitVersion0()
      throws JsonProcessingException {
    InvertedIndexConfig config = JsonUtils.stringToObject("{\"version\": 0}", InvertedIndexConfig.class);
    assertFalse(config.isDisabled());
    assertEquals(config.getVersion(), InvertedIndexConfig.VERSION_0);
  }

  @Test(expectedExceptions = Exception.class)
  public void withInvalidVersion()
      throws JsonProcessingException {
    JsonUtils.stringToObject("{\"version\": 99}", InvertedIndexConfig.class);
  }

  @Test(expectedExceptions = Exception.class)
  public void withNegativeVersion()
      throws JsonProcessingException {
    JsonUtils.stringToObject("{\"version\": -1}", InvertedIndexConfig.class);
  }

  @Test
  public void equalsAndHashCode() {
    InvertedIndexConfig a = new InvertedIndexConfig(1);
    InvertedIndexConfig b = new InvertedIndexConfig(1);
    InvertedIndexConfig c = new InvertedIndexConfig(0);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertFalse(a.equals(c));
  }
}
