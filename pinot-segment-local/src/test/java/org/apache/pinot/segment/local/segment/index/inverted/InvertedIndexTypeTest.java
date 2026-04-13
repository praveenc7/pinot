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
package org.apache.pinot.segment.local.segment.index.inverted;

import java.io.IOException;
import java.util.stream.Collectors;
import org.apache.pinot.segment.local.segment.index.AbstractSerdeIndexContract;
import org.apache.pinot.segment.spi.index.InvertedIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class InvertedIndexTypeTest {

  public static class ConfTest extends AbstractSerdeIndexContract {

    protected void assertEquals(InvertedIndexConfig expected) {
      Assert.assertEquals(getActualConfig("dimInt", StandardIndexes.inverted()), expected);
    }

    @Test
    public void oldConfNotFound()
        throws IOException {
      _tableConfig.getIndexingConfig()
          .setInvertedIndexColumns(parseStringList("[]")
      );

      assertEquals(InvertedIndexConfig.DISABLED);
    }

    @Test
    public void oldConfEnabled()
        throws IOException {
      _tableConfig.getIndexingConfig()
          .setInvertedIndexColumns(parseStringList("[\"dimInt\"]"));

      assertEquals(InvertedIndexConfig.ENABLED);
    }

    @Test
    public void newConfDisableByDefault()
        throws IOException {
      addFieldIndexConfig(
          "{\n"
          + "    \"name\": \"dimInt\","
          + "    \"indexes\" : {"
          + "    }\n"
          + "}");

      assertEquals(InvertedIndexConfig.DISABLED);
    }

    @Test
    public void newConfDisabled()
        throws IOException {
      addFieldIndexConfig(
          "{\n"
          + "    \"name\": \"dimInt\",\n"
          + "    \"indexes\" : {\n"
          + "      \"inverted\": {\n"
          + "         \"disabled\": true\n"
          + "      }\n"
          + "    }\n"
          + "}"
      );

      assertEquals(InvertedIndexConfig.DISABLED);
    }

    @Test
    public void newConfEnabled()
        throws IOException {
      addFieldIndexConfig(
          "{\n"
          + "    \"name\": \"dimInt\",\n"
          + "    \"indexes\" : {\n"
          + "      \"inverted\": {\n"
          + "         \"disabled\": false\n"
          + "      }\n"
          + "    }\n"
          + "}"
      );
      assertEquals(InvertedIndexConfig.ENABLED);
    }

    @Test
    public void newConfWithVersion1()
        throws IOException {
      addFieldIndexConfig(
          "{\n"
          + "    \"name\": \"dimInt\",\n"
          + "    \"indexes\" : {\n"
          + "      \"inverted\": {\n"
          + "         \"version\": 1\n"
          + "      }\n"
          + "    }\n"
          + "}"
      );
      InvertedIndexConfig actual = (InvertedIndexConfig) getActualConfig("dimInt", StandardIndexes.inverted());
      assertFalse(actual.isDisabled());
      Assert.assertEquals(actual.getVersion(), InvertedIndexConfig.VERSION_1);
    }

    @Test
    public void newConfWithVersion0()
        throws IOException {
      addFieldIndexConfig(
          "{\n"
          + "    \"name\": \"dimInt\",\n"
          + "    \"indexes\" : {\n"
          + "      \"inverted\": {\n"
          + "         \"version\": 0\n"
          + "      }\n"
          + "    }\n"
          + "}"
      );
      InvertedIndexConfig actual = (InvertedIndexConfig) getActualConfig("dimInt", StandardIndexes.inverted());
      assertFalse(actual.isDisabled());
      Assert.assertEquals(actual.getVersion(), InvertedIndexConfig.VERSION_0);
    }

    @Test
    public void newConfWithVersionAndDisabled()
        throws IOException {
      addFieldIndexConfig(
          "{\n"
          + "    \"name\": \"dimInt\",\n"
          + "    \"indexes\" : {\n"
          + "      \"inverted\": {\n"
          + "         \"disabled\": false,\n"
          + "         \"version\": 1\n"
          + "      }\n"
          + "    }\n"
          + "}"
      );
      InvertedIndexConfig actual = (InvertedIndexConfig) getActualConfig("dimInt", StandardIndexes.inverted());
      assertFalse(actual.isDisabled());
      Assert.assertEquals(actual.getVersion(), InvertedIndexConfig.VERSION_1);
    }

    @Test
    public void oldToNewConfConversion()
        throws IOException {
      _tableConfig.getIndexingConfig()
          .setInvertedIndexColumns(parseStringList("[\"dimInt\"]"));
      convertToUpdatedFormat();
      assertNotNull(_tableConfig.getFieldConfigList());
      assertFalse(_tableConfig.getFieldConfigList().isEmpty());
      FieldConfig fieldConfig = _tableConfig.getFieldConfigList().stream()
          .filter(fc -> fc.getName().equals("dimInt"))
          .collect(Collectors.toList()).get(0);
      assertNotNull(fieldConfig.getIndexes().get(InvertedIndexType.INDEX_DISPLAY_NAME));
      assertNull(_tableConfig.getIndexingConfig().getInvertedIndexColumns());
      assertTrue(fieldConfig.getIndexTypes().isEmpty());
    }
  }

  @Test
  public void testStandardIndex() {
    assertSame(StandardIndexes.inverted(), StandardIndexes.inverted(), "Inverted index should use the same as "
        + "the InvertedIndexType static instance");
  }
}
