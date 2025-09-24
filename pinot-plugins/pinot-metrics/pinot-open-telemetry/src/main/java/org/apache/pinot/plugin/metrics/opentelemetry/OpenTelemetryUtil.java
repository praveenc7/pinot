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
package org.apache.pinot.plugin.metrics.opentelemetry;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import java.util.Map;
import org.apache.pinot.common.metrics.MetricAttributeConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

/**
 * OpenTelemetryUtil provides utility functions to build OpenTelemetry metric attributes.
 */
public class OpenTelemetryUtil {

  private OpenTelemetryUtil() {
    // Utility class, no instantiation
  }

  public static Attributes toOpenTelemetryAttributes(Map<String, String> attributes) {
    AttributesBuilder attributesBuilder = Attributes.builder();
    if (attributes != null) {
      for (Map.Entry<String, String> entry : attributes.entrySet()) {
        String key = entry.getKey();
        // OpenTelemetry does not allow null attribute values, in case of null value, replace it with "-". This should
        // be rare and only happen when the attribute value is not set properly, but we should still handle it
        // gracefully here, otherwise metric emission will fail.
        String val = entry.getValue() == null ? "-" : entry.getValue();

        // PinotCore does not distinguish between rawTableName and tableNameWithType when emit metrics, so we need
        // to do the parsing here to help OpenTelemetry backend to do better aggregation and analysis.
        if (key.equals(MetricAttributeConstants.TABLE_NAME)) {
          String rawTableName = TableNameBuilder.extractRawTableName(val);
          String tableType = "-";
          String tableNameWithType = "-";
          if (!val.equals(rawTableName)) {
            tableNameWithType = val;
            tableType = TableNameBuilder.getTableTypeFromTableName(val) == null ? "-"
                : TableNameBuilder.getTableTypeFromTableName(val).name();
          }
          attributesBuilder.put(MetricAttributeConstants.RAW_TABLE_NAME, rawTableName);
          attributesBuilder.put(MetricAttributeConstants.TABLE_NAME_WITH_TYPE, tableNameWithType);
          attributesBuilder.put(MetricAttributeConstants.TABLE_TYPE, tableType);
        } else {
          // for other attributes, just put them directly
          attributesBuilder.put(key, val);
        }
      }
    }
    return attributesBuilder.build();
  }
}
