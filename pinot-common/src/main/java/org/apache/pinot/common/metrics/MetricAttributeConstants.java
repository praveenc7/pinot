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
package org.apache.pinot.common.metrics;

import java.util.HashMap;
import java.util.Map;


public class MetricAttributeConstants {

  private MetricAttributeConstants() {
  }

  public static final String TABLE_NAME = "TableName";
  public static final String RAW_TABLE_NAME = "RawTableName";
  public static final String TABLE_NAME_WITH_TYPE = "TableNameWithType";
  public static final String TABLE_TYPE = "TableType";
  public static final String TASK_TYPE = "TaskType";
  public static final String TASK_NAME = "TaskName";
  public static final String MERGE_LEVEL = "MergeLevel";
  public static final String SEGMENT_NAME = "SegmentName";
  public static final String RESOURCE_NAME = "ResourceName";
  public static final String STREAM_TOPIC_NAME = "StreamTopicName";
  public static final String STREAM_PARTITION_ID = "StreamPartitionId";
  public static final String STREAM_CLIENT_ID_SUFFIX = "StreamClientIdSuffix";
  public static final String COLUMN_NAME = "ColumnName";
  public static final String REPLICA_GROUP_TAG = "ReplicaGroupTag";
  public static final String REPLICA_GROUP_ID = "ReplicaGroupId";
  public static final String PINOT_METRIC_NAME = "PinotMetricName";
  public static final String WORKLOAD_NAME = "WorkloadName";

  // Dimensional-label attribute maps must be built with a null-tolerant Map (HashMap), NOT ImmutableMap:
  // Guava's ImmutableMap forbids null values and throws "null value in entry" at construction, which would
  // mask the caller's real error. A null label value is fine here -- the metric reporters replace it (the
  // OpenTelemetry reporter substitutes "-"). Use these helpers everywhere a label map is constructed.
  public static Map<String, String> attributes(String key, String value) {
    Map<String, String> attributes = new HashMap<>();
    attributes.put(key, value);
    return attributes;
  }

  public static Map<String, String> attributes(String key1, String value1, String key2, String value2) {
    Map<String, String> attributes = new HashMap<>();
    attributes.put(key1, value1);
    attributes.put(key2, value2);
    return attributes;
  }

  public static Map<String, String> attributes(String key1, String value1, String key2, String value2, String key3,
      String value3) {
    Map<String, String> attributes = new HashMap<>();
    attributes.put(key1, value1);
    attributes.put(key2, value2);
    attributes.put(key3, value3);
    return attributes;
  }

  public static Map<String, String> attributes(String key1, String value1, String key2, String value2, String key3,
      String value3, String key4, String value4) {
    Map<String, String> attributes = new HashMap<>();
    attributes.put(key1, value1);
    attributes.put(key2, value2);
    attributes.put(key3, value3);
    attributes.put(key4, value4);
    return attributes;
  }
}
