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
package org.apache.pinot.spi.metrics;

import java.util.Map;

/**
 * Metric Name in Pinot.
 */
public interface PinotMetricName {

  /**
   * Returns the actual metric name. Which is the full name with attributes being stringified in it. This is usually
   * used by metric plugins that do not support attributes natively such as Yammer and Dropwizard.
   */
  Object getMetricName();

  /**
   * Overrides the equals method. This is needed as {@link PinotMetricName} is used as the key of the key-value pair
   * inside the hashmap in MetricsRegistry. Without overriding equals() and hashCode() methods, all the existing k-v
   * pairs
   * stored in hashmap cannot be retrieved by initializing a new key.
   */
  boolean equals(Object obj);

  /**
   * Overrides the hashCode method. This method's contract is the same as equals() method.
   */
  int hashCode();

  /**
   * Overrides the toString method.
   * This could be used to print out the actual metrics name instead of the memory address under this wrapper.
   */
  String toString();

  /**
   * Returns the simplified metric name that stripped off the attributes. This is usually used by metric plugins that
   * support MDM (multi-dimensional metrics) natively such as OpenTelemetry. Please refer to the following docs for more
   * details about MDM and metric/dimension naming conventions:
   * @see <a href="https://super-dollop-preygyr.pages.github.io/docs/opentelemetry/metrics/core-concepts">
   *   OpenTelemetry core concepts</a>
   * @see <a href="https://docs.google.com/document/d/12ZK8ab5zz9tSXZJoHP0e5Mf3Nbz4MOsEu81gdaq-SoA">
   *   LinkedIn MDM metric/dimension naming conventions</a>
   */
  String getSimplifiedMetricName();

  /**
   * Returns the attributes associated with this metric name. This is usually used by metric plugins that
   * support MDM (multi-dimensional metrics) natively such as OpenTelemetry. Please refer to the following docs for more
   * details about MDM and metric/dimension naming conventions:
   * @see <a href="https://super-dollop-preygyr.pages.github.io/docs/opentelemetry/metrics/core-concepts">
   *   OpenTelemetry core concepts</a>
   * @see <a href="https://docs.google.com/document/d/12ZK8ab5zz9tSXZJoHP0e5Mf3Nbz4MOsEu81gdaq-SoA">
   *   LinkedIn MDM metric/dimension naming conventions</a>
   */
  Map<String, String> getAttributes();
}
