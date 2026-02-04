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

import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.sdk.metrics.export.AggregationTemporalitySelector;
import java.util.Map;
import org.apache.pinot.spi.metrics.PinotMetricReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * OpenTelemetryHttpReporter exports metrics to an OpenTelemetry collector on HTTP endpoint.
 */
public class OpenTelemetryHttpReporter implements PinotMetricReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(OpenTelemetryHttpReporter.class);

  public static final String DEFAULT_OTEL_COLLECTOR_ENDPOINT = "http://127.0.0.1:4318/v1/metrics";
  public static final int DEFAULT_EXPORT_INTERVAL_SECONDS = 1;
  private final String _otelEndpoint;
  private final Map<String, String> _otelHeaders;
  private final int _otelExportIntervalSeconds;

  public OpenTelemetryHttpReporter(String otelEndpoint, Map<String, String> otelHeaders,
      int otelExportIntervalSeconds) {
    _otelEndpoint = otelEndpoint;
    _otelHeaders = otelHeaders;
    _otelExportIntervalSeconds = otelExportIntervalSeconds;
  }

  @Override
  public void start() {
    LOGGER.info("Start OpenTelemetryHttpReporter to endpoint: {} which emit metrics every {} seconds with headers: {}",
        _otelEndpoint, _otelExportIntervalSeconds, _otelHeaders);
    OtlpHttpMetricExporter httpMetricExporter = OtlpHttpMetricExporter
        .builder()
        .setEndpoint(_otelEndpoint)
        .setHeaders(() -> _otelHeaders)
        .setAggregationTemporalitySelector(AggregationTemporalitySelector.deltaPreferred())
        .build();

    OpenTelemetryMetricsRegistry.init(httpMetricExporter, _otelExportIntervalSeconds);
  }
}
