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

import com.google.auto.service.AutoService;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleGauge;
import io.opentelemetry.api.metrics.LongGauge;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.annotations.metrics.MetricsFactory;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotGauge;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricReporter;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;

import static org.apache.pinot.spi.utils.CommonConstants.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT;
import static org.apache.pinot.spi.utils.CommonConstants.OTEL_EXPORTER_OTLP_METRICS_EXPORT_INTERVAL_SECONDS;
import static org.apache.pinot.spi.utils.CommonConstants.OTEL_EXPORTER_OTLP_METRICS_HEADERS;


@AutoService(PinotMetricsFactory.class)
@MetricsFactory
public class OpenTelemetryMetricsFactory implements PinotMetricsFactory {
  private final PinotMetricsRegistry _pinotMetricsRegistry = new OpenTelemetryMetricsRegistry();
  // OpenTelemetry collector endpoint to which metrics are to be exported
  private String _otelEndpoint;
  // Headers to be passed while creating OpenTelemetry exporter
  private final Map<String, String> _otelHeaders = new HashMap<>();
  // Interval in seconds to export metrics to OpenTelemetry collector
  private int _otelExportIntervalSeconds;

  @Override
  public void init(PinotConfiguration metricsConfiguration) {
    String otelEndpointStr = metricsConfiguration.getProperty(OTEL_EXPORTER_OTLP_METRICS_ENDPOINT);
    String otelExportIntervalStr = metricsConfiguration.getProperty(OTEL_EXPORTER_OTLP_METRICS_EXPORT_INTERVAL_SECONDS);
    String otelHeadersString = metricsConfiguration.getProperty(OTEL_EXPORTER_OTLP_METRICS_HEADERS);

    _otelEndpoint = StringUtils.isEmpty(otelEndpointStr) ? OpenTelemetryHttpReporter.DEFAULT_OTEL_COLLECTOR_ENDPOINT
        : otelEndpointStr;
    _otelExportIntervalSeconds =
        StringUtils.isEmpty(otelExportIntervalStr) ? OpenTelemetryHttpReporter.DEFAULT_EXPORT_INTERVAL_SECONDS
            : Integer.parseInt(otelExportIntervalStr);

    if (!StringUtils.isEmpty(otelHeadersString)) {
      // Headers are passed as key=value pairs separated by '='. Multiple headers are separated by ','
      // Currently supporting only 1 header
      String[] headers = otelHeadersString.split("=");
      _otelHeaders.put(headers[0], headers[1]);
    }
  }

  @Override
  public PinotMetricsRegistry getPinotMetricsRegistry() {
    return _pinotMetricsRegistry;
  }

  @Override
  public PinotMetricName makePinotMetricName(Class<?> klass, String fullName, String simplifiedName,
      Map<String, String> attributes) {
    return new OpenTelemetryMetricName(fullName, simplifiedName, attributes);
  }

  @Override
  public <T> PinotGauge<T> makePinotGauge(PinotMetricName pinotMetricName, Function<Void, T> condition) {
    T value = condition.apply(null);

    String simplifiedMetricName = pinotMetricName.getSimplifiedMetricName();
    Attributes attributes = OpenTelemetryUtil.toOpenTelemetryAttributes(pinotMetricName.getAttributes());

    if (value instanceof Integer || value instanceof Long) {
      LongGauge longGauge = OpenTelemetryMetricsRegistry.SharedOtelMetricRegistry
          .getOrCreateLongGauge(simplifiedMetricName);
      return new OpenTelemetryLongGauge<>(longGauge, attributes, condition);
    }
    if (value instanceof Double) {
      DoubleGauge doubleGauge = OpenTelemetryMetricsRegistry.SharedOtelMetricRegistry
          .getOrCreateDoubleGauge(simplifiedMetricName);
      return new OpenTelemetryDoubleGauge<>(doubleGauge, attributes, condition);
    }
    throw new IllegalArgumentException("Unsupported supplier type: " + condition.getClass().getName());
  }

  @Override
  public PinotMetricReporter makePinotMetricReporter(PinotMetricsRegistry metricsRegistry) {
    return new OpenTelemetryHttpReporter(_otelEndpoint, _otelHeaders, _otelExportIntervalSeconds);
  }

  @Override
  public String getMetricsFactoryName() {
    return "OpenTelemetry";
  }
}
