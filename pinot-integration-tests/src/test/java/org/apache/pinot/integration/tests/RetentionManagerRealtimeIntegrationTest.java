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
package org.apache.pinot.integration.tests;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.MetricValueUtils;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.retention.RetentionManager;
import org.apache.pinot.core.periodictask.PeriodicTask;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test that verifies the {@link RetentionManager} create-time guard behavior for a
 * realtime table. Two batch segments are uploaded to co-exist with the consuming LLC segments
 * created during cluster setup.
 *
 * <ul>
 *   <li>A sealed segment whose {@code creationTime} is within the 24-hour guard window is NOT purged.</li>
 *   <li>A sealed segment whose {@code creationTime} is outside the guard window IS purged.</li>
 *   <li>Consuming (IN_PROGRESS) LLC segments are never touched by RetentionManager.</li>
 * </ul>
 *
 * <p>This class extends {@link BaseClusterIntegrationTestSet} directly and defines its own
 * {@code setUp}/{@code tearDown}, so none of the inherited query-comparison tests run.
 */
public class RetentionManagerRealtimeIntegrationTest extends BaseClusterIntegrationTestSet {

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    // Prevent the periodic RealtimeSegmentValidationManager from interfering during the test
    properties.put(ControllerConf.ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_FREQUENCY_PERIOD, "2h");
    properties.put(
        ControllerConf.ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_INITIAL_DELAY_IN_SECONDS, 3600);
  }

  private static final String TIME_COLUMN = "eventTime";

  @Override
  protected String getTimeColumnName() {
    return TIME_COLUMN;
  }

  @Override
  protected Schema createSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(getTableName())
        .addSingleValueDimension("id", FieldSpec.DataType.INT)
        .addSingleValueDimension("name", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
  }

  /**
   * Generates 2 small Avro files in {@code outputDir}, each with 5 records, so the test does not
   * depend on the heavyweight airline-dataset tar.gz used by the default base-class implementation.
   */
  @Override
  protected List<File> unpackAvroData(File outputDir)
      throws IOException {
    org.apache.avro.Schema avroSchema =
        org.apache.avro.Schema.createRecord("retention_test_event", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field("id",
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, null),
        new org.apache.avro.Schema.Field("name",
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null),
        new org.apache.avro.Schema.Field(TIME_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG), null, null)));

    List<File> avroFiles = new ArrayList<>();
    long baseTimeMs = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(60);
    for (int fileIdx = 0; fileIdx < 2; fileIdx++) {
      File avroFile = new File(outputDir, "retention_test_" + fileIdx + ".avro");
      try (DataFileWriter<GenericData.Record> writer =
          new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
        writer.create(avroSchema, avroFile);
        for (int i = 0; i < 5; i++) {
          GenericData.Record record = new GenericData.Record(avroSchema);
          int rowId = fileIdx * 5 + i;
          record.put("id", rowId);
          record.put("name", "record_" + rowId);
          record.put(TIME_COLUMN, baseTimeMs + TimeUnit.MINUTES.toMillis(rowId));
          writer.append(record);
        }
      }
      avroFiles.add(avroFile);
    }
    return avroFiles;
  }

  @Override
  protected TableConfigBuilder getTableConfigBuilder(TableType tableType) {
    return new TableConfigBuilder(tableType)
        .setTableName(getTableName())
        .setTimeColumnName(getTimeColumnName())
        .setNumReplicas(getNumReplicas())
        .setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode())
        .setTaskConfig(getTaskConfig())
        .setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant())
        .setIngestionConfig(getIngestionConfig())
        .setQueryConfig(getQueryConfig())
        .setStreamConfigs(getStreamConfigs())
        .setNullHandlingEnabled(getNullHandlingEnabled())
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("60");
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    startZk();
    startController();
    startBroker();
    startServer();
    startKafka();

    List<File> avroFiles = unpackAvroData(_tempDir);
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createRealtimeTableConfig(avroFiles.get(0));
    addTableConfig(tableConfig);
    pushAvroIntoKafka(avroFiles);

    // Wait for at least one consuming LLC segment to be created by the server
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    TestUtils.waitForCondition(
        aVoid -> _helixResourceManager.getSegmentsZKMetadata(realtimeTableName)
            .stream().anyMatch(s -> s.getStatus() == Status.IN_PROGRESS),
        60_000L, "Timed out waiting for consuming LLC segments to appear in ZK");
  }

  @Override
  protected String getTableName() {
    return "RetentionManagerRTTest";
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropRealtimeTable(getTableName());
    waitForTableDataManagerRemoved(TableNameBuilder.REALTIME.tableNameWithType(getTableName()));
    waitForEVToDisappear(TableNameBuilder.REALTIME.tableNameWithType(getTableName()));
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  /**
   * Uploads 2 batch segments to the default realtime table so they co-exist with the consuming LLC
   * segments. Their ZK metadata is then overwritten to represent two create-time scenarios:
   * <ol>
   *   <li><b>Guarded</b>: endTime beyond retention, creationTime 1 hour ago — must NOT be purged.</li>
   *   <li><b>Purgeable</b>: endTime beyond retention, creationTime 48 hours ago — must be purged.</li>
   * </ol>
   *
   * <p>After running the retention manager, the test asserts that only the guarded uploaded segment
   * survives, the purgeable uploaded segment is deleted, consuming LLC segments are untouched, and
   * the {@code RETENTION_MANAGER_SEGMENTS_SKIPPED_BY_CREATE_TIME} gauge equals 1.
   */
  @Test
  public void testRetentionManagerCreateTimeGuard()
      throws Exception {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());

    // Add a 10-day retention window to the existing realtime table so RetentionManager processes it
    TableConfig tableConfig = _helixResourceManager.getTableConfig(realtimeTableName);
    tableConfig.getValidationConfig().setRetentionTimeUnit("DAYS");
    tableConfig.getValidationConfig().setRetentionTimeValue("10");
    _helixResourceManager.updateTableConfig(tableConfig);

    // Build 2 batch segments from the avro files already unpacked in setUp and upload them to the
    // realtime table. Uploaded segments get a DONE (sealed) status and co-exist with the consuming
    // LLC segments.
    List<File> avroFiles = new ArrayList<>(FileUtils.listFiles(_tempDir, new String[]{"avro"}, true));
    assertTrue(avroFiles.size() >= 2, "Expected at least 2 avro files in _tempDir");

    Schema schema = createSchema();
    TableConfig buildConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setTimeColumnName(getTimeColumnName())
        .build();
    File segmentBuildDir = new File(_tempDir, "rtGuardSegmentDir");
    File segmentTarDir = new File(_tempDir, "rtGuardTarDir");
    TestUtils.ensureDirectoriesExistAndEmpty(segmentBuildDir);
    TestUtils.ensureDirectoriesExistAndEmpty(segmentTarDir);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(
        avroFiles.subList(0, 2), buildConfig, schema, 100, segmentBuildDir, segmentTarDir);

    int segmentCountBefore = _helixResourceManager.getSegmentsZKMetadata(realtimeTableName).size();
    File[] uploadFiles = segmentTarDir.listFiles();
    assertNotNull(uploadFiles);
    assertEquals(uploadFiles.length, 2, "Expected exactly 2 built segment files");

    URI uploadUri = URI.create(getControllerRequestURLBuilder().forSegmentUpload());
    try (FileUploadDownloadClient client = new FileUploadDownloadClient()) {
      for (File segmentFile : uploadFiles) {
        client.uploadSegment(uploadUri, segmentFile.getName(), segmentFile, getTableName(), TableType.REALTIME);
      }
    }

    // Wait for both uploaded segments to appear in ZK
    int expectedCount = segmentCountBefore + 2;
    TestUtils.waitForCondition(
        aVoid -> _helixResourceManager.getSegmentsZKMetadata(realtimeTableName).size() == expectedCount,
        30_000L, "Timed out waiting for uploaded segments to be registered in ZK");

    // Identify uploaded segments by their non-LLC segment names
    List<SegmentZKMetadata> uploadedSegments = _helixResourceManager.getSegmentsZKMetadata(realtimeTableName)
        .stream()
        .filter(s -> !LLCSegmentName.isLowLevelConsumerSegmentName(s.getSegmentName()))
        .collect(Collectors.toList());
    assertEquals(uploadedSegments.size(), 2, "Expected exactly 2 non-LLC uploaded segments");

    // Snapshot the set of consuming segments before the retention run
    Set<String> consumingSegmentsBefore = _helixResourceManager.getSegmentsZKMetadata(realtimeTableName)
        .stream()
        .filter(s -> s.getStatus() == Status.IN_PROGRESS)
        .map(SegmentZKMetadata::getSegmentName)
        .collect(Collectors.toSet());

    long nowInMillis = System.currentTimeMillis();
    long endTime60DaysAgo = nowInMillis - TimeUnit.DAYS.toMillis(60);

    // Segment 0: created 1 hour ago — within the 24-hour guard window, must NOT be purged
    SegmentZKMetadata guardedSegment = uploadedSegments.get(0);
    guardedSegment.setTimeUnit(TimeUnit.MILLISECONDS);
    guardedSegment.setEndTime(endTime60DaysAgo);
    guardedSegment.setCreationTime(nowInMillis - Duration.ofHours(1).toMillis());
    ZKMetadataProvider.setSegmentZKMetadata(
        _helixResourceManager.getPropertyStore(), realtimeTableName, guardedSegment);

    // Segment 1: created 48 hours ago — outside the guard window, must be purged
    SegmentZKMetadata purgedSegment = uploadedSegments.get(1);
    purgedSegment.setTimeUnit(TimeUnit.MILLISECONDS);
    purgedSegment.setEndTime(endTime60DaysAgo);
    purgedSegment.setCreationTime(nowInMillis - Duration.ofHours(48).toMillis());
    ZKMetadataProvider.setSegmentZKMetadata(
        _helixResourceManager.getPropertyStore(), realtimeTableName, purgedSegment);

    // Build a RetentionManager with the 24-hour create-time guard enabled and scope it to the
    // realtime table only. Using the live _helixResourceManager and LeadControllerManager ensures
    // the guard runs against the real ZK state without waiting for the periodic scheduler.
    ControllerConf retentionConf = new ControllerConf(Map.of(
        ControllerConf.ControllerPeriodicTasksConf.SKIP_PURGING_RECENTLY_CREATED_SEGMENT_THRESHOLD_MS,
        Duration.ofHours(24).toMillis()));
    retentionConf.setRetentionControllerFrequencyInSeconds(0);
    retentionConf.setDeletedSegmentsRetentionInDays(0);
    retentionConf.setUntrackedSegmentDeletionEnabled(false);

    RetentionManager retentionManager = new RetentionManager(
        _helixResourceManager,
        _controllerStarter.getLeadControllerManager(),
        retentionConf,
        ControllerMetrics.get(),
        null /* BrokerServiceHelper — only needed for hybrid tables */);

    Properties taskProperties = new Properties();
    taskProperties.put(PeriodicTask.PROPERTY_KEY_TABLE_NAME, realtimeTableName);
    retentionManager.start();
    retentionManager.run(taskProperties);

    // Gauge must reflect exactly 1 segment skipped by the create-time guard
    assertEquals(
        MetricValueUtils.getTableGaugeValue(ControllerMetrics.get(), realtimeTableName,
            ControllerGauge.NUM_SEGMENTS_SKIPPED_RETENTION_BY_CREATE_TIME),
        1L, "Expected gauge to reflect 1 segment skipped by the create-time guard");

    TestUtils.waitForCondition(
        aVoid -> _helixResourceManager.getSegmentsZKMetadata(realtimeTableName).size() < expectedCount,
        30_000L, "Timed out waiting for segments are deleted by RetentionManager");

    List<SegmentZKMetadata> remainingSegments = _helixResourceManager.getSegmentsZKMetadata(realtimeTableName);
    // Consuming LLC segments must be completely untouched
    Set<String> consumingSegmentsAfter = remainingSegments
        .stream()
        .filter(s -> s.getStatus() == Status.IN_PROGRESS)
        .map(SegmentZKMetadata::getSegmentName)
        .collect(Collectors.toSet());
    assertEquals(consumingSegmentsAfter, consumingSegmentsBefore,
        "Consuming LLC segments must not be affected by RetentionManager");

    List<String> remainingNames = remainingSegments
        .stream().map(SegmentZKMetadata::getSegmentName).collect(Collectors.toList());

    // Guarded uploaded segment (created 1 h ago) must still be present
    assertTrue(remainingNames.contains(guardedSegment.getSegmentName()),
        "Segment protected by the create-time guard must not be purged");

    // Purgeable uploaded segment (created 48 h ago) must have been deleted
    assertFalse(remainingNames.contains(purgedSegment.getSegmentName()),
        "Segment outside the guard window must be purged");
  }
}
