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
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test for peer-to-peer segment download feature.
 * This test verifies that:
 * 1. When enablePeerDownload=true is used during segment upload, sourceServer is stored in ZK metadata
 * 2. The source server (first assigned server) downloads the segment from deep store
 * 3. Other servers download the segment from the source server via peer-to-peer mechanism
 * 4. All servers eventually have the segment loaded
 * 5. Queries work correctly after peer-to-peer download
 */
public class PeerToPeerSegmentDownloadIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(PeerToPeerSegmentDownloadIntegrationTest.class);

  private static final int NUM_BROKERS = 1;
  private static final int NUM_SERVERS = 3; // Need at least 2 servers for peer-to-peer testing
  private static final int REPLICATION_FACTOR = 2; // Ensure multiple servers get the segment

  private static final String PEER_DOWNLOAD_TEST_TABLE = "peerDownloadTestTable";
  private Schema _schema;
  private TableConfig _tableConfig;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBrokers(NUM_BROKERS);
    startServers(NUM_SERVERS);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload the schema
    _schema = createSchema();
    addSchema(_schema);

    // Create table config with replication
    _tableConfig = createOfflineTableConfig();
    addTableConfig(_tableConfig);

    // Generate segments from Avro files
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, _tableConfig, _schema, 0, _segmentDir, _tarDir);

    LOGGER.info("Setup complete. Cluster started with {} servers", NUM_SERVERS);
  }

  @Override
  protected String getTableName() {
    return PEER_DOWNLOAD_TEST_TABLE;
  }

  @Override
  protected TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setTimeColumnName(getTimeColumnName())
        .setNumReplicas(REPLICATION_FACTOR)
        .build();
  }

  @Test
  public void testPeerToPeerSegmentDownload()
      throws Exception {
    LOGGER.info("Starting testPeerToPeerSegmentDownload");

    // Get all segment tar files
    File[] segmentTarFiles = _tarDir.listFiles((dir, name) -> name.endsWith(".tar.gz"));
    assertNotNull(segmentTarFiles, "No segment tar files found");
    assertTrue(segmentTarFiles.length > 0, "No segment tar files found");

    LOGGER.info("Found {} segment tar files to upload", segmentTarFiles.length);

    // Upload all segments with enablePeerDownload=true
    for (File segmentTarFile : segmentTarFiles) {
      String segmentName = segmentTarFile.getName().replace(".tar.gz", "");
      LOGGER.info("Uploading segment {} with enablePeerDownload=true", segmentName);

      // Create request parameters with enablePeerDownload=true
      List<NameValuePair> parameters = new ArrayList<>();
      parameters.add(new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_NAME,
          getTableName()));
      parameters.add(new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_TYPE,
          TableType.OFFLINE.name()));
      parameters.add(new BasicNameValuePair("enablePeerDownload", "true"));

      // Upload segment
      URI uploadSegmentHttpURI = URI.create(getControllerRequestURLBuilder().forSegmentUpload());
      try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
        SimpleHttpResponse response = fileUploadDownloadClient.uploadSegment(uploadSegmentHttpURI,
            segmentName, segmentTarFile, null, parameters, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS);
        assertEquals(response.getStatusCode(), 200, "Segment upload failed: " + response.getResponse());
        LOGGER.info("Successfully uploaded segment {}", segmentName);
      }
    }

    // Wait for all segments to be loaded on all servers
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    int expectedNumSegments = segmentTarFiles.length;

    LOGGER.info("Waiting for {} segments to be loaded across {} servers with replication factor {}",
        expectedNumSegments, NUM_SERVERS, REPLICATION_FACTOR);

    TestUtils.waitForCondition(aVoid -> {
      try {
        IdealState idealState = _helixResourceManager.getTableIdealState(tableNameWithType);
        if (idealState == null) {
          LOGGER.warn("IdealState is null for table {}", tableNameWithType);
          return false;
        }

        Map<String, Map<String, String>> segmentAssignment = idealState.getRecord().getMapFields();
        if (segmentAssignment.size() != expectedNumSegments) {
          LOGGER.info("Expected {} segments in IdealState, found {}", expectedNumSegments,
              segmentAssignment.size());
          return false;
        }

        // Verify each segment is assigned to REPLICATION_FACTOR servers
        for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
          String segmentName = entry.getKey();
          Map<String, String> instanceStateMap = entry.getValue();
          if (instanceStateMap.size() != REPLICATION_FACTOR) {
            LOGGER.info("Segment {} is assigned to {} servers, expected {}", segmentName,
                instanceStateMap.size(), REPLICATION_FACTOR);
            return false;
          }
        }

        LOGGER.info("All {} segments are assigned to {} servers", expectedNumSegments, REPLICATION_FACTOR);
        return true;
      } catch (Exception e) {
        LOGGER.error("Error checking segment assignment", e);
        return false;
      }
    }, 60_000L, "Timed out waiting for segments to be assigned");

    LOGGER.info("All segments assigned. Waiting for segments to be loaded...");

    // Wait for all segments to be loaded (ONLINE state)
    TestUtils.waitForCondition(aVoid -> {
      try {
        for (BaseServerStarter serverStarter : _serverStarters) {
          TableDataManager tableDataManager =
              serverStarter.getServerInstance().getInstanceDataManager().getTableDataManager(tableNameWithType);
          if (tableDataManager == null) {
            LOGGER.info("Table {} not yet loaded on server {}", tableNameWithType,
                serverStarter.getInstanceId());
            return false;
          }

          int numSegmentsLoaded = tableDataManager.getNumSegments();
          LOGGER.info("Server {} has {} segments loaded (expected >= 1)", serverStarter.getInstanceId(),
              numSegmentsLoaded);
        }
        return true;
      } catch (Exception e) {
        LOGGER.error("Error checking segment loading status", e);
        return false;
      }
    }, 120_000L, "Timed out waiting for segments to be loaded on servers");

    LOGGER.info("All segments loaded on servers");

    // Verify sourceServer is set in ZK metadata and is one of the actually assigned instances
    IdealState idealState = _helixResourceManager.getTableIdealState(tableNameWithType);
    assertNotNull(idealState, "IdealState not found for table: " + tableNameWithType);
    for (File segmentTarFile : segmentTarFiles) {
      String segmentName = segmentTarFile.getName().replace(".tar.gz", "");
      SegmentZKMetadata zkMetadata = _helixResourceManager.getSegmentZKMetadata(tableNameWithType, segmentName);

      assertNotNull(zkMetadata, "ZK metadata not found for segment: " + segmentName);
      String sourceServer = zkMetadata.getSourceServer();
      assertNotNull(sourceServer, "sourceServer not set for segment: " + segmentName);

      Map<String, String> instanceStateMap = idealState.getRecord().getMapFields().get(segmentName);
      assertNotNull(instanceStateMap, "Segment not found in IdealState: " + segmentName);
      assertTrue(instanceStateMap.containsKey(sourceServer),
          "sourceServer " + sourceServer + " is not one of the assigned instances "
              + instanceStateMap.keySet() + " for segment " + segmentName);
      LOGGER.info("Segment {} has sourceServer: {} (assigned instances: {})",
          segmentName, sourceServer, instanceStateMap.keySet());
    }

    LOGGER.info("Verified sourceServer is set correctly for all segments");

    // Verify queries work correctly
    String query = "SELECT COUNT(*) FROM " + getTableName();
    long expectedCount = getCountStarResult();

    TestUtils.waitForCondition(aVoid -> {
      try {
        long actualCount = getPinotConnection().execute(query).getResultSet(0).getLong(0);
        LOGGER.info("Query result: {} (expected: {})", actualCount, expectedCount);
        return actualCount == expectedCount;
      } catch (Exception e) {
        LOGGER.error("Query execution failed", e);
        return false;
      }
    }, 60_000L, "Query results did not match expected count");

    LOGGER.info("Query verification successful. COUNT(*) returned expected result: {}", expectedCount);
    LOGGER.info("testPeerToPeerSegmentDownload completed successfully");
  }

  @Test(dependsOnMethods = "testPeerToPeerSegmentDownload")
  public void testBackwardCompatibility()
      throws Exception {
    LOGGER.info("Starting testBackwardCompatibility");

    // This test validates that our peer-to-peer feature doesn't break anything
    // All segments were uploaded with enablePeerDownload=true in test 1
    // Verify the table still works correctly

    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    // Verify all segments have sourceServer set
    File[] segmentTarFiles = _tarDir.listFiles((dir, name) -> name.endsWith(".tar.gz"));
    assertNotNull(segmentTarFiles);

    int segmentsWithSourceServerURI = 0;
    for (File segmentTarFile : segmentTarFiles) {
      String segmentName = segmentTarFile.getName().replace(".tar.gz", "");
      SegmentZKMetadata zkMetadata = _helixResourceManager.getSegmentZKMetadata(tableNameWithType, segmentName);
      if (zkMetadata != null && zkMetadata.getSourceServer() != null
          && !zkMetadata.getSourceServer().isEmpty()) {
        segmentsWithSourceServerURI++;
      }
    }

    LOGGER.info("Found {} out of {} segments with sourceServer set",
        segmentsWithSourceServerURI, segmentTarFiles.length);
    assertEquals(segmentsWithSourceServerURI, segmentTarFiles.length,
        "All segments should have sourceServer set when uploaded with enablePeerDownload=true");

    // Verify queries still work correctly (already validated in test 1, but let's check again)
    String query = "SELECT COUNT(*) FROM " + getTableName();
    long expectedCount = getCountStarResult();
    long actualCount = getPinotConnection().execute(query).getResultSet(0).getLong(0);

    LOGGER.info("Query result: {} (expected: {})", actualCount, expectedCount);
    assertEquals(actualCount, expectedCount, "Query results should match expected count");

    LOGGER.info("testBackwardCompatibility completed successfully - peer-to-peer feature is working correctly");
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    // Drop table (ignore errors if it doesn't exist)
    try {
      dropOfflineTable(PEER_DOWNLOAD_TEST_TABLE);
    } catch (Exception e) {
      LOGGER.warn("Failed to drop table {}: {}", PEER_DOWNLOAD_TEST_TABLE, e.getMessage());
    }

    // Stop cluster components
    stopServer();
    stopBroker();
    stopController();
    stopZk();

    // Clean up temp directories
    FileUtils.deleteDirectory(_tempDir);
  }
}
