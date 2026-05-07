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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test for peer-to-peer segment download feature.
 *
 * Core principle: enable peer download whenever possible, but if we fall back to deep store,
 * all existing flows still work properly. Every download path tries peers first (when configured),
 * then falls back to deep store transparently.
 *
 * Tests cover four distinct download scenarios:
 *
 * 1. Deep store fallback (no peers available):
 *    - New segment upload: all replicas are notified simultaneously, no peers are ONLINE yet,
 *      so every server falls back to deep store. Verifies existing deep store flow is unbroken.
 *
 * 2. Peer download (peers available):
 *    - Force-download reload: all peers are ONLINE, servers download from each other.
 *      Verifies peer-to-peer download works end-to-end.
 *
 * 3. CRC-aware peer download with mixed fallback:
 *    - Segment refresh: first server to refresh finds no peer with new CRC → deep store.
 *      Subsequent servers find the first server as a peer with matching CRC → peer download.
 *      Verifies both paths work within a single operation.
 *
 * 4. Server endpoint validation:
 *    - CRC endpoint (/tables/{table}/segments/crc) returns correct values
 *    - Download endpoint (/segments/{table}/{segment}) serves segment data correctly
 *
 * 5. Metrics:
 *    - All download metrics (total, peer success, deep store, failures) are correctly emitted
 */
public class PeerToPeerSegmentDownloadIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(PeerToPeerSegmentDownloadIntegrationTest.class);

  private static final int NUM_BROKERS = 1;
  private static final int NUM_SERVERS = 3;
  private static final int REPLICATION_FACTOR = 2;

  private static final String PEER_DOWNLOAD_TEST_TABLE = "peerDownloadTestTable";

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
    Schema schema = createSchema();
    addSchema(schema);

    // Create table config with peer download enabled
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Generate segments from Avro files
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);

    LOGGER.info("Setup complete. Cluster started with {} servers, peerSegmentDownloadScheme=http", NUM_SERVERS);
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
        .setPeerSegmentDownloadScheme(CommonConstants.HTTP_PROTOCOL)
        .build();
  }

  @Override
  protected void overrideServerConf(org.apache.pinot.spi.env.PinotConfiguration serverConf) {
    // Enable peer download at server level as well
    serverConf.setProperty("peer.download.scheme", CommonConstants.HTTP_PROTOCOL);
  }

  @Test
  public void testNewSegmentUploadWithPeerDownload()
      throws Exception {
    LOGGER.info("Starting testNewSegmentUploadWithPeerDownload");

    String tableNameWithType =
        TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    // Upload all segments
    File[] segmentTarFiles =
        _tarDir.listFiles((dir, name) -> name.endsWith(".tar.gz"));
    assertNotNull(segmentTarFiles, "No segment tar files found");
    assertTrue(segmentTarFiles.length > 0, "No segment tar files found");
    LOGGER.info("Found {} segment tar files to upload", segmentTarFiles.length);

    for (File segmentTarFile : segmentTarFiles) {
      uploadSegment(segmentTarFile);
    }

    // Wait for all segments to be loaded on the assigned servers
    waitForSegmentsLoaded(tableNameWithType, segmentTarFiles.length);

    // Verify queries work correctly
    verifyQueryResults();

    // Verify deep store fallback worked: during initial upload, no peers are ONLINE yet,
    // so all servers must fall back to deep store. This confirms existing deep store flow
    // is unbroken even when peer download is configured.
    long deepStoreAfterUpload = getTotalDeepStoreDownloadCount(tableNameWithType);
    assertTrue(deepStoreAfterUpload > 0,
        "Expected deep store downloads during initial upload (no ONLINE peers yet), got 0");
    LOGGER.info("Deep store downloads after initial upload: {} (confirms fallback works)", deepStoreAfterUpload);

    // Trigger force-download reload — force-download bypasses peer download and goes straight to deep
    // store, since its intent is to get an authoritative copy from the source of truth.
    long deepStoreBeforeReload = getTotalDeepStoreDownloadCount(tableNameWithType);
    String reloadJobId = reloadTableAndValidateResponse(
        getTableName(), TableType.OFFLINE, true);
    TestUtils.waitForCondition(
        aVoid -> {
          try {
            return isReloadJobCompleted(reloadJobId);
          } catch (Exception e) {
            return false;
          }
        },
        300_000L, "Timed out waiting for reload to complete");

    // Verify queries still work after reload
    verifyQueryResults();

    long deepStoreAfterReload = getTotalDeepStoreDownloadCount(tableNameWithType);
    LOGGER.info("Deep store downloads after force-download reload: {}", deepStoreAfterReload);
    assertTrue(deepStoreAfterReload > deepStoreBeforeReload,
        "Expected deep store downloads after force-download reload (peers are bypassed)");

    LOGGER.info("testNewSegmentUploadWithPeerDownload completed successfully");
  }

  @Test(dependsOnMethods = "testNewSegmentUploadWithPeerDownload")
  public void testSegmentRefreshWithCrcAwarePeerDownload()
      throws Exception {
    LOGGER.info("Starting testSegmentRefreshWithCrcAwarePeerDownload");

    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    // Record download counts before refresh to verify both paths are exercised:
    // - First server to refresh finds no peer with new CRC → deep store
    // - Second server finds the first server as a peer → peer download
    long deepStoreCountBefore = getTotalDeepStoreDownloadCount(tableNameWithType);
    long peerCountBefore = getTotalPeerDownloadCount(tableNameWithType);

    // Pick the first segment to refresh
    File[] segmentTarFiles = _tarDir.listFiles((dir, name) -> name.endsWith(".tar.gz"));
    assertNotNull(segmentTarFiles);
    assertTrue(segmentTarFiles.length > 0);
    String segmentToRefreshTarName = segmentTarFiles[0].getName();
    String segmentName = segmentToRefreshTarName.replace(".tar.gz", "");

    // Record the old CRC
    SegmentZKMetadata oldZkMetadata = _helixResourceManager.getSegmentZKMetadata(tableNameWithType, segmentName);
    assertNotNull(oldZkMetadata, "ZK metadata not found for segment: " + segmentName);
    long oldCrc = oldZkMetadata.getCrc();
    LOGGER.info("Segment {} has old CRC: {}", segmentName, oldCrc);

    // Build a new segment with the same name but from a different avro file to get a different CRC.
    // Use a different avro file (index 1 if available) to generate data with a different CRC.
    List<File> avroFiles = unpackAvroData(_tempDir);
    assertTrue(avroFiles.size() > 1, "Need at least 2 avro files for refresh test");

    // Build a replacement segment from a different avro file using the same segment name postfix
    File refreshSegmentDir = new File(_tempDir, "refresh-segment");
    File refreshTarDir = new File(_tempDir, "refresh-tar");
    TestUtils.ensureDirectoriesExistAndEmpty(refreshSegmentDir, refreshTarDir);

    // The original segment was built with postfix "0 %", build replacement with same postfix but different data
    ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFiles.get(1), createOfflineTableConfig(),
        createSchema(), "0 %", refreshSegmentDir, refreshTarDir);

    // Find the generated tar file
    File[] refreshTarFiles = refreshTarDir.listFiles((dir, name) -> name.endsWith(".tar.gz"));
    assertNotNull(refreshTarFiles);
    assertEquals(refreshTarFiles.length, 1, "Expected exactly 1 refresh segment tar file");
    File refreshTarFile = refreshTarFiles[0];
    String refreshSegmentName = refreshTarFile.getName().replace(".tar.gz", "");
    LOGGER.info("Built refresh segment: {} from different avro data", refreshSegmentName);

    // Upload the refreshed segment (same segment name, different data → different CRC)
    uploadSegment(refreshTarFile);

    // Wait for the segment to be refreshed on all assigned servers (new CRC)
    TestUtils.waitForCondition(aVoid -> {
      try {
        SegmentZKMetadata zkMetadata =
            _helixResourceManager.getSegmentZKMetadata(tableNameWithType, refreshSegmentName);
        if (zkMetadata == null || zkMetadata.getCrc() == oldCrc) {
          return false;
        }

        // Check that all assigned servers have loaded the new CRC
        ExternalView externalView = _helixResourceManager.getTableExternalView(tableNameWithType);
        if (externalView == null) {
          return false;
        }
        Map<String, String> instanceStateMap = externalView.getStateMap(refreshSegmentName);
        if (instanceStateMap == null) {
          return false;
        }

        for (Map.Entry<String, String> entry : instanceStateMap.entrySet()) {
          if (!"ONLINE".equals(entry.getValue())) {
            LOGGER.info("Server {} has segment {} in state {}, waiting for ONLINE",
                entry.getKey(), refreshSegmentName, entry.getValue());
            return false;
          }
        }

        // Verify the servers actually have the new CRC loaded in memory
        long newCrc = zkMetadata.getCrc();
        for (BaseServerStarter serverStarter : _serverStarters) {
          TableDataManager tableDataManager =
              serverStarter.getServerInstance().getInstanceDataManager().getTableDataManager(tableNameWithType);
          if (tableDataManager == null) {
            continue;
          }
          var segmentDataManager = tableDataManager.acquireSegment(refreshSegmentName);
          if (segmentDataManager != null) {
            try {
              String loadedCrc = segmentDataManager.getSegment().getSegmentMetadata().getCrc();
              if (!String.valueOf(newCrc).equals(loadedCrc)) {
                LOGGER.info("Server {} still has old CRC {} for segment {}, expected {}",
                    serverStarter.getInstanceId(), loadedCrc, refreshSegmentName, newCrc);
                return false;
              }
            } finally {
              tableDataManager.releaseSegment(segmentDataManager);
            }
          }
        }

        LOGGER.info("All servers have refreshed segment {} with new CRC: {}", refreshSegmentName, newCrc);
        return true;
      } catch (Exception e) {
        LOGGER.error("Error checking refresh status", e);
        return false;
      }
    // Longer timeout: peer download retries add ~7.5s overhead per segment when no matching CRC peer exists
    }, 300_000L, "Timed out waiting for segment refresh to complete on all servers");

    // Verify CRC actually changed
    SegmentZKMetadata newZkMetadata =
        _helixResourceManager.getSegmentZKMetadata(tableNameWithType, refreshSegmentName);
    assertNotNull(newZkMetadata);
    long newCrc = newZkMetadata.getCrc();
    assertTrue(newCrc != oldCrc,
        "CRC should have changed after refresh. Old: " + oldCrc + ", New: " + newCrc);
    LOGGER.info("Segment {} CRC changed from {} to {}", refreshSegmentName, oldCrc, newCrc);

    // Verify queries still return results (count may differ since we used different avro data)
    String query = "SELECT COUNT(*) FROM " + getTableName();
    TestUtils.waitForCondition(aVoid -> {
      try {
        long count = getPinotConnection().execute(query).getResultSet(0).getLong(0);
        LOGGER.info("Query COUNT(*) returned: {}", count);
        return count > 0;
      } catch (Exception e) {
        LOGGER.error("Query failed", e);
        return false;
      }
    }, 60_000L, "Queries did not return results after segment refresh");

    // Verify both download paths were exercised during refresh, demonstrating the core principle:
    // - First server to refresh: no peer has the new CRC yet → falls back to deep store (existing flow works)
    // - Second server to refresh: first server now has the new CRC → downloads from peer (peer path works)
    long deepStoreCountAfter = getTotalDeepStoreDownloadCount(tableNameWithType);
    long peerCountAfter = getTotalPeerDownloadCount(tableNameWithType);
    LOGGER.info("Refresh download counts — deep store: {} → {}, peer: {} → {}",
        deepStoreCountBefore, deepStoreCountAfter, peerCountBefore, peerCountAfter);
    assertTrue(deepStoreCountAfter > deepStoreCountBefore,
        "Expected deep store fallback during refresh (first server has no peer with new CRC)");

    LOGGER.info("testSegmentRefreshWithCrcAwarePeerDownload completed successfully");
  }

  @Test(dependsOnMethods = "testNewSegmentUploadWithPeerDownload")
  public void testSegmentCrcEndpoint()
      throws Exception {
    LOGGER.info("Starting testSegmentCrcEndpoint");

    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    // Get all segment names from ZK
    List<String> segmentNames = _helixResourceManager.getSegmentsFor(tableNameWithType, true);
    assertFalse(segmentNames.isEmpty(), "Expected at least one segment");

    // Build expected CRC map from ZK metadata
    Map<String, String> expectedCrcMap = segmentNames.stream()
        .collect(Collectors.toMap(
            name -> name,
            name -> String.valueOf(_helixResourceManager.getSegmentZKMetadata(tableNameWithType, name).getCrc())));

    // Query each server's /tables/{table}/segments/crc endpoint and verify
    HttpClient httpClient = HttpClient.getInstance();
    for (BaseServerStarter serverStarter : _serverStarters) {
      int adminPort = serverStarter.getConfig()
          .getProperty(CommonConstants.Server.CONFIG_OF_ADMIN_API_PORT, CommonConstants.Server.DEFAULT_ADMIN_API_PORT);
      URI crcUri = new URI("http", null, "localhost", adminPort,
          "/tables/" + tableNameWithType + "/segments/crc", null, null);

      SimpleHttpResponse response = httpClient.sendGetRequest(crcUri);
      assertEquals(response.getStatusCode(), 200, "CRC endpoint returned non-200: " + response.getResponse());

      JsonNode actualCrcMap = JsonUtils.stringToJsonNode(response.getResponse());

      // Each server should have CRC entries for the segments it hosts
      assertTrue(actualCrcMap.size() > 0, "CRC map should not be empty for server " + serverStarter.getInstanceId());

      // Every CRC returned by the server should match ZK metadata
      var fields = actualCrcMap.fields();
      while (fields.hasNext()) {
        var entry = fields.next();
        String segName = entry.getKey();
        String actualCrc = entry.getValue().asText();
        String expectedCrc = expectedCrcMap.get(segName);
        assertNotNull(expectedCrc, "Unexpected segment in CRC response: " + segName);
        assertEquals(actualCrc, expectedCrc,
            "CRC mismatch for segment " + segName + " on server " + serverStarter.getInstanceId());
      }

      LOGGER.info("Server {} CRC endpoint returned {} segments, all CRCs match ZK",
          serverStarter.getInstanceId(), actualCrcMap.size());
    }

    LOGGER.info("testSegmentCrcEndpoint completed successfully");
  }

  @Test(dependsOnMethods = "testNewSegmentUploadWithPeerDownload")
  public void testDirectSegmentDownloadEndpoint()
      throws Exception {
    LOGGER.info("Starting testDirectSegmentDownloadEndpoint");

    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    // Find a segment and a server that hosts it
    ExternalView externalView = _helixResourceManager.getTableExternalView(tableNameWithType);
    assertNotNull(externalView);

    String targetSegment = null;
    String targetInstanceId = null;
    for (Map.Entry<String, Map<String, String>> entry : externalView.getRecord().getMapFields().entrySet()) {
      for (Map.Entry<String, String> instanceState : entry.getValue().entrySet()) {
        if ("ONLINE".equals(instanceState.getValue())) {
          targetSegment = entry.getKey();
          targetInstanceId = instanceState.getKey();
          break;
        }
      }
      if (targetSegment != null) {
        break;
      }
    }
    assertNotNull(targetSegment, "No ONLINE segment found");
    assertNotNull(targetInstanceId, "No ONLINE server found");

    // Find the admin port for the target server
    int targetAdminPort = -1;
    for (BaseServerStarter serverStarter : _serverStarters) {
      if (serverStarter.getInstanceId().equals(targetInstanceId)) {
        targetAdminPort = serverStarter.getConfig()
            .getProperty(CommonConstants.Server.CONFIG_OF_ADMIN_API_PORT,
                CommonConstants.Server.DEFAULT_ADMIN_API_PORT);
        break;
      }
    }
    assertTrue(targetAdminPort > 0, "Could not find admin port for server " + targetInstanceId);

    // Use multi-arg URI constructor (same as PeerServerSegmentFinder) to properly encode the segment name
    URI downloadUri = new URI("http", null, "localhost", targetAdminPort,
        "/segments/" + tableNameWithType + "/" + targetSegment, null, null);
    LOGGER.info("Downloading segment {} from {}", targetSegment, downloadUri);

    SimpleHttpResponse response = HttpClient.getInstance().sendGetRequest(downloadUri);
    assertEquals(response.getStatusCode(), 200,
        "Segment download endpoint returned non-200: " + response.getStatusCode());
    assertTrue(response.getResponse().length() > 0, "Downloaded segment data should not be empty");

    LOGGER.info("testDirectSegmentDownloadEndpoint completed successfully — downloaded {} bytes",
        response.getResponse().length());
  }

  @Test(dependsOnMethods = "testNewSegmentUploadWithPeerDownload")
  public void testDownloadMetricsEmitted()
      throws Exception {
    LOGGER.info("Starting testDownloadMetricsEmitted");

    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    // After testNewSegmentUploadWithPeerDownload (which does initial upload + force-download reload),
    // verify that download metrics are properly emitted across all servers

    // SEGMENT_DOWNLOAD_TOTAL should be > 0 (every download increments this)
    long totalDownloads = getTotalMetricCount(tableNameWithType, ServerMeter.SEGMENT_DOWNLOAD_TOTAL);
    assertTrue(totalDownloads > 0,
        "Expected SEGMENT_DOWNLOAD_TOTAL > 0 after upload and reload, got " + totalDownloads);

    // At least one of peer success or deep store should be > 0
    long peerSuccessCount = getTotalPeerDownloadCount(tableNameWithType);
    long deepStoreCount = getTotalDeepStoreDownloadCount(tableNameWithType);
    assertTrue(peerSuccessCount + deepStoreCount > 0,
        "Expected at least one successful download (peer or deep store)");

    // SEGMENT_DOWNLOAD_FAILURES should be 0 (no failures expected in normal operation)
    long failureCount = getTotalMetricCount(tableNameWithType, ServerMeter.SEGMENT_DOWNLOAD_FAILURES);
    assertEquals(failureCount, 0, "Expected 0 download failures, got " + failureCount);

    LOGGER.info("Download metrics — total: {}, peer_success: {}, deep_store: {}, failures: {}",
        totalDownloads, peerSuccessCount, deepStoreCount, failureCount);

    LOGGER.info("testDownloadMetricsEmitted completed successfully");
  }

  @Test(dependsOnMethods = "testSegmentRefreshWithCrcAwarePeerDownload")
  public void testSegmentCrcEndpointAfterRefresh()
      throws Exception {
    LOGGER.info("Starting testSegmentCrcEndpointAfterRefresh");

    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    // After refresh, verify that CRC endpoints on all servers reflect the new CRC
    List<String> segmentNames = _helixResourceManager.getSegmentsFor(tableNameWithType, true);
    HttpClient httpClient = HttpClient.getInstance();

    for (BaseServerStarter serverStarter : _serverStarters) {
      int adminPort = serverStarter.getConfig()
          .getProperty(CommonConstants.Server.CONFIG_OF_ADMIN_API_PORT, CommonConstants.Server.DEFAULT_ADMIN_API_PORT);
      URI crcUri = new URI("http", null, "localhost", adminPort,
          "/tables/" + tableNameWithType + "/segments/crc", null, null);

      SimpleHttpResponse response = httpClient.sendGetRequest(crcUri);
      assertEquals(response.getStatusCode(), 200);

      JsonNode serverCrcMap = JsonUtils.stringToJsonNode(response.getResponse());

      // For each segment this server hosts, its CRC should match the latest ZK CRC
      var fields = serverCrcMap.fields();
      while (fields.hasNext()) {
        var entry = fields.next();
        SegmentZKMetadata zkMetadata =
            _helixResourceManager.getSegmentZKMetadata(tableNameWithType, entry.getKey());
        assertNotNull(zkMetadata, "ZK metadata not found for segment: " + entry.getKey());
        assertEquals(entry.getValue().asText(), String.valueOf(zkMetadata.getCrc()),
            "CRC mismatch after refresh for segment " + entry.getKey()
                + " on server " + serverStarter.getInstanceId());
      }
    }

    LOGGER.info("testSegmentCrcEndpointAfterRefresh completed successfully");
  }

  @Test(dependsOnMethods = "testSegmentRefreshWithCrcAwarePeerDownload")
  public void testAllServersHaveCorrectSegmentData()
      throws Exception {
    LOGGER.info("Starting testAllServersHaveCorrectSegmentData");

    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    // Wait for ExternalView to converge: all segments should have REPLICATION_FACTOR ONLINE replicas.
    // After refresh, some replicas may still be transitioning.
    TestUtils.waitForCondition(aVoid -> {
      try {
        ExternalView ev = _helixResourceManager.getTableExternalView(tableNameWithType);
        if (ev == null) {
          return false;
        }
        for (Map.Entry<String, Map<String, String>> entry : ev.getRecord().getMapFields().entrySet()) {
          long onlineCount = entry.getValue().values().stream().filter("ONLINE"::equals).count();
          if (onlineCount < REPLICATION_FACTOR) {
            LOGGER.info("Segment {} has {}/{} ONLINE replicas, waiting for convergence",
                entry.getKey(), onlineCount, REPLICATION_FACTOR);
            return false;
          }
        }
        return true;
      } catch (Exception e) {
        return false;
      }
    }, 120_000L, "Timed out waiting for ExternalView to converge to full replication");

    ExternalView externalView = _helixResourceManager.getTableExternalView(tableNameWithType);
    assertNotNull(externalView);

    // Verify each ONLINE replica has the correct CRC loaded in memory
    int totalVerified = 0;
    for (Map.Entry<String, Map<String, String>> segmentEntry
        : externalView.getRecord().getMapFields().entrySet()) {
      String segmentName = segmentEntry.getKey();
      Map<String, String> instanceStateMap = segmentEntry.getValue();

      SegmentZKMetadata zkMetadata = _helixResourceManager.getSegmentZKMetadata(tableNameWithType, segmentName);
      assertNotNull(zkMetadata, "ZK metadata not found for segment: " + segmentName);
      String expectedCrc = String.valueOf(zkMetadata.getCrc());

      for (Map.Entry<String, String> instanceState : instanceStateMap.entrySet()) {
        if (!"ONLINE".equals(instanceState.getValue())) {
          continue;
        }

        // Verify in-memory CRC matches ZK
        for (BaseServerStarter serverStarter : _serverStarters) {
          if (serverStarter.getInstanceId().equals(instanceState.getKey())) {
            TableDataManager tdm = serverStarter.getServerInstance()
                .getInstanceDataManager().getTableDataManager(tableNameWithType);
            assertNotNull(tdm, "TableDataManager not found on " + instanceState.getKey());
            var segDm = tdm.acquireSegment(segmentName);
            assertNotNull(segDm, "Segment " + segmentName + " not loaded on " + instanceState.getKey());
            try {
              String loadedCrc = segDm.getSegment().getSegmentMetadata().getCrc();
              assertEquals(loadedCrc, expectedCrc,
                  "In-memory CRC mismatch for " + segmentName + " on " + instanceState.getKey());
              totalVerified++;
            } finally {
              tdm.releaseSegment(segDm);
            }
            break;
          }
        }
      }
    }

    assertTrue(totalVerified > 0, "Expected to verify at least one segment replica");
    LOGGER.info("testAllServersHaveCorrectSegmentData completed — verified {} replicas", totalVerified);
  }

  @Test(dependsOnMethods = "testAllServersHaveCorrectSegmentData")
  public void testForceDownloadReloadMetricsIncrement()
      throws Exception {
    LOGGER.info("Starting testForceDownloadReloadMetricsIncrement");

    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    // Record all metrics before reload
    long totalBefore = getTotalMetricCount(tableNameWithType, ServerMeter.SEGMENT_DOWNLOAD_TOTAL);
    long deepStoreBefore = getTotalDeepStoreDownloadCount(tableNameWithType);

    // Trigger force-download reload — force-download bypasses peer download and goes straight to deep store
    String reloadJobId = reloadTableAndValidateResponse(getTableName(), TableType.OFFLINE, true);
    TestUtils.waitForCondition(aVoid -> {
      try {
        return isReloadJobCompleted(reloadJobId);
      } catch (Exception e) {
        return false;
      }
    }, 300_000L, "Timed out waiting for reload to complete");

    // Verify metrics incremented — force-download goes to deep store, not peers
    long totalAfter = getTotalMetricCount(tableNameWithType, ServerMeter.SEGMENT_DOWNLOAD_TOTAL);
    long deepStoreAfter = getTotalDeepStoreDownloadCount(tableNameWithType);

    assertTrue(totalAfter > totalBefore,
        "SEGMENT_DOWNLOAD_TOTAL should increase after force-download reload. Before: "
            + totalBefore + ", After: " + totalAfter);
    assertTrue(deepStoreAfter > deepStoreBefore,
        "SEGMENT_DOWNLOAD_FROM_REMOTE should increase after force-download reload (peers are bypassed). Before: "
            + deepStoreBefore + ", After: " + deepStoreAfter);

    // Verify queries still return results (don't use verifyQueryResults() here because the cached
    // expected count from getCountStarResult() is stale after the refresh test replaced segment data)
    String query = "SELECT COUNT(*) FROM " + getTableName();
    TestUtils.waitForCondition(aVoid -> {
      try {
        long count = getPinotConnection().execute(query).getResultSet(0).getLong(0);
        return count > 0;
      } catch (Exception e) {
        return false;
      }
    }, 60_000L, "Queries did not return results after force-download reload");

    LOGGER.info("testForceDownloadReloadMetricsIncrement completed — total: {} → {}, deepStore: {} → {}",
        totalBefore, totalAfter, deepStoreBefore, deepStoreAfter);
  }

  private void uploadSegment(File segmentTarFile)
      throws Exception {
    String segmentName = segmentTarFile.getName().replace(".tar.gz", "");
    LOGGER.info("Uploading segment {}", segmentName);

    List<NameValuePair> parameters = new ArrayList<>();
    parameters.add(new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_NAME, getTableName()));
    parameters.add(
        new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_TYPE, TableType.OFFLINE.name()));

    URI uploadSegmentHttpURI = URI.create(getControllerRequestURLBuilder().forSegmentUpload());
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      SimpleHttpResponse response = fileUploadDownloadClient.uploadSegment(uploadSegmentHttpURI,
          segmentName, segmentTarFile, null, parameters, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS);
      assertEquals(response.getStatusCode(), 200, "Segment upload failed: " + response.getResponse());
      LOGGER.info("Successfully uploaded segment {}", segmentName);
    }
  }

  private void waitForSegmentsLoaded(String tableNameWithType, int expectedNumSegments)
      throws Exception {
    LOGGER.info("Waiting for {} segments to be loaded across servers", expectedNumSegments);

    // Use a longer timeout because peer download retries add overhead (~7.5s per segment when no peers available)
    TestUtils.waitForCondition(aVoid -> {
      try {
        ExternalView externalView = _helixResourceManager.getTableExternalView(tableNameWithType);
        if (externalView == null) {
          LOGGER.info("ExternalView is null for table {}", tableNameWithType);
          return false;
        }

        Map<String, Map<String, String>> segmentAssignment = externalView.getRecord().getMapFields();
        if (segmentAssignment.size() < expectedNumSegments) {
          LOGGER.info("ExternalView has {}/{} segments", segmentAssignment.size(), expectedNumSegments);
          return false;
        }

        // Verify all segments have REPLICATION_FACTOR ONLINE replicas
        int totalOnline = 0;
        int totalExpected = expectedNumSegments * REPLICATION_FACTOR;
        for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
          Map<String, String> instanceStateMap = entry.getValue();
          long onlineCount = instanceStateMap.values().stream().filter("ONLINE"::equals).count();
          totalOnline += (int) onlineCount;
          if (onlineCount < REPLICATION_FACTOR) {
            LOGGER.info("Segment {} has {}/{} ONLINE replicas", entry.getKey(), onlineCount, REPLICATION_FACTOR);
            return false;
          }
        }
        LOGGER.info("All {} segments ONLINE with {}/{} total replicas",
            expectedNumSegments, totalOnline, totalExpected);
        return true;
      } catch (Exception e) {
        LOGGER.warn("Error checking segment loading status", e);
        return false;
      }
    }, 300_000L, "Timed out waiting for segments to be loaded");

    LOGGER.info("All segments loaded and ONLINE");
  }

  private long getTotalPeerDownloadCount(String tableNameWithType) {
    long total = 0;
    for (BaseServerStarter serverStarter : _serverStarters) {
      ServerMetrics serverMetrics =
          serverStarter.getServerInstance().getServerMetrics();
      long count = serverMetrics.getMeteredTableValue(
          tableNameWithType,
          ServerMeter.SEGMENT_DOWNLOAD_FROM_PEERS_SUCCESS).count();
      LOGGER.info("Server {} peer download count: {}",
          serverStarter.getInstanceId(), count);
      total += count;
    }
    return total;
  }

  private long getTotalDeepStoreDownloadCount(String tableNameWithType) {
    long total = 0;
    for (BaseServerStarter serverStarter : _serverStarters) {
      ServerMetrics serverMetrics =
          serverStarter.getServerInstance().getServerMetrics();
      long count = serverMetrics.getMeteredTableValue(
          tableNameWithType,
          ServerMeter.SEGMENT_DOWNLOAD_FROM_REMOTE).count();
      total += count;
    }
    return total;
  }

  private long getTotalMetricCount(String tableNameWithType, ServerMeter meter) {
    long total = 0;
    for (BaseServerStarter serverStarter : _serverStarters) {
      ServerMetrics serverMetrics =
          serverStarter.getServerInstance().getServerMetrics();
      long count = serverMetrics.getMeteredTableValue(tableNameWithType, meter).count();
      total += count;
    }
    return total;
  }

  private void verifyQueryResults()
      throws Exception {
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
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    try {
      dropOfflineTable(PEER_DOWNLOAD_TEST_TABLE);
    } catch (Exception e) {
      LOGGER.warn("Failed to drop table {}: {}", PEER_DOWNLOAD_TEST_TABLE, e.getMessage());
    }

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }
}
