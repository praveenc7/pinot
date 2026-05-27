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
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.HttpStatus;
import org.apache.pinot.common.restlet.resources.StartReplaceSegmentsRequest;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * End-to-end integration coverage for the delayed-consistent-push (STAGED lineage) protocol.
 *
 * <p>Each test drives the REST endpoints directly so the protocol layer is exercised independently of the push-job
 * tooling (which lives in a separate module and supplies its own staging logic). The flow per test:
 *
 * <ol>
 *   <li>Set up a real cluster (ZK + controller + broker + server).</li>
 *   <li>Upload segments with v1 data through the normal upload API; record the count(*) seen by the broker.</li>
 *   <li>Call {@code startReplaceSegments} with explicit segmentsFrom/segmentsTo, upload segments with v2 data, then
 *       call {@code endReplaceSegments?stageOnComplete=true} to leave the entry in STAGED.</li>
 *   <li>Assert the lineage entry is STAGED and the broker still serves the v1 count.</li>
 *   <li>Call {@code completeStagedLineage} (or {@code revertStagedLineage}) and assert the served data flips
 *       accordingly.</li>
 * </ol>
 */
public class StagedConsistentPushIntegrationTest extends BaseClusterIntegrationTest {
  private static String _tableNameSuffix;

  @Override
  protected Map<String, String> getStreamConfigs() {
    return null;
  }

  @Override
  protected String getSortedColumn() {
    return null;
  }

  @Override
  protected List<String> getInvertedIndexColumns() {
    return null;
  }

  @Override
  protected List<String> getNoDictionaryColumns() {
    return null;
  }

  @Override
  protected List<String> getRangeIndexColumns() {
    return null;
  }

  @Override
  protected List<String> getBloomFilterColumns() {
    return null;
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME + _tableNameSuffix;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
    startBroker();
    startServer();
  }

  @BeforeMethod
  public void setUpTest()
      throws IOException {
    _tableNameSuffix = RandomStringUtils.randomAlphabetic(12);
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
  }

  @AfterMethod
  public void tearDownTest() {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    try {
      dropOfflineTable(offlineTableName);
    } catch (Exception ignored) {
      // Some tests may run before the table is created; ignore.
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopServer();
    stopBroker();
    stopController();
    stopZk();
  }

  /**
   * Happy path: stage a replacement, observe queries still return the v1 doc count, then complete the staged lineage
   * and observe queries flip to the v2 doc count.
   */
  @Test
  public void testStagedCompleteFlipsServedData()
      throws Exception {
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    waitForEVToDisappear(tableConfig.getTableName());
    addTableConfig(tableConfig);

    List<File> avroFiles = getAllAvroFiles();

    // ---- v1: upload one segment, capture baseline count ----
    List<String> v1Segments = buildAndUploadSegments(tableConfig, schema, avroFiles.subList(0, 1), "_v1");
    long v1Count = sumDocs(v1Segments);
    waitForCountStar(v1Count);

    // ---- v2: build (do NOT upload yet) two segments with a different total doc count ----
    clearTarDir();
    List<String> v2Segments = buildSegments(tableConfig, schema, avroFiles.subList(1, 3), "_v2");
    long v2Count = sumDocsFromTarDir();
    assertTrue(v2Count != v1Count, "v1 and v2 should have different total doc counts so we can tell them apart "
        + "(v1=" + v1Count + ", v2=" + v2Count + ")");

    // ---- start the replace-segments, upload v2, end with stageOnComplete=true ----
    String lineageEntryId = startReplaceSegments(v1Segments, v2Segments);
    uploadTarredSegments();
    endReplaceSegments(lineageEntryId, true);

    assertLineageEntryState(lineageEntryId, "STAGED");
    // While STAGED, broker keeps routing to v1 — count must stay at the v1 baseline.
    waitForCountStar(v1Count);

    // ---- complete the staged campaign, expect routing to flip to v2 ----
    completeStagedLineage();
    assertLineageEntryState(lineageEntryId, "COMPLETED");
    waitForCountStar(v2Count);
  }

  /**
   * Operator aborts a staged campaign via {@code revertStagedLineage}. The entry flips to REVERTED, segmentsTo are
   * cleaned out of the ideal state, and queries continue to serve v1.
   */
  @Test
  public void testRevertStagedAbortsCampaign()
      throws Exception {
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    waitForEVToDisappear(tableConfig.getTableName());
    addTableConfig(tableConfig);

    List<File> avroFiles = getAllAvroFiles();

    List<String> v1Segments = buildAndUploadSegments(tableConfig, schema, avroFiles.subList(0, 1), "_v1");
    long v1Count = sumDocs(v1Segments);
    waitForCountStar(v1Count);

    clearTarDir();
    List<String> v2Segments = buildSegments(tableConfig, schema, avroFiles.subList(1, 3), "_v2");

    String lineageEntryId = startReplaceSegments(v1Segments, v2Segments);
    uploadTarredSegments();
    endReplaceSegments(lineageEntryId, true);
    assertLineageEntryState(lineageEntryId, "STAGED");

    revertStagedLineage(null);
    assertLineageEntryState(lineageEntryId, "REVERTED");

    // After revert, v2 segments must be removed from the ideal state and queries still serve v1.
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode segmentsList = getSegmentsList();
        for (JsonNode seg : segmentsList) {
          if (v2Segments.contains(seg.asText())) {
            return false;
          }
        }
        return true;
      } catch (Exception e) {
        return false;
      }
    }, 60_000L, "v2 segments should have been deleted after revertStagedLineage");
    waitForCountStar(v1Count);
  }

  /**
   * Multi-day backfill campaign with concurrent daily ingest.
   *
   * <p>Scenario: the table already has 3 days of v1 data. An operator launches a backfill campaign that pushes new
   * (v2) versions for each of those 3 days, staging each one. While the campaign is in flight a brand-new day 4
   * arrives via the normal upload path (not staged). The campaign is then committed via {@code completeStagedLineage}.
   *
   * <p>Invariants checked:
   * <ul>
   *   <li>While all 3 days are staged, the broker still serves the v1 totals.</li>
   *   <li>Day 4 ingested during the campaign is immediately visible — it is not held hostage by the staged campaign
   *       because it never went through the lineage protocol.</li>
   *   <li>{@code completeStagedLineage} with no entry-id list flips every STAGED entry on the table to COMPLETED in a
   *       single ZK write; the served totals jump from {@code v1Total + day4} to {@code v2Total + day4} atomically.
   *       Day 4 remains visible across the flip.</li>
   * </ul>
   */
  @Test
  public void testMultiDayBackfillCampaignWithConcurrentDailyIngest()
      throws Exception {
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    waitForEVToDisappear(tableConfig.getTableName());
    addTableConfig(tableConfig);

    List<File> avroFiles = getAllAvroFiles();
    int days = 3;
    assertTrue(avroFiles.size() >= 2 * days + 1,
        "Test requires at least " + (2 * days + 1) + " avro files (got " + avroFiles.size() + ")");

    // ---- Initial state: 3 days of v1 data, one segment per day ----
    Map<Integer, List<String>> v1ByDay = new HashMap<>();
    for (int day = 0; day < days; day++) {
      clearTarDir();
      List<String> v1Names =
          buildSegments(tableConfig, schema, avroFiles.subList(day, day + 1), "_d" + day + "_v1");
      uploadTarredSegments();
      v1ByDay.put(day, v1Names);
    }
    long v1Total = 0;
    for (List<String> names : v1ByDay.values()) {
      v1Total += sumDocs(names);
    }
    waitForCountStar(v1Total);

    // ---- Backfill campaign: stage v2 for each day, one push job at a time ----
    List<String> stagedEntryIds = new ArrayList<>();
    Map<Integer, List<String>> v2ByDay = new HashMap<>();
    for (int day = 0; day < days; day++) {
      clearTarDir();
      List<String> v2Names = buildSegments(tableConfig, schema, avroFiles.subList(days + day, days + day + 1),
          "_d" + day + "_v2");
      String entryId = startReplaceSegments(v1ByDay.get(day), v2Names);
      uploadTarredSegments();
      endReplaceSegments(entryId, true);
      assertLineageEntryState(entryId, "STAGED");
      stagedEntryIds.add(entryId);
      v2ByDay.put(day, v2Names);
    }

    // All 3 days are now staged; routing must still serve v1 only.
    waitForCountStar(v1Total);

    // ---- Day 4 arrives via the normal upload path during the campaign (no lineage) ----
    clearTarDir();
    List<String> day4Names =
        buildSegments(tableConfig, schema, avroFiles.subList(2 * days, 2 * days + 1), "_d" + days);
    uploadTarredSegments();
    long day4Count = sumDocs(day4Names);
    // Day 4 must be visible immediately — it is not part of the staged campaign.
    waitForCountStar(v1Total + day4Count);

    // v2 segments are uploaded (so metadata is readable) even though they are not yet query-routable.
    long v2Total = 0;
    for (List<String> names : v2ByDay.values()) {
      v2Total += sumDocs(names);
    }
    assertTrue(v1Total != v2Total,
        "Test setup expects v1 and v2 totals to differ so the flip is observable (v1=" + v1Total + ", v2=" + v2Total
            + ")");

    // ---- Commit the campaign: all 3 staged entries flip to COMPLETED atomically ----
    completeStagedLineage();
    for (String entryId : stagedEntryIds) {
      assertLineageEntryState(entryId, "COMPLETED");
    }
    waitForCountStar(v2Total + day4Count);
  }

  // -------- helpers --------

  private List<String> buildAndUploadSegments(TableConfig tableConfig, Schema schema, List<File> avroFiles,
      String suffix)
      throws Exception {
    List<String> segmentNames = buildSegments(tableConfig, schema, avroFiles, suffix);
    uploadTarredSegments();
    return segmentNames;
  }

  /**
   * Upload every segment (.tar files) in {@code _tarDir}.
   * The framework's {@code uploadSegments} helper toggles between full push and metadata-only push 50/50 on system time
   * parity; metadata-only leaves the download URL pointing at our local {@code _tarDir}, which we then wipe between
   * builds. That race is invisible most of the time but is what was sending one segment per multi-day run to ERROR
   * state in the external view.
   */
  private void uploadTarredSegments()
      throws Exception {
    File[] tarFiles = _tarDir.listFiles();
    assertNotNull(tarFiles);
    URI uploadUri = URI.create(_controllerRequestURLBuilder.forSegmentUpload());
    try (FileUploadDownloadClient client = new FileUploadDownloadClient()) {
      for (File tarFile : tarFiles) {
        int status =
            client.uploadSegment(uploadUri, tarFile.getName(), tarFile, getTableName(), TableType.OFFLINE)
                .getStatusCode();
        assertEquals(status, HttpStatus.SC_OK, "Upload failed for " + tarFile.getName());
      }
    }
  }

  private List<String> buildSegments(TableConfig tableConfig, Schema schema, List<File> avroFiles, String suffix)
      throws Exception {
    for (int i = 0; i < avroFiles.size(); i++) {
      ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFiles.get(i), tableConfig, schema, suffix + "_" + i,
          _segmentDir, _tarDir);
    }
    return segmentNamesFromTarDir();
  }

  private List<String> segmentNamesFromTarDir() {
    File[] tarFiles = _tarDir.listFiles();
    assertNotNull(tarFiles);
    List<String> names = new ArrayList<>(tarFiles.length);
    for (File f : tarFiles) {
      String name = f.getName();
      int dot = name.indexOf('.');
      names.add(dot >= 0 ? name.substring(0, dot) : name);
    }
    Collections.sort(names);
    return names;
  }

  private void clearTarDir() {
    File[] tarFiles = _tarDir.listFiles();
    if (tarFiles != null) {
      for (File f : tarFiles) {
        FileUtils.deleteQuietly(f);
      }
    }
    File[] segFiles = _segmentDir.listFiles();
    if (segFiles != null) {
      for (File f : segFiles) {
        FileUtils.deleteQuietly(f);
      }
    }
  }

  private long sumDocs(List<String> segmentNames)
      throws IOException {
    long total = 0;
    for (String name : segmentNames) {
      total += getNumDocs(name);
    }
    return total;
  }

  private long sumDocsFromTarDir() {
    // Sum the row count of each segment by extracting and reading metadata.properties is overkill here; instead, the
    // caller uploads them and asserts via getNumDocs after upload. For pre-upload counting we re-run a lightweight
    // sum by inspecting the segment directories (already produced by buildSegments).
    long total = 0;
    File[] dirs = _segmentDir.listFiles();
    assertNotNull(dirs);
    for (File dir : dirs) {
      if (!dir.isDirectory()) {
        continue;
      }
      try {
        File metaProps = new File(dir, "v3/metadata.properties");
        if (!metaProps.exists()) {
          metaProps = new File(dir, "metadata.properties");
        }
        assertTrue(metaProps.exists(), "metadata.properties not found under " + dir.getAbsolutePath());
        for (String line : java.nio.file.Files.readAllLines(metaProps.toPath())) {
          if (line.startsWith("segment.total.docs")) {
            total += Long.parseLong(line.substring(line.indexOf('=') + 1).trim());
            break;
          }
        }
      } catch (IOException e) {
        fail("Failed to read segment metadata for " + dir.getName() + ": " + e.getMessage());
      }
    }
    return total;
  }

  private long getNumDocs(String segmentName)
      throws IOException {
    return JsonUtils.stringToJsonNode(
            sendGetRequest(_controllerRequestURLBuilder.forSegmentMetadata(getTableName(), segmentName)))
        .get("segment.total.docs").asLong();
  }

  private JsonNode getSegmentsList()
      throws IOException {
    return JsonUtils.stringToJsonNode(sendGetRequest(
            _controllerRequestURLBuilder.forSegmentListAPI(getTableName(), TableType.OFFLINE.toString())))
        .get(0).get("OFFLINE");
  }

  private String startReplaceSegments(List<String> segmentsFrom, List<String> segmentsTo)
      throws Exception {
    String url = ControllerRequestURLBuilder.baseUrl(getControllerBaseApiUrl())
        .forStartReplaceSegments(getTableName(), TableType.OFFLINE.toString(), false);
    String body = JsonUtils.objectToString(new StartReplaceSegmentsRequest(segmentsFrom, segmentsTo));
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", "application/json");
    String response = ControllerTest.sendPostRequest(url, body, headers);
    JsonNode json = JsonUtils.stringToJsonNode(response);
    String id = json.get("segmentLineageEntryId").asText();
    assertNotNull(id);
    return id;
  }

  private void endReplaceSegments(String lineageEntryId, boolean stageOnComplete)
      throws Exception {
    String url = ControllerRequestURLBuilder.baseUrl(getControllerBaseApiUrl())
        .forEndReplaceSegments(getTableName(), TableType.OFFLINE.toString(), lineageEntryId, stageOnComplete);
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", "application/json");
    ControllerTest.sendPostRequest(url, "{}", headers);
  }

  private void completeStagedLineage()
      throws Exception {
    String url = ControllerRequestURLBuilder.baseUrl(getControllerBaseApiUrl())
        .forCompleteStagedLineage(getTableName(), TableType.OFFLINE.toString());
    ControllerTest.sendPostRequest(url);
  }

  private void revertStagedLineage(List<String> entryIds)
      throws Exception {
    String url = ControllerRequestURLBuilder.baseUrl(getControllerBaseApiUrl())
        .forRevertStagedLineage(getTableName(), TableType.OFFLINE.toString());
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", "application/json");
    String body = entryIds == null ? "" : JsonUtils.objectToString(entryIds);
    ControllerTest.sendPostRequest(url, body, headers);
  }

  private void assertLineageEntryState(String lineageEntryId, String expectedState)
      throws IOException {
    String response = ControllerTest.sendGetRequest(ControllerRequestURLBuilder.baseUrl(getControllerBaseApiUrl())
        .forListAllSegmentLineages(getTableName(), TableType.OFFLINE.toString()));
    JsonNode lineage = JsonUtils.stringToJsonNode(response);
    JsonNode entry = lineage.get("lineageEntries").get(lineageEntryId);
    assertNotNull(entry, "Lineage entry " + lineageEntryId + " not found in response: " + response);
    assertEquals(entry.get("state").asText(), expectedState,
        "Unexpected state for entry " + lineageEntryId + ": " + response);
  }

  private void waitForCountStar(long expected) {
    TestUtils.waitForCondition(aVoid -> {
      try {
        return getCurrentCountStarResult() == expected;
      } catch (Exception e) {
        return false;
      }
    }, 100L, 60_000L, "Expected count(*) = " + expected, true);
  }
}
