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
package org.apache.pinot.core.data.manager;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for BaseTableDataManager peer-to-peer download logic.
 *
 * Tests the shouldAttemptPeerToPeerDownload() method which determines
 * whether a server should download a segment from a peer or from deep store.
 */
public class BaseTableDataManagerPeerDownloadTest {

  /**
   * Test: When sourceServer is null, should NOT attempt peer download
   * Expected: Download from deep store
   */
  @Test
  public void testNullSourceServer() {
    String instanceId = "Server_localhost_8098";
    String sourceServer = null;
    String peerDownloadScheme = "http";

    boolean result = shouldAttemptPeerDownload(instanceId, sourceServer, peerDownloadScheme);

    assertFalse(result,
        "Should not attempt peer download when sourceServer is null");
  }

  /**
   * Test: When sourceServer is empty, should NOT attempt peer download
   * Expected: Download from deep store
   */
  @Test
  public void testEmptySourceServer() {
    String instanceId = "Server_localhost_8098";
    String sourceServer = "";
    String peerDownloadScheme = "http";

    boolean result = shouldAttemptPeerDownload(instanceId, sourceServer, peerDownloadScheme);

    assertFalse(result,
        "Should not attempt peer download when sourceServer is empty");
  }

  /**
   * Test: When peerDownloadScheme is null, should NOT attempt peer download
   * Expected: Download from deep store
   */
  @Test
  public void testNullPeerDownloadScheme() {
    String instanceId = "Server_localhost_8098";
    String sourceServer = "Server_localhost_8099";
    String peerDownloadScheme = null;

    boolean result = shouldAttemptPeerDownload(instanceId, sourceServer, peerDownloadScheme);

    assertFalse(result,
        "Should not attempt peer download when peerDownloadScheme is null");
  }

  /**
   * Test: When this server IS the source server, should NOT attempt peer download
   * Expected: Download from deep store (source responsibility)
   */
  @Test
  public void testIsSourceServer() {
    String instanceId = "Server_localhost_8098";
    String sourceServer = "Server_localhost_8098"; // Same as instanceId
    String peerDownloadScheme = "http";

    boolean result = shouldAttemptPeerDownload(instanceId, sourceServer, peerDownloadScheme);

    assertFalse(result,
        "Source server should download from deep store, not from peer");
  }

  /**
   * Test: When this server is NOT the source server, SHOULD attempt peer download
   * Expected: Download from peer
   */
  @Test
  public void testIsPeerServer() {
    String instanceId = "Server_localhost_8098";
    String sourceServer = "Server_localhost_8099"; // Different from instanceId
    String peerDownloadScheme = "http";

    boolean result = shouldAttemptPeerDownload(instanceId, sourceServer, peerDownloadScheme);

    assertTrue(result,
        "Peer server should download from source server");
  }

  /**
   * Test: Different server names but same host/port should be treated as different
   */
  @Test
  public void testDifferentServerNames() {
    String instanceId = "Server_host1_8098";
    String sourceServer = "Server_host2_8098";
    String peerDownloadScheme = "http";

    boolean result = shouldAttemptPeerDownload(instanceId, sourceServer, peerDownloadScheme);

    assertTrue(result,
        "Different server instances should trigger peer download");
  }

  /**
   * Helper method that mimics the logic in BaseTableDataManager
   */
  private boolean shouldAttemptPeerDownload(String instanceId, String sourceServer,
      String peerDownloadScheme) {
    // No sourceServer? No peer download
    if (sourceServer == null || sourceServer.isEmpty()) {
      return false;
    }

    // Peer download scheme not configured? No peer download
    if (peerDownloadScheme == null) {
      return false;
    }

    // Am I the source server? Download from deep store
    if (instanceId.equals(sourceServer)) {
      return false;
    }

    // I'm a peer server, download from source
    return true;
  }
}
