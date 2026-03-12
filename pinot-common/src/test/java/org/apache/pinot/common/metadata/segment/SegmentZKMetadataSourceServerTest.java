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
package org.apache.pinot.common.metadata.segment;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/**
 * Unit tests for SegmentZKMetadata sourceServer functionality.
 */
public class SegmentZKMetadataSourceServerTest {

  @Test
  public void testSourceServerGetterSetter() {
    SegmentZKMetadata metadata = new SegmentZKMetadata("testSegment");

    // Initially should be null
    assertNull(metadata.getSourceServer(), "sourceServer should be null initially");

    // Set and verify
    String sourceServer = "Server_localhost_8098";
    metadata.setSourceServer(sourceServer);
    assertEquals(metadata.getSourceServer(), sourceServer,
        "sourceServer should match what was set");
  }

  @Test
  public void testSourceServerOverwrite() {
    SegmentZKMetadata metadata = new SegmentZKMetadata("testSegment");

    // Set initial value
    metadata.setSourceServer("Server_host1_8098");
    assertEquals(metadata.getSourceServer(), "Server_host1_8098");

    // Overwrite with new value
    metadata.setSourceServer("Server_host2_8098");
    assertEquals(metadata.getSourceServer(), "Server_host2_8098",
        "sourceServer should be updated to new value");
  }

  @Test
  public void testSourceServerNullValue() {
    SegmentZKMetadata metadata = new SegmentZKMetadata("testSegment");

    // Set to non-null
    metadata.setSourceServer("Server_localhost_8098");
    assertEquals(metadata.getSourceServer(), "Server_localhost_8098");

    // Set back to null
    metadata.setSourceServer(null);
    assertNull(metadata.getSourceServer(),
        "sourceServer should be null after setting to null");
  }

  @Test
  public void testSourceServerEmptyString() {
    SegmentZKMetadata metadata = new SegmentZKMetadata("testSegment");

    // Set to empty string
    metadata.setSourceServer("");
    assertEquals(metadata.getSourceServer(), "",
        "sourceServer should be empty string");
  }

  @Test
  public void testSourceServerWithOtherFields() {
    SegmentZKMetadata metadata = new SegmentZKMetadata("testSegment");

    // Set various fields
    metadata.setCrc(12345L);
    metadata.setCreationTime(System.currentTimeMillis());
    metadata.setDownloadUrl("hdfs://namenode/path/to/segment");
    metadata.setSourceServer("Server_localhost_8098");
    metadata.setSizeInBytes(1024000L);

    // Verify all fields are independent
    assertEquals(metadata.getCrc(), 12345L);
    assertEquals(metadata.getDownloadUrl(), "hdfs://namenode/path/to/segment");
    assertEquals(metadata.getSourceServer(), "Server_localhost_8098");
    assertEquals(metadata.getSizeInBytes(), 1024000L);
  }
}
