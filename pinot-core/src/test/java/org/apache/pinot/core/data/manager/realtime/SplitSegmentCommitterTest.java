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
package org.apache.pinot.core.data.manager.realtime;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;


public class SplitSegmentCommitterTest {

  private static final String SEGMENT_NAME = "testTable__0__0__20240101T0000Z";

  @Test
  public void testUploadSegmentReturnsLocationOnSuccess()
      throws Exception {
    SegmentUploader segmentUploader = Mockito.mock(SegmentUploader.class);
    URI expectedUri = new URI("hdfs:///segments/testSegment.tar.gz");
    when(segmentUploader.uploadSegment(any(File.class), any(LLCSegmentName.class))).thenReturn(expectedUri);

    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    params.withSegmentName(SEGMENT_NAME);

    SplitSegmentCommitter committer = new SplitSegmentCommitter(
        Mockito.mock(Logger.class),
        Mockito.mock(ServerSegmentCompletionProtocolHandler.class),
        params,
        segmentUploader);

    String result = committer.uploadSegment(new File("/tmp/test.tar.gz"), segmentUploader, params);
    Assert.assertEquals(result, expectedUri.toString());
  }

  @Test
  public void testUploadSegmentReturnsNullOnFailure()
      throws Exception {
    SegmentUploader segmentUploader = Mockito.mock(SegmentUploader.class);
    when(segmentUploader.uploadSegment(any(File.class), any(LLCSegmentName.class))).thenReturn(null);

    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    params.withSegmentName(SEGMENT_NAME);

    SplitSegmentCommitter committer = new SplitSegmentCommitter(
        Mockito.mock(Logger.class),
        Mockito.mock(ServerSegmentCompletionProtocolHandler.class),
        params,
        segmentUploader);

    String result = committer.uploadSegment(new File("/tmp/test.tar.gz"), segmentUploader, params);
    Assert.assertNull(result);
  }

  @Test
  public void testCommitFailsWhenUploadFails() {
    SegmentUploader segmentUploader = Mockito.mock(SegmentUploader.class);
    when(segmentUploader.uploadSegment(any(File.class), any(LLCSegmentName.class))).thenReturn(null);

    ServerSegmentCompletionProtocolHandler protocolHandler =
        Mockito.mock(ServerSegmentCompletionProtocolHandler.class);
    SegmentCompletionProtocol.Response commitStartResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params()
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE));
    when(protocolHandler.segmentCommitStart(any())).thenReturn(commitStartResponse);

    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    params.withSegmentName(SEGMENT_NAME);

    SplitSegmentCommitter committer = new SplitSegmentCommitter(
        Mockito.mock(Logger.class), protocolHandler, params, segmentUploader);

    RealtimeSegmentDataManager.SegmentBuildDescriptor descriptor =
        Mockito.mock(RealtimeSegmentDataManager.SegmentBuildDescriptor.class);
    when(descriptor.getSegmentTarFile()).thenReturn(new File("/tmp/test.tar.gz"));

    SegmentCompletionProtocol.Response response = committer.commit(descriptor);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.FAILED);
  }

  @Test
  public void testCommitSucceedsWhenUploadSucceeds()
      throws Exception {
    SegmentUploader segmentUploader = Mockito.mock(SegmentUploader.class);
    URI segmentUri = new URI("hdfs:///segments/testSegment.tar.gz");
    when(segmentUploader.uploadSegment(any(File.class), any(LLCSegmentName.class))).thenReturn(segmentUri);

    ServerSegmentCompletionProtocolHandler protocolHandler =
        Mockito.mock(ServerSegmentCompletionProtocolHandler.class);
    SegmentCompletionProtocol.Response commitStartResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params()
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE));
    when(protocolHandler.segmentCommitStart(any())).thenReturn(commitStartResponse);

    SegmentCompletionProtocol.Response commitEndResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params()
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS));
    when(protocolHandler.segmentCommitEndWithMetadata(any(), any())).thenReturn(commitEndResponse);

    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    params.withSegmentName(SEGMENT_NAME);

    SplitSegmentCommitter committer = new SplitSegmentCommitter(
        Mockito.mock(Logger.class), protocolHandler, params, segmentUploader);

    RealtimeSegmentDataManager.SegmentBuildDescriptor descriptor =
        Mockito.mock(RealtimeSegmentDataManager.SegmentBuildDescriptor.class);
    when(descriptor.getSegmentTarFile()).thenReturn(new File("/tmp/test.tar.gz"));
    when(descriptor.getMetadataFiles()).thenReturn(new HashMap<>());

    SegmentCompletionProtocol.Response response = committer.commit(descriptor);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);
  }
}
