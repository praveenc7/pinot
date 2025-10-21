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
package org.apache.pinot.common.utils.fetcher;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class PinotFSSegmentFetcherTest {

  private PinotFSSegmentFetcher _fetcher;
  private File _tempDir;
  private URI _mockUri;
  private AtomicInteger _attempts;

  @BeforeMethod
  public void setUp() throws Exception {
    _fetcher = new PinotFSSegmentFetcher() {{
      _retryCount = 3;
      _retryWaitMs = 10;
      _retryDelayScaleFactor = 1.0;
    }};

    _tempDir = new File(System.getProperty("java.io.tmpdir"), "pinot_test_" + System.nanoTime());
    assertTrue(_tempDir.mkdirs());
    _mockUri = new URI("mock://localhost/segment.tar.gz");
    _attempts = new AtomicInteger();
  }

  @AfterMethod
  public void cleanUp() {
    if (_tempDir.exists()) {
      _tempDir.delete();
    }
  }

  @Test
  public void testFetchUntarSegmentToLocalStreamedSuccess() throws Exception {
    PinotFS mockFs = mock(PinotFS.class);
    InputStream fakeStream = new ByteArrayInputStream("fake-data".getBytes(StandardCharsets.UTF_8));

    try (MockedStatic<PinotFSFactory> fsFactoryMock = Mockito.mockStatic(PinotFSFactory.class);
        MockedStatic<TarCompressionUtils> tarMock = Mockito.mockStatic(TarCompressionUtils.class)) {

      fsFactoryMock.when(() -> PinotFSFactory.create("mock")).thenReturn(mockFs);
      when(mockFs.open(_mockUri)).thenReturn(fakeStream);

      // ✅ Correctly mock the method your code actually calls
      tarMock.when(() -> TarCompressionUtils.untarWithRateLimiter(
          any(InputStream.class),
          eq(_tempDir),
          anyLong()
      )).thenAnswer(i -> null);

      File result = _fetcher.fetchUntarSegmentToLocalStreamed(_mockUri, _tempDir, 0L, _attempts);

      assertEquals(result, _tempDir);
      assertTrue(_attempts.get() >= 0);
      tarMock.verify(() -> TarCompressionUtils.untarWithRateLimiter(
          any(InputStream.class), eq(_tempDir), anyLong()), times(1));
    }
  }

  @Test
  public void testFetchUntarSegmentToLocalStreamedRetryAndSucceed() throws Exception {
    PinotFS mockFs = mock(PinotFS.class);
    InputStream fakeStream = new ByteArrayInputStream("fake-data".getBytes(StandardCharsets.UTF_8));

    try (MockedStatic<PinotFSFactory> fsFactoryMock = Mockito.mockStatic(PinotFSFactory.class);
        MockedStatic<TarCompressionUtils> tarMock = Mockito.mockStatic(TarCompressionUtils.class)) {

      fsFactoryMock.when(() -> PinotFSFactory.create("mock")).thenReturn(mockFs);
      when(mockFs.open(_mockUri))
          .thenThrow(new java.io.IOException("first fail"))
          .thenReturn(fakeStream);

      tarMock.when(() -> TarCompressionUtils.untarWithRateLimiter(
          any(InputStream.class),
          eq(_tempDir),
          anyLong()
      )).thenAnswer(i -> null);

      File result = _fetcher.fetchUntarSegmentToLocalStreamed(_mockUri, _tempDir, 0L, _attempts);

      assertEquals(result, _tempDir);
      assertTrue(_attempts.get() >= 1);
      verify(mockFs, atLeast(2)).open(_mockUri);
    }
  }

  @Test
  public void testFetchUntarSegmentToLocalStreamedAllRetriesFail() throws Exception {
    PinotFS mockFs = mock(PinotFS.class);

    try (MockedStatic<PinotFSFactory> fsFactoryMock = Mockito.mockStatic(PinotFSFactory.class)) {
      fsFactoryMock.when(() -> PinotFSFactory.create("mock")).thenReturn(mockFs);
      when(mockFs.open(_mockUri)).thenThrow(new java.io.IOException("always fail"));

      assertThrows(AttemptsExceededException.class, () ->
          _fetcher.fetchUntarSegmentToLocalStreamed(_mockUri, _tempDir, 0L, _attempts));
    }
  }
}
