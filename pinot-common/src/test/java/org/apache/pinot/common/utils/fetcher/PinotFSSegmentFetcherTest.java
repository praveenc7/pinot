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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
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
import static org.testng.Assert.assertNotNull;
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

    File fakeSegmentRoot = new File(_tempDir, "testSegment");

    try (MockedStatic<PinotFSFactory> fsFactoryMock = Mockito.mockStatic(PinotFSFactory.class);
        MockedStatic<TarCompressionUtils> tarMock = Mockito.mockStatic(TarCompressionUtils.class)) {

      fsFactoryMock.when(() -> PinotFSFactory.create("mock")).thenReturn(mockFs);
      when(mockFs.open(_mockUri)).thenReturn(fakeStream);

      tarMock.when(() -> TarCompressionUtils.untarWithRateLimiter(
          any(InputStream.class),
          eq(_tempDir),
          anyLong()
      )).thenReturn(java.util.List.of(fakeSegmentRoot));

      File result = _fetcher.fetchUntarSegmentToLocalStreamed(_mockUri, _tempDir, 0L, _attempts);

      assertEquals(result, fakeSegmentRoot);
      assertTrue(_attempts.get() >= 0);

      tarMock.verify(() -> TarCompressionUtils.untarWithRateLimiter(
          any(InputStream.class), eq(_tempDir), anyLong()), times(1));
    }
  }

  @Test
  public void testFetchUntarSegmentToLocalStreamedRetryAndSucceed() throws Exception {
    PinotFS mockFs = mock(PinotFS.class);
    InputStream fakeStream = new ByteArrayInputStream("fake-data".getBytes(StandardCharsets.UTF_8));

    File fakeSegmentRoot = new File(_tempDir, "testSegment");

    try (MockedStatic<PinotFSFactory> fsFactoryMock = Mockito.mockStatic(PinotFSFactory.class);
        MockedStatic<TarCompressionUtils> tarMock = Mockito.mockStatic(TarCompressionUtils.class)) {

      fsFactoryMock.when(() -> PinotFSFactory.create("mock")).thenReturn(mockFs);
      when(mockFs.open(_mockUri))
          .thenThrow(new IOException("first fail"))
          .thenReturn(fakeStream);

      tarMock.when(() -> TarCompressionUtils.untarWithRateLimiter(
          any(InputStream.class),
          eq(_tempDir),
          anyLong()
      )).thenReturn(java.util.List.of(fakeSegmentRoot));

      File result = _fetcher.fetchUntarSegmentToLocalStreamed(_mockUri, _tempDir, 0L, _attempts);

      assertEquals(result, fakeSegmentRoot);
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

  /**
   * Verifies that segments packaged in the production tar layout
   * (segmentName/v3/...) are correctly normalized by the fetcher so that
   * the final files exist under destDir/v3/ after untar.
   */
  @Test
  public void testFetchUntarSegmentToLocalStreamedWithNestedLayout() throws Exception {
    String segmentName = "testSegment_0_0_0";

    File tempDir = java.nio.file.Files.createTempDirectory("pinot_test_").toFile();
    AtomicInteger attempts = new AtomicInteger();

    try {
      // Create segment structure
      File v3Dir = new File(tempDir, segmentName + "/v3");
      assertTrue(v3Dir.mkdirs());

      File metadata = new File(v3Dir, "metadata.properties");
      File creation = new File(v3Dir, "creation.meta");
      FileUtils.writeStringToFile(metadata, "foo=bar", "UTF-8");
      FileUtils.writeStringToFile(creation, "created", "UTF-8");

      // Create tar.gz
      File tarFile = new File(tempDir, segmentName + ".tar.gz");
      try (FileOutputStream fos = new FileOutputStream(tarFile);
          GZIPOutputStream gzos = new GZIPOutputStream(fos);
          TarArchiveOutputStream tar = new TarArchiveOutputStream(gzos)) {
        addFileToTar(tar, new File(tempDir, segmentName), segmentName);
      }

      // Destination directory
      File destDir = new File(tempDir, "untar-dest-" + System.currentTimeMillis());
      assertTrue(destDir.mkdirs());

      // Run fetcher
      PinotFSSegmentFetcher fetcher = new PinotFSSegmentFetcher();
      fetcher.init(new PinotConfiguration());

      File resultDir = fetcher.fetchUntarSegmentToLocalStreamed(tarFile.toURI(), destDir, -1, attempts);

      // Assertions
      assertNotNull(resultDir);
      assertEquals(resultDir.getName(), segmentName);

      File normalizedV3 = new File(resultDir, "v3");
      assertTrue(normalizedV3.exists(), "v3 directory should exist under segment root");

      assertTrue(new File(normalizedV3, "metadata.properties").exists());
      assertTrue(new File(normalizedV3, "creation.meta").exists());
    } finally {
      if (tempDir.exists()) {
        FileUtils.deleteDirectory(tempDir);
      }
    }
  }

  private void addFileToTar(TarArchiveOutputStream tar, File file, String entryName) throws Exception {
    TarArchiveEntry entry = new TarArchiveEntry(file, entryName);
    tar.putArchiveEntry(entry);

    if (file.isFile()) {
      tar.write(FileUtils.readFileToByteArray(file));
      tar.closeArchiveEntry();
    } else {
      tar.closeArchiveEntry();
      File[] children = file.listFiles();
      if (children != null) {
        for (File child : children) {
          addFileToTar(tar, child, entryName + "/" + child.getName());
        }
      }
    }
  }
}
