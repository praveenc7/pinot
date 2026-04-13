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
package org.apache.pinot.segment.local.segment.creator.inv;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitmapInvertedIndexWriter;
import org.apache.pinot.segment.local.segment.index.readers.BitmapInvertedIndexReader;
import org.apache.pinot.segment.spi.index.InvertedIndexConfig;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests bitmap inverted index behavior at scale.
 *
 * <p>The enabled tests exercise V1 and V0 format correctness at moderate scale (1000 bitmaps,
 * ~50 MB) to validate offset arithmetic and round-trip reads in CI.
 *
 * <p>The disabled tests verify behavior at the 4 GB boundary — VERSION_1 (64-bit offsets) handles
 * files &gt; 4 GB, while VERSION_0 (32-bit offsets) rejects them. These use
 * {@link BitmapInvertedIndexWriter#add(RoaringBitmap)} directly because the high-level creators
 * cannot produce &gt; 4 GB files ({@code numDocs} is an int, ~2.1 billion max).
 *
 * <p><b>Disabled tests</b> each write a ~4.5 GB temp file and are disabled to avoid CI disk pressure. Run manually:
 * <pre>
 *   mvn test -pl pinot-segment-local \
 *       -Dtest="BitmapInvertedIndexLargeFileTest#testVersion1HandlesFileOver4GB+testVersion0OverflowsAt4GB" \
 *       -DfailIfNoTests=false
 * </pre>
 */
public class BitmapInvertedIndexLargeFileTest implements PinotBuffersAfterMethodCheckRule {
  private static final Logger LOGGER = LoggerFactory.getLogger(BitmapInvertedIndexLargeFileTest.class);
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), BitmapInvertedIndexLargeFileTest.class.getSimpleName());
  private static final long FOUR_GB = 4_294_967_296L; // 2^32

  @BeforeMethod
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(TEMP_DIR);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  /**
   * CI-friendly variant: writes 1000 bitmaps (~50 MB) in V1 format and reads back bitmaps at the
   * beginning, middle, and end. Exercises the same offset arithmetic and reader auto-detection
   * logic as the disabled > 4 GB test, at a scale that is safe for CI.
   */
  @Test
  public void testVersion1RoundTripAtModerateScale()
      throws IOException {
    RoaringBitmap template = createTemplateBitmap();
    int numBitmaps = 1000;

    File indexFile = new File(TEMP_DIR, "moderate_v1.inv");
    try (FileChannel channel = new RandomAccessFile(indexFile, "rw").getChannel();
        BitmapInvertedIndexWriter writer = new BitmapInvertedIndexWriter(
            channel, numBitmaps, true, InvertedIndexConfig.VERSION_1)) {
      for (int i = 0; i < numBitmaps; i++) {
        writer.add(template);
      }
    }

    Assert.assertTrue(indexFile.length() > 0, "File should not be empty");

    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile)) {
      Assert.assertEquals(dataBuffer.getInt(0), BitmapInvertedIndexWriter.MAGIC_NUMBER,
          "File should start with VERSION_1 magic number");

      BitmapInvertedIndexReader reader = new BitmapInvertedIndexReader(dataBuffer, numBitmaps);
      int[] expectedArr = template.toArray();
      for (int idx : new int[]{0, numBitmaps / 2, numBitmaps - 1}) {
        ImmutableRoaringBitmap readBack = reader.getDocIds(idx);
        Assert.assertEquals(readBack.toArray(), expectedArr, "Bitmap mismatch at index " + idx);
      }
    }
  }

  /**
   * CI-friendly variant: writes 1000 bitmaps (~50 MB) in V0 format and reads back bitmaps at the
   * beginning, middle, and end. Verifies that V0 works correctly for files well within 4 GB.
   */
  @Test
  public void testVersion0RoundTripAtModerateScale()
      throws IOException {
    RoaringBitmap template = createTemplateBitmap();
    int numBitmaps = 1000;

    File indexFile = new File(TEMP_DIR, "moderate_v0.inv");
    try (FileChannel channel = new RandomAccessFile(indexFile, "rw").getChannel();
        BitmapInvertedIndexWriter writer = new BitmapInvertedIndexWriter(
            channel, numBitmaps, true, InvertedIndexConfig.VERSION_0)) {
      for (int i = 0; i < numBitmaps; i++) {
        writer.add(template);
      }
    }

    Assert.assertTrue(indexFile.length() > 0, "File should not be empty");

    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile)) {
      // V0 files should NOT have the magic number
      Assert.assertNotEquals(dataBuffer.getInt(0), BitmapInvertedIndexWriter.MAGIC_NUMBER,
          "V0 file should not start with VERSION_1 magic number");

      BitmapInvertedIndexReader reader = new BitmapInvertedIndexReader(dataBuffer, numBitmaps);
      int[] expectedArr = template.toArray();
      for (int idx : new int[]{0, numBitmaps / 2, numBitmaps - 1}) {
        ImmutableRoaringBitmap readBack = reader.getDocIds(idx);
        Assert.assertEquals(readBack.toArray(), expectedArr, "Bitmap mismatch at index " + idx);
      }
    }
  }

  // -------------------------------------------------------------------------
  // Disabled: require ~10 GB free disk (run manually)
  // -------------------------------------------------------------------------

  /**
   * Writes a VERSION_1 file larger than 4 GB using {@link BitmapInvertedIndexWriter#add(RoaringBitmap)}
   * (the same method the creators use in seal()), then reads back bitmaps at the beginning,
   * middle, and end via {@link BitmapInvertedIndexReader} to verify that 64-bit offsets work
   * correctly across the 4 GB boundary.
   */
  @Test(enabled = false)
  public void testVersion1HandlesFileOver4GB()
      throws IOException {
    RoaringBitmap template = createTemplateBitmap();
    int templateSize = template.serializedSizeInBytes();
    int numBitmaps = (int) ((FOUR_GB + 500_000_000L) / templateSize) + 1;

    LOGGER.info("Writing VERSION_1 file with {} bitmaps (~{} bytes each), target > 4 GB",
        numBitmaps, templateSize);

    File indexFile = new File(TEMP_DIR, "large_v1.inv");
    try (FileChannel channel = new RandomAccessFile(indexFile, "rw").getChannel();
        BitmapInvertedIndexWriter writer = new BitmapInvertedIndexWriter(
            channel, numBitmaps, true, InvertedIndexConfig.VERSION_1)) {
      for (int i = 0; i < numBitmaps; i++) {
        writer.add(template);
      }
    }

    long fileSize = indexFile.length();
    LOGGER.info("VERSION_1 file written: {} bytes ({} GB)", fileSize,
        String.format("%.2f", fileSize / (1024.0 * 1024 * 1024)));
    Assert.assertTrue(fileSize > FOUR_GB,
        "File size " + fileSize + " should exceed 4 GB (" + FOUR_GB + ")");

    // Read back using the production reader (BitmapInvertedIndexReader).
    // Verify first, middle, and last bitmaps to exercise offsets both below and above 4 GB.
    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile)) {
      BitmapInvertedIndexReader reader = new BitmapInvertedIndexReader(dataBuffer, numBitmaps);

      Assert.assertEquals(dataBuffer.getInt(0), BitmapInvertedIndexWriter.MAGIC_NUMBER,
          "File should start with VERSION_1 magic number");
      Assert.assertEquals(dataBuffer.getInt(Integer.BYTES), InvertedIndexConfig.VERSION_1);

      int[] expectedArr = template.toArray();
      for (int idx : new int[]{0, numBitmaps / 2, numBitmaps - 1}) {
        ImmutableRoaringBitmap readBack = reader.getDocIds(idx);
        Assert.assertEquals(readBack.toArray(), expectedArr, "Bitmap mismatch at index " + idx);
      }
    }
  }

  /**
   * Verifies that VERSION_0 (32-bit offsets) correctly detects and rejects data that would
   * exceed the 4 GB offset limit, rather than silently producing a corrupt file.
   */
  @Test(enabled = false)
  public void testVersion0OverflowsAt4GB()
      throws IOException {
    RoaringBitmap template = createTemplateBitmap();
    int templateSize = template.serializedSizeInBytes();
    int numBitmaps = (int) ((FOUR_GB + 500_000_000L) / templateSize) + 1;

    LOGGER.info("Writing VERSION_0 file with {} bitmaps (~{} bytes each), expecting overflow",
        numBitmaps, templateSize);

    File indexFile = new File(TEMP_DIR, "large_v0.inv");
    boolean overflowed = false;

    // Don't use try-with-resources for the writer: its close() also calls putOffset()
    // which would throw again after the overflow. The FileChannel is still closed by
    // the outer try-with-resources.
    try (FileChannel channel = new RandomAccessFile(indexFile, "rw").getChannel()) {
      BitmapInvertedIndexWriter writer = new BitmapInvertedIndexWriter(
          channel, numBitmaps, false, InvertedIndexConfig.VERSION_0);
      try {
        for (int i = 0; i < numBitmaps; i++) {
          writer.add(template);
        }
        Assert.fail("VERSION_0 should overflow with > 4 GB data");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("overflowed 4GB"),
            "Expected '4GB overflow' message but got: " + e.getMessage());
        overflowed = true;
        LOGGER.info("VERSION_0 correctly rejected > 4 GB data: {}", e.getMessage());
      }
    }

    Assert.assertTrue(overflowed, "VERSION_0 should have thrown overflow exception");
  }

  /**
   * Creates a RoaringBitmap with 6 bitmap containers (~49 KB serialized).
   * Uses alternating values (every other int) within each container range to force
   * bitmap containers (>4096 values → 8 KB fixed size, vs. smaller array or run containers).
   */
  private static RoaringBitmap createTemplateBitmap() {
    RoaringBitmap bm = new RoaringBitmap();
    for (int c = 0; c < 6; c++) {
      int base = c * 65536;
      // >4096 values per container forces bitmap container format (8 KB each)
      for (int v = 0; v < 65536; v += 2) {
        bm.add(base + v);
      }
    }
    return bm;
  }
}
