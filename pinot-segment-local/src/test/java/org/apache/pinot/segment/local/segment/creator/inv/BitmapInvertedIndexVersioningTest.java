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
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitmapInvertedIndexWriter;
import org.apache.pinot.segment.local.segment.index.readers.BitmapInvertedIndexReader;
import org.apache.pinot.segment.spi.index.InvertedIndexConfig;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Low-level file format tests for the bitmap inverted index: binary layout verification,
 * reader auto-detection between VERSION_0 and VERSION_1, and magic number collision safety.
 *
 * <p>Round-trip and edge-case tests live in {@link InvertedIndexVersionConfigTest}.
 */
public class BitmapInvertedIndexVersioningTest implements PinotBuffersAfterMethodCheckRule {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), BitmapInvertedIndexVersioningTest.class.getSimpleName());
  private static final String INDEX_FILE_NAME = "test.bitmap.inv";
  private static final Random RANDOM = new Random(42);

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

  // -------------------------------------------------------------------------
  // File layout assertions for VERSION_1
  // -------------------------------------------------------------------------

  @Test
  public void testVersion1FileLayout()
      throws IOException {
    File indexFile = new File(TEMP_DIR, INDEX_FILE_NAME);
    int numBitmaps = 2;
    RoaringBitmap bm0 = RoaringBitmap.bitmapOf(1, 3, 5);
    RoaringBitmap bm1 = RoaringBitmap.bitmapOf(2, 4, 6);

    try (FileChannel channel = new RandomAccessFile(indexFile, "rw").getChannel();
        BitmapInvertedIndexWriter writer =
            new BitmapInvertedIndexWriter(channel, numBitmaps, true, InvertedIndexConfig.VERSION_1)) {
      writer.add(bm0);
      writer.add(bm1);
    }

    // VERSION_1 layout:
    // [0..3]   MAGIC_NUMBER
    // [4..7]   VERSION_1 = 1
    // [8..15]  offset_0 (long) = absolute position of bm0 data
    // [16..23] offset_1 (long) = absolute position of bm1 data
    // [24..31] offset_2 (long) = end of file
    // [32..]   bm0 data, bm1 data
    int expectedHeaderAndOffsets = BitmapInvertedIndexWriter.HEADER_SIZE_V1 + (numBitmaps + 1) * Long.BYTES;
    // = 8 + 3*8 = 32 bytes before first bitmap

    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile)) {
      Assert.assertEquals(dataBuffer.getInt(0), BitmapInvertedIndexWriter.MAGIC_NUMBER);
      Assert.assertEquals(dataBuffer.getInt(Integer.BYTES), InvertedIndexConfig.VERSION_1);

      long offset0 = dataBuffer.getLong(BitmapInvertedIndexWriter.HEADER_SIZE_V1);
      Assert.assertEquals(offset0, expectedHeaderAndOffsets,
          "First bitmap should start immediately after header + offset array");

      long offset1 = dataBuffer.getLong(BitmapInvertedIndexWriter.HEADER_SIZE_V1 + Long.BYTES);
      long offset2 = dataBuffer.getLong(BitmapInvertedIndexWriter.HEADER_SIZE_V1 + 2L * Long.BYTES);

      Assert.assertEquals(offset1 - offset0, bm0.serializedSizeInBytes(),
          "Offset difference should match bm0 serialized size");
      Assert.assertEquals(offset2 - offset1, bm1.serializedSizeInBytes(),
          "Offset difference should match bm1 serialized size");
      Assert.assertEquals(offset2, indexFile.length(), "Last offset should equal file length");
    }
  }

  @Test
  public void testVersion1OffsetsStoredAsLongs()
      throws IOException {
    // Verify that VERSION_1 stores offsets as 64-bit longs, not 32-bit ints.
    File indexFile = new File(TEMP_DIR, INDEX_FILE_NAME);
    int numBitmaps = 1;
    RoaringBitmap bitmap = RoaringBitmap.bitmapOf(42);

    try (FileChannel channel = new RandomAccessFile(indexFile, "rw").getChannel();
        BitmapInvertedIndexWriter writer =
            new BitmapInvertedIndexWriter(channel, numBitmaps, true, InvertedIndexConfig.VERSION_1)) {
      writer.add(bitmap);
    }

    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile)) {
      // Read the first offset as a long — it must not be sign-extended or truncated
      long offset0 = dataBuffer.getLong(BitmapInvertedIndexWriter.HEADER_SIZE_V1);
      int expectedStart = BitmapInvertedIndexWriter.HEADER_SIZE_V1 + (numBitmaps + 1) * Long.BYTES;
      Assert.assertEquals(offset0, expectedStart);

      // The offset width is 8 bytes (long), not 4 bytes (int)
      long endOffset = dataBuffer.getLong(BitmapInvertedIndexWriter.HEADER_SIZE_V1 + Long.BYTES);
      Assert.assertEquals(endOffset - offset0, bitmap.serializedSizeInBytes(),
          "Offset difference should match bitmap serialized size");
      Assert.assertEquals(endOffset, indexFile.length(), "End offset should equal file length");
    }
  }

  // -------------------------------------------------------------------------
  // VERSION_0 file layout
  // -------------------------------------------------------------------------

  @Test
  public void testVersion0FileLayout()
      throws IOException {
    File indexFile = new File(TEMP_DIR, INDEX_FILE_NAME);
    int numBitmaps = 2;
    RoaringBitmap bm0 = RoaringBitmap.bitmapOf(1, 3, 5);
    RoaringBitmap bm1 = RoaringBitmap.bitmapOf(2, 4, 6);

    try (FileChannel channel = new RandomAccessFile(indexFile, "rw").getChannel();
        BitmapInvertedIndexWriter writer =
            new BitmapInvertedIndexWriter(channel, numBitmaps, true, InvertedIndexConfig.VERSION_0)) {
      writer.add(bm0);
      writer.add(bm1);
    }

    // VERSION_0 layout (no header):
    // [0..3]   offset_0 (int) = start of bm0 data
    // [4..7]   offset_1 (int) = start of bm1 data
    // [8..11]  offset_2 (int) = end of file
    // [12..]   bm0 data, bm1 data
    int expectedOffsetTableSize = (numBitmaps + 1) * Integer.BYTES; // = 12 bytes

    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile)) {
      int offset0 = dataBuffer.getInt(0);
      Assert.assertEquals(offset0, expectedOffsetTableSize,
          "First bitmap should start immediately after offset table");

      int offset1 = dataBuffer.getInt(Integer.BYTES);
      int offset2 = dataBuffer.getInt(2 * Integer.BYTES);

      Assert.assertEquals(offset1 - offset0, bm0.serializedSizeInBytes(),
          "Offset difference should match bm0 serialized size");
      Assert.assertEquals(offset2 - offset1, bm1.serializedSizeInBytes(),
          "Offset difference should match bm1 serialized size");
      Assert.assertEquals(offset2, (int) indexFile.length(),
          "Last offset should equal file length");
    }
  }

  // -------------------------------------------------------------------------
  // Reader auto-detection
  // -------------------------------------------------------------------------

  @Test
  public void testReaderAutoDetectsVersionFromFile()
      throws IOException {
    int numBitmaps = 3;
    RoaringBitmap[] bitmaps = createRandomBitmaps(numBitmaps, 500);

    // Write a V0 file
    File v0File = new File(TEMP_DIR, "v0.inv");
    try (FileChannel channel = new RandomAccessFile(v0File, "rw").getChannel();
        BitmapInvertedIndexWriter writer =
            new BitmapInvertedIndexWriter(channel, numBitmaps, true, InvertedIndexConfig.VERSION_0)) {
      for (RoaringBitmap bm : bitmaps) {
        writer.add(bm);
      }
    }

    // Write a V1 file with the same bitmaps
    File v1File = new File(TEMP_DIR, "v1.inv");
    try (FileChannel channel = new RandomAccessFile(v1File, "rw").getChannel();
        BitmapInvertedIndexWriter writer =
            new BitmapInvertedIndexWriter(channel, numBitmaps, true, InvertedIndexConfig.VERSION_1)) {
      for (RoaringBitmap bm : bitmaps) {
        writer.add(bm);
      }
    }

    // The same reader constructor handles both — it auto-detects from file content
    try (PinotDataBuffer v0Buf = PinotDataBuffer.mapReadOnlyBigEndianFile(v0File);
        BitmapInvertedIndexReader v0Reader = new BitmapInvertedIndexReader(v0Buf, numBitmaps);
        PinotDataBuffer v1Buf = PinotDataBuffer.mapReadOnlyBigEndianFile(v1File);
        BitmapInvertedIndexReader v1Reader = new BitmapInvertedIndexReader(v1Buf, numBitmaps)) {
      for (int i = 0; i < numBitmaps; i++) {
        int[] v0Arr = v0Reader.getDocIds(i).toArray();
        int[] v1Arr = v1Reader.getDocIds(i).toArray();
        int[] expected = bitmaps[i].toArray();
        Assert.assertEquals(v0Arr, expected, "V0 bitmap mismatch at index " + i);
        Assert.assertEquals(v1Arr, expected, "V1 bitmap mismatch at index " + i);
      }
    }
  }

  // -------------------------------------------------------------------------
  // Magic number collision safety
  // -------------------------------------------------------------------------

  /**
   * Validates that the V0 first offset (which is the first 4 bytes of the file) can never
   * equal the V1 MAGIC_NUMBER, ensuring unambiguous format detection by the reader.
   *
   * <p>In V0, the first offset = (numBitmaps + 1) * 4, which is always a multiple of 4.
   * MAGIC_NUMBER (0x494E5601) is odd, so they can never collide. This holds because all
   * production writers create files from position 0 (verified by code audit of all 6 call sites).
   */
  @Test
  public void testV0FirstOffsetCanNeverEqualMagicNumber()
      throws IOException {
    // MAGIC_NUMBER must be odd (not a multiple of 4)
    Assert.assertNotEquals(BitmapInvertedIndexWriter.MAGIC_NUMBER % 4, 0,
        "MAGIC_NUMBER must not be a multiple of 4 for V0/V1 disambiguation to work");

    // Verify for a range of numBitmaps: the first 4 bytes are always (numBitmaps+1)*4
    for (int numBitmaps : new int[]{1, 2, 3, 10, 100, 1000}) {
      File indexFile = new File(TEMP_DIR, "magic_test_" + numBitmaps + ".inv");
      RoaringBitmap bm = RoaringBitmap.bitmapOf(0);

      try (FileChannel channel = new RandomAccessFile(indexFile, "rw").getChannel();
          BitmapInvertedIndexWriter writer =
              new BitmapInvertedIndexWriter(channel, numBitmaps, true, InvertedIndexConfig.VERSION_0)) {
        for (int i = 0; i < numBitmaps; i++) {
          writer.add(bm);
        }
      }

      try (PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile)) {
        int firstOffset = buf.getInt(0);
        int expectedOffset = (numBitmaps + 1) * Integer.BYTES;

        Assert.assertEquals(firstOffset, expectedOffset,
            "V0 first offset should equal (numBitmaps+1)*4 for numBitmaps=" + numBitmaps);
        Assert.assertEquals(firstOffset % 4, 0,
            "V0 first offset must be a multiple of 4 for numBitmaps=" + numBitmaps);
        Assert.assertNotEquals(firstOffset, BitmapInvertedIndexWriter.MAGIC_NUMBER,
            "V0 first offset must never equal MAGIC_NUMBER for numBitmaps=" + numBitmaps);
      }
    }
  }

  // -------------------------------------------------------------------------
  // Reader rejects unsupported versions in header
  // -------------------------------------------------------------------------

  /**
   * Verifies that the reader throws when it encounters a file with the magic number but an
   * unsupported version (e.g., 0 or 99). Only VERSION_1 is valid after the magic header.
   */
  @Test
  public void testReaderRejectsUnsupportedVersionInHeader()
      throws IOException {
    for (int badVersion : new int[]{0, 2, 99, -1}) {
      File indexFile = new File(TEMP_DIR, "bad_version_" + badVersion + ".inv");

      // Hand-craft a file with magic number + unsupported version + dummy offset data
      try (PinotDataBuffer buf = PinotDataBuffer.mapFile(indexFile, false, 0,
          BitmapInvertedIndexWriter.HEADER_SIZE_V1 + 2 * Long.BYTES, ByteOrder.BIG_ENDIAN,
          "test")) {
        buf.putInt(0, BitmapInvertedIndexWriter.MAGIC_NUMBER);
        buf.putInt(Integer.BYTES, badVersion);
      }

      try (PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile)) {
        Assert.assertThrows(IllegalStateException.class,
            () -> new BitmapInvertedIndexReader(buf, 1));
      }
    }
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private static RoaringBitmap[] createRandomBitmaps(int count, int maxDocId) {
    RoaringBitmap[] bitmaps = new RoaringBitmap[count];
    for (int i = 0; i < count; i++) {
      bitmaps[i] = new RoaringBitmap();
      int numDocs = RANDOM.nextInt(maxDocId / count) + 1;
      for (int j = 0; j < numDocs; j++) {
        bitmaps[i].add(RANDOM.nextInt(maxDocId));
      }
    }
    return bitmaps;
  }
}
