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
package org.apache.pinot.segment.local.segment.index.creator.inv;

import com.google.common.collect.Collections2;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitmapInvertedIndexWriter;
import org.apache.pinot.segment.local.segment.index.readers.BitmapInvertedIndexReader;
import org.apache.pinot.segment.spi.index.InvertedIndexConfig;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;


public class BitmapInvertedIndexWriterTest implements PinotBuffersAfterMethodCheckRule {

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BitmapInvertedIndexWriterTest");

  private RoaringBitmap[] _bitmaps;

  @BeforeClass
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);
    RoaringBitmap huge = huge();
    RoaringBitmap small = small();
    RoaringBitmap empty = empty();
    _bitmaps = new RoaringBitmap[] { huge, small, empty };
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.forceDelete(INDEX_DIR);
    _bitmaps = null;
  }

  private File _file;

  @BeforeTest
  public void before() {
    _file = new File(INDEX_DIR, UUID.randomUUID().toString());
  }

  @AfterTest
  public void after() {
    FileUtils.deleteQuietly(_file);
  }

  @DataProvider(name = "bitmaps")
  public Object[][] bitmaps() {
    // don't return bitmaps directly because TestNG will create huge test names
    Collection<List<Integer>> permutations = Collections2.permutations(
        IntStream.range(0, _bitmaps.length + 1).boxed().collect(Collectors.toList())
    );
    int[] versions = {InvertedIndexConfig.VERSION_0, InvertedIndexConfig.VERSION_1};
    List<Object[]> testCases = new ArrayList<>();
    for (int version : versions) {
      for (List<Integer> permutation : permutations) {
        int[] ints = new int[permutation.size()];
        int j = 0;
        for (Integer boxed : permutation) {
          ints[j++] = boxed % _bitmaps.length;
        }
        testCases.add(new Object[]{ints, version});
      }
    }
    return testCases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "bitmaps", testName = "test write bitmaps with permutation = ")
  public void testWriteBitmaps(int[] indices, int version)
      throws IOException {
    // indirection because TestNG will create huge test names otherwise
    RoaringBitmap[] bitmaps = new RoaringBitmap[indices.length];
    int i = 0;
    for (int index : indices) {
      bitmaps[i++] = _bitmaps[index];
    }
    try (FileChannel channel = new RandomAccessFile(_file, "rw").getChannel();
        BitmapInvertedIndexWriter writer =
            new BitmapInvertedIndexWriter(channel, bitmaps.length, true, version)) {
      for (RoaringBitmap bitmap : bitmaps) {
        writer.add(bitmap);
      }
    }
    verifyReadable(bitmaps, version);
  }

  @DataProvider(name = "versions")
  public Object[][] versions() {
    return new Object[][]{
        {InvertedIndexConfig.VERSION_0},
        {InvertedIndexConfig.VERSION_1}
    };
  }

  @Test(dataProvider = "versions")
  public void testWriteByteArrayBitmaps(int version)
      throws IOException {
    RoaringBitmap[] bitmaps = new RoaringBitmap[]{small(), huge(), empty()};
    try (FileChannel channel = new RandomAccessFile(_file, "rw").getChannel();
        BitmapInvertedIndexWriter writer =
            new BitmapInvertedIndexWriter(channel, bitmaps.length, true, version)) {
      for (RoaringBitmap bitmap : bitmaps) {
        byte[] bytes = new byte[bitmap.serializedSizeInBytes()];
        bitmap.serialize(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN));
        writer.add(bytes);
      }
    }
    verifyReadable(bitmaps, version);
  }

  private void verifyReadable(RoaringBitmap[] bitmaps, int version)
      throws IOException {
    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(_file);
         BitmapInvertedIndexReader reader = new BitmapInvertedIndexReader(buffer, bitmaps.length)) {
      // Verify the correct version was written
      int firstInt = buffer.getInt(0);
      if (version == InvertedIndexConfig.VERSION_1) {
        assertEquals(firstInt, BitmapInvertedIndexWriter.MAGIC_NUMBER,
            "VERSION_1 file should start with magic number");
        assertEquals(buffer.getInt(Integer.BYTES), InvertedIndexConfig.VERSION_1,
            "VERSION_1 file should have version 1 in header");
      } else {
        assertNotEquals(firstInt, BitmapInvertedIndexWriter.MAGIC_NUMBER,
            "VERSION_0 file should not start with magic number");
      }
      // Verify bitmap data is readable and contents match
      int dictId = 0;
      for (RoaringBitmap bitmap : bitmaps) {
        ImmutableRoaringBitmap persisted = reader.getDocIds(dictId);
        assertEquals(persisted.getCardinality(), bitmap.getCardinality(),
            "Bitmap cardinality mismatch at dictId " + dictId);
        // For bitmaps small enough to materialize, verify exact content.
        // The huge() bitmap has ~1B entries so toArray() would OOM.
        if (bitmap.getCardinality() < 1_000_000) {
          assertEquals(persisted.toArray(), bitmap.toArray(),
              "Bitmap content mismatch at dictId " + dictId);
        }
        dictId++;
      }
    }
  }

  private static RoaringBitmap empty() {
    return new RoaringBitmap();
  }

  private static RoaringBitmap small() {
    return RoaringBitmap.bitmapOf(1, 10, 100, 1000, 10000, 100000, 1000000, 10000000);
  }

  private static RoaringBitmap huge() {
    RoaringBitmapWriter<RoaringBitmap> bitmap = RoaringBitmapWriter.writer().constantMemory().get();
    for (int i = 0; i < Integer.MAX_VALUE - 1; i += 2) {
      bitmap.add(i);
    }
    return bitmap.get();
  }
}
