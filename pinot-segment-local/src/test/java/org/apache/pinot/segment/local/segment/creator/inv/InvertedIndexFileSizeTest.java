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
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.segment.creator.impl.inv.OffHeapBitmapInvertedIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.BitmapInvertedIndexReader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.InvertedIndexConfig;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Reference test (disabled by default) that creates actual inverted index files using
 * {@link OffHeapBitmapInvertedIndexCreator} at varying cardinalities, writes both V0 and V1,
 * and compares file sizes and read correctness side by side.
 *
 * <p>Disabled because it requires ~10 GB of free disk space and takes 3+ minutes.
 * Run manually to validate V0 vs V1 behavior at scale:
 * <pre>
 *   mvn test -pl pinot-segment-local \
 *       -Dtest="InvertedIndexFileSizeTest#compareV0AndV1At500MDocs" \
 *       -DfailIfNoTests=false
 * </pre>
 *
 * <p>Note: To observe V0 overflow corruption (rather than the overflow guard throwing),
 * temporarily comment out the {@code Preconditions.checkArgument} in
 * {@link org.apache.pinot.segment.local.segment.creator.impl.inv.BitmapInvertedIndexWriter#asUnsignedInt}.
 *
 * <p>Sample output (500M docs, V0 overflow guard disabled):
 * <pre>
 * Card.    V0 Total        V1 Total        V0 Read      V1 Read
 * 2        125,132,028     125,132,048     2 OK         2 OK
 * 10,000   1,610,520,004   1,610,560,016   10,000 OK    10,000 OK
 * 50,000   4,052,406,852   4,052,606,864   50,000 OK    50,000 OK
 * 100,000  5,001,200,004   5,001,600,016   14,122 FAIL  100,000 OK
 *   V0 error: dictId 85,878: Offset is -380272
 * </pre>
 */
public class InvertedIndexFileSizeTest implements PinotBuffersAfterMethodCheckRule {

  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), InvertedIndexFileSizeTest.class.getSimpleName());
  private static final String COLUMN = "col";
  private static final long FOUR_GB = 4_294_967_296L;

  private static final int[] CARDINALITIES =
      {2, 5, 10, 16, 20, 50, 100, 500, 1_000, 5_000, 10_000, 50_000, 100_000};

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

  @Test(enabled = false)
  public void compareV0AndV1At500MDocs()
      throws IOException {
    int numDocs = 500_000_000;

    System.out.println();
    System.out.printf("=== V0 vs V1 comparison (numDocs = %,d) ===%n", numDocs);
    System.out.printf("%-12s  %-14s  %-10s  %-18s  %-18s  %-30s  %-30s  %-14s  %-14s%n",
        "Cardinality", "Docs/DictId", "Offsets",
        "V0 Offset Tbl", "V1 Offset Tbl",
        "V0 Total (bytes)", "V1 Total (bytes)",
        "V0 Read", "V1 Read");
    System.out.println("-".repeat(175));

    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN, FieldSpec.DataType.INT, true);

    for (int card : CARDINALITIES) {
      // Write V0
      File v0Dir = new File(TEMP_DIR, "v0_card_" + card);
      FileUtils.forceMkdir(v0Dir);
      writeIndex(v0Dir, fieldSpec, card, numDocs, InvertedIndexConfig.VERSION_0);
      File v0File = indexFile(v0Dir);
      long v0Size = v0File.length();

      // Write V1
      File v1Dir = new File(TEMP_DIR, "v1_card_" + card);
      FileUtils.forceMkdir(v1Dir);
      writeIndex(v1Dir, fieldSpec, card, numDocs, InvertedIndexConfig.VERSION_1);
      File v1File = indexFile(v1Dir);
      long v1Size = v1File.length();

      // Read V0
      ReadResult v0Read = readAllBitmaps(v0File, card, numDocs);

      // Read V1
      ReadResult v1Read = readAllBitmaps(v1File, card, numDocs);

      // Offset table sizes: V0 = (card+1)*4, V1 = 8 + (card+1)*8
      int numOffsets = card + 1;
      long v0OffsetTbl = (long) numOffsets * Integer.BYTES;
      long v1OffsetTbl = 8L + (long) numOffsets * Long.BYTES;

      String v0ReadStr = v0Read._failures == 0
          ? String.format("%,d OK", v0Read._successes)
          : String.format("%,d FAIL", v0Read._failures);
      String v1ReadStr = v1Read._failures == 0
          ? String.format("%,d OK", v1Read._successes)
          : String.format("%,d FAIL", v1Read._failures);

      System.out.printf("%-12s  %-14s  %-10s  %-18s  %-18s  %-30s  %-30s  %-14s  %-14s%n",
          String.format("%,d", card),
          String.format("%,d", numDocs / card),
          String.format("%,d", numOffsets),
          String.format("%,d B", v0OffsetTbl),
          String.format("%,d B", v1OffsetTbl),
          String.format("%,d", v0Size),
          String.format("%,d", v1Size),
          v0ReadStr,
          v1ReadStr);

      if (v0Read._firstError != null) {
        System.out.printf("  V0 first error: %s%n", v0Read._firstError);
      }
      if (v1Read._firstError != null) {
        System.out.printf("  V1 first error: %s%n", v1Read._firstError);
      }

      FileUtils.deleteDirectory(v0Dir);
      FileUtils.deleteDirectory(v1Dir);
    }
  }

  private void writeIndex(File dir, FieldSpec fieldSpec, int card, int numDocs, int version)
      throws IOException {
    try (OffHeapBitmapInvertedIndexCreator creator =
        new OffHeapBitmapInvertedIndexCreator(dir, fieldSpec, card, numDocs, numDocs,
            version)) {
      for (int doc = 0; doc < numDocs; doc++) {
        creator.add(doc % card);
      }
      creator.seal();
    }
  }

  private ReadResult readAllBitmaps(File indexFile, int card, int numDocs) {
    int successes = 0;
    int failures = 0;
    String firstError = null;

    try (PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        BitmapInvertedIndexReader reader = new BitmapInvertedIndexReader(buf, card)) {
      int expectedPerDictId = numDocs / card;
      for (int dictId = 0; dictId < card; dictId++) {
        try {
          int cardinality = reader.getDocIds(dictId).getCardinality();
          if (Math.abs(cardinality - expectedPerDictId) > 1) {
            failures++;
            if (firstError == null) {
              firstError = String.format(
                  "dictId %,d: expected ~%,d docs, got %,d",
                  dictId, expectedPerDictId, cardinality);
            }
          } else {
            successes++;
          }
        } catch (Exception e) {
          failures++;
          if (firstError == null) {
            firstError = String.format("dictId %,d: %s: %s",
                dictId, e.getClass().getSimpleName(), e.getMessage());
          }
        }
      }
    } catch (Exception e) {
      failures = card;
      firstError = String.format("Failed to open: %s: %s",
          e.getClass().getSimpleName(), e.getMessage());
    }

    return new ReadResult(successes, failures, firstError);
  }

  private File indexFile(File dir) {
    return new File(dir,
        COLUMN + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
  }

  private static class ReadResult {
    final int _successes;
    final int _failures;
    final String _firstError;

    ReadResult(int successes, int failures, String firstError) {
      _successes = successes;
      _failures = failures;
      _firstError = firstError;
    }
  }
}
