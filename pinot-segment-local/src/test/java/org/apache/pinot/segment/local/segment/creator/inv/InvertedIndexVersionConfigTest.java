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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitmapInvertedIndexWriter;
import org.apache.pinot.segment.local.segment.creator.impl.inv.OffHeapBitmapInvertedIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.OnHeapBitmapInvertedIndexCreator;
import org.apache.pinot.segment.local.segment.index.inverted.InvertedIndexType;
import org.apache.pinot.segment.local.segment.index.readers.BitmapInvertedIndexReader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.InvertedIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.DictionaryBasedInvertedIndexCreator;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests that the inverted index version flows correctly from TableConfig / InvertedIndexConfig
 * through the creators to the on-disk file format.
 */
public class InvertedIndexVersionConfigTest implements PinotBuffersAfterMethodCheckRule {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), InvertedIndexVersionConfigTest.class.getSimpleName());
  private static final String COLUMN = "testCol";
  private static final int CARDINALITY = 5;
  private static final int NUM_DOCS = 20;

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
  // InvertedIndexConfig version routing
  // -------------------------------------------------------------------------

  @Test
  public void testOnHeapCreatorDefaultsToCurrentVersion()
      throws IOException {
    File indexFile = indexFile();
    try (OnHeapBitmapInvertedIndexCreator creator =
        new OnHeapBitmapInvertedIndexCreator(TEMP_DIR, COLUMN, CARDINALITY)) {
      addDocs(creator);
      creator.seal();
    }
    // Default is VERSION_0 (legacy format, no magic header)
    assertNoMagic(indexFile);
  }

  @Test
  public void testOnHeapCreatorVersion0WritesLegacyFormat()
      throws IOException {
    File indexFile = indexFile();
    try (OnHeapBitmapInvertedIndexCreator creator =
        new OnHeapBitmapInvertedIndexCreator(TEMP_DIR, COLUMN, CARDINALITY,
            InvertedIndexConfig.VERSION_0)) {
      addDocs(creator);
      creator.seal();
    }
    assertNoMagic(indexFile);
  }

  @Test
  public void testOnHeapCreatorVersion1WritesVersionedFormat()
      throws IOException {
    File indexFile = indexFile();
    try (OnHeapBitmapInvertedIndexCreator creator =
        new OnHeapBitmapInvertedIndexCreator(TEMP_DIR, COLUMN, CARDINALITY,
            InvertedIndexConfig.VERSION_1)) {
      addDocs(creator);
      creator.seal();
    }
    assertVersion(indexFile, InvertedIndexConfig.VERSION_1);
  }

  @Test
  public void testOffHeapCreatorDefaultsToCurrentVersion()
      throws IOException {
    File indexFile = indexFile();
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN, FieldSpec.DataType.INT, true);
    try (OffHeapBitmapInvertedIndexCreator creator =
        new OffHeapBitmapInvertedIndexCreator(TEMP_DIR, fieldSpec, CARDINALITY, NUM_DOCS, 0)) {
      addDocs(creator);
      creator.seal();
    }
    // Default is VERSION_0 (legacy format, no magic header)
    assertNoMagic(indexFile);
  }

  @Test
  public void testOffHeapCreatorVersion0WritesLegacyFormat()
      throws IOException {
    File indexFile = indexFile();
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN, FieldSpec.DataType.INT, true);
    try (OffHeapBitmapInvertedIndexCreator creator =
        new OffHeapBitmapInvertedIndexCreator(TEMP_DIR, fieldSpec, CARDINALITY, NUM_DOCS, 0,
            InvertedIndexConfig.VERSION_0)) {
      addDocs(creator);
      creator.seal();
    }
    assertNoMagic(indexFile);
  }

  @Test
  public void testOffHeapCreatorVersion1WritesVersionedFormat()
      throws IOException {
    File indexFile = indexFile();
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN, FieldSpec.DataType.INT, true);
    try (OffHeapBitmapInvertedIndexCreator creator =
        new OffHeapBitmapInvertedIndexCreator(TEMP_DIR, fieldSpec, CARDINALITY, NUM_DOCS, 0,
            InvertedIndexConfig.VERSION_1)) {
      addDocs(creator);
      creator.seal();
    }
    assertVersion(indexFile, InvertedIndexConfig.VERSION_1);
  }

  // -------------------------------------------------------------------------
  // InvertedIndexType version routing from InvertedIndexConfig
  // -------------------------------------------------------------------------

  @Test
  public void testInvertedIndexTypeDeserializesVersionFromTableConfig()
      throws IOException {
    // When invertedIndexVersion is not set (defaults to DEFAULT_VERSION), the config should carry DEFAULT_VERSION.
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("t")
        .setInvertedIndexColumns(Collections.singletonList(COLUMN))
        .build();

    InvertedIndexType invertedIndexType = (InvertedIndexType) StandardIndexes.inverted();
    Map<String, InvertedIndexConfig> configMap =
        invertedIndexType.getConfig(tableConfig, null);

    Assert.assertTrue(configMap.containsKey(COLUMN));
    Assert.assertEquals(configMap.get(COLUMN).getVersion(), InvertedIndexConfig.DEFAULT_VERSION);
  }

  @Test
  public void testInvertedIndexTypeVersion1ViaTableConfig() {
    // Explicitly setting invertedIndexVersion=1 in IndexingConfig propagates to the InvertedIndexConfig.
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("t")
        .setInvertedIndexColumns(Collections.singletonList(COLUMN))
        .build();
    tableConfig.getIndexingConfig().setInvertedIndexVersion(InvertedIndexConfig.VERSION_1);

    InvertedIndexType invertedIndexType = (InvertedIndexType) StandardIndexes.inverted();
    Map<String, InvertedIndexConfig> configMap =
        invertedIndexType.getConfig(tableConfig, null);

    Assert.assertEquals(configMap.get(COLUMN).getVersion(), InvertedIndexConfig.VERSION_1);
  }

  @Test
  public void testInvertedIndexTypeVersion0ViaTableConfigWritesLegacyFormat()
      throws IOException {
    // Explicitly setting invertedIndexVersion=0 should produce a legacy (no-magic) file.
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("t")
        .setInvertedIndexColumns(Collections.singletonList(COLUMN))
        .build();
    tableConfig.getIndexingConfig().setInvertedIndexVersion(InvertedIndexConfig.VERSION_0);

    File indexFile = createIndexViaType(tableConfig);
    assertNoMagic(indexFile);
  }

  @Test
  public void testInvertedIndexTypeVersion1ViaTableConfigWritesVersionedFormat()
      throws IOException {
    // Explicitly setting invertedIndexVersion=1 should produce a versioned (magic+header) file.
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("t")
        .setInvertedIndexColumns(Collections.singletonList(COLUMN))
        .build();
    tableConfig.getIndexingConfig().setInvertedIndexVersion(InvertedIndexConfig.VERSION_1);

    File indexFile = createIndexViaType(tableConfig);
    assertVersion(indexFile, InvertedIndexConfig.VERSION_1);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidInvertedIndexVersionRejected() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("t")
        .setInvertedIndexColumns(Collections.singletonList(COLUMN))
        .build();
    tableConfig.getIndexingConfig().setInvertedIndexVersion(99);

    InvertedIndexType invertedIndexType = (InvertedIndexType) StandardIndexes.inverted();
    invertedIndexType.getConfig(tableConfig, null);
  }

  // -------------------------------------------------------------------------
  // IndexingConfig getter/setter
  // -------------------------------------------------------------------------

  @Test
  public void testIndexingConfigInvertedIndexVersionDefaultIsCurrentVersion() {
    IndexingConfig indexingConfig = new IndexingConfig();
    // Default should match current writer version
    Assert.assertEquals(indexingConfig.getInvertedIndexVersion(), InvertedIndexConfig.DEFAULT_VERSION);
  }

  @Test
  public void testIndexingConfigInvertedIndexVersionSetGet() {
    IndexingConfig indexingConfig = new IndexingConfig();
    indexingConfig.setInvertedIndexVersion(InvertedIndexConfig.VERSION_0);
    Assert.assertEquals(indexingConfig.getInvertedIndexVersion(), InvertedIndexConfig.VERSION_0);
    indexingConfig.setInvertedIndexVersion(InvertedIndexConfig.VERSION_1);
    Assert.assertEquals(indexingConfig.getInvertedIndexVersion(), InvertedIndexConfig.VERSION_1);
  }

  // -------------------------------------------------------------------------
  // End-to-end: files written with each version are readable
  // -------------------------------------------------------------------------

  @Test
  public void testVersion0FilesAreReadable()
      throws IOException {
    File indexFile = indexFile();
    int[] docIds = new int[NUM_DOCS];
    for (int i = 0; i < NUM_DOCS; i++) {
      docIds[i] = i % CARDINALITY;
    }

    try (OnHeapBitmapInvertedIndexCreator creator =
        new OnHeapBitmapInvertedIndexCreator(TEMP_DIR, COLUMN, CARDINALITY,
            InvertedIndexConfig.VERSION_0)) {
      for (int dictId : docIds) {
        creator.add(dictId);
      }
      creator.seal();
    }

    try (PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        BitmapInvertedIndexReader reader = new BitmapInvertedIndexReader(buf, CARDINALITY)) {
      for (int dictId = 0; dictId < CARDINALITY; dictId++) {
        ImmutableRoaringBitmap bitmap = reader.getDocIds(dictId);
        for (int docId = 0; docId < NUM_DOCS; docId++) {
          if (docIds[docId] == dictId) {
            Assert.assertTrue(bitmap.contains(docId), "docId " + docId + " should be in bitmap for dictId " + dictId);
          } else {
            Assert.assertFalse(bitmap.contains(docId),
                "docId " + docId + " should not be in bitmap for dictId " + dictId);
          }
        }
      }
    }
  }

  @Test
  public void testVersion1FilesAreReadable()
      throws IOException {
    File indexFile = indexFile();
    int[] docIds = new int[NUM_DOCS];
    for (int i = 0; i < NUM_DOCS; i++) {
      docIds[i] = i % CARDINALITY;
    }

    try (OnHeapBitmapInvertedIndexCreator creator =
        new OnHeapBitmapInvertedIndexCreator(TEMP_DIR, COLUMN, CARDINALITY,
            InvertedIndexConfig.VERSION_1)) {
      for (int dictId : docIds) {
        creator.add(dictId);
      }
      creator.seal();
    }

    try (PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        BitmapInvertedIndexReader reader = new BitmapInvertedIndexReader(buf, CARDINALITY)) {
      for (int dictId = 0; dictId < CARDINALITY; dictId++) {
        ImmutableRoaringBitmap bitmap = reader.getDocIds(dictId);
        for (int docId = 0; docId < NUM_DOCS; docId++) {
          if (docIds[docId] == dictId) {
            Assert.assertTrue(bitmap.contains(docId));
          } else {
            Assert.assertFalse(bitmap.contains(docId));
          }
        }
      }
    }
  }

  // -------------------------------------------------------------------------
  // OffHeap end-to-end readability
  // -------------------------------------------------------------------------

  @Test
  public void testOffHeapVersion0FilesAreReadable()
      throws IOException {
    verifyOffHeapSingleValueReadable(InvertedIndexConfig.VERSION_0);
  }

  @Test
  public void testOffHeapVersion1FilesAreReadable()
      throws IOException {
    verifyOffHeapSingleValueReadable(InvertedIndexConfig.VERSION_1);
  }

  private void verifyOffHeapSingleValueReadable(int version)
      throws IOException {
    File indexFile = indexFile();
    int[] docDictIds = new int[NUM_DOCS];
    for (int i = 0; i < NUM_DOCS; i++) {
      docDictIds[i] = i % CARDINALITY;
    }

    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN, FieldSpec.DataType.INT, true);
    try (OffHeapBitmapInvertedIndexCreator creator =
        new OffHeapBitmapInvertedIndexCreator(TEMP_DIR, fieldSpec, CARDINALITY, NUM_DOCS, 0,
            version)) {
      for (int dictId : docDictIds) {
        creator.add(dictId);
      }
      creator.seal();
    }

    try (PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        BitmapInvertedIndexReader reader = new BitmapInvertedIndexReader(buf, CARDINALITY)) {
      for (int dictId = 0; dictId < CARDINALITY; dictId++) {
        ImmutableRoaringBitmap bitmap = reader.getDocIds(dictId);
        for (int docId = 0; docId < NUM_DOCS; docId++) {
          if (docDictIds[docId] == dictId) {
            Assert.assertTrue(bitmap.contains(docId),
                "docId " + docId + " should be in bitmap for dictId " + dictId);
          } else {
            Assert.assertFalse(bitmap.contains(docId),
                "docId " + docId + " should NOT be in bitmap for dictId " + dictId);
          }
        }
      }
    }
  }

  // -------------------------------------------------------------------------
  // Multi-value column round-trip
  // -------------------------------------------------------------------------

  // 10 docs, cardinality 5, 2 dict IDs per doc = 20 total values
  private static final int[][] MV_DOC_DICT_IDS = {
      {0, 1}, {1, 2}, {2, 3}, {3, 4}, {0, 4},
      {0, 2}, {1, 3}, {2, 4}, {0, 3}, {1, 4}
  };
  private static final int MV_NUM_DOCS = 10;
  private static final int MV_NUM_VALUES = 20;
  private static final int MV_CARDINALITY = 5;
  // Expected inverted index (dictId -> set of docIds):
  //   0: {0, 4, 5, 8}   1: {0, 1, 6, 9}   2: {1, 2, 5, 7}
  //   3: {2, 3, 6, 8}   4: {3, 4, 7, 9}
  private static final int[][] EXPECTED_MV_BITMAPS = {
      {0, 4, 5, 8}, {0, 1, 6, 9}, {1, 2, 5, 7}, {2, 3, 6, 8}, {3, 4, 7, 9}
  };

  @Test
  public void testMultiValueVersion0RoundTrip()
      throws IOException {
    verifyMultiValueRoundTrip(InvertedIndexConfig.VERSION_0);
  }

  @Test
  public void testMultiValueVersion1RoundTrip()
      throws IOException {
    verifyMultiValueRoundTrip(InvertedIndexConfig.VERSION_1);
  }

  private void verifyMultiValueRoundTrip(int version)
      throws IOException {
    File indexFile = indexFile();
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN, FieldSpec.DataType.INT, false);
    try (OffHeapBitmapInvertedIndexCreator creator =
        new OffHeapBitmapInvertedIndexCreator(TEMP_DIR, fieldSpec, MV_CARDINALITY,
            MV_NUM_DOCS, MV_NUM_VALUES, version)) {
      for (int[] dictIds : MV_DOC_DICT_IDS) {
        creator.add(dictIds, dictIds.length);
      }
      creator.seal();
    }

    try (PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        BitmapInvertedIndexReader reader =
            new BitmapInvertedIndexReader(buf, MV_CARDINALITY)) {
      for (int dictId = 0; dictId < MV_CARDINALITY; dictId++) {
        ImmutableRoaringBitmap bitmap = reader.getDocIds(dictId);
        Assert.assertEquals(bitmap.toArray(), EXPECTED_MV_BITMAPS[dictId],
            "Multi-value bitmap mismatch for dictId " + dictId);
      }
    }
  }

  // -------------------------------------------------------------------------
  // Config upgrade scenario: existing V0 segments remain readable after config changes to V1
  // -------------------------------------------------------------------------

  /**
   * Simulates the scenario where:
   * 1. A segment was created with VERSION_0 (legacy, the current default)
   * 2. The table config is later updated to invertedIndexVersion=1
   * 3. Existing V0 segments must still be readable (reader auto-detects from file content)
   * 4. Only new segments should be written with VERSION_1
   */
  @Test
  public void testExistingV0SegmentReadableAfterConfigUpgradeToV1()
      throws IOException {
    // Step 1: Create a V0 index (simulating an existing segment)
    int[] docDictIds = new int[NUM_DOCS];
    for (int i = 0; i < NUM_DOCS; i++) {
      docDictIds[i] = i % CARDINALITY;
    }

    File v0Dir = new File(TEMP_DIR, "v0_segment");
    org.apache.commons.io.FileUtils.forceMkdir(v0Dir);
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN, FieldSpec.DataType.INT, true);
    try (OffHeapBitmapInvertedIndexCreator creator =
        new OffHeapBitmapInvertedIndexCreator(v0Dir, fieldSpec, CARDINALITY,
            NUM_DOCS, 0, InvertedIndexConfig.VERSION_0)) {
      for (int dictId : docDictIds) {
        creator.add(dictId);
      }
      creator.seal();
    }
    File v0IndexFile = new File(v0Dir,
        COLUMN + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
    assertNoMagic(v0IndexFile);

    // Step 2: Config is now set to VERSION_1 (simulating table config update)
    // The reader should still handle V0 files because it auto-detects from file content
    try (PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(v0IndexFile);
        BitmapInvertedIndexReader reader = new BitmapInvertedIndexReader(buf, CARDINALITY)) {
      for (int dictId = 0; dictId < CARDINALITY; dictId++) {
        ImmutableRoaringBitmap bitmap = reader.getDocIds(dictId);
        for (int docId = 0; docId < NUM_DOCS; docId++) {
          if (docDictIds[docId] == dictId) {
            Assert.assertTrue(bitmap.contains(docId),
                "docId " + docId + " should be in bitmap for dictId " + dictId);
          } else {
            Assert.assertFalse(bitmap.contains(docId),
                "docId " + docId + " should NOT be in bitmap for dictId " + dictId);
          }
        }
      }
    }

    // Step 3: Create a new segment with VERSION_1 (simulating new ingestion)
    File v1Dir = new File(TEMP_DIR, "v1_segment");
    org.apache.commons.io.FileUtils.forceMkdir(v1Dir);
    try (OffHeapBitmapInvertedIndexCreator creator =
        new OffHeapBitmapInvertedIndexCreator(v1Dir, fieldSpec, CARDINALITY,
            NUM_DOCS, 0, InvertedIndexConfig.VERSION_1)) {
      for (int dictId : docDictIds) {
        creator.add(dictId);
      }
      creator.seal();
    }
    File v1IndexFile = new File(v1Dir,
        COLUMN + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
    assertVersion(v1IndexFile, InvertedIndexConfig.VERSION_1);

    // Step 4: Both files coexist and are readable by the same reader
    try (PinotDataBuffer v0Buf = PinotDataBuffer.mapReadOnlyBigEndianFile(v0IndexFile);
        BitmapInvertedIndexReader v0Reader = new BitmapInvertedIndexReader(v0Buf, CARDINALITY);
        PinotDataBuffer v1Buf = PinotDataBuffer.mapReadOnlyBigEndianFile(v1IndexFile);
        BitmapInvertedIndexReader v1Reader = new BitmapInvertedIndexReader(v1Buf, CARDINALITY)) {
      for (int dictId = 0; dictId < CARDINALITY; dictId++) {
        int[] v0Arr = v0Reader.getDocIds(dictId).toArray();
        int[] v1Arr = v1Reader.getDocIds(dictId).toArray();
        Assert.assertEquals(v0Arr, v1Arr,
            "V0 and V1 bitmaps should contain the same data for dictId " + dictId);
      }
    }
  }

  // -------------------------------------------------------------------------
  // Per-column version via fieldConfigList.indexes block
  // -------------------------------------------------------------------------

  private static final String COLUMN_A = "colA";
  private static final String COLUMN_B = "colB";

  /**
   * Tests that two columns in the same table can have different inverted index versions
   * configured via the fieldConfigList.indexes block: colA uses VERSION_0, colB uses VERSION_1.
   * Both columns should produce the correct file format and be independently readable.
   */
  @Test
  public void testPerColumnVersionViaFieldConfigIndexesBlock()
      throws Exception {
    // Build TableConfig with per-column inverted index versions via indexes block
    JsonNode colAIndexes = JsonUtils.stringToJsonNode(
        "{\"inverted\": {\"version\": 0}}");
    JsonNode colBIndexes = JsonUtils.stringToJsonNode(
        "{\"inverted\": {\"version\": 1}}");
    List<FieldConfig> fieldConfigs = new ArrayList<>();
    fieldConfigs.add(new FieldConfig(COLUMN_A, null, null, null, null, null, colAIndexes, null, null));
    fieldConfigs.add(new FieldConfig(COLUMN_B, null, null, null, null, null, colBIndexes, null, null));

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("t")
        .setFieldConfigList(fieldConfigs)
        .build();

    // Deserialize configs
    InvertedIndexType invertedIndexType = (InvertedIndexType) StandardIndexes.inverted();
    Map<String, InvertedIndexConfig> configMap = invertedIndexType.getConfig(tableConfig, null);

    Assert.assertTrue(configMap.containsKey(COLUMN_A));
    Assert.assertTrue(configMap.containsKey(COLUMN_B));
    Assert.assertEquals(configMap.get(COLUMN_A).getVersion(), InvertedIndexConfig.VERSION_0);
    Assert.assertEquals(configMap.get(COLUMN_B).getVersion(), InvertedIndexConfig.VERSION_1);

    // Create indexes for both columns using the deserialized configs
    int[] docDictIds = new int[NUM_DOCS];
    for (int i = 0; i < NUM_DOCS; i++) {
      docDictIds[i] = i % CARDINALITY;
    }

    File colADir = new File(TEMP_DIR, "colA");
    FileUtils.forceMkdir(colADir);
    File colBDir = new File(TEMP_DIR, "colB");
    FileUtils.forceMkdir(colBDir);

    // Write colA with VERSION_0
    FieldSpec fieldSpecA = new DimensionFieldSpec(COLUMN_A, FieldSpec.DataType.INT, true);
    IndexCreationContext.Common ctxA = IndexCreationContext.builder()
        .withIndexDir(colADir).withFieldSpec(fieldSpecA)
        .withCardinality(CARDINALITY).withTotalDocs(NUM_DOCS)
        .withTotalNumberOfEntries(NUM_DOCS).build();
    try (DictionaryBasedInvertedIndexCreator creator =
        invertedIndexType.createIndexCreator(ctxA, configMap.get(COLUMN_A))) {
      for (int dictId : docDictIds) {
        creator.add(dictId);
      }
      creator.seal();
    }

    // Write colB with VERSION_1
    FieldSpec fieldSpecB = new DimensionFieldSpec(COLUMN_B, FieldSpec.DataType.INT, true);
    IndexCreationContext.Common ctxB = IndexCreationContext.builder()
        .withIndexDir(colBDir).withFieldSpec(fieldSpecB)
        .withCardinality(CARDINALITY).withTotalDocs(NUM_DOCS)
        .withTotalNumberOfEntries(NUM_DOCS).build();
    try (DictionaryBasedInvertedIndexCreator creator =
        invertedIndexType.createIndexCreator(ctxB, configMap.get(COLUMN_B))) {
      for (int dictId : docDictIds) {
        creator.add(dictId);
      }
      creator.seal();
    }

    // Verify file formats
    File colAFile = new File(colADir,
        COLUMN_A + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
    File colBFile = new File(colBDir,
        COLUMN_B + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
    assertNoMagic(colAFile);
    assertVersion(colBFile, InvertedIndexConfig.VERSION_1);

    // Read both and verify identical data
    try (PinotDataBuffer bufA = PinotDataBuffer.mapReadOnlyBigEndianFile(colAFile);
        BitmapInvertedIndexReader readerA = new BitmapInvertedIndexReader(bufA, CARDINALITY);
        PinotDataBuffer bufB = PinotDataBuffer.mapReadOnlyBigEndianFile(colBFile);
        BitmapInvertedIndexReader readerB = new BitmapInvertedIndexReader(bufB, CARDINALITY)) {
      for (int dictId = 0; dictId < CARDINALITY; dictId++) {
        int[] arrA = readerA.getDocIds(dictId).toArray();
        int[] arrB = readerB.getDocIds(dictId).toArray();
        Assert.assertEquals(arrA, arrB,
            "colA (V0) and colB (V1) should have identical bitmaps for dictId " + dictId);
      }
    }
  }

  // -------------------------------------------------------------------------
  // Edge cases: empty bitmaps, single bitmap, byte array path
  // -------------------------------------------------------------------------

  @Test
  public void testVersion1SingleBitmap()
      throws IOException {
    File indexFile = new File(TEMP_DIR, INDEX_FILE_NAME);
    RoaringBitmap bitmap = RoaringBitmap.bitmapOf(0, 1, 2, 99);

    try (FileChannel channel = new RandomAccessFile(indexFile, "rw").getChannel();
        BitmapInvertedIndexWriter writer =
            new BitmapInvertedIndexWriter(channel, 1, true, InvertedIndexConfig.VERSION_1)) {
      writer.add(bitmap);
    }

    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        BitmapInvertedIndexReader reader = new BitmapInvertedIndexReader(dataBuffer, 1)) {
      Assert.assertEquals(reader.getDocIds(0).toArray(), bitmap.toArray());
    }
  }

  @Test
  public void testVersion1EmptyBitmaps()
      throws IOException {
    File indexFile = new File(TEMP_DIR, INDEX_FILE_NAME);
    int numBitmaps = 3;

    try (FileChannel channel = new RandomAccessFile(indexFile, "rw").getChannel();
        BitmapInvertedIndexWriter writer =
            new BitmapInvertedIndexWriter(channel, numBitmaps, true, InvertedIndexConfig.VERSION_1)) {
      for (int i = 0; i < numBitmaps; i++) {
        writer.add(new RoaringBitmap());
      }
    }

    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        BitmapInvertedIndexReader reader = new BitmapInvertedIndexReader(dataBuffer, numBitmaps)) {
      for (int i = 0; i < numBitmaps; i++) {
        Assert.assertEquals(reader.getDocIds(i).getCardinality(), 0);
      }
    }
  }

  @Test
  public void testVersion0EmptyBitmaps()
      throws IOException {
    File indexFile = new File(TEMP_DIR, INDEX_FILE_NAME);
    int numBitmaps = 3;

    try (FileChannel channel = new RandomAccessFile(indexFile, "rw").getChannel();
        BitmapInvertedIndexWriter writer =
            new BitmapInvertedIndexWriter(channel, numBitmaps, true, InvertedIndexConfig.VERSION_0)) {
      for (int i = 0; i < numBitmaps; i++) {
        writer.add(new RoaringBitmap());
      }
    }

    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        BitmapInvertedIndexReader reader = new BitmapInvertedIndexReader(dataBuffer, numBitmaps)) {
      for (int i = 0; i < numBitmaps; i++) {
        Assert.assertEquals(reader.getDocIds(i).getCardinality(), 0);
      }
    }
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private static final String INDEX_FILE_NAME = "test.bitmap.inv";

  private File indexFile() {
    return new File(TEMP_DIR, COLUMN + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
  }

  private void addDocs(DictionaryBasedInvertedIndexCreator creator) {
    for (int i = 0; i < NUM_DOCS; i++) {
      creator.add(i % CARDINALITY);
    }
  }

  private void assertVersion(File indexFile, int expectedVersion)
      throws IOException {
    try (PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile)) {
      Assert.assertEquals(buf.getInt(0), BitmapInvertedIndexWriter.MAGIC_NUMBER,
          "Expected magic number for version " + expectedVersion);
      Assert.assertEquals(buf.getInt(Integer.BYTES), expectedVersion,
          "Expected version " + expectedVersion);
    }
  }

  private void assertNoMagic(File indexFile)
      throws IOException {
    try (PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile)) {
      Assert.assertNotEquals(buf.getInt(0), BitmapInvertedIndexWriter.MAGIC_NUMBER,
          "Legacy file should not start with MAGIC_NUMBER");
    }
  }

  /**
   * Creates an inverted index file using the InvertedIndexType (the real config→creator path)
   * with the version configured in the given TableConfig.
   */
  private File createIndexViaType(TableConfig tableConfig)
      throws IOException {
    InvertedIndexType invertedIndexType = (InvertedIndexType) StandardIndexes.inverted();
    Map<String, InvertedIndexConfig> configMap =
        invertedIndexType.getConfig(tableConfig, null);
    InvertedIndexConfig config = configMap.get(COLUMN);

    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN, FieldSpec.DataType.INT, true);
    IndexCreationContext.Common context = IndexCreationContext.builder()
        .withIndexDir(TEMP_DIR)
        .withFieldSpec(fieldSpec)
        .withCardinality(CARDINALITY)
        .withTotalDocs(NUM_DOCS)
        .withTotalNumberOfEntries(NUM_DOCS)
        .build();

    try (DictionaryBasedInvertedIndexCreator creator =
        invertedIndexType.createIndexCreator(context, config)) {
      addDocs(creator);
      creator.seal();
    }

    return indexFile();
  }
}
