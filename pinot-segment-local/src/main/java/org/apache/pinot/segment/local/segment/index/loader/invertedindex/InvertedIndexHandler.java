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
package org.apache.pinot.segment.local.segment.index.loader.invertedindex;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitmapInvertedIndexWriter;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexType;
import org.apache.pinot.segment.local.segment.index.loader.BaseIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.InvertedIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.DictionaryBasedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings({"rawtypes", "unchecked"})
public class InvertedIndexHandler extends BaseIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(InvertedIndexHandler.class);

  private final Set<String> _columnsToAddIdx;

  public InvertedIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> fieldIndexConfigs,
      @Nullable TableConfig tableConfig) {
    super(segmentDirectory, fieldIndexConfigs, tableConfig);
    _columnsToAddIdx = FieldIndexConfigsUtil.columnsWithIndexEnabled(StandardIndexes.inverted(), _fieldIndexConfigs);
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> columnsToAddIdx = new HashSet<>(_columnsToAddIdx);
    Set<String> existingColumns =
        segmentReader.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.inverted());
    // Check if any existing index need to be removed.
    for (String column : existingColumns) {
      if (!columnsToAddIdx.remove(column)) {
        LOGGER.info("Need to remove existing inverted index from segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    // Check if any new index need to be added.
    for (String column : columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (shouldCreateInvertedIndex(columnMetadata)) {
        LOGGER.info("Need to create new inverted index for segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    // Check if any existing index needs version conversion (V0→V1).
    Set<String> existingConfiguredColumns = new HashSet<>(existingColumns);
    existingConfiguredColumns.retainAll(_columnsToAddIdx);
    for (String column : existingConfiguredColumns) {
      int onDiskVersion;
      try {
        onDiskVersion = detectOnDiskVersion(segmentReader, column);
      } catch (IOException e) {
        LOGGER.warn("Unable to detect inverted index version for segment: {}, column: {}", segmentName, column, e);
        continue;
      }
      int configuredVersion = getConfiguredVersion(column);
      if (onDiskVersion == InvertedIndexConfig.VERSION_0 && configuredVersion == InvertedIndexConfig.VERSION_1
          && segmentReader.hasIndexFor(column, StandardIndexes.forward())) {
        LOGGER.info("Need to convert inverted index from VERSION_0 to VERSION_1 for segment: {}, column: {}",
            segmentName, column);
        return true;
      }
    }
    return false;
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter)
      throws Exception {
    // Remove indices not set in table config any more.
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> columnsToAddIdx = new HashSet<>(_columnsToAddIdx);
    Set<String> existingColumns =
        segmentWriter.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.inverted());
    for (String column : existingColumns) {
      if (!columnsToAddIdx.remove(column)) {
        LOGGER.info("Removing existing inverted index from segment: {}, column: {}", segmentName, column);
        segmentWriter.removeIndex(column, StandardIndexes.inverted());
        LOGGER.info("Removed existing inverted index from segment: {}, column: {}", segmentName, column);
      }
    }
    for (String column : columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (shouldCreateInvertedIndex(columnMetadata)) {
        createInvertedIndexForColumn(segmentWriter, columnMetadata);
      }
    }
    // Convert existing inverted indexes from VERSION_0 to VERSION_1 where configured.
    for (String column : getColumnsToConvertV0ToV1(segmentWriter, existingColumns)) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      LOGGER.info("Converting inverted index from VERSION_0 to VERSION_1 for segment: {}, column: {}",
          segmentName, column);
      segmentWriter.removeIndex(column, StandardIndexes.inverted());
      createInvertedIndexForColumn(segmentWriter, columnMetadata);
    }
  }

  @Override
  public void postUpdateIndicesCleanup(SegmentDirectory.Writer segmentWriter)
      throws Exception {
  }

  /**
   * Returns the set of columns that have an existing VERSION_0 inverted index and need conversion to VERSION_1
   * based on the configured version. Skips columns without a forward index (needed for recreation) and logs
   * warnings for unsupported V1→V0 downgrade requests.
   */
  private Set<String> getColumnsToConvertV0ToV1(SegmentDirectory.Reader segmentReader,
      Set<String> existingColumns) {
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> existingConfiguredColumns = new HashSet<>(existingColumns);
    existingConfiguredColumns.retainAll(_columnsToAddIdx);
    Set<String> columnsToConvert = new HashSet<>();
    for (String column : existingConfiguredColumns) {
      int onDiskVersion;
      try {
        onDiskVersion = detectOnDiskVersion(segmentReader, column);
      } catch (IOException e) {
        LOGGER.warn("Unable to detect inverted index version for segment: {}, column: {}, skipping conversion",
            segmentName, column, e);
        continue;
      }
      int configuredVersion = getConfiguredVersion(column);
      if (onDiskVersion == configuredVersion) {
        continue;
      }
      if (onDiskVersion == InvertedIndexConfig.VERSION_0 && configuredVersion == InvertedIndexConfig.VERSION_1) {
        if (!segmentReader.hasIndexFor(column, StandardIndexes.forward())) {
          LOGGER.warn("Cannot convert inverted index from VERSION_0 to VERSION_1 for segment: {}, column: {} "
              + "because forward index is not available", segmentName, column);
          continue;
        }
        columnsToConvert.add(column);
      } else if (onDiskVersion == InvertedIndexConfig.VERSION_1
          && configuredVersion == InvertedIndexConfig.VERSION_0) {
        LOGGER.warn("Downgrade from inverted index VERSION_1 to VERSION_0 is not supported for segment: {}, "
            + "column: {}. Index will remain VERSION_1.", segmentName, column);
      }
    }
    return columnsToConvert;
  }

  /**
   * Detects the on-disk inverted index format version by reading the first 4 bytes. If the first int
   * matches {@link BitmapInvertedIndexWriter#MAGIC_NUMBER}, the index is VERSION_1; otherwise VERSION_0.
   * Works for both V1 (file-per-index) and V3 (single-file) segment formats.
   */
  private int detectOnDiskVersion(SegmentDirectory.Reader reader, String column)
      throws IOException {
    PinotDataBuffer indexBuffer = reader.getIndexFor(column, StandardIndexes.inverted());
    int firstInt = indexBuffer.getInt(0);
    return firstInt == BitmapInvertedIndexWriter.MAGIC_NUMBER
        ? InvertedIndexConfig.VERSION_1 : InvertedIndexConfig.VERSION_0;
  }

  private int getConfiguredVersion(String column) {
    FieldIndexConfigs fieldIndexConfigs = _fieldIndexConfigs.get(column);
    if (fieldIndexConfigs == null) {
      return InvertedIndexConfig.DEFAULT_VERSION;
    }
    return fieldIndexConfigs.getConfig(StandardIndexes.inverted()).getVersion();
  }

  private boolean shouldCreateInvertedIndex(ColumnMetadata columnMetadata) {
    // Only create inverted index on dictionary-encoded unsorted columns.
    return columnMetadata != null && !columnMetadata.isSorted() && columnMetadata.hasDictionary();
  }

  private void createInvertedIndexForColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws Exception {
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    String columnName = columnMetadata.getColumnName();
    File inProgress = new File(indexDir, columnName + ".inv.inprogress");
    File invertedIndexFile = new File(indexDir, columnName + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.
      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.
      // Remove inverted index if exists.
      // For v1 and v2, it's the actual inverted index. For v3, it's the temporary inverted index.
      FileUtils.deleteQuietly(invertedIndexFile);
    }

    // Create new inverted index for the column.
    LOGGER.info("Creating new inverted index for segment: {}, column: {}", segmentName, columnName);
    int numDocs = columnMetadata.getTotalDocs();

    IndexCreationContext.Common context = IndexCreationContext.builder()
        .withIndexDir(indexDir)
        .withColumnMetadata(columnMetadata)
        .build();

    FieldIndexConfigs fieldIndexConfigs = _fieldIndexConfigs.get(columnName);
    InvertedIndexConfig invertedIndexConfig = fieldIndexConfigs != null
        ? fieldIndexConfigs.getConfig(StandardIndexes.inverted()) : InvertedIndexConfig.ENABLED;
    try (DictionaryBasedInvertedIndexCreator creator = StandardIndexes.inverted()
        .createIndexCreator(context, invertedIndexConfig)) {
      try (ForwardIndexReader forwardIndexReader = ForwardIndexType.read(segmentWriter, columnMetadata);
          ForwardIndexReaderContext readerContext = forwardIndexReader.createContext()) {
        if (columnMetadata.isSingleValue()) {
          // Single-value column.
          for (int i = 0; i < numDocs; i++) {
            creator.add(forwardIndexReader.getDictId(i, readerContext));
          }
        } else {
          // Multi-value column.
          int[] dictIds = new int[columnMetadata.getMaxNumberOfMultiValues()];
          for (int i = 0; i < numDocs; i++) {
            int length = forwardIndexReader.getDictIdMV(i, dictIds, readerContext);
            creator.add(dictIds, length);
          }
        }
        creator.seal();
      }
    }

    // For v3, write the generated inverted index file into the single file and remove it.
    if (_segmentDirectory.getSegmentMetadata().getVersion() == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(segmentWriter, columnName, invertedIndexFile, StandardIndexes.inverted());
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created inverted index for segment: {}, column: {}", segmentName, columnName);
  }
}
