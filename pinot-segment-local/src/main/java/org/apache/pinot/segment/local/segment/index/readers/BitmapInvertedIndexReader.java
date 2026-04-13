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
package org.apache.pinot.segment.local.segment.index.readers;

import java.nio.ByteOrder;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitmapInvertedIndexWriter;
import org.apache.pinot.segment.spi.index.InvertedIndexConfig;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Reader for bitmap based inverted index. Please reference
 * {@link BitmapInvertedIndexWriter} for the index file layout.
 *
 * <p>Supports two formats:
 * <ul>
 *   <li>VERSION_0 (legacy): no header, 32-bit offsets. Detected by the absence of the magic number.</li>
 *   <li>VERSION_1: magic + version header, 64-bit offsets. Detected by magic number at byte 0.</li>
 * </ul>
 */
public class BitmapInvertedIndexReader implements InvertedIndexReader<ImmutableRoaringBitmap> {
  public static final Logger LOGGER = LoggerFactory.getLogger(BitmapInvertedIndexReader.class);

  private final int _version;
  private final PinotDataBuffer _offsetBuffer;
  private final PinotDataBuffer _bitmapBuffer;

  // Offset of the first bitmap (absolute position in the file). Used to compute positions within _bitmapBuffer.
  private final long _firstOffset;

  public BitmapInvertedIndexReader(PinotDataBuffer dataBuffer, int numBitmaps) {
    int firstInt = dataBuffer.getInt(0);
    if (firstInt == BitmapInvertedIndexWriter.MAGIC_NUMBER) {
      _version = dataBuffer.getInt(Integer.BYTES);
      if (_version == InvertedIndexConfig.VERSION_1) {
        long offsetsStart = BitmapInvertedIndexWriter.HEADER_SIZE_V1;
        long offsetsEnd = offsetsStart + (long) (numBitmaps + 1) * Long.BYTES;
        _offsetBuffer = dataBuffer.view(offsetsStart, offsetsEnd, ByteOrder.BIG_ENDIAN);
        _bitmapBuffer = dataBuffer.view(offsetsEnd, dataBuffer.size());
      } else {
        throw new IllegalStateException("Unknown inverted index version: " + _version);
      }
    } else {
      // Legacy VERSION_0: no header, 32-bit offsets
      _version = InvertedIndexConfig.VERSION_0;
      long offsetBufferEndOffset = (long) (numBitmaps + 1) * Integer.BYTES;
      _offsetBuffer = dataBuffer.view(0, offsetBufferEndOffset, ByteOrder.BIG_ENDIAN);
      _bitmapBuffer = dataBuffer.view(offsetBufferEndOffset, dataBuffer.size());
    }
    _firstOffset = getOffset(0);
  }

  @SuppressWarnings("unchecked")
  @Override
  public ImmutableRoaringBitmap getDocIds(int dictId) {
    long offset = getOffset(dictId);
    long length = getOffset(dictId + 1) - offset;
    return new ImmutableRoaringBitmap(_bitmapBuffer.toDirectByteBuffer(offset - _firstOffset, (int) length));
  }

  private long getOffset(int dictId) {
    if (_version == InvertedIndexConfig.VERSION_1) {
      return _offsetBuffer.getLong((long) dictId * Long.BYTES);
    }
    return _offsetBuffer.getInt((long) dictId * Integer.BYTES) & 0xFFFFFFFFL;
  }

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
  }
}
