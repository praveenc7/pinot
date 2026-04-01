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
package org.apache.pinot.segment.local.segment.index.readers.sorted;

import com.google.common.base.Preconditions;
import java.io.IOException;
import org.apache.pinot.segment.local.io.util.FixedByteValueReaderWriter;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.SortedIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.utils.Pairs;


/**
 * Implementation of sorted index reader.
 */
public class SortedIndexReaderImpl implements SortedIndexReader<SortedIndexReaderImpl.Context> {
  private final FixedByteValueReaderWriter _reader;
  private final int _cardinality;

  public SortedIndexReaderImpl(PinotDataBuffer dataBuffer, int cardinality) {
    // 2 values per dictionary id
    Preconditions.checkState(dataBuffer.size() == 2L * cardinality * Integer.BYTES);
    _reader = new FixedByteValueReaderWriter(dataBuffer);
    _cardinality = cardinality;
  }

  @Override
  public Context createContext() {
    return new Context();
  }

  @Override
  public int getDictId(int docId, Context context) {
    // Only one value in dictionary
    if (_cardinality == 1) {
      return 0;
    }

    int contextDictId = context._dictId;
    int contextStartOffset = context._startOffset;
    int contextEndOffset = context._endOffset;
    if (docId >= contextStartOffset) {
      // Same value
      if (docId <= contextEndOffset) {
        return contextDictId;
      }

      // Next value in dictionary
      int nextDictId = contextDictId + 1;
      int nextEndOffset = _reader.getInt(2 * nextDictId + 1);
      if (docId <= nextEndOffset) {
        context._dictId = nextDictId;
        context._startOffset = contextEndOffset + 1;
        context._endOffset = nextEndOffset;
        return nextDictId;
      }
    }

    int dictId;
    if (docId < contextStartOffset) {
      dictId = binarySearch(docId, 0, contextDictId - 1);
    } else {
      dictId = binarySearch(docId, contextDictId + 2, _cardinality - 1);
    }
    context._dictId = dictId;
    context._startOffset = _reader.getInt(2 * dictId);
    context._endOffset = _reader.getInt(2 * dictId + 1);
    return dictId;
  }

  private int binarySearch(int row, int low, int high) {
    while (low <= high) {
      int mid = (low + high) / 2;
      if (_reader.getInt(2 * mid) <= row) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return high;
  }

  @Override
  public void readDictIds(int[] docIds, int length, int[] dictIdBuffer, Context context) {
    if (_cardinality == 1) {
      for (int i = 0; i < length; i++) {
        dictIdBuffer[i] = 0;
      }
    } else {
      for (int i = 0; i < length; i++) {
        dictIdBuffer[i] = getDictId(docIds[i], context);
      }
    }
  }

  @Override
  public Pairs.IntPair getDocIds(int dictId) {
    // The sorted index stores 2 x 4-byte ints (startDocId, endDocId) per dictionary ID,
    // laid out contiguously in big-endian order. For 3 dictionary IDs (6 int entries):
    //
    //  int index:   [  0  ][  1  ][  2  ][  3  ][  4  ][  5  ]
    //  byte offset:  0     4     8    12    16    20    24
    //               [start][end  ][start][end  ][start][end  ]
    //               |<- dictId=0->|<- dictId=1->|<- dictId=2->|
    //
    // Old (2 x 4-byte reads):
    //   dictId=0: getInt(0) @ byte 0, getInt(1) @ byte 4
    //   dictId=1: getInt(2) @ byte 8, getInt(3) @ byte 12
    //   dictId=2: getInt(4) @ byte 16, getInt(5) @ byte 20
    //
    // New (single 8-byte read — getLong(i) reads at byte offset i*8):
    //   dictId=0: getLong(0) @ bytes 0-7   → high 32 bits = start, low 32 bits = end
    //   dictId=1: getLong(1) @ bytes 8-15  → high 32 bits = start, low 32 bits = end
    //   dictId=2: getLong(2) @ bytes 16-23 → high 32 bits = start, low 32 bits = end
    long pair = _reader.getLong(dictId);
    return new Pairs.IntPair((int) (pair >>> 32), (int) pair);
  }

  @Override
  public void close()
      throws IOException {
    _reader.close();
  }

  public static class Context implements ForwardIndexReaderContext {
    private int _dictId = -1;
    private int _startOffset = -1;
    // Inclusive
    private int _endOffset = -1;

    @Override
    public void close() {
    }
  }
}
