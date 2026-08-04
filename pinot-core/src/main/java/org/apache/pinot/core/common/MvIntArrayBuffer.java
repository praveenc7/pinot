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
package org.apache.pinot.core.common;

import com.google.common.base.Preconditions;
import java.util.Arrays;


/**
 * Reusable flat representation of a block of multi-value integers.
 *
 * <p>Values for all documents are stored back-to-back in one array, and an offsets array identifies each document's
 * range. The values for document {@code i} are {@code values[offsets[i] .. offsets[i + 1])}. Both arrays grow on
 * demand and are reused across blocks.
 *
 * <p>This class is not thread-safe. The owner may mutate the values in place, for example to replace dictionary ids
 * with group ids.
 */
public final class MvIntArrayBuffer {
  private static final int MIN_VALUE_CAPACITY = 16;

  private int[] _values = new int[MIN_VALUE_CAPACITY];
  private int[] _offsets = new int[]{0};
  private int _numDocs;
  private int _numValues;

  /**
   * Prepares the buffer for a new block. Existing array storage is retained.
   */
  public void startFill(int numDocs) {
    Preconditions.checkArgument(numDocs >= 0, "Number of documents must be non-negative, got: %s", numDocs);
    if (_offsets.length < numDocs + 1) {
      _offsets = new int[numDocs + 1];
    }
    _offsets[0] = 0;
    _numDocs = 0;
    _numValues = 0;
  }

  /**
   * Appends the first {@code numValues} entries for the next document.
   */
  public void append(int[] values, int numValues) {
    ensureValueCapacity(_numValues + numValues);
    System.arraycopy(values, 0, _values, _numValues, numValues);
    _numValues += numValues;
    _offsets[++_numDocs] = _numValues;
  }

  /**
   * Fills the buffer from a row-major representation.
   */
  public void fill(int[][] values, int numDocs) {
    startFill(numDocs);
    for (int i = 0; i < numDocs; i++) {
      int[] documentValues = values[i];
      append(documentValues, documentValues.length);
    }
  }

  public int[] getValues() {
    return _values;
  }

  public int[] getOffsets() {
    return _offsets;
  }

  public int getNumDocs() {
    return _numDocs;
  }

  public int getNumValues() {
    return _numValues;
  }

  public int getStartOffset(int docIndex) {
    return _offsets[docIndex];
  }

  public int getNumValues(int docIndex) {
    return _offsets[docIndex + 1] - _offsets[docIndex];
  }

  private void ensureValueCapacity(int capacity) {
    int currentCapacity = _values.length;
    if (currentCapacity < capacity) {
      int newCapacity = Math.max(Math.max(currentCapacity + (currentCapacity >> 1), capacity), MIN_VALUE_CAPACITY);
      _values = Arrays.copyOf(_values, newCapacity);
    }
  }
}
