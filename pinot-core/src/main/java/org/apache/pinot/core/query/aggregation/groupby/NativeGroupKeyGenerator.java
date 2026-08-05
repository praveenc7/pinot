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
package org.apache.pinot.core.query.aggregation.groupby;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.segment.spi.index.reader.Dictionary;


/**
 * A {@link GroupKeyGenerator} that materializes group keys from the output of the native GROUP BY
 * engine ({@link org.apache.pinot.nativeengine.groupby.PinotNativeGroupBy}).
 *
 * <p>The native driver assigns its own dense, insertion-ordered {@code group_id}s and records the
 * dict_id that allocated each group. This generator adapts that representation back to Pinot's
 * {@link GroupKey} iteration model by decoding each group's dict_id through the segment-local
 * {@link Dictionary} — exactly mirroring the single-column path of
 * {@link DictionaryBasedGroupKeyGenerator} ({@code new Object[]{dictionary.getInternal(dictId)}}).
 *
 * <p>Decoding is type-agnostic: the dict_id probe path is identical for INT/LONG/FLOAT/DOUBLE
 * (and other dict-encoded) key columns, and {@link Dictionary#getInternal(int)} returns the
 * correct internal-typed value object for each. This is why a single i32 dict-id driver covers all
 * dict-encoded fixed-width key types (design doc §19 step 1c).
 *
 * <p>This generator is read-only: {@link #generateKeysForBlock} is never invoked because the native
 * executor performs key generation itself inside the Rust driver. The block-key methods therefore
 * throw {@link UnsupportedOperationException}.
 */
/**
 * A {@link GroupKeyGenerator} that materializes group keys from the output of the native GROUP BY
 * engine ({@link org.apache.pinot.nativeengine.groupby.PinotNativeGroupBy}).
 *
 * <p>The native driver assigns its own dense, insertion-ordered {@code group_id}s and records the
 * dict_id(s) that allocated each group. This generator adapts that representation back to Pinot's
 * {@link GroupKey} iteration model by decoding each group's per-column dict_id through the
 * corresponding segment-local {@link Dictionary} — exactly mirroring
 * {@link DictionaryBasedGroupKeyGenerator} ({@code dictionary.getInternal(dictId)} per key column).
 *
 * <p>Decoding is type-agnostic: the dict_id probe path is identical for INT/LONG/FLOAT/DOUBLE
 * (and other dict-encoded) key columns, and {@link Dictionary#getInternal(int)} returns the
 * correct internal-typed value object for each. This covers single- and multi-column dict-encoded
 * GROUP BY keys (design doc §19 step 1c / §17.9).
 *
 * <p>This generator is read-only: {@link #generateKeysForBlock} is never invoked because the native
 * executor performs key generation itself inside the Rust driver. The block-key methods therefore
 * throw {@link UnsupportedOperationException}.
 */
public class NativeGroupKeyGenerator implements GroupKeyGenerator {
  private final Dictionary[] _dictionaries;
  // _dictIds[c][g] is the dict_id of key column c that allocated native group_id g, in group_id order.
  private final int[][] _dictIds;
  private final int _numKeyColumns;
  private final int _numKeys;
  private final int _globalUpperBound;

  /** Single-column convenience constructor. */
  public NativeGroupKeyGenerator(Dictionary dictionary, int[] dictIds, int numKeys) {
    this(new Dictionary[]{dictionary}, new int[][]{dictIds}, numKeys);
  }

  /**
   * @param dictionaries one segment-local dictionary per group-by key column, in grouping order.
   * @param dictIds      per-column dict-id arrays (each length {@code >= numKeys}), in group_id order.
   * @param numKeys      the native combined group count.
   */
  public NativeGroupKeyGenerator(Dictionary[] dictionaries, int[][] dictIds, int numKeys) {
    _dictionaries = dictionaries;
    _dictIds = dictIds;
    _numKeyColumns = dictionaries.length;
    _numKeys = numKeys;
    long product = 1L;
    for (Dictionary dictionary : dictionaries) {
      product *= dictionary.length();
      if (product >= Integer.MAX_VALUE) {
        product = Integer.MAX_VALUE;
        break;
      }
    }
    _globalUpperBound = (int) product;
  }

  @Override
  public int getGlobalGroupKeyUpperBound() {
    // Product of per-column dict cardinalities (capped): the native group count never exceeds it.
    return _globalUpperBound;
  }

  @Override
  public void generateKeysForBlock(ValueBlock valueBlock, int[] groupKeys) {
    throw new UnsupportedOperationException(
        "NativeGroupKeyGenerator does not generate keys; the native executor owns key generation");
  }

  @Override
  public void generateKeysForBlock(ValueBlock valueBlock, int[][] groupKeys) {
    throw new UnsupportedOperationException(
        "NativeGroupKeyGenerator does not generate keys; the native executor owns key generation");
  }

  @Override
  public int getCurrentGroupKeyUpperBound() {
    return _numKeys;
  }

  @Override
  public Iterator<GroupKey> getGroupKeys() {
    return new Iterator<GroupKey>() {
      // Reuse a single GroupKey instance across next() calls, matching
      // DictionaryBasedGroupKeyGenerator. The consumer reads it before advancing.
      private final GroupKey _groupKey = new GroupKey();
      private int _current = 0;

      @Override
      public boolean hasNext() {
        return _current < _numKeys;
      }

      @Override
      public GroupKey next() {
        if (_current >= _numKeys) {
          throw new NoSuchElementException();
        }
        _groupKey._groupId = _current;
        Object[] keys = new Object[_numKeyColumns];
        for (int c = 0; c < _numKeyColumns; c++) {
          keys[c] = _dictionaries[c].getInternal(_dictIds[c][_current]);
        }
        _groupKey._keys = keys;
        _current++;
        return _groupKey;
      }
    };
  }

  @Override
  public int getNumKeys() {
    return _numKeys;
  }
}
