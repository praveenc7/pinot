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
package org.apache.pinot.core.query.aggregation.function;

import java.math.BigDecimal;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;


/**
 * Minimal {@link BlockValSet} stub used by the {@code Native*AggregationFunction} tests for
 * single-value primitive columns. Only the methods the native aggregation kernels actually
 * call are implemented; the rest throw {@code UnsupportedOperationException} so any
 * unintended use surfaces immediately in tests instead of producing silent zeros.
 *
 * <p>Construct via the factory methods ({@link #forInt}, {@link #forLong}, etc.) — they
 * keep the constructor private and prevent type/array mismatch.
 */
final class NativePrimitiveBlockValSet implements BlockValSet {
  private final DataType _type;
  @Nullable
  private final int[] _intValues;
  @Nullable
  private final long[] _longValues;
  @Nullable
  private final float[] _floatValues;
  @Nullable
  private final double[] _doubleValues;

  private NativePrimitiveBlockValSet(DataType type, @Nullable int[] intValues,
      @Nullable long[] longValues, @Nullable float[] floatValues, @Nullable double[] doubleValues) {
    _type = type;
    _intValues = intValues;
    _longValues = longValues;
    _floatValues = floatValues;
    _doubleValues = doubleValues;
  }

  static NativePrimitiveBlockValSet forInt(int[] values) {
    return new NativePrimitiveBlockValSet(DataType.INT, values, null, null, null);
  }

  static NativePrimitiveBlockValSet forLong(long[] values) {
    return new NativePrimitiveBlockValSet(DataType.LONG, null, values, null, null);
  }

  static NativePrimitiveBlockValSet forFloat(float[] values) {
    return new NativePrimitiveBlockValSet(DataType.FLOAT, null, null, values, null);
  }

  static NativePrimitiveBlockValSet forDouble(double[] values) {
    return new NativePrimitiveBlockValSet(DataType.DOUBLE, null, null, null, values);
  }

  @Nullable
  @Override
  public RoaringBitmap getNullBitmap() {
    return null;
  }

  @Override
  public DataType getValueType() {
    return _type;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Nullable
  @Override
  public Dictionary getDictionary() {
    return null;
  }

  @Override
  public int[] getDictionaryIdsSV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] getIntValuesSV() {
    if (_intValues == null) {
      throw new UnsupportedOperationException("no int values configured");
    }
    return _intValues;
  }

  @Override
  public long[] getLongValuesSV() {
    if (_longValues == null) {
      throw new UnsupportedOperationException("no long values configured");
    }
    return _longValues;
  }

  @Override
  public float[] getFloatValuesSV() {
    if (_floatValues == null) {
      throw new UnsupportedOperationException("no float values configured");
    }
    return _floatValues;
  }

  @Override
  public double[] getDoubleValuesSV() {
    if (_doubleValues == null) {
      throw new UnsupportedOperationException("no double values configured");
    }
    return _doubleValues;
  }

  @Override
  public BigDecimal[] getBigDecimalValuesSV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[] getStringValuesSV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[][] getBytesValuesSV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] getDictionaryIdsMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] getIntValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long[][] getLongValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public float[][] getFloatValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public double[][] getDoubleValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[][] getStringValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[][][] getBytesValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] getNumMVEntries() {
    throw new UnsupportedOperationException();
  }
}
