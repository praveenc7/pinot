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
package org.apache.pinot.nativeengine.agg;

import java.util.Random;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class PinotNativeAggTest {

  @BeforeClass
  public void skipIfNativeUnavailable() {
    if (!PinotNativeAgg.isAvailable()) {
      throw new SkipException("Pinot native library not available on this platform; "
          + "set -Dpinot.native.lib.path=<path-to-libpinot_native> to enable.");
    }
  }

  @Test
  public void probeReturnsMagic() {
    assertEquals(PinotNativeAgg.probe(), 0x5049_4E4F);
  }

  @Test
  public void sumLongEmptyIsZero() {
    assertEquals(PinotNativeAgg.sumLong(new long[0], 0), 0.0);
  }

  @Test
  public void sumLongSmallRange() {
    long[] values = new long[100];
    for (int i = 0; i < 100; i++) {
      values[i] = i + 1;
    }
    double expected = 0.0;
    for (int i = 0; i < 100; i++) {
      expected += values[i];
    }
    assertEquals(PinotNativeAgg.sumLong(values, 100), expected);
  }

  @Test
  public void sumLongLargeRandomMatchesJava() {
    Random rng = new Random(42);
    int n = 1_000_000;
    long[] values = new long[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextLong() % 1_000_000L;
    }

    double javaSum = 0.0;
    for (int i = 0; i < n; i++) {
      javaSum += values[i];
    }
    double nativeSum = PinotNativeAgg.sumLong(values, n);

    // For magnitudes within mantissa range we expect exact equality. Allow a tiny
    // relative tolerance to absorb the chunk-reduction-order difference.
    double tolerance = Math.max(1.0, Math.abs(javaSum) * 1e-15);
    assertTrue(Math.abs(nativeSum - javaSum) <= tolerance,
        "native=" + nativeSum + " java=" + javaSum + " diff=" + (nativeSum - javaSum));
  }

  @Test
  public void sumLongRespectsLengthArgument() {
    long[] values = {10, 20, 30, 40, 50};
    // Only sum the first 3 elements
    assertEquals(PinotNativeAgg.sumLong(values, 3), 60.0);
  }

  // --- sumLongScalar (benchmark-only forced-scalar entry point) ---------------------

  @Test
  public void sumLongScalarEmptyIsZero() {
    assertEquals(PinotNativeAgg.sumLongScalar(new long[0], 0), 0.0);
  }

  @Test
  public void sumLongScalarRespectsLengthArgument() {
    long[] values = {10, 20, 30, 40, 50};
    assertEquals(PinotNativeAgg.sumLongScalar(values, 3), 60.0);
  }

  /**
   * Asserts that the forced-scalar entry point produces results numerically equivalent to the
   * SIMD-dispatched entry point. Prevents silent divergence between the two kernels (which would
   * make the benchmark attribution in §11.A of phase-1-design.md meaningless).
   */
  @Test
  public void sumLongScalarMatchesSumLong() {
    Random rng = new Random(42);
    int n = 1_000_000;
    long[] values = new long[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextLong() % 1_000_000L;
    }
    double dispatched = PinotNativeAgg.sumLong(values, n);
    double scalar = PinotNativeAgg.sumLongScalar(values, n);
    double tolerance = Math.max(1.0, Math.abs(dispatched) * 1e-15);
    assertTrue(Math.abs(dispatched - scalar) <= tolerance,
        "dispatched=" + dispatched + " scalar=" + scalar + " diff=" + (dispatched - scalar));
  }

  // --- sumInt ----------------------------------------------------------------------

  @Test
  public void sumIntEmptyIsZero() {
    assertEquals(PinotNativeAgg.sumInt(new int[0], 0), 0.0);
  }

  @Test
  public void sumIntRespectsLengthArgument() {
    int[] values = {10, 20, 30, 40, 50};
    assertEquals(PinotNativeAgg.sumInt(values, 3), 60.0);
  }

  @Test
  public void sumIntScalarMatchesSumInt() {
    Random rng = new Random(42);
    int n = 1_000_000;
    int[] values = new int[n];
    for (int i = 0; i < n; i++) {
      values[i] = (rng.nextInt() % 1_000_000);
    }
    double dispatched = PinotNativeAgg.sumInt(values, n);
    double scalar = PinotNativeAgg.sumIntScalar(values, n);
    double tolerance = Math.max(1.0, Math.abs(dispatched) * 1e-15);
    assertTrue(Math.abs(dispatched - scalar) <= tolerance,
        "dispatched=" + dispatched + " scalar=" + scalar);
  }

  @Test
  public void sumIntLargeRandomMatchesJava() {
    Random rng = new Random(42);
    int n = 1_000_000;
    int[] values = new int[n];
    // Values in [-1M, 1M) keep partial sums small enough that lane-reordering drift
    // stays within the integer-domain exact range (representable in f64 mantissa).
    for (int i = 0; i < n; i++) {
      values[i] = (rng.nextInt() % 1_000_000);
    }
    double javaSum = 0.0;
    for (int i = 0; i < n; i++) {
      javaSum += values[i];
    }
    double nativeSum = PinotNativeAgg.sumInt(values, n);
    double tolerance = Math.max(1.0, Math.abs(javaSum) * 1e-15);
    assertTrue(Math.abs(nativeSum - javaSum) <= tolerance,
        "native=" + nativeSum + " java=" + javaSum + " diff=" + (nativeSum - javaSum));
  }

  // --- sumFloat --------------------------------------------------------------------

  @Test
  public void sumFloatEmptyIsZero() {
    assertEquals(PinotNativeAgg.sumFloat(new float[0], 0), 0.0);
  }

  @Test
  public void sumFloatRespectsLengthArgument() {
    float[] values = {10.0f, 20.0f, 30.0f, 40.0f, 50.0f};
    assertEquals(PinotNativeAgg.sumFloat(values, 3), 60.0);
  }

  @Test
  public void sumFloatScalarMatchesSumFloat() {
    Random rng = new Random(42);
    int n = 1_000_000;
    float[] values = new float[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextFloat() * 2_000_000.0f - 1_000_000.0f;
    }
    double dispatched = PinotNativeAgg.sumFloat(values, n);
    double scalar = PinotNativeAgg.sumFloatScalar(values, n);
    double tolerance = Math.max(1.0, Math.abs(dispatched) * 1e-12);
    assertTrue(Math.abs(dispatched - scalar) <= tolerance,
        "dispatched=" + dispatched + " scalar=" + scalar);
  }

  @Test
  public void sumFloatLargeRandomMatchesJava() {
    Random rng = new Random(42);
    int n = 1_000_000;
    float[] values = new float[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextFloat() * 2_000_000.0f - 1_000_000.0f;
    }
    double javaSum = 0.0;
    for (int i = 0; i < n; i++) {
      javaSum += values[i];
    }
    double nativeSum = PinotNativeAgg.sumFloat(values, n);
    double tolerance = Math.max(1.0, Math.abs(javaSum) * 1e-12);
    assertTrue(Math.abs(nativeSum - javaSum) <= tolerance,
        "native=" + nativeSum + " java=" + javaSum + " diff=" + (nativeSum - javaSum));
  }

  // --- sumDouble -------------------------------------------------------------------

  @Test
  public void sumDoubleEmptyIsZero() {
    assertEquals(PinotNativeAgg.sumDouble(new double[0], 0), 0.0);
  }

  @Test
  public void sumDoubleRespectsLengthArgument() {
    double[] values = {10.0, 20.0, 30.0, 40.0, 50.0};
    assertEquals(PinotNativeAgg.sumDouble(values, 3), 60.0);
  }

  @Test
  public void sumDoubleScalarMatchesSumDouble() {
    Random rng = new Random(42);
    int n = 1_000_000;
    double[] values = new double[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextDouble() * 2_000_000.0 - 1_000_000.0;
    }
    double dispatched = PinotNativeAgg.sumDouble(values, n);
    double scalar = PinotNativeAgg.sumDoubleScalar(values, n);
    double tolerance = Math.max(1.0, Math.abs(dispatched) * 1e-12);
    assertTrue(Math.abs(dispatched - scalar) <= tolerance,
        "dispatched=" + dispatched + " scalar=" + scalar);
  }

  @Test
  public void sumDoubleLargeRandomMatchesJava() {
    Random rng = new Random(42);
    int n = 1_000_000;
    double[] values = new double[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextDouble() * 2_000_000.0 - 1_000_000.0;
    }
    double javaSum = 0.0;
    for (int i = 0; i < n; i++) {
      javaSum += values[i];
    }
    double nativeSum = PinotNativeAgg.sumDouble(values, n);
    double tolerance = Math.max(1.0, Math.abs(javaSum) * 1e-12);
    assertTrue(Math.abs(nativeSum - javaSum) <= tolerance,
        "native=" + nativeSum + " java=" + javaSum + " diff=" + (nativeSum - javaSum));
  }

  // ===========================================================================
  // MIN  (INT / LONG / FLOAT / DOUBLE)
  // ===========================================================================

  @Test
  public void minIntEmptyReturnsPosInfinity() {
    assertEquals(PinotNativeAgg.minInt(new int[0], 0), Double.POSITIVE_INFINITY);
  }

  @Test
  public void minIntLargeRandomMatchesJava() {
    Random rng = new Random(42);
    int n = 1_000_000;
    int[] values = new int[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextInt() % 1_000_000;
    }
    int javaMin = Integer.MAX_VALUE;
    for (int v : values) {
      javaMin = Math.min(javaMin, v);
    }
    assertEquals(PinotNativeAgg.minInt(values, n), (double) javaMin);
  }

  @Test
  public void minLongEmptyReturnsPosInfinity() {
    assertEquals(PinotNativeAgg.minLong(new long[0], 0), Double.POSITIVE_INFINITY);
  }

  @Test
  public void minLongLargeRandomMatchesJava() {
    Random rng = new Random(42);
    int n = 1_000_000;
    long[] values = new long[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextLong() % 1_000_000L;
    }
    long javaMin = Long.MAX_VALUE;
    for (long v : values) {
      javaMin = Math.min(javaMin, v);
    }
    assertEquals(PinotNativeAgg.minLong(values, n), (double) javaMin);
  }

  @Test
  public void minFloatEmptyReturnsPosInfinity() {
    assertEquals(PinotNativeAgg.minFloat(new float[0], 0), Double.POSITIVE_INFINITY);
  }

  @Test
  public void minFloatLargeRandomMatchesJava() {
    Random rng = new Random(42);
    int n = 1_000_000;
    float[] values = new float[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextFloat() * 2_000_000.0f - 1_000_000.0f;
    }
    float javaMin = Float.POSITIVE_INFINITY;
    for (float v : values) {
      javaMin = Math.min(javaMin, v);
    }
    assertEquals(PinotNativeAgg.minFloat(values, n), (double) javaMin);
  }

  @Test
  public void minFloatNanPropagates() {
    float[] values = {1.0f, 2.0f, Float.NaN, 3.0f};
    assertTrue(Double.isNaN(PinotNativeAgg.minFloat(values, 4)));
  }

  @Test
  public void minDoubleEmptyReturnsPosInfinity() {
    assertEquals(PinotNativeAgg.minDouble(new double[0], 0), Double.POSITIVE_INFINITY);
  }

  @Test
  public void minDoubleLargeRandomMatchesJava() {
    Random rng = new Random(42);
    int n = 1_000_000;
    double[] values = new double[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextDouble() * 2_000_000.0 - 1_000_000.0;
    }
    double javaMin = Double.POSITIVE_INFINITY;
    for (double v : values) {
      javaMin = Math.min(javaMin, v);
    }
    assertEquals(PinotNativeAgg.minDouble(values, n), javaMin);
  }

  @Test
  public void minDoubleNanPropagates() {
    double[] values = {1.0, 2.0, Double.NaN, 3.0};
    assertTrue(Double.isNaN(PinotNativeAgg.minDouble(values, 4)));
  }

  // ===========================================================================
  // MAX  (INT / LONG / FLOAT / DOUBLE)
  // ===========================================================================

  @Test
  public void maxIntEmptyReturnsNegInfinity() {
    assertEquals(PinotNativeAgg.maxInt(new int[0], 0), Double.NEGATIVE_INFINITY);
  }

  @Test
  public void maxIntLargeRandomMatchesJava() {
    Random rng = new Random(42);
    int n = 1_000_000;
    int[] values = new int[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextInt() % 1_000_000;
    }
    int javaMax = Integer.MIN_VALUE;
    for (int v : values) {
      javaMax = Math.max(javaMax, v);
    }
    assertEquals(PinotNativeAgg.maxInt(values, n), (double) javaMax);
  }

  @Test
  public void maxLongEmptyReturnsNegInfinity() {
    assertEquals(PinotNativeAgg.maxLong(new long[0], 0), Double.NEGATIVE_INFINITY);
  }

  @Test
  public void maxLongLargeRandomMatchesJava() {
    Random rng = new Random(42);
    int n = 1_000_000;
    long[] values = new long[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextLong() % 1_000_000L;
    }
    long javaMax = Long.MIN_VALUE;
    for (long v : values) {
      javaMax = Math.max(javaMax, v);
    }
    assertEquals(PinotNativeAgg.maxLong(values, n), (double) javaMax);
  }

  @Test
  public void maxFloatEmptyReturnsNegInfinity() {
    assertEquals(PinotNativeAgg.maxFloat(new float[0], 0), Double.NEGATIVE_INFINITY);
  }

  @Test
  public void maxFloatLargeRandomMatchesJava() {
    Random rng = new Random(42);
    int n = 1_000_000;
    float[] values = new float[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextFloat() * 2_000_000.0f - 1_000_000.0f;
    }
    float javaMax = Float.NEGATIVE_INFINITY;
    for (float v : values) {
      javaMax = Math.max(javaMax, v);
    }
    assertEquals(PinotNativeAgg.maxFloat(values, n), (double) javaMax);
  }

  @Test
  public void maxFloatNanPropagates() {
    float[] values = {1.0f, 2.0f, Float.NaN, 3.0f};
    assertTrue(Double.isNaN(PinotNativeAgg.maxFloat(values, 4)));
  }

  @Test
  public void maxDoubleEmptyReturnsNegInfinity() {
    assertEquals(PinotNativeAgg.maxDouble(new double[0], 0), Double.NEGATIVE_INFINITY);
  }

  @Test
  public void maxDoubleLargeRandomMatchesJava() {
    Random rng = new Random(42);
    int n = 1_000_000;
    double[] values = new double[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextDouble() * 2_000_000.0 - 1_000_000.0;
    }
    double javaMax = Double.NEGATIVE_INFINITY;
    for (double v : values) {
      javaMax = Math.max(javaMax, v);
    }
    assertEquals(PinotNativeAgg.maxDouble(values, n), javaMax);
  }

  @Test
  public void maxDoubleNanPropagates() {
    double[] values = {1.0, 2.0, Double.NaN, 3.0};
    assertTrue(Double.isNaN(PinotNativeAgg.maxDouble(values, 4)));
  }
}
