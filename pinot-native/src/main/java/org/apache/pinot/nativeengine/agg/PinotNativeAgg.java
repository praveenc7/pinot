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

/**
 * Java entry points to Pinot's native aggregation kernels.
 *
 * <p>Methods are static native; symbols resolve into the {@code libpinot_native} shared library
 * loaded by {@link NativeLibLoader} at class initialization. If the native library cannot be
 * loaded (unsupported platform, missing binary, etc.), {@link #isAvailable()} returns
 * {@code false} and callers must fall back to the Java implementation. Calling a native method
 * when {@code isAvailable()} is false will throw {@link UnsatisfiedLinkError}.
 *
 * <p>Thread safety: all kernels are stateless and safe to call concurrently from any thread.
 */
public final class PinotNativeAgg {
  private static final boolean AVAILABLE;

  static {
    AVAILABLE = NativeLibLoader.tryLoad();
  }

  private PinotNativeAgg() {
  }

  /**
   * @return {@code true} if the native library was loaded successfully and kernels are callable.
   */
  public static boolean isAvailable() {
    return AVAILABLE;
  }

  /**
   * Self-test entry point. Returns a known magic number (0x5049_4E4F == 'PINO') when the
   * native library is correctly loaded and JNI symbol resolution succeeds.
   */
  public static native int probe();

  /**
   * Computes {@code SUM} over a {@code long[]} as a {@code double}, matching
   * {@code SumAggregationFunction.aggregateSV(LONG)} semantics: per-value {@code long -> double}
   * conversion with straight {@code +=} accumulation.
   *
   * @param values input array (must be non-null)
   * @param length number of leading elements of {@code values} to aggregate
   * @return the sum, or {@link Double#NaN} if the native call failed unexpectedly
   */
  public static native double sumLong(long[] values, int length);

  /**
   * Forced-scalar variant of {@link #sumLong(long[], int)} that bypasses the runtime SIMD
   * dispatch and always invokes the 4-way unrolled scalar kernel. Intended for benchmarking
   * only — lets the JMH harness isolate the SIMD contribution from the JNI + Rust-language
   * contribution. Not used by {@code NativeAggregationRouter} in production routing.
   */
  public static native double sumLongScalar(long[] values, int length);

  /**
   * Computes {@code SUM} over an {@code int[]} as a {@code double}, matching
   * {@code SumAggregationFunction.aggregateSV(INT)} semantics: per-value {@code int -> double}
   * conversion with straight {@code +=} accumulation.
   *
   * @param values input array (must be non-null)
   * @param length number of leading elements of {@code values} to aggregate
   * @return the sum, or {@link Double#NaN} if the native call failed unexpectedly
   */
  public static native double sumInt(int[] values, int length);

  /** Forced-scalar variant of {@link #sumInt(int[], int)} — see {@link #sumLongScalar}. */
  public static native double sumIntScalar(int[] values, int length);

  /**
   * Computes {@code SUM} over a {@code float[]} as a {@code double}, matching
   * {@code SumAggregationFunction.aggregateSV(FLOAT)} semantics: per-value {@code float -> double}
   * conversion with straight {@code +=} accumulation.
   *
   * @param values input array (must be non-null)
   * @param length number of leading elements of {@code values} to aggregate
   * @return the sum, or {@link Double#NaN} if the native call failed unexpectedly
   */
  public static native double sumFloat(float[] values, int length);

  /** Forced-scalar variant of {@link #sumFloat(float[], int)} — see {@link #sumLongScalar}. */
  public static native double sumFloatScalar(float[] values, int length);

  /**
   * Computes {@code SUM} over a {@code double[]}, matching
   * {@code SumAggregationFunction.aggregateSV(DOUBLE)} semantics: straight {@code +=}
   * accumulation, no conversion.
   *
   * @param values input array (must be non-null)
   * @param length number of leading elements of {@code values} to aggregate
   * @return the sum, or {@link Double#NaN} if the native call failed unexpectedly
   */
  public static native double sumDouble(double[] values, int length);

  /** Forced-scalar variant of {@link #sumDouble(double[], int)} — see {@link #sumLongScalar}. */
  public static native double sumDoubleScalar(double[] values, int length);

  // --- MIN ----------------------------------------------------------------

  /**
   * Computes {@code MIN} over an {@code int[]} as a {@code double}, matching
   * {@code MinAggregationFunction.aggregateSV(INT)} semantics: per-element {@code Math.min}
   * in i32 space, then widening to double at return. Empty input returns
   * {@link Double#POSITIVE_INFINITY} (the Pinot Java-side default value).
   */
  public static native double minInt(int[] values, int length);

  /** Forced-scalar variant of {@link #minInt(int[], int)} — see {@link #sumLongScalar}. */
  public static native double minIntScalar(int[] values, int length);

  /**
   * Computes {@code MIN} over a {@code long[]} as a {@code double}. Min is computed in i64
   * space; the f64 conversion at return is lossy for |min| &gt; 2^53, matching Pinot's
   * Java-side behavior. Empty input returns {@link Double#POSITIVE_INFINITY}.
   */
  public static native double minLong(long[] values, int length);

  /** Forced-scalar variant of {@link #minLong(long[], int)} — see {@link #sumLongScalar}. */
  public static native double minLongScalar(long[] values, int length);

  /**
   * Computes {@code MIN} over a {@code float[]} as a {@code double}. NaN-propagating to match
   * Java's {@code Math.min(float, float)}. Empty input returns {@link Double#POSITIVE_INFINITY}.
   */
  public static native double minFloat(float[] values, int length);

  /** Forced-scalar variant of {@link #minFloat(float[], int)} — see {@link #sumLongScalar}. */
  public static native double minFloatScalar(float[] values, int length);

  /**
   * Computes {@code MIN} over a {@code double[]}. NaN-propagating to match Java's
   * {@code Math.min(double, double)}. Empty input returns {@link Double#POSITIVE_INFINITY}.
   */
  public static native double minDouble(double[] values, int length);

  /** Forced-scalar variant of {@link #minDouble(double[], int)} — see {@link #sumLongScalar}. */
  public static native double minDoubleScalar(double[] values, int length);

  // --- MAX ----------------------------------------------------------------

  /** {@code MAX} over an {@code int[]}. Empty input returns {@link Double#NEGATIVE_INFINITY}. */
  public static native double maxInt(int[] values, int length);

  /** Forced-scalar variant of {@link #maxInt(int[], int)} — see {@link #sumLongScalar}. */
  public static native double maxIntScalar(int[] values, int length);

  /** {@code MAX} over a {@code long[]}. Empty input returns {@link Double#NEGATIVE_INFINITY}. */
  public static native double maxLong(long[] values, int length);

  /** Forced-scalar variant of {@link #maxLong(long[], int)} — see {@link #sumLongScalar}. */
  public static native double maxLongScalar(long[] values, int length);

  /**
   * {@code MAX} over a {@code float[]}. NaN-propagating to match Java's
   * {@code Math.max(float, float)}. Empty input returns {@link Double#NEGATIVE_INFINITY}.
   */
  public static native double maxFloat(float[] values, int length);

  /** Forced-scalar variant of {@link #maxFloat(float[], int)} — see {@link #sumLongScalar}. */
  public static native double maxFloatScalar(float[] values, int length);

  /**
   * {@code MAX} over a {@code double[]}. NaN-propagating to match Java's
   * {@code Math.max(double, double)}. Empty input returns {@link Double#NEGATIVE_INFINITY}.
   */
  public static native double maxDouble(double[] values, int length);

  /** Forced-scalar variant of {@link #maxDouble(double[], int)} — see {@link #sumLongScalar}. */
  public static native double maxDoubleScalar(double[] values, int length);
}
