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
package org.apache.pinot.perf.aggregation;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.MaxAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.MinAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.NativeMaxAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.NativeMinAggregationFunction;
import org.apache.pinot.nativeengine.agg.PinotNativeAgg;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.roaringbitmap.RoaringBitmap;


/**
 * Microbenchmark comparing the native (Rust + JNI) MIN and MAX kernels against the Java
 * reference across the full numeric primitive matrix {@code INT, LONG, FLOAT, DOUBLE} crossed
 * with three engines ({@code java}, {@code native-scalar}, {@code native}) and both ops
 * {@code MIN, MAX}. Mirrors {@link BenchmarkNativeSumAggregation}.
 *
 * <p>Methodology: each trial constructs one aggregation function and one
 * {@link BlockValSet} of the specified type and length, then measures the per-invocation
 * cost of {@code aggregate()}. Constructors are called directly (not through
 * {@code AggregationFunctionFactory}) so the benchmark measures kernel + JNI cost, not
 * routing logic.
 *
 * <p>The three engines:
 * <ul>
 *   <li>{@code java} — reference {@link MinAggregationFunction} / {@link MaxAggregationFunction},
 *       no native code.</li>
 *   <li>{@code native-scalar} — Rust 4-way unrolled scalar kernel via the {@code min<Type>Scalar}
 *       / {@code max<Type>Scalar} JNI entries; same JNI plumbing as production, bypasses runtime
 *       SIMD dispatch. Isolates JNI + Rust-language contribution from SIMD contribution.</li>
 *   <li>{@code native} — Rust runtime-dispatched kernel (NEON / AVX2 / AVX-512F / scalar
 *       fallback) via {@code min<Type>} / {@code max<Type>}; represents the production native
 *       path.</li>
 * </ul>
 *
 * <p>FP NaN handling: the random generator emits values in {@code (-1M, +1M)} with no NaN
 * injection, so the Java side does not have to propagate NaN. This isolates the kernel +
 * JNI cost from the per-chunk sticky-NaN tracking on the native FP path.
 *
 * <p>Run with:
 * <pre>
 *   ./mvnw -pl pinot-native -am package
 *   cd pinot-perf
 *   java -cp "target/classes:target/test-classes:$(cat /tmp/pinot-perf-cp.txt)" \
 *     org.apache.pinot.perf.aggregation.BenchmarkNativeMinMaxAggregation
 * </pre>
 */
@State(Scope.Benchmark)
@Fork(value = 3, jvmArgsAppend = {})
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 10, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 20, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class BenchmarkNativeMinMaxAggregation {

  private static final ExpressionContext EXPR = ExpressionContext.forIdentifier("col");

  @Param({"java", "native-scalar", "native"})
  public String _engine;

  @Param({"MIN", "MAX"})
  public String _op;

  @Param({"INT", "LONG", "FLOAT", "DOUBLE"})
  public String _type;

  @Param({"100", "1000", "10000", "100000"})
  public int _length;

  private AggregationFunction<?, ?> _function;
  private Map<ExpressionContext, BlockValSet> _blockValSetMap;
  private AggregationResultHolder _resultHolder;

  public static void main(String[] args)
      throws RunnerException {
    String libPath = resolveNativeLibPath();
    OptionsBuilder builder =
        (OptionsBuilder) new OptionsBuilder().include(BenchmarkNativeMinMaxAggregation.class.getSimpleName());
    if (libPath != null) {
      builder.jvmArgsAppend("-Dpinot.native.lib.path=" + libPath);
    } else {
      System.err.println("[WARN] native library not found at the expected dev path. "
          + "Run './mvnw -pl pinot-native package' first; otherwise 'native' trials will throw.");
    }
    new Runner(builder.build()).run();
  }

  @Setup(Level.Trial)
  public void setUp() {
    DataType dataType = DataType.valueOf(_type);
    Random rng = new Random(42);
    BlockValSet bvs;
    switch (dataType) {
      case INT: {
        int[] values = new int[_length];
        for (int i = 0; i < _length; i++) {
          values[i] = (rng.nextInt() % 1_000_000);
        }
        bvs = new PrimitiveBlockValSet(DataType.INT, values, null, null, null);
        break;
      }
      case LONG: {
        long[] values = new long[_length];
        for (int i = 0; i < _length; i++) {
          values[i] = rng.nextLong() % 1_000_000L;
        }
        bvs = new PrimitiveBlockValSet(DataType.LONG, null, values, null, null);
        break;
      }
      case FLOAT: {
        float[] values = new float[_length];
        for (int i = 0; i < _length; i++) {
          values[i] = rng.nextFloat() * 2_000_000.0f - 1_000_000.0f;
        }
        bvs = new PrimitiveBlockValSet(DataType.FLOAT, null, null, values, null);
        break;
      }
      case DOUBLE: {
        double[] values = new double[_length];
        for (int i = 0; i < _length; i++) {
          values[i] = rng.nextDouble() * 2_000_000.0 - 1_000_000.0;
        }
        bvs = new PrimitiveBlockValSet(DataType.DOUBLE, null, null, null, values);
        break;
      }
      default:
        throw new IllegalStateException("unsupported _type: " + _type);
    }
    _blockValSetMap = Collections.singletonMap(EXPR, bvs);

    boolean isMin = "MIN".equals(_op);
    switch (_engine) {
      case "native":
        if (!PinotNativeAgg.isAvailable()) {
          throw new IllegalStateException("native engine requested but pinot-native library is "
              + "not loadable. Build it with './mvnw -pl pinot-native package' and ensure "
              + "-Dpinot.native.lib.path is passed to the JMH JVM.");
        }
        _function = isMin
            ? new NativeMinAggregationFunction(Collections.singletonList(EXPR), false)
            : new NativeMaxAggregationFunction(Collections.singletonList(EXPR), false);
        break;
      case "native-scalar":
        if (!PinotNativeAgg.isAvailable()) {
          throw new IllegalStateException("native engine requested but pinot-native library is "
              + "not loadable. Build it with './mvnw -pl pinot-native package' and ensure "
              + "-Dpinot.native.lib.path is passed to the JMH JVM.");
        }
        _function = isMin
            ? new NativeScalarMinAggregationFunction(Collections.singletonList(EXPR), false)
            : new NativeScalarMaxAggregationFunction(Collections.singletonList(EXPR), false);
        break;
      case "java":
        _function = isMin
            ? new MinAggregationFunction(Collections.singletonList(EXPR), false)
            : new MaxAggregationFunction(Collections.singletonList(EXPR), false);
        break;
      default:
        throw new IllegalStateException("unknown _engine: " + _engine);
    }
    _resultHolder = _function.createAggregationResultHolder();
  }

  @Benchmark
  public double aggregate() {
    // MIN holder seed = +INF, MAX holder seed = -INF (matches Pinot's Java parent defaults).
    _resultHolder.setValue("MIN".equals(_op) ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY);
    _function.aggregate(_length, _resultHolder, _blockValSetMap);
    return _resultHolder.getDoubleResult();
  }

  @Nullable
  private static String resolveNativeLibPath() {
    String os = System.getProperty("os.name", "").toLowerCase();
    String libFile;
    if (os.contains("mac") || os.contains("darwin")) {
      libFile = "libpinot_native.dylib";
    } else if (os.contains("windows")) {
      libFile = "pinot_native.dll";
    } else {
      libFile = "libpinot_native.so";
    }
    Path candidate =
        Paths.get("..", "pinot-native", "native", "target", "release", libFile).toAbsolutePath();
    return Files.exists(candidate) ? candidate.toString() : null;
  }

  /**
   * Self-contained {@link BlockValSet} for primitive single-value columns. Duplicated from
   * {@link BenchmarkNativeSumAggregation} so each benchmark file is self-sufficient.
   */
  private static final class PrimitiveBlockValSet implements BlockValSet {
    private final DataType _bvsType;
    @Nullable
    private final int[] _intValues;
    @Nullable
    private final long[] _longValues;
    @Nullable
    private final float[] _floatValues;
    @Nullable
    private final double[] _doubleValues;

    PrimitiveBlockValSet(DataType type, @Nullable int[] intValues, @Nullable long[] longValues,
        @Nullable float[] floatValues, @Nullable double[] doubleValues) {
      _bvsType = type;
      _intValues = intValues;
      _longValues = longValues;
      _floatValues = floatValues;
      _doubleValues = doubleValues;
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap() {
      return null;
    }

    @Override
    public DataType getValueType() {
      return _bvsType;
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
        throw new UnsupportedOperationException("no int values");
      }
      return _intValues;
    }

    @Override
    public long[] getLongValuesSV() {
      if (_longValues == null) {
        throw new UnsupportedOperationException("no long values");
      }
      return _longValues;
    }

    @Override
    public float[] getFloatValuesSV() {
      if (_floatValues == null) {
        throw new UnsupportedOperationException("no float values");
      }
      return _floatValues;
    }

    @Override
    public double[] getDoubleValuesSV() {
      if (_doubleValues == null) {
        throw new UnsupportedOperationException("no double values");
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
}
