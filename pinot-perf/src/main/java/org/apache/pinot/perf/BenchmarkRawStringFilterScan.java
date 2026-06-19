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
package org.apache.pinot.perf;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.core.operator.filter.predicate.EqualsPredicateEvaluatorFactory;
import org.apache.pinot.core.operator.filter.predicate.InPredicateEvaluatorFactory;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV4;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkForwardIndexReaderV4;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;


/**
 * Benchmarks a raw (non-dictionary) SV STRING column filter scan for EQ and IN predicates, comparing the legacy String
 * path ({@code reader.getString(doc) -> applySV(String)}) against the byte path
 * ({@code reader.getBytes(doc) -> applySV(byte[])}). Both paths read from the same LZ4-compressed forward index, so the
 * chunk decompression cost is included in both and the measured delta isolates the String-materialization + hashing
 * that the byte path removes (plus, for IN, the length-gate that rejects most non-matching rows without hashing).
 *
 * Values are variable-length so the IN length-gate is exercised; a fixed-width column would show a smaller IN delta
 * (gate inactive) but the same EQ delta.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class BenchmarkRawStringFilterScan {
  private static final File TARGET_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkRawStringFilterScan");
  private static final ExpressionContext COLUMN = ExpressionContext.forIdentifier("col");
  private static final int MAX_CHUNK_SIZE = 1 << 20;

  @State(Scope.Benchmark)
  public abstract static class BaseState {
    // Values are all-unique, so the scanned row count equals the cardinality.
    @Param({"1000000", "2000000", "3000000"})
    int _cardinality;
    // Base value length in chars; actual lengths vary by a few so the length-gate is realistic.
    @Param("48")
    int _valueLength;

    String[] _distinctValues;
    File _file;
    PinotDataBuffer _buffer;
    VarByteChunkForwardIndexReaderV4 _reader;
    PredicateEvaluator _eval;

    void baseSetup()
        throws Exception {
      FileUtils.forceMkdir(TARGET_DIR);
      _distinctValues = new String[_cardinality];
      for (int i = 0; i < _cardinality; i++) {
        _distinctValues[i] = makeValue(i, _valueLength);
      }
      _file = new File(TARGET_DIR, UUID.randomUUID().toString());
      try (VarByteChunkForwardIndexWriterV4 writer =
          new VarByteChunkForwardIndexWriterV4(_file, ChunkCompressionType.LZ4, MAX_CHUNK_SIZE)) {
        for (int i = 0; i < _cardinality; i++) {
          writer.putBytes(_distinctValues[i].getBytes(StandardCharsets.UTF_8));
        }
      }
      _buffer = PinotDataBuffer.loadBigEndianFile(_file);
      _reader = new VarByteChunkForwardIndexReaderV4(_buffer, FieldSpec.DataType.STRING, true);
    }

    @TearDown(Level.Trial)
    public void tearDown()
        throws Exception {
      if (_reader != null) {
        _reader.close();
      }
      if (_buffer != null) {
        _buffer.close();
      }
      FileUtils.deleteQuietly(_file);
      FileUtils.deleteQuietly(TARGET_DIR);
    }

    // Unique, deterministic, variable-length value for row i.
    private static String makeValue(int i, int baseLength) {
      int length = baseLength + (i & 0x3); // small spread of lengths
      StringBuilder sb = new StringBuilder(length);
      sb.append("urn:li:member:").append(i);
      while (sb.length() < length) {
        sb.append((char) ('a' + ((i + sb.length()) % 26)));
      }
      return sb.length() > length ? sb.substring(0, length) : sb.toString();
    }
  }

  @State(Scope.Benchmark)
  public static class EqState extends BaseState {
    @Setup(Level.Trial)
    public void setup()
        throws Exception {
      baseSetup();
      EqPredicate predicate = new EqPredicate(COLUMN, _distinctValues[_cardinality / 2]);
      _eval = EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, FieldSpec.DataType.STRING);
    }
  }

  @State(Scope.Benchmark)
  public static class InState extends BaseState {
    @Param({"10", "25", "50", "100"})
    int _inSize;

    @Setup(Level.Trial)
    public void setup()
        throws Exception {
      baseSetup();
      // Pick _inSize present values spread across the distinct set (the "IN a few common values" shape).
      List<String> values = new ArrayList<>(_inSize);
      int step = Math.max(1, _cardinality / _inSize);
      for (int k = 0; k < _inSize; k++) {
        values.add(_distinctValues[(k * step) % _cardinality]);
      }
      InPredicate predicate = new InPredicate(COLUMN, values);
      _eval = InPredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, FieldSpec.DataType.STRING);
    }
  }

  @Benchmark
  public int eqStringPath(EqState state)
      throws Exception {
    return scanStringPath(state);
  }

  @Benchmark
  public int eqBytePath(EqState state)
      throws Exception {
    return scanBytePath(state);
  }

  @Benchmark
  public int inStringPath(InState state)
      throws Exception {
    return scanStringPath(state);
  }

  @Benchmark
  public int inBytePath(InState state)
      throws Exception {
    return scanBytePath(state);
  }

  private static int scanStringPath(BaseState state)
      throws Exception {
    PredicateEvaluator eval = state._eval;
    VarByteChunkForwardIndexReaderV4 reader = state._reader;
    int matches = 0;
    try (VarByteChunkForwardIndexReaderV4.ReaderContext context = reader.createContext()) {
      for (int i = 0; i < state._cardinality; i++) {
        if (eval.applySV(reader.getString(i, context))) {
          matches++;
        }
      }
    }
    return matches;
  }

  private static int scanBytePath(BaseState state)
      throws Exception {
    PredicateEvaluator eval = state._eval;
    VarByteChunkForwardIndexReaderV4 reader = state._reader;
    int matches = 0;
    try (VarByteChunkForwardIndexReaderV4.ReaderContext context = reader.createContext()) {
      for (int i = 0; i < state._cardinality; i++) {
        if (eval.applySV(reader.getBytes(i, context))) {
          matches++;
        }
      }
    }
    return matches;
  }
}
