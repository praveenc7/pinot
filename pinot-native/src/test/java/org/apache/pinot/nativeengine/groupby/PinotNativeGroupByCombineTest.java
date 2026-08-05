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
package org.apache.pinot.nativeengine.groupby;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.pinot.nativeengine.groupby.PinotNativeGroupBy.NativeAggKind;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;


/**
 * End-to-end test for the native server-combine JNI surface ({@link PinotNativeGroupByCombine}),
 * over <b>all key types</b>: feed synthetic per-segment partials across the boundary, run the
 * parallel merge, and verify the combined result against a Java reference.
 *
 * <p>Query shape: {@code SELECT key, SUM(v)::double, MIN(v)::int, COUNT(*) GROUP BY key}, keyed by
 * LONG / DOUBLE / STRING, on both Path C backends where applicable.
 */
public class PinotNativeGroupByCombineTest {
  private static final byte[] AGG_KINDS = NativeAggKind.toBytes(
      NativeAggKind.SUM_DOUBLE, NativeAggKind.MIN_INT, NativeAggKind.COUNT);
  private static final int NUM_SEGMENTS = 40;
  private static final int CARDINALITY = 600;

  @BeforeClass
  public void skipIfNativeUnavailable() {
    if (!PinotNativeGroupByCombine.isAvailable()) {
      throw new SkipException("Pinot native library not available on this platform");
    }
  }

  @DataProvider(name = "keyTypesAndBackends")
  public Object[][] keyTypesAndBackends() {
    return new Object[][]{
        {"long-hashbrown", PinotNativeGroupByCombine.KEY_TYPE_LONG, true},
        {"double-hashbrown", PinotNativeGroupByCombine.KEY_TYPE_DOUBLE, true},
        {"string", PinotNativeGroupByCombine.KEY_TYPE_STRING, false},
    };
  }

  /** Per-group partial: (sum as double, min as int, count as long). */
  private static final class Agg {
    double _sum;
    int _min = Integer.MAX_VALUE;
    long _count;
  }

  @Test(dataProvider = "keyTypesAndBackends")
  public void combineMatchesJavaReference(String name, int keyType, boolean useHashbrown) {
    Map<Object, Agg> reference = new HashMap<>();
    long handle = feedAndFinish(keyType, useHashbrown, reference);
    try {
      int g = PinotNativeGroupByCombine.numGroups(handle);
      assertEquals(g, reference.size(), name + ": combined group count");

      double[] sums = new double[g];
      int[] mins = new int[g];
      long[] counts = new long[g];
      PinotNativeGroupByCombine.extractAggDouble(handle, 0, sums);
      PinotNativeGroupByCombine.extractAggInt(handle, 1, mins);
      PinotNativeGroupByCombine.extractAggLong(handle, 2, counts);
      Object[] keys = extractKeys(handle, keyType, g);

      for (int i = 0; i < g; i++) {
        Agg ref = reference.get(keys[i]);
        assertEquals(sums[i], ref._sum, name + ": sum for " + keys[i]);
        assertEquals(mins[i], ref._min, name + ": min for " + keys[i]);
        assertEquals(counts[i], ref._count, name + ": count for " + keys[i]);
      }
    } finally {
      PinotNativeGroupByCombine.destroy(handle);
    }
  }

  /**
   * Feed {@link #NUM_SEGMENTS} synthetic segment partials across the boundary and run the parallel
   * merge, returning the finished handle. Fills {@code reference} with the Java-side ground truth.
   */
  private long feedAndFinish(int keyType, boolean useHashbrown, Map<Object, Agg> reference) {
    Random rng = new Random(20260620L);
    long handle = PinotNativeGroupByCombine.createCombine(AGG_KINDS, keyType);
    assertNotEquals(handle, 0L, "create failed");
    for (int s = 0; s < NUM_SEGMENTS; s++) {
      LinkedHashMap<Object, Agg> seg = new LinkedHashMap<>();
      int rows = 1 + rng.nextInt(300);
      for (int r = 0; r < rows; r++) {
        Object key = makeKey(keyType, rng.nextInt(CARDINALITY));
        int v = rng.nextInt(1_000_000) - 500_000;
        accumulate(seg.computeIfAbsent(key, k -> new Agg()), v);
        accumulate(reference.computeIfAbsent(key, k -> new Agg()), v);
      }
      feedPartial(handle, keyType, seg);
    }
    PinotNativeGroupByCombine.finish(handle, 6);
    return handle;
  }

  // --- select() : ORDER BY top-K / no-ORDER-BY cap (design §26.3/§26.7) ---

  /** No ORDER BY: {@code select} caps the result to {@code resultSize} groups (arbitrary subset). */
  @Test(dataProvider = "keyTypesAndBackends")
  public void selectNoOrderByCapsCount(String name, int keyType, boolean useHashbrown) {
    Map<Object, Agg> reference = new HashMap<>();
    long handle = feedAndFinish(keyType, useHashbrown, reference);
    try {
      int cap = 50;
      PinotNativeGroupByCombine.select(handle, new int[0], new boolean[0], cap);
      int g = PinotNativeGroupByCombine.numGroups(handle);
      assertEquals(g, Math.min(cap, reference.size()), name + ": capped group count");
      // The subset is arbitrary (§26.7-3), but every kept group must carry correct aggregates.
      for (Row row : extractRows(handle, keyType)) {
        Agg ref = reference.get(row._key);
        assertTrue(ref != null, name + ": kept key not in reference: " + row._key);
        assertEquals(row._sum, ref._sum, name + ": sum for " + row._key);
        assertEquals(row._min, ref._min, name + ": min for " + row._key);
        assertEquals(row._count, ref._count, name + ": count for " + row._key);
      }
    } finally {
      PinotNativeGroupByCombine.destroy(handle);
    }
  }

  /** ORDER BY group key asc/desc: exact top-K, returned sorted, aggregates preserved. */
  @Test(dataProvider = "keyTypesAndBackends")
  public void selectOrderByKeyTopK(String name, int keyType, boolean useHashbrown) {
    for (boolean ascending : new boolean[]{true, false}) {
      Map<Object, Agg> reference = new HashMap<>();
      long handle = feedAndFinish(keyType, useHashbrown, reference);
      try {
        int k = 25;
        PinotNativeGroupByCombine.select(handle, new int[]{PinotNativeGroupByCombine.ORDER_REF_KEY},
            new boolean[]{ascending}, k);
        List<Row> rows = extractRows(handle, keyType);
        List<Object> expected =
            expectedOrderedKeys(reference, keyType, PinotNativeGroupByCombine.ORDER_REF_KEY, ascending, k);
        String tag = name + " ascending=" + ascending;
        assertEquals(rows.size(), expected.size(), tag + ": topK size");
        for (int i = 0; i < rows.size(); i++) {
          assertEquals(rows.get(i)._key, expected.get(i), tag + ": key at rank " + i);
          Agg ref = reference.get(rows.get(i)._key);
          assertEquals(rows.get(i)._sum, ref._sum, tag + ": sum for " + rows.get(i)._key);
          assertEquals(rows.get(i)._count, ref._count, tag + ": count for " + rows.get(i)._key);
        }
      } finally {
        PinotNativeGroupByCombine.destroy(handle);
      }
    }
  }

  /** ORDER BY COUNT(*) desc/asc (aggregation-result column): exact top-K with key-asc tie-break. */
  @Test(dataProvider = "keyTypesAndBackends")
  public void selectOrderByCountTopK(String name, int keyType, boolean useHashbrown) {
    int countAggRef = 2; // COUNT is aggregation index 2 in AGG_KINDS
    for (boolean ascending : new boolean[]{false, true}) {
      Map<Object, Agg> reference = new HashMap<>();
      long handle = feedAndFinish(keyType, useHashbrown, reference);
      try {
        int k = 25;
        PinotNativeGroupByCombine.select(handle, new int[]{countAggRef}, new boolean[]{ascending}, k);
        List<Row> rows = extractRows(handle, keyType);
        List<Object> expected = expectedOrderedKeys(reference, keyType, countAggRef, ascending, k);
        String tag = name + " ascending=" + ascending;
        assertEquals(rows.size(), expected.size(), tag + ": topK size");
        for (int i = 0; i < rows.size(); i++) {
          Agg ref = reference.get(rows.get(i)._key);
          assertEquals(rows.get(i)._key, expected.get(i),
              tag + ": key at rank " + i + " (count=" + ref._count + ")");
          assertEquals(rows.get(i)._count, ref._count, tag + ": count for " + rows.get(i)._key);
        }
        for (int i = 1; i < rows.size(); i++) {
          long prev = rows.get(i - 1)._count;
          long cur = rows.get(i)._count;
          assertTrue(ascending ? prev <= cur : prev >= cur, tag + ": count monotonicity at " + i);
        }
      } finally {
        PinotNativeGroupByCombine.destroy(handle);
      }
    }
  }

  /** One combined group's key + aggregate results, drained in native storage order. */
  private static final class Row {
    Object _key;
    double _sum;
    int _min;
    long _count;
  }

  private static List<Row> extractRows(long handle, int keyType) {
    int g = PinotNativeGroupByCombine.numGroups(handle);
    double[] sums = new double[g];
    int[] mins = new int[g];
    long[] counts = new long[g];
    PinotNativeGroupByCombine.extractAggDouble(handle, 0, sums);
    PinotNativeGroupByCombine.extractAggInt(handle, 1, mins);
    PinotNativeGroupByCombine.extractAggLong(handle, 2, counts);
    Object[] keys = extractKeys(handle, keyType, g);
    List<Row> rows = new ArrayList<>(g);
    for (int i = 0; i < g; i++) {
      Row row = new Row();
      row._key = keys[i];
      row._sum = sums[i];
      row._min = mins[i];
      row._count = counts[i];
      rows.add(row);
    }
    return rows;
  }

  /**
   * The exact ordered top-{@code k} keys the native {@code select} must return, computed
   * independently from the Java reference. Mirrors the native comparator: the order term
   * ({@code aggRef == ORDER_REF_KEY} → group key, else COUNT) in the requested direction, with a
   * deterministic <b>key-ascending</b> tie-break (group keys are unique here, so the further
   * group-index tie-break never triggers).
   */
  private static List<Object> expectedOrderedKeys(Map<Object, Agg> reference, int keyType, int aggRef,
      boolean ascending, int k) {
    Comparator<Object> keyCmp = keyComparator(keyType);
    Comparator<Map.Entry<Object, Agg>> cmp;
    if (aggRef == PinotNativeGroupByCombine.ORDER_REF_KEY) {
      Comparator<Map.Entry<Object, Agg>> byKey =
          Comparator.comparing((Map.Entry<Object, Agg> e) -> e.getKey(), keyCmp);
      cmp = ascending ? byKey : byKey.reversed();
    } else {
      Comparator<Map.Entry<Object, Agg>> byCount =
          Comparator.comparingLong((Map.Entry<Object, Agg> e) -> e.getValue()._count);
      cmp = (ascending ? byCount : byCount.reversed())
          .thenComparing((Map.Entry<Object, Agg> e) -> e.getKey(), keyCmp);
    }
    return reference.entrySet().stream().sorted(cmp).limit(k).map(Map.Entry::getKey)
        .collect(Collectors.toList());
  }

  /** Group-key total order matching the native side: numeric for LONG/DOUBLE, byte/UTF-8 for STRING. */
  private static Comparator<Object> keyComparator(int keyType) {
    switch (keyType) {
      case PinotNativeGroupByCombine.KEY_TYPE_LONG:
        return Comparator.comparingLong(o -> (Long) o);
      case PinotNativeGroupByCombine.KEY_TYPE_DOUBLE:
        return Comparator.comparingDouble(o -> (Double) o);
      default:
        return Comparator.comparing(o -> (String) o); // ASCII keys → UTF-16 order == UTF-8 byte order
    }
  }

  private static void accumulate(Agg a, int v) {
    a._sum += v;
    a._min = Math.min(a._min, v);
    a._count += 1;
  }

  private static Object makeKey(int keyType, int raw) {
    switch (keyType) {
      case PinotNativeGroupByCombine.KEY_TYPE_LONG:
        return (long) raw;
      case PinotNativeGroupByCombine.KEY_TYPE_DOUBLE:
        return raw * 0.25;
      default:
        return "k-" + raw;
    }
  }

  private static void feedPartial(long handle, int keyType, LinkedHashMap<Object, Agg> seg) {
    int g = seg.size();
    double[] sums = new double[g];
    int[] mins = new int[g];
    long[] counts = new long[g];
    int i = 0;
    List<Object> keyList = new ArrayList<>(seg.keySet());
    for (Object k : keyList) {
      Agg a = seg.get(k);
      sums[i] = a._sum;
      mins[i] = a._min;
      counts[i] = a._count;
      i++;
    }
    switch (keyType) {
      case PinotNativeGroupByCombine.KEY_TYPE_LONG: {
        long[] keys = new long[g];
        for (int j = 0; j < g; j++) {
          keys[j] = (Long) keyList.get(j);
        }
        PinotNativeGroupByCombine.beginPartialLong(handle, keys);
        break;
      }
      case PinotNativeGroupByCombine.KEY_TYPE_DOUBLE: {
        double[] keys = new double[g];
        for (int j = 0; j < g; j++) {
          keys[j] = (Double) keyList.get(j);
        }
        PinotNativeGroupByCombine.beginPartialDouble(handle, keys);
        break;
      }
      default: {
        StringBuilder buffer = new StringBuilder();
        int[] offsets = new int[g + 1];
        for (int j = 0; j < g; j++) {
          buffer.append((String) keyList.get(j));
          offsets[j + 1] = buffer.toString().getBytes(StandardCharsets.UTF_8).length;
        }
        PinotNativeGroupByCombine.beginPartialString(handle, buffer.toString().getBytes(StandardCharsets.UTF_8),
            offsets);
        break;
      }
    }
    PinotNativeGroupByCombine.setAggDouble(handle, 0, sums);
    PinotNativeGroupByCombine.setAggInt(handle, 1, mins);
    PinotNativeGroupByCombine.setAggLong(handle, 2, counts);
    PinotNativeGroupByCombine.commitPartial(handle);
  }

  private static Object[] extractKeys(long handle, int keyType, int g) {
    Object[] result = new Object[g];
    switch (keyType) {
      case PinotNativeGroupByCombine.KEY_TYPE_LONG: {
        long[] keys = new long[g];
        PinotNativeGroupByCombine.extractKeysLong(handle, keys);
        for (int i = 0; i < g; i++) {
          result[i] = keys[i];
        }
        break;
      }
      case PinotNativeGroupByCombine.KEY_TYPE_DOUBLE: {
        double[] keys = new double[g];
        PinotNativeGroupByCombine.extractKeysDouble(handle, keys);
        for (int i = 0; i < g; i++) {
          result[i] = keys[i];
        }
        break;
      }
      default: {
        int totalBytes = PinotNativeGroupByCombine.stringKeysTotalBytes(handle);
        byte[] buffer = new byte[totalBytes];
        int[] offsets = new int[g + 1];
        PinotNativeGroupByCombine.extractKeysString(handle, buffer, offsets);
        for (int i = 0; i < g; i++) {
          result[i] = new String(buffer, offsets[i], offsets[i + 1] - offsets[i], StandardCharsets.UTF_8);
        }
        break;
      }
    }
    return result;
  }

  // --- Multi-column combine (raw-value tuple key, design §17.9 / §23.1) ---

  private static final byte[] MULTI_AGG_KINDS = NativeAggKind.toBytes(NativeAggKind.SUM_DOUBLE, NativeAggKind.COUNT);

  @DataProvider(name = "multiColStrategies")
  public Object[][] multiColStrategies() {
    return new Object[][]{
        {"columnwise", PinotNativeGroupByCombine.MULTICOL_STRATEGY_COLUMNWISE},
        {"packed", PinotNativeGroupByCombine.MULTICOL_STRATEGY_PACKED},
    };
  }

  /**
   * A (LONG, STRING) composite-key combine across many synthetic segment partials: verify the merged
   * per-group SUM/COUNT against a tuple-keyed Java reference, then that {@code select} ORDER BY the
   * LONG key column ascending returns the exact top-K. Run under both merge strategies (the STRING
   * column makes PackedKeys fall back to column-wise — both must match the reference).
   */
  @Test(dataProvider = "multiColStrategies")
  public void multiColumnCombineMatchesReferenceAndSelects(String name, int strategy) {
    byte[] colTypes = {
        (byte) PinotNativeGroupByCombine.KEY_TYPE_LONG, (byte) PinotNativeGroupByCombine.KEY_TYPE_STRING
    };
    int[] colWidths = {64, 0};
    long handle = PinotNativeGroupByCombine.createCombineMulti(MULTI_AGG_KINDS, colTypes, colWidths, strategy);
    assertNotEquals(handle, 0L, name + ": multi-column create failed");
    Random rng = new Random(20260723L);
    Map<List<Object>, double[]> reference = new HashMap<>(); // [longKey, strKey] -> {sum, count}
    try {
      for (int s = 0; s < NUM_SEGMENTS; s++) {
        LinkedHashMap<List<Object>, double[]> seg = new LinkedHashMap<>();
        int rows = 1 + rng.nextInt(200);
        for (int r = 0; r < rows; r++) {
          long k0 = rng.nextInt(40);
          String k1 = "s" + rng.nextInt(8);
          int v = rng.nextInt(1000);
          List<Object> key = Arrays.asList(k0, k1);
          seg.computeIfAbsent(key, k -> new double[2]);
          double[] segAcc = seg.get(key);
          segAcc[0] += v;
          segAcc[1] += 1;
          double[] refAcc = reference.computeIfAbsent(key, k -> new double[2]);
          refAcc[0] += v;
          refAcc[1] += 1;
        }
        feedMultiColumnPartial(handle, seg);
      }

      PinotNativeGroupByCombine.finish(handle, 6);
      int g = PinotNativeGroupByCombine.numGroups(handle);
      assertEquals(g, reference.size(), "multi-column combined group count");
      verifyMultiColumn(handle, g, reference);

      // ORDER BY the LONG key column (colIdx 0) ascending, LIMIT 10 → exact top-10 by k0.
      int limit = 10;
      PinotNativeGroupByCombine.select(handle, new int[]{PinotNativeGroupByCombine.ORDER_REF_KEY}, new boolean[]{true},
          limit);
      int gSel = PinotNativeGroupByCombine.numGroups(handle);
      assertEquals(gSel, Math.min(limit, reference.size()), "selected group count");
      long[] longKeys = new long[gSel];
      PinotNativeGroupByCombine.extractKeyColumnLong(handle, 0, longKeys);
      for (int i = 1; i < gSel; i++) {
        assertTrue(longKeys[i - 1] <= longKeys[i], "long key column not ascending after select");
      }
      // Every selected group's aggregates still match the reference.
      verifyMultiColumn(handle, gSel, reference);
    } finally {
      PinotNativeGroupByCombine.destroy(handle);
    }
  }

  /**
   * A (LONG, DOUBLE) all-fixed-width composite key: under {@code MULTICOL_STRATEGY_PACKED} this
   * genuinely packs into a 128-bit key (both columns 64 bits); under column-wise it compares
   * per-column. Both must match the tuple reference — proving the packed i128 path end-to-end.
   */
  @Test(dataProvider = "multiColStrategies")
  public void multiColumnPackedFixedWidthMatchesReference(String name, int strategy) {
    byte[] colTypes = {
        (byte) PinotNativeGroupByCombine.KEY_TYPE_LONG, (byte) PinotNativeGroupByCombine.KEY_TYPE_DOUBLE
    };
    int[] colWidths = {64, 64};
    long handle = PinotNativeGroupByCombine.createCombineMulti(MULTI_AGG_KINDS, colTypes, colWidths, strategy);
    assertNotEquals(handle, 0L, name + ": create failed");
    Random rng = new Random(20260724L);
    Map<List<Object>, double[]> reference = new HashMap<>();
    try {
      for (int s = 0; s < NUM_SEGMENTS; s++) {
        LinkedHashMap<List<Object>, double[]> seg = new LinkedHashMap<>();
        int rows = 1 + rng.nextInt(200);
        for (int r = 0; r < rows; r++) {
          long k0 = rng.nextInt(50) - 25;
          double k1 = (rng.nextInt(20) - 10) * 0.5;
          int v = rng.nextInt(1000);
          List<Object> key = Arrays.asList(k0, k1);
          seg.computeIfAbsent(key, k -> new double[2]);
          seg.get(key)[0] += v;
          seg.get(key)[1] += 1;
          reference.computeIfAbsent(key, k -> new double[2]);
          reference.get(key)[0] += v;
          reference.get(key)[1] += 1;
        }
        int g = seg.size();
        long[] k0 = new long[g];
        double[] k1 = new double[g];
        double[] sums = new double[g];
        long[] counts = new long[g];
        int i = 0;
        for (Map.Entry<List<Object>, double[]> e : seg.entrySet()) {
          k0[i] = (Long) e.getKey().get(0);
          k1[i] = (Double) e.getKey().get(1);
          sums[i] = e.getValue()[0];
          counts[i] = (long) e.getValue()[1];
          i++;
        }
        PinotNativeGroupByCombine.beginPartialMulti(handle);
        PinotNativeGroupByCombine.setKeyLong(handle, 0, k0);
        PinotNativeGroupByCombine.setKeyDouble(handle, 1, k1);
        PinotNativeGroupByCombine.setAggDouble(handle, 0, sums);
        PinotNativeGroupByCombine.setAggLong(handle, 1, counts);
        PinotNativeGroupByCombine.commitPartial(handle);
      }
      PinotNativeGroupByCombine.finish(handle, 6);
      int g = PinotNativeGroupByCombine.numGroups(handle);
      assertEquals(g, reference.size(), name + ": group count");
      long[] longKeys = new long[g];
      double[] doubleKeys = new double[g];
      double[] sums = new double[g];
      long[] counts = new long[g];
      PinotNativeGroupByCombine.extractKeyColumnLong(handle, 0, longKeys);
      PinotNativeGroupByCombine.extractKeyColumnDouble(handle, 1, doubleKeys);
      PinotNativeGroupByCombine.extractAggDouble(handle, 0, sums);
      PinotNativeGroupByCombine.extractAggLong(handle, 1, counts);
      for (int i = 0; i < g; i++) {
        double[] ref = reference.get(Arrays.asList(longKeys[i], doubleKeys[i]));
        assertNotEquals(ref, null, name + ": group (" + longKeys[i] + "," + doubleKeys[i] + ") missing");
        assertEquals(sums[i], ref[0], name + ": sum");
        assertEquals(counts[i], (long) ref[1], name + ": count");
      }
    } finally {
      PinotNativeGroupByCombine.destroy(handle);
    }
  }

  private static void feedMultiColumnPartial(long handle, LinkedHashMap<List<Object>, double[]> seg) {
    int g = seg.size();
    long[] k0 = new long[g];
    StringBuilder buffer = new StringBuilder();
    int[] offsets = new int[g + 1];
    double[] sums = new double[g];
    long[] counts = new long[g];
    int i = 0;
    for (Map.Entry<List<Object>, double[]> e : seg.entrySet()) {
      k0[i] = (Long) e.getKey().get(0);
      buffer.append((String) e.getKey().get(1));
      offsets[i + 1] = buffer.toString().getBytes(StandardCharsets.UTF_8).length;
      sums[i] = e.getValue()[0];
      counts[i] = (long) e.getValue()[1];
      i++;
    }
    PinotNativeGroupByCombine.beginPartialMulti(handle);
    PinotNativeGroupByCombine.setKeyLong(handle, 0, k0);
    PinotNativeGroupByCombine.setKeyString(handle, 1, buffer.toString().getBytes(StandardCharsets.UTF_8), offsets);
    PinotNativeGroupByCombine.setAggDouble(handle, 0, sums);
    PinotNativeGroupByCombine.setAggLong(handle, 1, counts);
    PinotNativeGroupByCombine.commitPartial(handle);
  }

  private static void verifyMultiColumn(long handle, int g, Map<List<Object>, double[]> reference) {
    long[] longKeys = new long[g];
    PinotNativeGroupByCombine.extractKeyColumnLong(handle, 0, longKeys);
    int totalBytes = PinotNativeGroupByCombine.keyColumnStringTotalBytes(handle, 1);
    byte[] strBuf = new byte[totalBytes];
    int[] strOffsets = new int[g + 1];
    PinotNativeGroupByCombine.extractKeyColumnString(handle, 1, strBuf, strOffsets);
    double[] sums = new double[g];
    long[] counts = new long[g];
    PinotNativeGroupByCombine.extractAggDouble(handle, 0, sums);
    PinotNativeGroupByCombine.extractAggLong(handle, 1, counts);
    for (int i = 0; i < g; i++) {
      String strKey = new String(strBuf, strOffsets[i], strOffsets[i + 1] - strOffsets[i], StandardCharsets.UTF_8);
      double[] ref = reference.get(Arrays.asList(longKeys[i], strKey));
      assertNotEquals(ref, null, "group (" + longKeys[i] + "," + strKey + ") not in reference");
      assertEquals(sums[i], ref[0], "sum for (" + longKeys[i] + "," + strKey + ")");
      assertEquals(counts[i], (long) ref[1], "count for (" + longKeys[i] + "," + strKey + ")");
    }
  }
}
