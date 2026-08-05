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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.IntToLongFunction;
import org.apache.pinot.nativeengine.groupby.PinotNativeGroupBy.NativeAggKind;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;


/**
 * Differential test for the Phase 1.D-core-D (Task #47) native GROUP BY surface.
 *
 * <p>Tests both backends (HashbrownTable and HashbrownTable) against a Java reference
 * computed via {@link HashMap}. The reference exactly matches Pinot's
 * {@code SumAggregationFunction.aggregateGroupBySV} semantics for the
 * {@code SUM(longCol) GROUP BY dictEncodedIntCol} case.
 *
 * <p>{@link DataProvider} drives tests against the supported hashbrown backend.
 */
public class PinotNativeGroupByTest {

  @BeforeClass
  public void skipIfNativeUnavailable() {
    if (!PinotNativeGroupBy.isAvailable()) {
      throw new SkipException("Pinot native library not available on this platform; "
          + "set -Dpinot.native.lib.path=<path-to-libpinot_native> to enable.");
    }
  }

  /** Test parameter: supported backend factory. */
  @DataProvider(name = "backends")
  public Object[][] backends() {
    IntToLongFunction createHashbrown = PinotNativeGroupBy::createHashbrown;
    return new Object[][] {{"hashbrown", createHashbrown}};
  }

  // --- Lifecycle smoke tests ---------------------------------------------

  @Test(dataProvider = "backends")
  public void createAndDestroy(String name, IntToLongFunction factory) {
    long handle = factory.applyAsLong(64);
    try {
      assertNotEquals(handle, 0L, name + ": expected non-zero handle from create");
      assertEquals(PinotNativeGroupBy.numGroups(handle), 0,
          name + ": empty driver should report 0 groups");
    } finally {
      PinotNativeGroupBy.destroy(handle);
    }
  }

  @Test
  public void destroyOnZeroIsNoop() {
    // Should not crash. (No assertion needed; just exercise the path.)
    PinotNativeGroupBy.destroy(0L);
  }

  @Test
  public void numGroupsOnZeroReturnsZeroOrNegative() {
    int result = PinotNativeGroupBy.numGroups(0L);
    // Per FFI contract: 0 for zero handle (treated as empty). -1 on error.
    // Both are acceptable for this safety probe.
    org.testng.Assert.assertTrue(result <= 0,
        "numGroups(0) should be <= 0, got " + result);
  }

  // --- Single-block correctness ------------------------------------------

  @Test(dataProvider = "backends")
  public void singleBlockEmpty(String name, IntToLongFunction factory) {
    long handle = factory.applyAsLong(0);
    try {
      PinotNativeGroupBy.processBlock(handle, new int[0], new long[0], 0);
      assertEquals(PinotNativeGroupBy.numGroups(handle), 0, name);
    } finally {
      PinotNativeGroupBy.destroy(handle);
    }
  }

  @Test(dataProvider = "backends")
  public void singleBlockSingleDictId(String name, IntToLongFunction factory) {
    long handle = factory.applyAsLong(8);
    try {
      int[] dictIds = {42, 42, 42, 42, 42};
      long[] values = {10L, 20L, 30L, 40L, 50L};
      PinotNativeGroupBy.processBlock(handle, dictIds, values, 5);

      assertEquals(PinotNativeGroupBy.numGroups(handle), 1, name);
      int[] outKeys = new int[1];
      long[] outSums = new long[1];
      PinotNativeGroupBy.extractKeys(handle, outKeys);
      PinotNativeGroupBy.extractSums(handle, outSums);
      assertEquals(outKeys[0], 42, name);
      assertEquals(outSums[0], 150L, name);
    } finally {
      PinotNativeGroupBy.destroy(handle);
    }
  }

  @Test(dataProvider = "backends")
  public void singleBlockMixedDictIds(String name, IntToLongFunction factory) {
    long handle = factory.applyAsLong(8);
    try {
      int[] dictIds = {1, 2, 1, 3, 2, 1};
      long[] values = {10L, 20L, 30L, 40L, 50L, 60L};
      PinotNativeGroupBy.processBlock(handle, dictIds, values, 6);

      // Insertion order: 1 first, then 2, then 3.
      assertEquals(PinotNativeGroupBy.numGroups(handle), 3, name);
      int[] outKeys = new int[3];
      long[] outSums = new long[3];
      PinotNativeGroupBy.extractKeys(handle, outKeys);
      PinotNativeGroupBy.extractSums(handle, outSums);
      assertEquals(outKeys, new int[]{1, 2, 3}, name);
      // 1: 10+30+60 = 100, 2: 20+50 = 70, 3: 40.
      assertEquals(outSums, new long[]{100L, 70L, 40L}, name);
    } finally {
      PinotNativeGroupBy.destroy(handle);
    }
  }

  // --- Multi-block accumulation ------------------------------------------

  @Test(dataProvider = "backends")
  public void multiBlockAccumulation(String name, IntToLongFunction factory) {
    long handle = factory.applyAsLong(16);
    try {
      // Block 1: introduces dict_ids 1, 2.
      PinotNativeGroupBy.processBlock(handle,
          new int[]{1, 2, 1}, new long[]{10L, 20L, 30L}, 3);
      // Block 2: revisits 1, 2 and introduces 3.
      PinotNativeGroupBy.processBlock(handle,
          new int[]{2, 3, 1}, new long[]{40L, 50L, 60L}, 3);
      // Block 3: revisits everything + adds 4.
      PinotNativeGroupBy.processBlock(handle,
          new int[]{4, 1, 2, 3}, new long[]{70L, 80L, 90L, 100L}, 4);

      assertEquals(PinotNativeGroupBy.numGroups(handle), 4, name);
      int[] outKeys = new int[4];
      long[] outSums = new long[4];
      PinotNativeGroupBy.extractKeys(handle, outKeys);
      PinotNativeGroupBy.extractSums(handle, outSums);

      // Insertion-order keys: 1, 2, 3, 4.
      assertEquals(outKeys, new int[]{1, 2, 3, 4}, name);
      // 1: 10+30+60+80 = 180
      // 2: 20+40+90    = 150
      // 3: 50+100      = 150
      // 4: 70          = 70
      assertEquals(outSums, new long[]{180L, 150L, 150L, 70L}, name);
    } finally {
      PinotNativeGroupBy.destroy(handle);
    }
  }

  // --- Differential vs Java reference (large randomized inputs) ----------

  @Test(dataProvider = "backends")
  public void largeRandomDifferentialSingleBlock(
      String name, IntToLongFunction factory) {
    final int n = 100_000;
    final int dictCardinality = 200;
    final Random rng = new Random(42);

    int[] dictIds = new int[n];
    long[] values = new long[n];
    for (int i = 0; i < n; i++) {
      dictIds[i] = rng.nextInt(dictCardinality);
      values[i] = rng.nextLong() % 1_000_000L;
    }

    // Java reference: build a LinkedHashMap so iteration order matches
    // insertion order, matching the native driver's group_id assignment.
    LinkedHashMap<Integer, Long> reference = new LinkedHashMap<>();
    for (int i = 0; i < n; i++) {
      reference.merge(dictIds[i], values[i], Long::sum);
    }

    long handle = factory.applyAsLong(dictCardinality);
    try {
      PinotNativeGroupBy.processBlock(handle, dictIds, values, n);
      int g = PinotNativeGroupBy.numGroups(handle);
      assertEquals(g, reference.size(), name + ": group count mismatch");

      int[] outKeys = new int[g];
      long[] outSums = new long[g];
      PinotNativeGroupBy.extractKeys(handle, outKeys);
      PinotNativeGroupBy.extractSums(handle, outSums);

      // Same insertion order — reference Map is LinkedHashMap.
      int idx = 0;
      for (Map.Entry<Integer, Long> e : reference.entrySet()) {
        assertEquals(outKeys[idx], e.getKey().intValue(),
            name + ": key mismatch at group_id=" + idx);
        assertEquals(outSums[idx], e.getValue().longValue(),
            name + ": sum mismatch at group_id=" + idx);
        idx++;
      }
    } finally {
      PinotNativeGroupBy.destroy(handle);
    }
  }

  @Test(dataProvider = "backends")
  public void largeRandomDifferentialMultiBlock(
      String name, IntToLongFunction factory) {
    final int blockSize = 10_000;
    final int numBlocks = 10;
    final int totalRows = blockSize * numBlocks;
    final int dictCardinality = 500;
    final Random rng = new Random(0xc0ffeeL);

    // Generate one stream, then split into blocks for both paths.
    int[] allDictIds = new int[totalRows];
    long[] allValues = new long[totalRows];
    for (int i = 0; i < totalRows; i++) {
      allDictIds[i] = rng.nextInt(dictCardinality);
      allValues[i] = rng.nextLong() % 1_000_000L;
    }

    LinkedHashMap<Integer, Long> reference = new LinkedHashMap<>();
    for (int i = 0; i < totalRows; i++) {
      reference.merge(allDictIds[i], allValues[i], Long::sum);
    }

    long handle = factory.applyAsLong(dictCardinality);
    try {
      // Feed the stream block-by-block to exercise the cross-block
      // accumulation path.
      int[] blockDictIds = new int[blockSize];
      long[] blockValues = new long[blockSize];
      for (int b = 0; b < numBlocks; b++) {
        System.arraycopy(allDictIds, b * blockSize, blockDictIds, 0, blockSize);
        System.arraycopy(allValues, b * blockSize, blockValues, 0, blockSize);
        PinotNativeGroupBy.processBlock(handle, blockDictIds, blockValues, blockSize);
      }

      int g = PinotNativeGroupBy.numGroups(handle);
      assertEquals(g, reference.size(), name + ": group count mismatch");

      int[] outKeys = new int[g];
      long[] outSums = new long[g];
      PinotNativeGroupBy.extractKeys(handle, outKeys);
      PinotNativeGroupBy.extractSums(handle, outSums);

      int idx = 0;
      for (Map.Entry<Integer, Long> e : reference.entrySet()) {
        assertEquals(outKeys[idx], e.getKey().intValue(),
            name + ": key mismatch at group_id=" + idx);
        assertEquals(outSums[idx], e.getValue().longValue(),
            name + ": sum mismatch at group_id=" + idx);
        idx++;
      }
    } finally {
      PinotNativeGroupBy.destroy(handle);
    }
  }

  // --- Cross-backend parity ----------------------------------------------

  /**
   * Both backends MUST produce identical (keys, sums) for identical input.
   * This is the invariant the Task #59 JMH harness relies on — we measure
   * perf differences only after correctness equivalence is established.
   */
  @Test
  public void hashbrownMatchesReferenceOnLargeStream() {
    final int n = 50_000;
    final int dictCardinality = 1_000;
    final Random rng = new Random(0xbaad_f00dL);

    int[] dictIds = new int[n];
    long[] values = new long[n];
    for (int i = 0; i < n; i++) {
      dictIds[i] = rng.nextInt(dictCardinality);
      values[i] = rng.nextLong() % 1_000_000L;
    }

    long hbHandle = PinotNativeGroupBy.createHashbrown(dictCardinality);
    try {
      PinotNativeGroupBy.processBlock(hbHandle, dictIds, values, n);

      int gHb = PinotNativeGroupBy.numGroups(hbHandle);
      int[] keysHb = new int[gHb];
      long[] sumsHb = new long[gHb];
      PinotNativeGroupBy.extractKeys(hbHandle, keysHb);
      PinotNativeGroupBy.extractSums(hbHandle, sumsHb);

      Map<Integer, Long> expected = new HashMap<>();
      for (int i = 0; i < n; i++) {
        expected.merge(dictIds[i], values[i], Long::sum);
      }
      assertEquals(toMap(keysHb, sumsHb), expected, "hashbrown backend diverged from Java reference");
    } finally {
      PinotNativeGroupBy.destroy(hbHandle);
    }
  }

  private static Map<Integer, Long> toMap(int[] keys, long[] values) {
    Map<Integer, Long> out = new HashMap<>();
    for (int i = 0; i < keys.length; i++) {
      out.put(keys[i], values[i]);
    }
    return out;
  }

  // --- Edge cases --------------------------------------------------------

  @Test(dataProvider = "backends")
  public void allDistinctKeysProduceDenseGroupIds(
      String name, IntToLongFunction factory) {
    final int n = 1_024;
    long handle = factory.applyAsLong(n);
    try {
      int[] dictIds = new int[n];
      long[] values = new long[n];
      for (int i = 0; i < n; i++) {
        dictIds[i] = i;
        values[i] = i * 7L;
      }
      PinotNativeGroupBy.processBlock(handle, dictIds, values, n);
      assertEquals(PinotNativeGroupBy.numGroups(handle), n, name);

      int[] outKeys = new int[n];
      long[] outSums = new long[n];
      PinotNativeGroupBy.extractKeys(handle, outKeys);
      PinotNativeGroupBy.extractSums(handle, outSums);
      // Insertion order: 0, 1, ..., n-1.
      for (int i = 0; i < n; i++) {
        assertEquals(outKeys[i], i, name + ": key at group_id=" + i);
        assertEquals(outSums[i], i * 7L, name + ": sum at group_id=" + i);
      }
    } finally {
      PinotNativeGroupBy.destroy(handle);
    }
  }

  @Test(dataProvider = "backends")
  public void extremeI64ValuesUseWrappingAddition(
      String name, IntToLongFunction factory) {
    long handle = factory.applyAsLong(1);
    try {
      // Two values that overflow if added with checked arithmetic.
      PinotNativeGroupBy.processBlock(handle,
          new int[]{0, 0}, new long[]{Long.MAX_VALUE, 1L}, 2);
      int[] outKeys = new int[1];
      long[] outSums = new long[1];
      PinotNativeGroupBy.extractKeys(handle, outKeys);
      PinotNativeGroupBy.extractSums(handle, outSums);
      assertEquals(outKeys[0], 0, name);
      // wrapping_add(MAX, 1) = MIN.
      assertEquals(outSums[0], Long.MIN_VALUE, name);
    } finally {
      PinotNativeGroupBy.destroy(handle);
    }
  }

  // --- Multi-column tier ladder (design §17.9 + tier ladder) -------------

  /**
   * Cardinality sets that force each segment tier, with the expected {@link
   * PinotNativeGroupBy#tierTag(long)} code. Verified against {@code select_tier}:
   * bit budget 20, slot budget 2^20.
   */
  @DataProvider(name = "multiColumnTiers")
  public Object[][] multiColumnTiers() {
    return new Object[][]{
        // Σbits = 8+4+6 = 18 <= 20 -> BitPackedDirect (T1)
        {"T1 bit-packed-direct", new int[]{200, 10, 64}, 0},
        // Σbits = 7*3 = 21 > 20, ∏ = 1_000_000 <= 2^20 -> RadixDirect (T0)
        {"T0 radix-direct", new int[]{100, 100, 100}, 1},
        // Σbits = 11*2 = 22 > 20, ∏ = 4_000_000 > 2^20 -> BitPackedHash (T2)
        {"T2 bit-packed-hash", new int[]{2000, 2000}, 2},
    };
  }

  /**
   * Differential parity through the actual tier code path: for each tier, create
   * the multi-column driver (asserting it selected the expected tier), feed
   * several column-major dict-id blocks with SUM(long) + COUNT, and verify the
   * combined per-group result and the unpacked per-column dict-ids against a Java
   * reference keyed on the mixed-radix tuple. Confirms T0/T1 (no-hash dense) and
   * T2 (hash) each produce byte-identical results.
   */
  @Test(dataProvider = "multiColumnTiers")
  public void multiColumnTierGroupByMatchesReference(String label, int[] cardinalities, int expectedTier) {
    int numColumns = cardinalities.length;
    byte[] aggKinds = NativeAggKind.toBytes(NativeAggKind.SUM_LONG, NativeAggKind.COUNT);
    long handle = PinotNativeGroupBy.createGroupBy(aggKinds, cardinalities);
    assertNotEquals(handle, 0L, label + ": create failed");
    try {
      assertEquals(PinotNativeGroupBy.tierTag(handle), expectedTier, label + ": selected tier");

      Random rng = new Random(20260731L);
      Map<Long, long[]> reference = new HashMap<>(); // mixed-radix key -> {sum, count}
      for (int block = 0; block < 5; block++) {
        int n = 500 + rng.nextInt(500);
        int[] flat = new int[numColumns * n];
        long[] vals = new long[n];
        for (int r = 0; r < n; r++) {
          long refKey = 0;
          for (int c = 0; c < numColumns; c++) {
            int d = rng.nextInt(cardinalities[c]);
            flat[c * n + r] = d;
            refKey = refKey * cardinalities[c] + d;
          }
          long v = rng.nextInt(1_000_000) - 500_000;
          vals[r] = v;
          long[] acc = reference.computeIfAbsent(refKey, k -> new long[2]);
          acc[0] += v;
          acc[1] += 1;
        }
        PinotNativeGroupBy.processBlockKeys(handle, flat, numColumns, n);
        PinotNativeGroupBy.applyAggLong(handle, 0, vals, n);
        PinotNativeGroupBy.applyAggCount(handle, 1);
      }

      int g = PinotNativeGroupBy.numGroups(handle);
      assertEquals(g, reference.size(), label + ": group count");
      long[] sums = new long[g];
      long[] counts = new long[g];
      PinotNativeGroupBy.extractAggLong(handle, 0, sums);
      PinotNativeGroupBy.extractAggLong(handle, 1, counts);
      int[] keyCols = new int[numColumns * g];
      PinotNativeGroupBy.extractGroupKeys(handle, keyCols, numColumns);
      for (int i = 0; i < g; i++) {
        long refKey = 0;
        for (int c = 0; c < numColumns; c++) {
          refKey = refKey * cardinalities[c] + keyCols[c * g + i];
        }
        long[] ref = reference.get(refKey);
        assertNotEquals(ref, null, label + ": unpacked key not in reference (group " + i + ")");
        assertEquals(sums[i], ref[0], label + ": sum for group " + i);
        assertEquals(counts[i], ref[1], label + ": count for group " + i);
      }
    } finally {
      PinotNativeGroupBy.destroy(handle);
    }
  }

  /** A packed key wider than 64 bits is rejected at creation ({@code 0} handle → Java fallback). */
  @Test
  public void multiColumnRejectsKeysWiderThan64Bits() {
    byte[] aggKinds = NativeAggKind.toBytes(NativeAggKind.COUNT);
    int[] cardinalities = {1 << 30, 1 << 30, 1 << 30}; // 30+30+30 = 90 bits > 64
    assertEquals(PinotNativeGroupBy.createGroupBy(aggKinds, cardinalities), 0L,
        "should reject > 64-bit packed key");
  }
}
