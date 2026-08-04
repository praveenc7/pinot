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

import java.util.Arrays;
import java.util.Random;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;


public class MvIntArrayBufferTest {

  @Test
  public void testAppendAndReset() {
    MvIntArrayBuffer buffer = new MvIntArrayBuffer();
    buffer.startFill(3);
    buffer.append(new int[]{1, 2, 3, 99}, 3);
    buffer.append(new int[]{4, 99}, 1);
    buffer.append(new int[0], 0);

    assertEquals(buffer.getNumDocs(), 3);
    assertEquals(buffer.getNumValues(), 4);
    assertEquals(Arrays.copyOf(buffer.getOffsets(), 4), new int[]{0, 3, 4, 4});
    assertEquals(Arrays.copyOf(buffer.getValues(), 4), new int[]{1, 2, 3, 4});
    assertEquals(buffer.getStartOffset(1), 3);
    assertEquals(buffer.getNumValues(1), 1);

    buffer.startFill(1);
    buffer.append(new int[]{7}, 1);
    assertEquals(buffer.getNumDocs(), 1);
    assertEquals(buffer.getNumValues(), 1);
    assertEquals(buffer.getValues()[0], 7);
  }

  @Test
  public void testFillAndReuse() {
    MvIntArrayBuffer buffer = new MvIntArrayBuffer();
    Random random = new Random(0);
    int[][] values = randomValues(random, 512, 128);
    buffer.fill(values, values.length);
    verify(buffer, values);

    int[] valueArray = buffer.getValues();
    int[] offsets = buffer.getOffsets();
    for (int i = 0; i < 5; i++) {
      values = randomValues(random, 512, 128);
      buffer.fill(values, values.length);
      verify(buffer, values);
    }
    assertSame(buffer.getValues(), valueArray);
    assertSame(buffer.getOffsets(), offsets);
  }

  @Test
  public void testNegativeNumDocsRejected() {
    MvIntArrayBuffer buffer = new MvIntArrayBuffer();
    assertThrows(IllegalArgumentException.class, () -> buffer.startFill(-1));
  }

  private static int[][] randomValues(Random random, int numDocs, int maxValues) {
    int[][] values = new int[numDocs][];
    for (int i = 0; i < numDocs; i++) {
      int numValues = random.nextInt(maxValues);
      values[i] = new int[numValues];
      for (int j = 0; j < numValues; j++) {
        values[i][j] = random.nextInt();
      }
    }
    return values;
  }

  private static void verify(MvIntArrayBuffer buffer, int[][] expected) {
    assertEquals(buffer.getNumDocs(), expected.length);
    int offset = 0;
    for (int i = 0; i < expected.length; i++) {
      assertEquals(buffer.getStartOffset(i), offset);
      assertEquals(buffer.getNumValues(i), expected[i].length);
      for (int j = 0; j < expected[i].length; j++) {
        assertEquals(buffer.getValues()[offset + j], expected[i][j]);
      }
      offset += expected[i].length;
    }
    assertEquals(buffer.getNumValues(), offset);
    assertEquals(buffer.getOffsets()[expected.length], offset);
  }
}
