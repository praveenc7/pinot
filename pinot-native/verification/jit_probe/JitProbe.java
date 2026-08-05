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
// HotSpot autovec probe. Tests:
//   1. FP SUM single-accumulator   (Java baseline shape)
//   2. FP SUM 4-accumulator         (Rust scalar kernel shape)
//   3. INT SUM single-accumulator   (control — should autovec)
//   4. FP MIN with Math.min        (Java baseline)
//   5. INT MIN with Math.min       (control — should autovec)
//
// Run with:
//   java -XX:+UnlockDiagnosticVMOptions -XX:+LogCompilation \
//        -XX:LogFile=jit.log -XX:-TieredCompilation \
//        -Xbatch JitProbe
//
// Grep jit.log for:
//   * <intrinsic ...>                — built-in vector intrinsics
//   * vector_size                    — vectorization width
//   * superword                      — SuperWord pass markers
//   * "loop not vectorizable"        — explicit reasons
//
// Plus stdout shows per-call timing — large ns/element gap between FP
// SUM and INT SUM = independent indirect evidence of HotSpot not
// auto-vectorizing FP SUM.

public class JitProbe {
    static final int N = 1_000_000;
    static final int ITERS = 200;

    public static double sumF64SingleAcc(double[] arr) {
        double acc = 0.0;
        for (int i = 0; i < arr.length; i++) {
            acc += arr[i];
        }
        return acc;
    }

    public static double sumF64FourAccs(double[] arr) {
        double s0 = 0, s1 = 0, s2 = 0, s3 = 0;
        int i = 0;
        for (; i + 4 <= arr.length; i += 4) {
            s0 += arr[i];
            s1 += arr[i + 1];
            s2 += arr[i + 2];
            s3 += arr[i + 3];
        }
        double tail = 0;
        for (; i < arr.length; i++) tail += arr[i];
        return ((s0 + s1) + (s2 + s3)) + tail;
    }

    public static long sumI64SingleAcc(long[] arr) {
        long acc = 0;
        for (int i = 0; i < arr.length; i++) {
            acc += arr[i];
        }
        return acc;
    }

    public static double minF64MathMin(double[] arr) {
        double acc = Double.POSITIVE_INFINITY;
        for (int i = 0; i < arr.length; i++) {
            acc = Math.min(acc, arr[i]);
        }
        return acc;
    }

    public static long minI64MathMin(long[] arr) {
        long acc = Long.MAX_VALUE;
        for (int i = 0; i < arr.length; i++) {
            acc = Math.min(acc, arr[i]);
        }
        return acc;
    }

    public static void main(String[] args) {
        double[] dArr = new double[N];
        long[] lArr = new long[N];
        for (int i = 0; i < N; i++) {
            dArr[i] = (double) (i % 1000);
            lArr[i] = (long) (i % 1000);
        }

        // Warm-up: trigger JIT at C2 tier.
        double dWarm = 0;
        long lWarm = 0;
        for (int w = 0; w < 50; w++) {
            dWarm += sumF64SingleAcc(dArr);
            dWarm += sumF64FourAccs(dArr);
            lWarm += sumI64SingleAcc(lArr);
            dWarm += minF64MathMin(dArr);
            lWarm += minI64MathMin(lArr);
        }
        System.out.println("warm: " + dWarm + " " + lWarm);

        // Measure.
        timeIt("sumF64SingleAcc", () -> sumF64SingleAcc(dArr));
        timeIt("sumF64FourAccs ", () -> sumF64FourAccs(dArr));
        timeIt("sumI64SingleAcc", () -> (double) sumI64SingleAcc(lArr));
        timeIt("minF64MathMin  ", () -> minF64MathMin(dArr));
        timeIt("minI64MathMin  ", () -> (double) minI64MathMin(lArr));
    }

    interface Bench { double run(); }

    static void timeIt(String name, Bench f) {
        long t0 = System.nanoTime();
        double sink = 0;
        for (int i = 0; i < ITERS; i++) sink += f.run();
        long t1 = System.nanoTime();
        double nsPerElem = (double) (t1 - t0) / ((double) ITERS * N);
        System.out.printf("%s : %.3f ns/elem  (sink=%g)%n", name, nsPerElem, sink);
    }
}
