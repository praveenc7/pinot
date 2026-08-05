<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Autovectorization verification artifacts

Standalone probes that surfaced the autovec findings recorded in
`docs/native/phase-1-design.md` §20 (the "scalar Rust includes SLP
vectorization" correction). Not part of the production build. Re-run
when porting to a new host (especially x86) to confirm the same
qualitative pattern holds.

## asm_probe — LLVM autovec inspection

Tiny Rust crate with `#[no_mangle]` + `#[inline(never)]` wrappers around
the loop shapes we want to inspect. Compile and dump asm:

```
cd asm_probe
cargo rustc --release --lib -- --emit=asm -C target-cpu=apple-m1
# (or target-cpu=native, or a specific x86 target like x86-64-v3)
ls target/release/deps/*.s
grep -A 30 "sum_f64_four_accs" target/release/deps/asm_probe-*.s
```

Look for:
- `fadd.2d` / `vaddpd` — SIMD f64 add → autovec happened
- `fadd d0` / `addsd` — scalar f64 add → no autovec
- `add.2d` / `vpaddq` — SIMD i64 add → integer SUM autovec
- `cmgt.2d` / `vpcmpgtq` — SIMD i64 min synthesis

## jit_probe — HotSpot autovec timing inference

Tiny Java program with the same loop shapes. Run with LogCompilation
and measure ns/element to infer HotSpot's vectorization decisions:

```
cd jit_probe
javac JitProbe.java
java -XX:+UnlockDiagnosticVMOptions -XX:+LogCompilation \
     -XX:LogFile=jit.log -XX:-TieredCompilation -Xbatch JitProbe
```

Interpretation: if `sumF64SingleAcc` ns/elem >> `sumF64FourAccs` ns/elem,
HotSpot left the single-accumulator FP SUM scalar (which it does — FP
non-associativity blocks SuperWord). If `sumI64SingleAcc` matches the
4-acc throughput, integer SUM was autovec'd.

## Findings recorded as of 2026-06-05 on Apple M-series (NEON)

See `docs/native/phase-1-design.md` §20 for the full table and corrected
attribution wording.

Key headline: our "scalar Rust" SUM kernel is **NOT** scalar — both
LLVM and HotSpot pack 4 independent accumulators into 2-lane SIMD via
SLP. The total Java→Rust SIMD speedup numbers are unchanged; only the
3-way decomposition wording needs updating.
