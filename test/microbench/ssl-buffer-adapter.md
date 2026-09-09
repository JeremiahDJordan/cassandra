<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# SSL buffer comparison

`SslBufferAdapterBench` measures a JSSE wrap/unwrap round trip with AES-128-GCM,
128-byte and 8 KiB messages, TLS 1.2 and TLS 1.3. Handshakes, allocations of pooled
buffers, and plaintext validation occur before measurement. `adapter=true` forces
the adapter even on JDK 17 and 21, allowing its cost to be measured independently
of the production runtime gate. `adapter=false` uses JSSE directly. On JDK 25 it
preserves existing Buffer markers or installs synthetic markers during setup,
so both modes can run on the rewritten pool. That setup is excluded from timing.

`BufferPoolGetPutBench` separately measures allocation and return for 128-byte and
8 KiB buffers, with chunk recycling enabled and disabled. It prints the loaded
BufferPool location to help verify comparisons using different classpaths.

Build with the JDK being measured, or use JDK 17 bytecode for all three runtimes
when isolating runtime differences. Never reuse newer bytecode on an older JDK.
From the repository root, with JAVA_HOME and PATH set to the selected JDK:

```sh
.build/build-jars.sh -s
ant build-test
ant microbench -Dno-build-test=true \
    -Dbenchmark.name='(SslBufferAdapterBench|BufferPoolGetPutBench)' \
    -Dtest.jvm.args=-Dcassandra.debugrefcount=false \
    "-Djmh.args=-f 2 -wi 3 -w 1s -i 3 -r 1s -t 1 -prof gc"
```

Run one benchmark JVM at a time on an otherwise idle host. Preserve the JMH JSON
output before the next run. Report time, confidence intervals, and allocation
bytes per operation. Reference tracing must be disabled for production costs;
repeat with tracing enabled separately if investigating debug overhead.

To compare against the actual attachment implementation, compile BufferPool from
the baseline revision into a separate directory and place that directory first
on the benchmark classpath. Run that variant with `-p adapter=false`, then the
rewritten classes with `-p adapter=true`. Keep every other class and JVM argument
identical. This includes the original JDK 25 marker map in the get/put measurement
and retains its real per-chunk marker sharing in the TLS measurement. The
synthetic-marker mode alone does **not** measure the original map cost.

These are single-thread microbenchmarks, not database throughput measurements.
The TLS benchmark uses one gathered application buffer and borrowed buffers held
for the trial; it excludes connection establishment, Netty scheduling, and pool
churn. Do not add the two benchmark timings to claim an end-to-end speedup.
