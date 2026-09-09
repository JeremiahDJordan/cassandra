/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.test.microbench;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.utils.memory.BufferPool;

/** Measures buffer allocation/return with and without chunk recycling; compare revisions using identical JVM settings. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(2)
public class BufferPoolGetPutBench
{
    @Param({ "128", "8192" })
    public int size;

    @Param({ "false", "true" })
    public boolean recycle;

    private BufferPool.LocalPool local;

    @Setup
    public void setup()
    {
        BufferPool pool = new BufferPool("review_bench", 8 << 20, false);
        local = pool.create().recycleWhenFree(recycle);
        System.out.println("Benchmark BufferPool loaded from "
                           + BufferPool.class.getProtectionDomain().getCodeSource().getLocation());
    }

    @TearDown
    public void teardown()
    {
        local.release();
    }

    @Benchmark
    public void getPut()
    {
        ByteBuffer buffer = local.get(size);
        local.put(buffer);
    }
}
