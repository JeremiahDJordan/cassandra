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

import java.io.FileInputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.TrustManagerFactory;

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
import org.apache.cassandra.utils.memory.MemoryUtil;

import io.netty.handler.ssl.CassandraSslEngine;

/**
 * Measures TLS record processing separately from pool allocation. The baseline uses the original pooled attachment
 * on JDK 17/21 and a Buffer marker on JDK 25, where passing ownership metadata directly to JSSE fails. Marker setup
 * is outside measurement, so this deliberately excludes the old map's allocation/return overhead. The adapter uses
 * unmodified pooled buffers on every JDK. Run from the repository root with reference tracing disabled for production
 * costs; use the GC profiler to compare allocations as well as time. Handshakes and correctness checks precede timing.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(2)
public class SslBufferAdapterBench
{
    @Param({ "false", "true" })
    public boolean adapter;

    @Param({ "128", "8192" })
    public int size;

    @Param({ "TLSv1.2", "TLSv1.3" })
    public String protocol;

    private BufferPool.LocalPool pool;
    private SSLEngine client;
    private SSLEngine server;
    private ByteBuffer input;
    private ByteBuffer wire;
    private ByteBuffer output;
    private ByteBuffer[] sources;
    private Object[] owners;

    @Setup
    public void setup() throws Exception
    {
        pool = new BufferPool("ssl_bench", 8 << 20, false).create();
        input = pool.get(32768);
        wire = pool.get(65536);
        output = pool.get(32768);
        sources = new ByteBuffer[]{ input };
        owners = new Object[]{ MemoryUtil.getAttachment(input), MemoryUtil.getAttachment(wire), MemoryUtil.getAttachment(output) };
        if (!adapter && Runtime.version().feature() >= 25)
        {
            for (ByteBuffer buffer : new ByteBuffer[]{ input, wire, output })
            {
                if (!(MemoryUtil.getAttachment(buffer) instanceof Buffer))
                    MemoryUtil.setAttachment(buffer, MemoryUtil.getByteBuffer(MemoryUtil.getAddress(buffer), buffer.capacity()));
            }
        }
        SSLContext context = context();
        client = engine(context, true);
        server = engine(context, false);
        ByteBuffer reverseWire = ByteBuffer.allocateDirect(65536);
        for (int i = 0; ; ++i)
        {
            if (i == 100)
                throw new IllegalStateException("TLS handshake stalled");
            transfer(client, server, wire, output);
            transfer(server, client, reverseWire, input);
            if (client.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING
                && server.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING
                && wire.position() == 0 && reverseWire.position() == 0)
                break;
        }
        MemoryUtil.clean(reverseWire);
        input.clear();
        for (int i = 0; i < size; ++i)
            input.put(i, (byte) i);
        if (roundTrip() != size || input.hasRemaining() || wire.hasRemaining())
            throw new IllegalStateException("Incomplete TLS record");
        for (int i = 0; i < size; ++i)
            if (output.get(i) != (byte) i)
                throw new IllegalStateException("TLS plaintext mismatch");
    }

    private SSLEngine engine(SSLContext context, boolean clientMode) throws Exception
    {
        SSLEngine engine = context.createSSLEngine();
        engine.setUseClientMode(clientMode);
        engine.setEnabledProtocols(new String[]{ protocol });
        engine.setEnabledCipherSuites(new String[]{ protocol.equals("TLSv1.3")
                                                   ? "TLS_AES_128_GCM_SHA256" : "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256" });
        if (adapter)
            engine = new CassandraSslEngine(engine);
        engine.beginHandshake();
        return engine;
    }

    @Benchmark
    public int roundTrip() throws Exception
    {
        input.clear().limit(size);
        wire.clear();
        output.clear();
        // Netty's JDK path gathers application buffers and unwraps into one destination.
        client.wrap(sources, 0, sources.length, wire);
        wire.flip();
        server.unwrap(wire, output);
        return output.position();
    }

    @TearDown
    public void teardown()
    {
        ByteBuffer[] buffers = new ByteBuffer[]{ input, wire, output };
        for (int i = 0; i < buffers.length; ++i)
        {
            MemoryUtil.setAttachment(buffers[i], owners[i]);
            pool.put(buffers[i]);
        }
        pool.release();
    }

    private static void transfer(SSLEngine from, SSLEngine to, ByteBuffer wire, ByteBuffer app) throws Exception
    {
        if (from.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_WRAP)
            from.wrap(ByteBuffer.allocate(0), wire);
        wire.flip();
        if (wire.hasRemaining())
            to.unwrap(wire, app);
        wire.compact();
        for (SSLEngine engine : new SSLEngine[]{ from, to })
        {
            Runnable task;
            while ((task = engine.getDelegatedTask()) != null)
                task.run();
        }
    }

    private static SSLContext context() throws Exception
    {
        KeyStore keys = KeyStore.getInstance("JKS");
        try (FileInputStream in = new FileInputStream("test/conf/cassandra_ssl_test.keystore"))
        {
            keys.load(in, "cassandra".toCharArray());
        }
        KeyManagerFactory km = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        km.init(keys, "cassandra".toCharArray());
        KeyStore trusts = KeyStore.getInstance("JKS");
        try (FileInputStream in = new FileInputStream("test/conf/cassandra_ssl_test.truststore"))
        {
            trusts.load(in, "cassandra".toCharArray());
        }
        TrustManagerFactory tm = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tm.init(trusts);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(km.getKeyManagers(), tm.getTrustManagers(), null);
        return context;
    }
}
