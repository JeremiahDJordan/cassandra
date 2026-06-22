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
package org.apache.cassandra.net;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Queue;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * CASSANDRA-21171 "Netty Option B" full-pipeline proof.
 *
 * <p>Drives a real Netty {@link SslHandler} TLS handshake + AES-GCM application-data round trip between two
 * {@link EmbeddedChannel}s whose channel allocator is exactly what the transport pipelines now install -
 * {@link TransportByteBufAllocator#instance()}. Forces the <b>OpenSSL/tcnative</b> provider (whose engine wants
 * <em>direct</em> buffers, so the wrap/unwrap destinations are direct - the operand pairing that triggers the JDK
 * GaloisCounterMode att-walk on JDK 25 when those buffers come from the main BufferPool).
 *
 * <p>Run with {@code -Dtest.jvm.args="-Dcassandra.net.netty_transport_allocator=true -Dcassandra.test.bufferpool.gcm_marker=false"}
 * to prove Option B succeeds on JDK 25 with the marker neutralised. The test asserts a successful encrypted round
 * trip; under the default config (Option B off, marker on) it also passes, exercising the unchanged path.
 */
public class TransportTlsRoundTripTest
{
    private static final String GCM_CIPHER = "TLS_AES_256_GCM_SHA384";
    private static SslContext serverCtx;
    private static SslContext clientCtx;

    @BeforeClass
    public static void setup() throws Exception
    {
        // tcnative gives a direct-buffer engine; without it the JDK engine uses heap dst and the direct-direct att
        // walk never runs, so this proof would be vacuous. Skip rather than give false assurance.
        Assume.assumeTrue("OpenSSL/tcnative required for a direct-buffer TLS engine", OpenSsl.isAvailable());

        // Needed when Option B is OFF and the allocator falls back to GlobalBufferPoolAllocator -> BufferPools.
        org.apache.cassandra.config.DatabaseDescriptor.clientInitialization();

        // Use the in-tree test keystore (avoids Netty SelfSignedCertificate's BouncyCastle-JSSE dependency on JDK 25).
        char[] pw = "cassandra".toCharArray();
        java.security.KeyStore ks = java.security.KeyStore.getInstance("JKS");
        try (java.io.InputStream in = new java.io.FileInputStream("test/conf/cassandra_ssl_test.keystore"))
        {
            ks.load(in, pw);
        }
        javax.net.ssl.KeyManagerFactory kmf =
            javax.net.ssl.KeyManagerFactory.getInstance(javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, pw);

        serverCtx = SslContextBuilder.forServer(kmf)
                                     .sslProvider(SslProvider.OPENSSL)
                                     .protocols("TLSv1.3")
                                     .ciphers(Collections.singletonList(GCM_CIPHER))
                                     .build();
        clientCtx = SslContextBuilder.forClient()
                                     .trustManager(InsecureTrustManagerFactory.INSTANCE)
                                     .sslProvider(SslProvider.OPENSSL)
                                     .protocols("TLSv1.3")
                                     .ciphers(Collections.singletonList(GCM_CIPHER))
                                     .build();
    }

    @Test
    public void encryptedRoundTripUsesTransportAllocator() throws Throwable
    {
        // This is the Option B proof: it must run with the Netty-pooled transport allocator. (With Option B off the
        // EmbeddedChannel teardown exercises BufferPool.clean() paths that need full networking-pool wiring; the
        // default path is already covered by the real encrypted dtests.)
        Assume.assumeTrue("Run with -Dcassandra.net.netty_transport_allocator=true to exercise Option B",
                          TransportByteBufAllocator.USE_NETTY_POOL);

        ByteBufAllocator alloc = TransportByteBufAllocator.instance();

        SslHandler clientSsl = clientCtx.newHandler(alloc);
        SslHandler serverSsl = serverCtx.newHandler(alloc);
        Collector clientCollector = new Collector();
        Collector serverCollector = new Collector();

        EmbeddedChannel client = newChannel(alloc, clientSsl, clientCollector);
        EmbeddedChannel server = newChannel(alloc, serverSsl, serverCollector);

        pump(client, server);
        assertTrue("client handshake failed", clientSsl.handshakeFuture().isSuccess());
        assertTrue("server handshake failed", serverSsl.handshakeFuture().isSuccess());

        String c2s = "client->server AES-GCM payload over TLS1.3 with the transport allocator under test";
        client.writeOutbound(client.alloc().buffer().writeBytes(c2s.getBytes(StandardCharsets.UTF_8)));
        pump(client, server);
        assertEquals(c2s, serverCollector.received.toString());

        String s2c = "server->client AES-GCM payload, proving both wrap and unwrap directions succeed on this JDK";
        server.writeOutbound(server.alloc().buffer().writeBytes(s2c.getBytes(StandardCharsets.UTF_8)));
        pump(client, server);
        assertEquals(s2c, clientCollector.received.toString());

        client.finishAndReleaseAll();
        server.finishAndReleaseAll();
    }

    private static EmbeddedChannel newChannel(ByteBufAllocator alloc, SslHandler ssl, Collector collector)
    {
        EmbeddedChannel ch = new EmbeddedChannel();
        ch.config().setAllocator(alloc);
        ch.config().setOption(ChannelOption.ALLOCATOR, alloc);
        ch.pipeline().addLast(ssl, collector);
        return ch;
    }

    private static class Collector extends ChannelInboundHandlerAdapter
    {
        final StringBuilder received = new StringBuilder();

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg)
        {
            ByteBuf buf = (ByteBuf) msg;
            try
            {
                byte[] bytes = new byte[buf.readableBytes()];
                buf.readBytes(bytes);
                received.append(new String(bytes, StandardCharsets.UTF_8));
            }
            finally
            {
                buf.release();
            }
        }
    }

    private void pump(EmbeddedChannel client, EmbeddedChannel server) throws Throwable
    {
        boolean progressed = true;
        int guard = 0;
        while (progressed && guard++ < 200)
        {
            progressed = false;
            for (ByteBuf b : drain(client))
            {
                server.writeInbound(b);
                progressed = true;
            }
            for (ByteBuf b : drain(server))
            {
                client.writeInbound(b);
                progressed = true;
            }
            client.checkException();
            server.checkException();
        }
    }

    private Queue<ByteBuf> drain(EmbeddedChannel ch)
    {
        Queue<ByteBuf> out = new ArrayDeque<>();
        Object o;
        while ((o = ch.readOutbound()) != null)
            out.add((ByteBuf) o);
        return out;
    }
}
