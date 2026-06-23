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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * CASSANDRA-21171 "Netty Option B" END-TO-END frame proof.
 *
 * <p>This is the bar the prototype-phase {@link TransportTlsRoundTripTest} could not reach: it drives the REAL frame
 * codec through a REAL Netty {@link SslHandler}, with the channel allocator set to {@link TransportByteBufAllocator}
 * and the AES-GCM marker neutralised. The pipeline on each side is exactly:
 *
 * <pre>  outbound: FrameEncoderCrc -> SslHandler            (encrypt)
 *   inbound:  SslHandler  -> FrameDecoderCrc           (decrypt, then frame-decode)</pre>
 *
 * so it exercises (a) the encrypt-path <b>wrap source</b> now produced by {@link TransportByteBufAllocator#wrapFrame}
 * (a Netty buffer under Option B), and (b) the inbound {@link FrameDecoder#channelRead} branch that accepts a Netty
 * {@link ByteBuf} (the {@code SslHandler}'s decrypted output) rather than a {@link BufferPoolAllocator.Wrapped}.
 *
 * <p>Forces the OpenSSL/tcnative provider so the TLS engine wants <em>direct</em> buffers - the operand pairing that
 * triggers JDK 25's {@code GaloisCounterMode.overlapDetection} att-walk. Run with
 * {@code -Dtest.jvm.args="-Dcassandra.net.netty_transport_allocator=true -Dcassandra.test.bufferpool.gcm_marker=false"}
 * to prove the full transport frame path round-trips on JDK 25 with NO marker and NO ClassCastException.
 */
public class TransportFrameTlsRoundTripTest
{
    private static final String GCM_CIPHER = "TLS_AES_256_GCM_SHA384";
    private static SslContext serverCtx;
    private static SslContext clientCtx;

    @BeforeClass
    public static void setup() throws Exception
    {
        Assume.assumeTrue("OpenSSL/tcnative required for a direct-buffer TLS engine", OpenSsl.isAvailable());

        DatabaseDescriptor.daemonInitialization();
        ClusterMetadataTestHelper.setInstanceForTest();

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
    public void framedEncryptedRoundTripCrc() throws Throwable
    {
        framedEncryptedRoundTrip(FrameEncoderCrc.instance,
                                 FrameDecoderCrc.create(GlobalBufferPoolAllocator.instance));
    }

    @Test
    public void framedEncryptedRoundTripLz4() throws Throwable
    {
        // T1: an LZ4 round trip through the full TLS frame path under Option B.
        framedEncryptedRoundTrip(FrameEncoderLZ4.fastInstance,
                                 FrameDecoderLZ4.fast(GlobalBufferPoolAllocator.instance));
    }

    private void framedEncryptedRoundTrip(FrameEncoder encoder, FrameDecoder serverDecoder) throws Throwable
    {
        Assume.assumeTrue("Run with -Dcassandra.net.netty_transport_allocator=true to exercise Option B",
                          TransportByteBufAllocator.USE_NETTY_POOL);

        ByteBufAllocator alloc = TransportByteBufAllocator.instance();

        SslHandler clientSsl = clientCtx.newHandler(alloc);
        SslHandler serverSsl = serverCtx.newHandler(alloc);

        // Server inbound: SslHandler decrypts -> FrameDecoder decodes frames. FrameDecoder talks to its processor
        // out-of-band (not via the pipeline), so we register a collecting FrameProcessor and activate it.
        FrameCollector serverFrames = new FrameCollector();

        EmbeddedChannel client = newChannel(alloc);
        EmbeddedChannel server = newChannel(alloc);

        client.pipeline().addLast(clientSsl, encoder);
        server.pipeline().addLast(serverSsl);
        serverDecoder.addLastTo(server.pipeline());
        serverDecoder.activate(serverFrames);

        // Handshake.
        pump(client, server);
        assertTrue("client handshake failed", clientSsl.handshakeFuture().isSuccess());
        assertTrue("server handshake failed", serverSsl.handshakeFuture().isSuccess());

        // Send a real frame from client to server: build a FrameEncoder.Payload, write into it, hand to the encoder.
        String message = "CASSANDRA-21171 Option B: a full FrameEncoder -> SslHandler -> FrameDecoder round trip "
                         + "over the Netty transport allocator on JDK 25 with the AES-GCM marker disabled.";
        byte[] payload = message.getBytes(StandardCharsets.UTF_8);

        FrameEncoder.Payload p = encoder.allocator().allocate(true, payload.length);
        p.buffer.put(payload);
        p.finish();
        client.writeOutbound(p);

        pump(client, server);
        client.checkException();
        server.checkException();
        assertNull("unexpected pipeline error", error.get());

        assertEquals("exactly one frame expected", 1, serverFrames.payloads.size());
        assertEquals(message, new String(serverFrames.payloads.get(0), StandardCharsets.UTF_8));

        serverDecoder.discard();
        client.finishAndReleaseAll();
        server.finishAndReleaseAll();
    }

    private final AtomicReference<Throwable> error = new AtomicReference<>();

    private EmbeddedChannel newChannel(ByteBufAllocator alloc)
    {
        EmbeddedChannel ch = new EmbeddedChannel();
        ch.config().setAllocator(alloc);
        ch.config().setOption(ChannelOption.ALLOCATOR, alloc);
        // FrameDecoder sets autoRead(false) and drives reads via ctx.read(); EmbeddedChannel honours this.
        return ch;
    }

    /** Collects decoded frame payload bytes; copies them out before the frame is released. */
    private static class FrameCollector implements FrameDecoder.FrameProcessor
    {
        final List<byte[]> payloads = new ArrayList<>();

        @Override
        public boolean process(FrameDecoder.Frame frame)
        {
            FrameDecoder.IntactFrame intact = (FrameDecoder.IntactFrame) frame;
            ByteBuffer buf = intact.contents.get();
            byte[] bytes = new byte[buf.remaining()];
            buf.duplicate().get(bytes);
            payloads.add(bytes);
            intact.consume();
            return true;
        }
    }

    private void pump(EmbeddedChannel client, EmbeddedChannel server) throws Throwable
    {
        boolean progressed = true;
        int guard = 0;
        while (progressed && guard++ < 500)
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
