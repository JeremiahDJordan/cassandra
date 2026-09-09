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

import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Collections;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.TrustManagerFactory;

import org.junit.Test;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.concurrent.Ref.DirectBufferRef;
import org.apache.cassandra.utils.memory.MemoryUtil;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.CassandraSslEngine;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import sun.nio.ch.DirectBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SocketFactoryTest
{
    @Test
    public void testTls12WithNonBufferAttachments() throws Exception
    {
        roundTrip("TLSv1.2", "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
    }

    @Test
    public void testTls13WithNonBufferAttachments() throws Exception
    {
        roundTrip("TLSv1.3", "TLS_AES_128_GCM_SHA256");
    }

    @Test
    public void testPeerSettingsArePreserved() throws Exception
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        try
        {
            SslContext context = new JdkSslContext(sslContext(), true, ClientAuth.NONE);
            SSLEngine engine = SocketFactory.newSslHandler(channel, context,
                                                         InetSocketAddress.createUnresolved("localhost", 7000)).engine();
            assertEquals("localhost", engine.getPeerHost());
            assertEquals(7000, engine.getPeerPort());
            assertEquals("HTTPS", engine.getSSLParameters().getEndpointIdentificationAlgorithm());
            assertEquals(Runtime.version().feature() >= 25, engine instanceof CassandraSslEngine);
        }
        finally
        {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void testStartTlsIsPreserved() throws Exception
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        try
        {
            SslContext context = new JdkSslContext(sslContext(), false, null, IdentityCipherSuiteFilter.INSTANCE,
                                                  ApplicationProtocolConfig.DISABLED, ClientAuth.NONE, null, true);
            channel.pipeline().addLast(SocketFactory.newSslHandler(channel, context, null));
            byte[] message = new byte[]{ 1, 2, 3 };
            channel.writeOutbound(Unpooled.wrappedBuffer(message));
            ByteBuf plain = channel.readOutbound();
            try
            {
                byte[] actual = new byte[plain.readableBytes()];
                plain.readBytes(actual);
                assertArrayEquals(message, actual);
            }
            finally
            {
                plain.release();
            }
        }
        finally
        {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void testTls12PipelineWithPooledBuffers() throws Exception
    {
        pipelineRoundTrip("TLSv1.2", "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
    }

    @Test
    public void testTls13PipelineWithPooledBuffers() throws Exception
    {
        pipelineRoundTrip("TLSv1.3", "TLS_AES_128_GCM_SHA256");
    }

    private static void pipelineRoundTrip(String protocol, String cipher) throws Exception
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.getRawConfig().networking_cache_size = new DataStorageSpec.IntMebibytesBound(32);
        SSLContext context = sslContext();
        EmbeddedChannel client = new EmbeddedChannel();
        EmbeddedChannel server = new EmbeddedChannel();
        client.config().setAllocator(GlobalBufferPoolAllocator.instance);
        server.config().setAllocator(GlobalBufferPoolAllocator.instance);
        JdkSslContext clientContext = new JdkSslContext(context, true, Collections.singletonList(cipher),
                                                       IdentityCipherSuiteFilter.INSTANCE, ApplicationProtocolConfig.DISABLED,
                                                       ClientAuth.NONE, new String[]{ protocol }, false);
        JdkSslContext serverContext = new JdkSslContext(context, false, Collections.singletonList(cipher),
                                                       IdentityCipherSuiteFilter.INSTANCE, ApplicationProtocolConfig.DISABLED,
                                                       ClientAuth.REQUIRE, new String[]{ protocol }, false);
        SslHandler clientHandler = SocketFactory.newSslHandler(client, clientContext, null);
        SslHandler serverHandler = SocketFactory.newSslHandler(server, serverContext, null);
        long initialSize = GlobalBufferPoolAllocator.instance.usedSizeInBytes();
        try
        {
            server.pipeline().addLast(serverHandler);
            client.pipeline().addLast(clientHandler);
            for (int i = 0; i < 100 && (!clientHandler.handshakeFuture().isDone() || !serverHandler.handshakeFuture().isDone()); ++i)
                exchange(client, server);
            assertTrue(String.valueOf(clientHandler.handshakeFuture().cause()), clientHandler.handshakeFuture().isSuccess());
            assertTrue(String.valueOf(serverHandler.handshakeFuture().cause()), serverHandler.handshakeFuture().isSuccess());
            for (int size : new int[]{ 128, 8192, 131073 })
            {
                byte[] expected = new byte[size];
                for (int i = 0; i < size; ++i)
                    expected[i] = (byte) i;
                ByteBuf message = GlobalBufferPoolAllocator.instance.directBuffer(size);
                message.writeBytes(expected);
                client.writeOutbound(message);
                exchange(client, server);
                byte[] actual = new byte[size];
                int offset = 0;
                ByteBuf received;
                while ((received = server.readInbound()) != null)
                {
                    try
                    {
                        int length = received.readableBytes();
                        received.readBytes(actual, offset, length);
                        offset += length;
                    }
                    finally
                    {
                        received.release();
                    }
                }
                assertEquals(size, offset);
                assertArrayEquals(expected, actual);
            }
        }
        finally
        {
            client.finishAndReleaseAll();
            server.finishAndReleaseAll();
        }
        assertEquals(initialSize, GlobalBufferPoolAllocator.instance.usedSizeInBytes());
    }

    private static void exchange(EmbeddedChannel client, EmbeddedChannel server)
    {
        for (int i = 0; i < 100; ++i)
        {
            client.runPendingTasks();
            server.runPendingTasks();
            boolean transferred = false;
            ByteBuf message;
            while ((message = client.readOutbound()) != null)
            {
                server.writeInbound(message);
                transferred = true;
            }
            while ((message = server.readOutbound()) != null)
            {
                client.writeInbound(message);
                transferred = true;
            }
            if (!transferred)
                return;
        }
        fail("TLS pipeline did not settle");
    }

    static SSLContext sslContext() throws Exception
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

    private static void roundTrip(String protocol, String suite) throws Exception
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        SSLContext context = sslContext();
        SslContext clientContext = new JdkSslContext(context, true, ClientAuth.NONE);
        SslContext serverContext = new JdkSslContext(context, false, ClientAuth.NONE);
        SSLEngine client = SocketFactory.newSslHandler(channel, clientContext, null).engine();
        SSLEngine server = SocketFactory.newSslHandler(channel, serverContext, null).engine();
        for (SSLEngine engine : Arrays.asList(client, server))
        {
            engine.setEnabledProtocols(new String[]{ protocol });
            engine.setEnabledCipherSuites(new String[]{ suite });
            engine.beginHandshake();
        }
        try (AttachedBuffer c2s = new AttachedBuffer(65536); AttachedBuffer s2c = new AttachedBuffer(65536);
             AttachedBuffer ca = new AttachedBuffer(32768); AttachedBuffer sa = new AttachedBuffer(32768))
        {
            for (int i = 0; ; ++i)
            {
                if (i == 100)
                    fail("Handshake stalled");
                transferHandshake(client, server, c2s.buffer, sa.buffer);
                transferHandshake(server, client, s2c.buffer, ca.buffer);
                if (client.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING
                    && server.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING
                    && c2s.buffer.position() == 0 && s2c.buffer.position() == 0)
                    break;
            }
            byte[] message = new byte[8192];
            for (int i = 0; i < message.length; ++i)
                message[i] = (byte) i;
            ca.buffer.clear().put(message).flip();
            sa.buffer.clear();
            SSLEngineResult overflow = client.wrap(ca.buffer, ByteBuffer.allocate(1));
            assertEquals(SSLEngineResult.Status.BUFFER_OVERFLOW, overflow.getStatus());
            assertEquals(0, ca.buffer.position());
            client.wrap(ca.buffer, c2s.buffer);
            c2s.buffer.flip();
            int packetLimit = c2s.buffer.limit();
            c2s.buffer.limit(4);
            SSLEngineResult underflow = server.unwrap(c2s.buffer, sa.buffer);
            assertEquals(SSLEngineResult.Status.BUFFER_UNDERFLOW, underflow.getStatus());
            assertEquals(0, c2s.buffer.position());
            c2s.buffer.limit(packetLimit);
            server.unwrap(c2s.buffer, sa.buffer);
            sa.buffer.flip();
            byte[] result = new byte[sa.buffer.remaining()];
            sa.buffer.get(result);
            assertArrayEquals(message, result);
            assertFalse(ca.buffer.hasRemaining());
            assertFalse(c2s.buffer.hasRemaining());
            for (AttachedBuffer allocation : Arrays.asList(c2s, s2c, ca, sa))
                assertSame(allocation.attachment, MemoryUtil.getAttachment(allocation.buffer));
        }
        finally
        {
            channel.finishAndReleaseAll();
        }
    }

    private static void transferHandshake(SSLEngine from, SSLEngine to, ByteBuffer wire, ByteBuffer app) throws Exception
    {
        if (from.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_WRAP)
            from.wrap(ByteBuffer.allocate(0), wire);
        wire.flip();
        if (wire.hasRemaining())
            to.unwrap(wire, app);
        wire.compact();
        for (SSLEngine engine : Arrays.asList(from, to))
        {
            Runnable task;
            while ((task = engine.getDelegatedTask()) != null)
                task.run();
        }
    }

    private static class AttachedBuffer implements AutoCloseable
    {
        final ByteBuffer buffer;
        final DirectBufferRef<DirectBuffer> attachment;

        AttachedBuffer(int capacity)
        {
            buffer = ByteBuffer.allocateDirect(capacity);
            // Reproduce the ownership metadata used by BufferPool before the marker-map workaround.
            ByteBuffer root = MemoryUtil.getByteBuffer(MemoryUtil.getAddress(buffer), capacity);
            attachment = new DirectBufferRef<>((DirectBuffer) root, null);
            MemoryUtil.setAttachment(buffer, attachment);
        }

        public void close()
        {
            MemoryUtil.setAttachment(buffer, null);
            attachment.release();
            MemoryUtil.clean(buffer);
        }
    }
}
