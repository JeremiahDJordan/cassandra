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
package io.netty.handler.ssl;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import javax.net.ssl.X509ExtendedTrustManager;

import org.junit.Test;

import org.apache.cassandra.utils.memory.MemoryUtil;

import sun.nio.ch.DirectBuffer;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class CassandraSslEngineTest
{
    private static final SSLEngineResult RESULT = new SSLEngineResult(SSLEngineResult.Status.OK,
                                                                     SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING, 1, 1);

    @Test
    public void testGatherViewsPreserveAliasingAndAreReused() throws Exception
    {
        SSLEngine delegate = mock(SSLEngine.class);
        CassandraSslEngine engine = new CassandraSslEngine(delegate);
        AtomicReference<ByteBuffer> previous = new AtomicReference<>();
        AtomicReference<ByteBuffer[]> prepared = new AtomicReference<>();
        try (Attached source = new Attached(); Attached destination = new Attached())
        {
            ByteBuffer heap = ByteBuffer.allocate(16);
            ByteBuffer[] sources = new ByteBuffer[]{ null, source.buffer, heap, source.buffer, null };
            when(delegate.wrap(any(ByteBuffer[].class), eq(0), eq(3), any(ByteBuffer.class))).thenAnswer(call -> {
                ByteBuffer[] views = call.getArgument(0);
                ByteBuffer output = call.getArgument(3);
                assertSame(views[0], views[2]);
                assertSame(heap, views[1]);
                assertNull(((DirectBuffer) views[0]).attachment());
                assertNull(((DirectBuffer) output).attachment());
                assertEquals(MemoryUtil.getAddress(source.buffer), MemoryUtil.getAddress(views[0]));
                assertEquals(source.buffer.limit(), views[0].limit());
                if (previous.get() != null)
                    assertSame(previous.get(), views[0]);
                previous.set(views[0]);
                prepared.set(views);
                output.put(views[0].get());
                views[2].get();
                return RESULT;
            });
            for (int i = 0; i < 2; ++i)
            {
                source.buffer.position(3).limit(13);
                source.buffer.mark();
                destination.buffer.clear();
                assertSame(RESULT, engine.wrap(sources, 1, 3, destination.buffer));
                assertEquals(5, source.buffer.position());
                assertEquals(1, destination.buffer.position());
                assertEquals(13, source.buffer.limit());
                source.buffer.reset();
                assertEquals(3, source.buffer.position());
                assertSame(source.attachment, MemoryUtil.getAttachment(source.buffer));
                assertArrayEquals(new ByteBuffer[]{ null, null, null }, prepared.get());
            }
            assertSame(source.buffer, sources[1]);
            assertSame(source.buffer, sources[3]);
        }
    }

    @Test
    public void testScatterPositionsArePropagatedOnFailure() throws Exception
    {
        SSLEngine delegate = mock(SSLEngine.class);
        CassandraSslEngine engine = new CassandraSslEngine(delegate);
        AtomicReference<ByteBuffer[]> prepared = new AtomicReference<>();
        try (Attached source = new Attached(); Attached destination = new Attached())
        {
            ByteBuffer[] destinations = new ByteBuffer[]{ null, destination.buffer, null };
            SSLException failure = new SSLException("test failure");
            when(delegate.unwrap(any(ByteBuffer.class), any(ByteBuffer[].class), eq(0), eq(1))).thenAnswer(call -> {
                ByteBuffer input = call.getArgument(0);
                ByteBuffer[] output = call.getArgument(1);
                output[0].put(input.get());
                prepared.set(output);
                throw failure;
            });
            assertThatThrownBy(() -> engine.unwrap(source.buffer, destinations, 1, 1)).isSameAs(failure);
            assertEquals(1, source.buffer.position());
            assertEquals(1, destination.buffer.position());
            assertSame(destination.attachment, MemoryUtil.getAttachment(destination.buffer));
            assertNull(prepared.get()[0]);
        }
    }

    @Test
    public void testReadOnlyAndOrdinaryBuffers() throws Exception
    {
        SSLEngine delegate = mock(SSLEngine.class);
        CassandraSslEngine engine = new CassandraSslEngine(delegate);
        try (Attached source = new Attached(); Attached destination = new Attached())
        {
            ByteBuffer readOnly = source.buffer.asReadOnlyBuffer();
            ByteBuffer heap = ByteBuffer.allocate(16);
            when(delegate.wrap(any(ByteBuffer.class), any(ByteBuffer.class))).thenAnswer(call -> {
                ByteBuffer input = call.getArgument(0);
                assertTrue(input.isReadOnly());
                assertSame(heap, call.getArgument(1));
                assertThatThrownBy(() -> input.put((byte) 0)).isInstanceOf(ReadOnlyBufferException.class);
                assertEquals(((DirectBuffer) source.buffer).address(), ((DirectBuffer) input).address());
                input.get();
                return RESULT;
            });
            assertSame(RESULT, engine.wrap(readOnly, heap));
            assertEquals(1, readOnly.position());
            when(delegate.unwrap(any(ByteBuffer.class), any(ByteBuffer.class))).thenAnswer(call -> {
                ByteBuffer output = call.getArgument(1);
                assertTrue(output.isReadOnly());
                throw new ReadOnlyBufferException();
            });
            assertThatThrownBy(() -> engine.unwrap(source.buffer, destination.buffer.asReadOnlyBuffer())).isInstanceOf(ReadOnlyBufferException.class);
            ByteBuffer ordinary = ByteBuffer.allocateDirect(16);
            try
            {
                when(delegate.unwrap(same(ordinary), same(heap))).thenReturn(RESULT);
                assertSame(RESULT, engine.unwrap(ordinary, heap));
            }
            finally
            {
                MemoryUtil.clean(ordinary);
            }
        }
    }

    @Test
    public void testInvalidRangesAreValidatedByDelegate() throws Exception
    {
        SSLEngine delegate = mock(SSLEngine.class);
        CassandraSslEngine engine = new CassandraSslEngine(delegate);
        ByteBuffer[] buffers = new ByteBuffer[]{ ByteBuffer.allocate(16) };
        ByteBuffer other = ByteBuffer.allocate(16);
        when(delegate.wrap(same(buffers), eq(-1), eq(1), same(other))).thenThrow(IndexOutOfBoundsException.class);
        when(delegate.unwrap(same(other), same(buffers), eq(0), eq(2))).thenThrow(IndexOutOfBoundsException.class);
        when(delegate.wrap(isNull(), eq(0), eq(0), same(other))).thenThrow(IllegalArgumentException.class);
        assertThatThrownBy(() -> engine.wrap(buffers, -1, 1, other)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> engine.unwrap(other, buffers, 0, 2)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> engine.wrap((ByteBuffer[]) null, other)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testWrapAndUnwrapCanRunConcurrently() throws Exception
    {
        SSLEngine delegate = mock(SSLEngine.class);
        CassandraSslEngine engine = new CassandraSslEngine(delegate);
        CountDownLatch entered = new CountDownLatch(2);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (Attached ws = new Attached(); Attached wd = new Attached();
             Attached us = new Attached(); Attached ud = new Attached())
        {
            when(delegate.wrap(any(ByteBuffer.class), any(ByteBuffer.class))).thenAnswer(call -> {
                ByteBuffer input = call.getArgument(0);
                entered.countDown();
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                assertEquals(MemoryUtil.getAddress(ws.buffer), MemoryUtil.getAddress(input));
                input.get();
                return RESULT;
            });
            when(delegate.unwrap(any(ByteBuffer.class), any(ByteBuffer.class))).thenAnswer(call -> {
                ByteBuffer input = call.getArgument(0);
                entered.countDown();
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                assertEquals(MemoryUtil.getAddress(us.buffer), MemoryUtil.getAddress(input));
                input.get();
                return RESULT;
            });
            Future<SSLEngineResult> wrapped = executor.submit(() -> engine.wrap(ws.buffer, wd.buffer));
            Future<SSLEngineResult> unwrapped = executor.submit(() -> engine.unwrap(us.buffer, ud.buffer));
            assertSame(RESULT, wrapped.get(10, TimeUnit.SECONDS));
            assertSame(RESULT, unwrapped.get(10, TimeUnit.SECONDS));
            assertEquals(1, ws.buffer.position());
            assertEquals(1, us.buffer.position());
        }
        finally
        {
            executor.shutdownNow();
        }
    }

    @Test
    public void testEngineConfigurationIsDelegated() throws Exception
    {
        SSLEngine delegate = mock(SSLEngine.class);
        CassandraSslEngine engine = new CassandraSslEngine(delegate);
        for (Method method : SSLEngine.class.getDeclaredMethods())
        {
            if (!java.lang.reflect.Modifier.isPublic(method.getModifiers())
                || method.getName().equals("wrap") || method.getName().equals("unwrap")
                || method.getName().contains("Selector"))
                continue;
            Object[] arguments = new Object[method.getParameterCount()];
            for (int i = 0; i < arguments.length; ++i)
            {
                Class<?> type = method.getParameterTypes()[i];
                arguments[i] = type == boolean.class ? true : type == SSLParameters.class ? new SSLParameters() : new String[]{ "test" };
            }
            clearInvocations(delegate);
            method.invoke(engine, arguments);
            method.invoke(verify(delegate), arguments);
        }
    }

    @Test
    public void testAlpnAndResumptionIdentity() throws Exception
    {
        SSLEngine delegate = mock(SSLEngine.class);
        CassandraSslEngine engine = new CassandraSslEngine(delegate);
        BiFunction<SSLEngine, List<String>, String> selector = (selectedEngine, protocols) -> {
            assertSame(engine, selectedEngine);
            return protocols.get(0);
        };
        AtomicReference<BiFunction<SSLEngine, List<String>, String>> installed = new AtomicReference<>();
        doAnswer(call -> { installed.set(call.getArgument(0)); return null; }).when(delegate).setHandshakeApplicationProtocolSelector(any());
        when(delegate.getHandshakeApplicationProtocolSelector()).thenAnswer(call -> installed.get());
        engine.setHandshakeApplicationProtocolSelector(selector);
        assertSame(selector, engine.getHandshakeApplicationProtocolSelector());
        assertEquals("h2", installed.get().apply(delegate, Arrays.asList("h2")));
        engine.setHandshakeApplicationProtocolSelector(null);
        assertNull(engine.getHandshakeApplicationProtocolSelector());
        assertSame(delegate, ((JdkSslEngine) engine).getWrappedEngine());
        when(delegate.getApplicationProtocol()).thenReturn("h2");
        assertEquals("h2", engine.getNegotiatedApplicationProtocol());
    }

    @Test
    public void testResumptionControllerSeesTheUnderlyingEngine() throws Exception
    {
        SSLEngine delegate = mock(SSLEngine.class);
        SSLSession session = mock(SSLSession.class);
        when(delegate.getSession()).thenReturn(session);
        when(delegate.getUseClientMode()).thenReturn(true);
        when(session.isValid()).thenReturn(true);
        X509Certificate[] chain = new X509Certificate[0];
        when(session.getPeerCertificates()).thenReturn(chain);
        X509ExtendedTrustManager trust = mock(X509ExtendedTrustManager.class,
                                             withSettings().extraInterfaces(ResumableX509ExtendedTrustManager.class));
        ResumptionController controller = new ResumptionController();
        X509ExtendedTrustManager wrappedTrust = (X509ExtendedTrustManager) controller.wrapIfNeeded(trust);
        // ALPN contexts already decorate the underlying JDK engine.
        CassandraSslEngine engine = new CassandraSslEngine(new JdkSslEngine(delegate));
        wrappedTrust.checkServerTrusted(chain, "RSA", delegate);
        assertFalse(controller.validateResumeIfNeeded(engine));
        assertTrue(controller.validateResumeIfNeeded(engine));
        verify((ResumableX509ExtendedTrustManager) trust).resumeServerTrusted(chain, delegate);
    }

    private static class Attached implements AutoCloseable
    {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(32);
        final Object attachment = new Object();

        Attached()
        {
            MemoryUtil.setAttachment(buffer, attachment);
        }

        public void close()
        {
            MemoryUtil.setAttachment(buffer, null);
            MemoryUtil.clean(buffer);
        }
    }
}
