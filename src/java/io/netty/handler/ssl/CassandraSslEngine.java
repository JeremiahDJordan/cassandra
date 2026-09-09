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

import java.lang.ref.Reference;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

import org.apache.cassandra.utils.memory.MemoryUtil;

import io.netty.buffer.ByteBufAllocator;
import io.netty.util.concurrent.ImmediateExecutor;
import sun.nio.ch.DirectBuffer;

/**
 * Presents pooled buffers to JSSE without exposing their ownership attachments. JDK 25's AES-GCM implementation
 * casts attachments to Buffer. The original buffers retain their owners while reusable views borrow their memory
 * for each call; SSLEngine requires disjoint input/output memory and does not retain application data between calls.
 *
 * This class resides in Netty's package so its resumption controller can unwrap it as a JdkSslEngine, and so handlers
 * retain the context's resumption controller. A plain SSLEngine decorator would hide the engine's validation identity.
 */
public final class CassandraSslEngine extends JdkSslEngine
{
    private static final Field START_TLS;
    private final SSLEngine delegate;

    static
    {
        try
        {
            // Netty exposes neither a getter nor a handler factory accepting a decorated engine.
            START_TLS = SslContext.class.getDeclaredField("startTls");
            START_TLS.setAccessible(true);
        }
        catch (ReflectiveOperationException e)
        {
            throw new ExceptionInInitializerError(e);
        }
    }

    // wrap and unwrap may run concurrently; two calls in the same direction must be serialized by the caller.
    private final Buffers wrapBuffers = new Buffers();
    private final Buffers unwrapBuffers = new Buffers();

    private BiFunction<SSLEngine, List<String>, String> selector;
    private BiFunction<SSLEngine, List<String>, String> delegateSelector;

    public CassandraSslEngine(SSLEngine engine)
    {
        super(engine);
        delegate = engine;
    }

    @Override
    public SSLEngine getWrappedEngine()
    {
        // Netty's resumption controller unwraps only one layer, including when ALPN already wrapped the engine.
        SSLEngine engine = delegate;
        while (engine instanceof JdkSslEngine)
            engine = ((JdkSslEngine) engine).getWrappedEngine();
        return engine;
    }

    public static SslHandler newHandler(JdkSslContext context, ByteBufAllocator allocator, String peerHost, int peerPort)
    {
        SSLEngine engine = peerHost == null ? context.newEngine(allocator) : context.newEngine(allocator, peerHost, peerPort);
        try
        {
            return new SslHandler(engine instanceof ConscryptAlpnSslEngine ? engine : new CassandraSslEngine(engine),
                                  START_TLS.getBoolean(context),
                                  ImmediateExecutor.INSTANCE, context.resumptionController);
        }
        catch (IllegalAccessException e)
        {
            throw new IllegalStateException("Cannot read Netty's STARTTLS configuration", e);
        }
    }

    @Override
    public SSLEngineResult wrap(ByteBuffer src, ByteBuffer dst) throws SSLException
    {
        return single(src, dst, true);
    }

    @Override
    public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer dst) throws SSLException
    {
        return single(src, dst, false);
    }

    private SSLEngineResult single(ByteBuffer src, ByteBuffer dst, boolean wrap) throws SSLException
    {
        Buffers buffers = wrap ? wrapBuffers : unwrapBuffers;
        ByteBuffer source = buffers.single.prepare(src);
        ByteBuffer destination = buffers.one.prepare(dst);
        try
        {
            return wrap ? delegate.wrap(source, destination) : delegate.unwrap(source, destination);
        }
        finally
        {
            finish(src, source);
            finish(dst, destination);
        }
    }

    @Override
    public SSLEngineResult wrap(ByteBuffer[] srcs, ByteBuffer dst) throws SSLException
    {
        return wrap(srcs, 0, srcs == null ? 0 : srcs.length, dst);
    }

    @Override
    public SSLEngineResult wrap(ByteBuffer[] srcs, int offset, int length, ByteBuffer dst) throws SSLException
    {
        if (!validRange(srcs, offset, length))
            return delegate.wrap(srcs, offset, length, dst);
        ByteBuffer[] sources = wrapBuffers.prepare(srcs, offset, length);
        ByteBuffer destination = wrapBuffers.one.prepare(dst);
        try
        {
            return delegate.wrap(sources, 0, length, destination);
        }
        finally
        {
            wrapBuffers.finish(srcs, offset, length);
            finish(dst, destination);
        }
    }

    @Override
    public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dsts) throws SSLException
    {
        return unwrap(src, dsts, 0, dsts == null ? 0 : dsts.length);
    }

    @Override
    public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dsts, int offset, int length) throws SSLException
    {
        if (!validRange(dsts, offset, length))
            return delegate.unwrap(src, dsts, offset, length);
        ByteBuffer source = unwrapBuffers.one.prepare(src);
        ByteBuffer[] destinations = unwrapBuffers.prepare(dsts, offset, length);
        try
        {
            return delegate.unwrap(source, destinations, 0, length);
        }
        finally
        {
            finish(src, source);
            unwrapBuffers.finish(dsts, offset, length);
        }
    }

    private static boolean validRange(ByteBuffer[] buffers, int offset, int length)
    {
        return buffers != null && offset >= 0 && length >= 0 && offset <= buffers.length - length;
    }

    private static void finish(ByteBuffer original, ByteBuffer view)
    {
        if (original != view)
            original.position(view.position());
        Reference.reachabilityFence(original);
    }

    private static final class View
    {
        private ByteBuffer writable;
        private ByteBuffer readOnly;

        ByteBuffer prepare(ByteBuffer buffer)
        {
            if (buffer == null || !buffer.isDirect())
                return buffer;
            Object attachment = ((DirectBuffer) buffer).attachment();
            while (attachment instanceof Buffer && attachment instanceof DirectBuffer)
                attachment = ((DirectBuffer) attachment).attachment();
            if (attachment == null)
                return buffer; // Preserve ordinary NIO buffers, including scoped memory-segment buffers.

            if (writable == null)
                writable = MemoryUtil.getHollowDirectByteBuffer();
            writable.clear();
            MemoryUtil.duplicateDirectByteBuffer(buffer, writable);
            if (!buffer.isReadOnly())
                return writable;
            if (readOnly == null)
                readOnly = writable.asReadOnlyBuffer();
            readOnly.clear();
            // The read-only view's attachment is writable, with the same base address and no parent attachment.
            return MemoryUtil.duplicateDirectByteBuffer(buffer, readOnly);
        }
    }

    private static final class Buffers
    {
        final View single = new View();
        final View one = new View();
        private View[] views = new View[0];
        private ByteBuffer[] prepared = new ByteBuffer[0];

        ByteBuffer[] prepare(ByteBuffer[] originals, int offset, int length)
        {
            if (views.length < length)
            {
                int oldLength = views.length;
                views = Arrays.copyOf(views, length);
                prepared = new ByteBuffer[length];
                for (int i = oldLength; i < length; ++i)
                    views[i] = new View();
            }
            for (int i = 0; i < length; ++i)
            {
                ByteBuffer original = originals[offset + i];
                int alias = 0;
                while (alias < i && originals[offset + alias] != original)
                    ++alias;
                prepared[i] = alias == i ? views[i].prepare(original) : prepared[alias];
            }
            return prepared;
        }

        void finish(ByteBuffer[] originals, int offset, int length)
        {
            try
            {
                for (int i = 0; i < length; ++i)
                    CassandraSslEngine.finish(originals[offset + i], prepared[i]);
            }
            finally
            {
                Arrays.fill(prepared, 0, length, null);
                Reference.reachabilityFence(originals);
            }
        }
    }

    @Override
    public String getNegotiatedApplicationProtocol()
    {
        SSLEngine engine = delegate;
        return engine instanceof ApplicationProtocolAccessor
               ? ((ApplicationProtocolAccessor) engine).getNegotiatedApplicationProtocol()
               : engine.getApplicationProtocol();
    }

    @Override
    public String getApplicationProtocol()
    {
        return delegate.getApplicationProtocol();
    }

    @Override
    public String getHandshakeApplicationProtocol()
    {
        return delegate.getHandshakeApplicationProtocol();
    }

    @Override
    public void setHandshakeApplicationProtocolSelector(BiFunction<SSLEngine, List<String>, String> selector)
    {
        BiFunction<SSLEngine, List<String>, String> delegateSelector = selector == null
                                                                     ? null
                                                                     : (engine, protocols) -> selector.apply(this, protocols);
        delegate.setHandshakeApplicationProtocolSelector(delegateSelector);
        this.selector = selector;
        this.delegateSelector = delegateSelector;
    }

    @Override
    public BiFunction<SSLEngine, List<String>, String> getHandshakeApplicationProtocolSelector()
    {
        BiFunction<SSLEngine, List<String>, String> selector = delegate.getHandshakeApplicationProtocolSelector();
        return selector == delegateSelector ? this.selector : selector;
    }
}
