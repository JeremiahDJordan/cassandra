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

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.security.Provider;
import java.security.Security;
import java.util.Random;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.memory.BufferPool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * CASSANDRA-21171 / "Netty Option B" transport-level validation.
 *
 * <p>JDK 25 rewrote {@code com.sun.crypto.provider.GaloisCounterMode.overlapDetection(src, dst)} to, when both
 * operands are direct, walk each buffer's {@code att} chain casting every link to {@code java.nio.Buffer}. A pooled
 * buffer sliced from a {@link BufferPool} macro chunk carries a {@code Chunk} (or, under {@code -Dcassandra.debugrefcount},
 * a {@code Ref.DirectBufferRef}) in {@code att} - neither is a {@code java.nio.Buffer} - so AES-GCM (the default TLS
 * cipher) throws {@link ClassCastException} on JDK 25 and breaks every encrypted connection.
 *
 * <p>This exercises the exact AES-GCM operation that {@code SSLEngine.wrap/unwrap} performs - direct src + direct dst
 * via the {@code SunJCE} {@code GaloisCounterMode} - on buffers drawn from two allocators, and asserts:
 *
 * <ul>
 *   <li>{@link #aesGcmWithNettyPooledBuffersWorksWithoutMarker()} - operands from Netty's
 *       {@link PooledByteBufAllocator#nioBuffer()} succeed on every JDK <b>regardless of the marker</b>, because
 *       Netty's nioBuffer views have a legal att chain (this is the Option B claim: transport buffers need no marker).</li>
 *   <li>{@link #aesGcmWithSlicedBufferPoolBuffersDependsOnMarker()} - operands sliced from a {@link BufferPool} macro
 *       chunk throw {@code ClassCastException} on JDK&nbsp;25 <b>iff the marker is disabled</b>, proving the marker
 *       (not some other factor) is what currently rescues BufferPool buffers, and that Option B's Netty buffers do
 *       not rely on it.</li>
 * </ul>
 *
 * <p>The marker gate is a JVM-wide static; to exercise the marker-off case, run with
 * {@code -Dcassandra.test.bufferpool.gcm_marker=false} (passed via {@code -Dtest.jvm.args}). The assertions adapt to
 * the actual gate value so the test is correct under both settings and on every JDK.
 */
public class SslHandlerGcmAllocatorTest
{
    private static final int LEN = 8192;
    private static SecretKeySpec KEY;
    private static byte[] IV;
    private static Provider SUN_JCE;
    private static boolean MARKER_ENABLED;

    @BeforeClass
    public static void setup() throws Exception
    {
        byte[] keyBytes = new byte[32];
        new Random(0).nextBytes(keyBytes);
        KEY = new SecretKeySpec(keyBytes, "AES");
        IV = new byte[12];
        new Random(1).nextBytes(IV);
        SUN_JCE = Security.getProvider("SunJCE");
        Assume.assumeTrue("SunJCE provider required to exercise OpenJDK GaloisCounterMode", SUN_JCE != null);

        // Read the (already-initialised) marker gate so assertions can adapt.
        Field f = BufferPool.class.getDeclaredField("GCM_NEEDS_BUFFER_ATTACHMENT");
        f.setAccessible(true);
        MARKER_ENABLED = f.getBoolean(null);
    }

    /** Encrypt {@code src} into {@code dst} with AES/GCM; both are direct nio operands -> GaloisCounterMode. */
    private static void aesGcmEncrypt(ByteBuffer src, ByteBuffer dst) throws Exception
    {
        int len = src.remaining();
        for (int i = 0; i < len; i++)
            src.put(src.position() + i, (byte) i);
        Cipher enc = Cipher.getInstance("AES/GCM/NoPadding", SUN_JCE);
        enc.init(Cipher.ENCRYPT_MODE, KEY, new GCMParameterSpec(128, IV));
        dst.clear();
        enc.doFinal(src, dst);   // direct src + direct dst -> overlapDetection att-walk on JDK 25
        dst.flip();
    }

    /**
     * Option B: Netty's PooledByteBufAllocator nioBuffer views carry a legal att chain, so AES-GCM succeeds on every
     * JDK whether or not the marker is enabled. This is the core justification for moving transport onto Netty's pool.
     */
    @Test
    public void aesGcmWithNettyPooledBuffersWorksWithoutMarker() throws Exception
    {
        ByteBufAllocator alloc = PooledByteBufAllocator.DEFAULT;
        ByteBuf plain = alloc.directBuffer(LEN);
        ByteBuf cipher = alloc.directBuffer(LEN + 16);
        try
        {
            ByteBuffer src = plain.nioBuffer(0, LEN);
            ByteBuffer dst = cipher.nioBuffer(0, LEN + 16);
            assertTrue(src.isDirect() && dst.isDirect());
            aesGcmEncrypt(src, dst);
            assertEquals(LEN + 16, dst.remaining());
        }
        finally
        {
            plain.release();
            cipher.release();
        }
    }

    /**
     * Control: buffers sliced from a single BufferPool macro chunk (att = Chunk / DirectBufferRef). On JDK 25 the
     * AES-GCM op throws ClassCastException when the marker is disabled, and succeeds when it is enabled - proving
     * the marker is exactly what rescues BufferPool operands today, and (contrasted with the test above) that Netty
     * buffers do not need it.
     */
    @Test
    public void aesGcmWithSlicedBufferPoolBuffersDependsOnMarker() throws Exception
    {
        BufferPool pool = new BufferPool("gcm_optionb_test", 8 * 1024 * 1024, true);
        ByteBuffer plain = pool.get(LEN, BufferType.OFF_HEAP);
        ByteBuffer cipher = pool.get(LEN + 16, BufferType.OFF_HEAP);
        try
        {
            plain.position(0).limit(LEN);
            boolean threw = false;
            try
            {
                aesGcmEncrypt(plain, cipher);
            }
            catch (ClassCastException cce)
            {
                threw = true;
            }

            boolean expectThrow = Runtime.version().feature() >= 25 && !MARKER_ENABLED;
            if (expectThrow)
                assertTrue("Expected sliced-BufferPool AES-GCM to throw ClassCastException on JDK 25 with the marker disabled "
                           + "(run with -Dtest.jvm.args=-Dcassandra.test.bufferpool.gcm_marker=false)", threw);
            else if (!threw)
                ; // marker enabled, or JDK <= 24: success is correct
            else
                fail("Unexpected ClassCastException: marker enabled (" + MARKER_ENABLED + ") on JDK "
                     + Runtime.version().feature() + " should have rescued the BufferPool operand");
        }
        finally
        {
            pool.put(plain);
            pool.put(cipher);
        }
    }
}
