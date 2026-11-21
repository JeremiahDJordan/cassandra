/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import org.apache.cassandra.CassandraIsolatedJunit4ClassRunner;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.config.CassandraRelevantProperties.BUFFER_POOL_ATTACHMENT_MODE;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_DEBUG_REF_COUNT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

/** Exercise both the production slab directory and the traced lease lookup, regardless of the test JVM defaults. */
@RunWith(Enclosed.class)
public class BufferPoolSlabTest
{
    @RunWith(CassandraIsolatedJunit4ClassRunner.class)
    public abstract static class Slab extends BufferPoolTest
    {
        private static final WithProperties properties = new WithProperties();

        static void configure(boolean trace)
        {
            properties.set(BUFFER_POOL_ATTACHMENT_MODE, "slab").set(TEST_DEBUG_REF_COUNT, trace);
            assertEquals(trace, Ref.TRACE_ENABLED);
        }

        @AfterClass
        public static void tearDownIsolatedClass() throws InterruptedException, TimeoutException
        {
            properties.close();
            Ref.shutdownReferenceReaper(10, TimeUnit.SECONDS);
        }
    }

    public static class Untraced extends Slab
    {
        @BeforeClass
        public static void disableTracing()
        {
            configure(false);
        }
    }

    public static class Traced extends Slab
    {
        @BeforeClass
        public static void enableTracing()
        {
            configure(true);
        }

        @Test
        public void testTracedBufferReturnedTwice()
        {
            ByteBuffer buffer = bufferPool.tryGet(512);
            CompletableFuture.runAsync(() -> bufferPool.put(buffer)).join();
            assertThatThrownBy(() -> bufferPool.put(buffer))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("already been released");
            assertEquals(0, bufferPool.usedSizeInBytes());
            assertEquals(0, bufferPool.overflowMemoryInBytes());
        }
    }
}
