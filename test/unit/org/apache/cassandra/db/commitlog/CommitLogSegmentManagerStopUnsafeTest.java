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

package org.apache.cassandra.db.commitlog;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests that {@link AbstractCommitLogSegmentManager#stopUnsafe(boolean)} waits for the segment allocator thread to
 * terminate when assertions are disabled.
 */
@RunWith(BMUnitRunner.class)
public class CommitLogSegmentManagerStopUnsafeTest
{
    static
    {
        // Disable assertions for the segment manager before its class is initialized, so this test runs the code as
        // it runs with -da. This has no effect if the class has already been initialized in this JVM.
        CommitLogSegmentManagerStopUnsafeTest.class.getClassLoader()
                                                   .setClassAssertionStatus("org.apache.cassandra.db.commitlog.AbstractCommitLogSegmentManager", false);
    }

    private static final AtomicBoolean delayAllocatorShutdown = new AtomicBoolean();
    private static final CountDownLatch delayedAllocatorShutDown = new CountDownLatch(1);
    private static volatile boolean delayed;

    public static void maybeDelayAllocatorShutdown() throws InterruptedException
    {
        if (delayAllocatorShutdown.compareAndSet(true, false))
        {
            delayed = true;
            Thread.sleep(2000);
        }
    }

    public static void allocatorShutDown()
    {
        if (delayed)
            delayedAllocatorShutDown.countDown();
    }

    @BeforeClass
    public static void setUp() throws ReflectiveOperationException
    {
        DatabaseDescriptor.daemonInitialization();

        // If the segment manager was initialized before the static block above, its assertions are enabled, and the
        // test would pass without waiting outside the assert.
        Field assertionsDisabled = AbstractCommitLogSegmentManager.class.getDeclaredField("$assertionsDisabled");
        assertionsDisabled.setAccessible(true);
        assertTrue("Assertions must be disabled for AbstractCommitLogSegmentManager", assertionsDisabled.getBoolean(null));
    }

    @Test
    @BMRules(rules = { @BMRule(name = "Delay the allocator thread when it shuts down",
                               targetClass = "AbstractCommitLogSegmentManager$AllocatorRunnable",
                               targetMethod = "run",
                               targetLocation = "AT INVOKE org.apache.cassandra.db.commitlog.AbstractCommitLogSegmentManager.discardAvailableSegment",
                               action = "org.apache.cassandra.db.commitlog.CommitLogSegmentManagerStopUnsafeTest.maybeDelayAllocatorShutdown()"),
                       @BMRule(name = "Signal that the allocator thread has shut down",
                               targetClass = "AbstractCommitLogSegmentManager$AllocatorRunnable",
                               targetMethod = "run",
                               targetLocation = "AFTER INVOKE org.apache.cassandra.db.commitlog.AbstractCommitLogSegmentManager.discardAvailableSegment",
                               action = "org.apache.cassandra.db.commitlog.CommitLogSegmentManagerStopUnsafeTest.allocatorShutDown()") })
    public void testStopUnsafeWaitsForAllocator() throws Throwable
    {
        File directory = new File(Files.createTempDirectory("commitlog-stop-unsafe"));
        CommitLogSegmentManagerStandard manager = new CommitLogSegmentManagerStandard(CommitLog.instance, directory.path());
        manager.start();
        try
        {
            delayAllocatorShutdown.set(true);
            manager.stopUnsafe(true);
            assertEquals("stopUnsafe returned before the allocator thread shut down", 0, delayedAllocatorShutDown.getCount());

            // If stopUnsafe returned while the old allocator thread was still shutting down, that thread could discard
            // the segment the new allocator prepares, and allocating the next segment would wait forever.
            manager.start();
            CommitLogSegment first = manager.allocatingFrom();
            CompletableFuture.runAsync(() -> manager.advanceAllocatingFrom(first)).get(30, TimeUnit.SECONDS);
            assertNotSame(first, manager.allocatingFrom());
        }
        finally
        {
            manager.stopUnsafe(true);
        }
    }
}
