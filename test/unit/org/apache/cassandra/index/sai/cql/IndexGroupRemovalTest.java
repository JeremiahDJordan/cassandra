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

package org.apache.cassandra.index.sai.cql;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableContextManager;
import org.apache.cassandra.index.sai.StorageAttachedIndexBuilder;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class IndexGroupRemovalTest extends SAITester
{
    private static volatile boolean holdBuild;
    private static volatile boolean buildHeld;
    private static volatile boolean holdDrop;
    private static volatile boolean dropHeld;

    /** called by the injected code: holds the index build there while {@link #holdBuild} is set */
    public static void holdBuild() throws InterruptedException
    {
        if (!holdBuild || Arrays.stream(Thread.currentThread().getStackTrace()).noneMatch(e -> e.getMethodName().equals("completeSSTable")))
            return;
        buildHeld = true;
        while (holdBuild)
            Thread.sleep(10);
    }

    /** called by the injected code: holds the drop of the last index there while {@link #holdDrop} is set */
    public static void holdDrop() throws InterruptedException
    {
        if (!holdDrop)
            return;
        dropHeld = true;
        while (holdDrop)
            Thread.sleep(10);
    }

    @Before
    public void setup()
    {
        requireNetwork();
    }

    @Test
    public void testDropAndRecreate() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, value text, PRIMARY KEY (pk))");
        populateOneSSTable();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        Tracker tracker = cfs.getTracker();

        // create index and drop it: StorageAttachedIndexGroup should be removed
        createIndex("CREATE CUSTOM INDEX sai ON %s(value) USING 'StorageAttachedIndex'");

        StorageAttachedIndexGroup group = (StorageAttachedIndexGroup) cfs.indexManager.getIndexGroup(StorageAttachedIndexGroup.GROUP_KEY);
        assertTrue(tracker.contains(group));
        assertEquals(1, group.sstableContextManager().size());

        dropIndex(format("DROP INDEX %s.sai", KEYSPACE));
        assertFalse(tracker.contains(group));
        assertEquals(0, group.sstableContextManager().size()); // sstable should be cleared from old group
        assertNull(cfs.indexManager.getIndexGroup(StorageAttachedIndexGroup.GROUP_KEY));

        // populate 2nd sstable. Old group should not track it
        populateOneSSTable();
        assertEquals(0, group.sstableContextManager().size());

        // create index again: expect a new StorageAttachedIndexGroup to be registered into tracker
        createIndex("CREATE CUSTOM INDEX sai ON %s(value) USING 'StorageAttachedIndex'");

        StorageAttachedIndexGroup newGroup = (StorageAttachedIndexGroup) cfs.indexManager.getIndexGroup(StorageAttachedIndexGroup.GROUP_KEY);
        assertNotSame(group, newGroup);
        assertTrue(tracker.contains(newGroup));
        assertEquals(2, newGroup.sstableContextManager().size());

        // populate 3rd sstable. new group should  track it
        populateOneSSTable();
        assertEquals(3, newGroup.sstableContextManager().size());
    }

    @Test
    public void testDropLastIndexWhileBuildCompletesSSTable() throws Throwable
    {
        // the drop releases the contexts after the build has checked that its index exists, and after it has loaded
        // the SSTable for the views
        testDropLastIndexWhileBuildCompletesSSTable(InvokePointBuilder.newInvokePoint()
                                                                      .onClass(StorageAttachedIndexBuilder.class)
                                                                      .onMethod("completeSSTable")
                                                                      .after("INVOKE validateIndexes"));
        testDropLastIndexWhileBuildCompletesSSTable(InvokePointBuilder.newInvokePoint()
                                                                      .onClass(StorageAttachedIndexGroup.class)
                                                                      .onMethod("onSSTableChanged")
                                                                      .after("INVOKE " + SSTableContextManager.class.getName() + ".update"));
    }

    private void testDropLastIndexWhileBuildCompletesSSTable(InvokePointBuilder holdBuildAt) throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, value text, PRIMARY KEY (pk))");
        populateOneSSTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        Injections.Counter failures = Injections.newCounter("failures_" + System.nanoTime())
                                                .add(InvokePointBuilder.newInvokePoint()
                                                                       .onClass(StorageAttachedIndexBuilder.class)
                                                                       .onMethod("completeSSTable")
                                                                       .atExceptionExit())
                                                .build();
        Injections.Counter completions = Injections.newCounter("completions_" + System.nanoTime())
                                                   .add(InvokePointBuilder.newInvokePoint()
                                                                          .onClass(StorageAttachedIndexBuilder.class)
                                                                          .onMethod("completeSSTable")
                                                                          .atExit())
                                                   .build();
        Injections.inject(failures,
                          completions,
                          hold("hold_build_", holdBuildAt, "holdBuild"),
                          // once the drop has released the contexts, before it deletes their files
                          hold("hold_drop_",
                               InvokePointBuilder.newInvokePoint()
                                                 .onClass(StorageAttachedIndexGroup.class)
                                                 .onMethod("removeIndex")
                                                 .at("INVOKE forEach 2"),
                               "holdDrop"));
        try
        {
            buildHeld = false;
            dropHeld = false;
            holdBuild = true;
            holdDrop = true;
            String index = createIndexAsync("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");
            StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
            waitForAssert(() -> assertTrue(buildHeld), 60, TimeUnit.SECONDS);

            // the drop is held once it has released the contexts, unless it waits for the build's view update
            AtomicReference<Throwable> dropFailure = new AtomicReference<>();
            Thread drop = new Thread(() -> {
                try
                {
                    dropIndex("DROP INDEX %s." + index);
                }
                catch (Throwable t)
                {
                    dropFailure.set(t);
                }
            });
            drop.start();
            waitForAssert(() -> assertTrue(dropHeld || (group.getIndexes().isEmpty() && drop.getState() == Thread.State.BLOCKED)),
                          60, TimeUnit.SECONDS);
            holdBuild = false;
            waitForAssert(() -> assertEquals(1, failures.get() + completions.get()), 60, TimeUnit.SECONDS);
            holdDrop = false;
            drop.join(TimeUnit.SECONDS.toMillis(60));
            assertFalse(drop.isAlive());
            assertNull(dropFailure.get());
            assertNull(cfs.indexManager.getIndexGroup(StorageAttachedIndexGroup.GROUP_KEY));

            assertEquals(0, failures.get());
            assertEquals(0, group.sstableContextManager().size());
        }
        finally
        {
            holdBuild = false;
            holdDrop = false;
            Injections.deleteAll();
        }
    }

    private static Injection hold(String name, InvokePointBuilder at, String method)
    {
        return Injections.newCustom(name + System.nanoTime())
                         .add(at)
                         .add(ActionBuilder.newActionBuilder()
                                           .actions()
                                           .doAction(Expression.expr(IndexGroupRemovalTest.class.getName()).method(method).args()))
                         .build();
    }

    private void populateOneSSTable() throws Throwable
    {
        execute("INSERT INTO %s(pk, value) VALUES('k', 'v')");
        flush();
    }
}