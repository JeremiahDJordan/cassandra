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

package org.apache.cassandra.index.internal;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(BMUnitRunner.class)
public class CassandraIndexDropTest extends CQLTester
{
    private static volatile boolean startCompaction;
    private static volatile CompactionInFlight compaction;

    /**
     * Called right before {@link CassandraIndex#invalidate} invalidates the index table: starts a compaction of its
     * sstables, as one started after the compactions of the index table were stopped, for instance once the flush of
     * the index table completes.
     */
    public static void startCompaction(ColumnFamilyStore indexCfs)
    {
        if (startCompaction)
        {
            startCompaction = false;
            compaction = new CompactionInFlight(indexCfs);
        }
    }

    /**
     * Called by {@link org.apache.cassandra.db.lifecycle.Tracker#dropSSTables} once it has removed sstables from the live
     * set, before it releases them: holds the stopped compaction, whose transaction drops the sstables of the invalid
     * table, there.
     */
    public static void pauseBeforeReleasingDroppedSSTables()
    {
        if (compaction != null && Thread.currentThread() == compaction.thread)
        {
            compaction.held.countDown();
            Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
        }
    }

    /**
     * Called by {@link CassandraIndex#invalidate} once the index table is invalid: ends the compaction, which drops the
     * sstables it holds from the invalid table, and waits until it is held there.
     */
    public static void endCompactionAndAwaitHold()
    {
        compaction.abort = true;
        if (!Uninterruptibles.awaitUninterruptibly(compaction.held, 20, TimeUnit.SECONDS))
            throw new AssertionError("The compaction didn't reach the drop of its sstables");
    }

    @Test
    @BMRule(name = "Start a compaction of the index table right before it is invalidated",
            targetClass = "CassandraIndex",
            targetMethod = "invalidate",
            targetLocation = "AT INVOKE org.apache.cassandra.db.ColumnFamilyStore.invalidate()",
            action = "org.apache.cassandra.index.internal.CassandraIndexDropTest.startCompaction($0.getIndexCfs())")
    public void testRecreateIndexWhileCompactionOfDroppedIndexInFlight() throws Throwable
    {
        checkRecreateIndexWhileCompactionOfDroppedIndexInFlight();
    }

    @Test
    @BMRules(rules = { @BMRule(name = "Start a compaction of the index table right before it is invalidated",
                               targetClass = "CassandraIndex",
                               targetMethod = "invalidate",
                               targetLocation = "AT INVOKE org.apache.cassandra.db.ColumnFamilyStore.invalidate()",
                               action = "org.apache.cassandra.index.internal.CassandraIndexDropTest.startCompaction($0.getIndexCfs())"),
                       @BMRule(name = "Hold the stopped compaction after it removes the sstables of the invalid table from the live set",
                               targetClass = "Tracker",
                               targetMethod = "dropSSTables",
                               targetLocation = "AT INVOKE finish",
                               action = "org.apache.cassandra.index.internal.CassandraIndexDropTest.pauseBeforeReleasingDroppedSSTables()") })
    public void testRecreateIndexWhileCompactionOfDroppedIndexReleasesItsSSTables() throws Throwable
    {
        checkRecreateIndexWhileCompactionOfDroppedIndexInFlight();
    }

    @Test
    @BMRules(rules = { @BMRule(name = "Start a compaction of the index table right before it is invalidated",
                               targetClass = "CassandraIndex",
                               targetMethod = "invalidate",
                               targetLocation = "AT INVOKE org.apache.cassandra.db.ColumnFamilyStore.invalidate()",
                               action = "org.apache.cassandra.index.internal.CassandraIndexDropTest.startCompaction($0.getIndexCfs())"),
                       @BMRule(name = "End the compaction once the index table is invalid",
                               targetClass = "CassandraIndex",
                               targetMethod = "invalidate",
                               targetLocation = "AFTER INVOKE org.apache.cassandra.db.ColumnFamilyStore.invalidate()",
                               action = "org.apache.cassandra.index.internal.CassandraIndexDropTest.endCompactionAndAwaitHold()"),
                       @BMRule(name = "Hold the compaction after it removes the sstables of the invalid table from the live set",
                               targetClass = "Tracker",
                               targetMethod = "dropSSTables",
                               targetLocation = "AT INVOKE finish",
                               action = "org.apache.cassandra.index.internal.CassandraIndexDropTest.pauseBeforeReleasingDroppedSSTables()") })
    public void testRecreateIndexWhileCompactionEndedAfterInvalidationReleasesItsSSTables() throws Throwable
    {
        checkRecreateIndexWhileCompactionOfDroppedIndexInFlight();
    }

    private void checkRecreateIndexWhileCompactionOfDroppedIndexInFlight() throws Throwable
    {
        compaction = null;
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        createIndex("CREATE INDEX idx ON %s(v) USING 'legacy_local_table'");
        ColumnFamilyStore indexCfs = indexCfs();
        indexCfs.disableAutoCompaction();
        for (int i = 0; i < 2; i++)
        {
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, i);
            flush();
        }
        Set<Descriptor> droppedIndexSSTables = descriptors(indexCfs.getLiveSSTables());
        assertTrue(droppedIndexSSTables.size() >= 2);

        startCompaction = true;
        dropIndex("DROP INDEX %s.idx");
        assertNotNull(compaction);

        createIndex("CREATE INDEX idx ON %s(v) USING 'legacy_local_table'");
        Set<Descriptor> loaded = descriptors(indexCfs().getLiveSSTables());
        loaded.retainAll(droppedIndexSSTables);

        // the compaction completes, unless the drop stopped it
        compaction.complete();

        dropIndex("DROP INDEX %s.idx");
        assertTrue("The new index loaded sstables of the dropped index: " + loaded, loaded.isEmpty());
    }

    private ColumnFamilyStore indexCfs()
    {
        return ((CassandraIndex) getCurrentColumnFamilyStore().indexManager.getIndexByName("idx")).getIndexCfs();
    }

    private static Set<Descriptor> descriptors(Set<SSTableReader> sstables)
    {
        return sstables.stream().map(sstable -> sstable.descriptor).collect(Collectors.toSet());
    }

    /**
     * A compaction that runs until it is stopped, in which case it aborts, or until {@link #complete} is called, in
     * which case it obsoletes the sstables it compacts.
     */
    private static class CompactionInFlight
    {
        private final Thread thread;
        private final CountDownLatch held = new CountDownLatch(1);
        private volatile boolean complete;
        private volatile boolean abort;

        CompactionInFlight(ColumnFamilyStore cfs)
        {
            Set<SSTableReader> sstables = cfs.getLiveSSTables();
            LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
            assertNotNull(txn);
            List<ISSTableScanner> scanners = sstables.stream().map(SSTableReader::getScanner).collect(Collectors.toList());
            CompactionController controller = new CompactionController(cfs, sstables, Integer.MIN_VALUE);
            CompactionIterator ci = new CompactionIterator(txn.opType(), scanners, controller, FBUtilities.nowInSeconds(), nextTimeUUID());
            CompactionManager.instance.active.beginCompaction(ci);

            thread = new Thread(() -> {
                try
                {
                    while (!complete && !abort && !ci.isStopRequested())
                        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);

                    ci.close();
                    controller.close();
                    if (abort || ci.isStopRequested())
                    {
                        txn.abort();
                    }
                    else
                    {
                        txn.obsoleteOriginals();
                        txn.finish();
                    }
                }
                finally
                {
                    txn.close();
                    CompactionManager.instance.active.finishCompaction(ci);
                }
            });
            thread.start();
        }

        void complete() throws InterruptedException
        {
            complete = true;
            thread.join();
        }
    }
}
