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
import org.apache.cassandra.db.compaction.TableOperation;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NonThrowingCloseable;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(BMUnitRunner.class)
public class CassandraIndexDropTest extends CQLTester
{
    private static volatile boolean compactionRequested;
    private static volatile CompactionInFlight compaction;
    private static volatile boolean stoppedBeforeInvalidate;

    /**
     * Called by {@link CassandraIndex#invalidate} once it has stopped the compactions of the index table: starts a
     * compaction of its sstables, as one started after that, for instance once the flush of the index table completes.
     */
    public static void startCompaction(ColumnFamilyStore indexCfs)
    {
        if (compactionRequested)
        {
            compactionRequested = false;
            compaction = new CompactionInFlight(indexCfs);
        }
    }

    public static void recordStopped()
    {
        stoppedBeforeInvalidate = compaction != null && compaction.isStopRequested();
    }

    @Test
    @BMRule(name = "Start a compaction of the index table right before it is invalidated",
            targetClass = "CassandraIndex",
            targetMethod = "invalidate",
            targetLocation = "AT INVOKE org.apache.cassandra.db.ColumnFamilyStore.invalidate()",
            action = "org.apache.cassandra.index.internal.CassandraIndexDropTest.startCompaction($0.getIndexCfs())")
    public void testRecreateIndexWhileCompactionOfDroppedIndexInFlight() throws Throwable
    {
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

        compactionRequested = true;
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

    @Test
    @BMRules(rules = { @BMRule(name = "Start a compaction of the index table while the drop waits for its compactions",
                               targetClass = "CassandraIndex",
                               targetMethod = "invalidate",
                               targetLocation = "AT INVOKE org.apache.cassandra.db.compaction.CompactionManager.waitForCessation",
                               action = "org.apache.cassandra.index.internal.CassandraIndexDropTest.startCompaction($0.getIndexCfs())"),
                       @BMRule(name = "Record whether the compaction is stopped before the index table is invalidated",
                               targetClass = "CassandraIndex",
                               targetMethod = "invalidate",
                               targetLocation = "AT INVOKE org.apache.cassandra.db.ColumnFamilyStore.invalidate()",
                               action = "org.apache.cassandra.index.internal.CassandraIndexDropTest.recordStopped()") })
    public void testDropStopsCompactionStartedWhileWaitingForCompactions() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        createIndex("CREATE INDEX idx ON %s(v) USING 'legacy_local_table'");
        ColumnFamilyStore indexCfs = indexCfs();
        indexCfs.disableAutoCompaction();
        for (int i = 0; i < 2; i++)
        {
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, i);
            flush();
        }

        compaction = null;
        stoppedBeforeInvalidate = false;
        compactionRequested = true;
        dropIndex("DROP INDEX %s.idx");
        assertNotNull(compaction);
        compaction.complete();

        assertTrue("The drop didn't stop the compaction started while it waited for the compactions of the index table",
                   stoppedBeforeInvalidate);
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
        private final TableOperation op;
        private volatile boolean complete;

        CompactionInFlight(ColumnFamilyStore cfs)
        {
            Set<SSTableReader> sstables = cfs.getLiveSSTables();
            LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
            assertNotNull(txn);
            List<ISSTableScanner> scanners = sstables.stream().map(SSTableReader::getScanner).collect(Collectors.toList());
            CompactionController controller = new CompactionController(cfs, sstables, Integer.MIN_VALUE);
            CompactionIterator ci = new CompactionIterator(txn.opType(), scanners, controller, FBUtilities.nowInSeconds(), nextTimeUUID());
            op = ci.getOperation();
            NonThrowingCloseable registration = CompactionManager.instance.active.onOperationStart(op);

            thread = new Thread(() -> {
                try
                {
                    while (!complete && !op.isStopRequested())
                        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);

                    ci.close();
                    controller.close();
                    if (op.isStopRequested())
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
                    registration.close();
                }
            });
            thread.start();
        }

        boolean isStopRequested()
        {
            return op.isStopRequested();
        }

        void complete() throws InterruptedException
        {
            complete = true;
            thread.join();
        }
    }
}
