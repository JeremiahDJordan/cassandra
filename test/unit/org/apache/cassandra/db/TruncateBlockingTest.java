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
package org.apache.cassandra.db;

import java.util.Collections;

import org.assertj.core.api.Assertions;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.compaction.AbstractTableOperation;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.TruncateException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.NonThrowingCloseable;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@RunWith(BMUnitRunner.class)
public class TruncateBlockingTest extends CQLTester
{
    @Test
    public void testTruncateFailsWhenCompactionsCannotBeDisabled()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v text)");

        execute("INSERT INTO %s (id, v) VALUES (1, 'a')");
        execute("INSERT INTO %s (id, v) VALUES (2, 'b')");
        execute("INSERT INTO %s (id, v) VALUES (3, 'c')");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        // Register a P0-priority compaction holder for this table to force
        // runWithCompactionsDisabled to return null immediately.
        AbstractTableOperation holder = new AbstractTableOperation()
        {
            public OperationProgress getProgress()
            {
                return new OperationProgress(cfs.metadata(),
                        OperationType.P0,
                        0,
                        100,
                        nextTimeUUID(),
                        Collections.emptySet());
            }

            public boolean isGlobal()
            {
                return false;
            }
        };

        NonThrowingCloseable holderCloseable = CompactionManager.instance.active.onOperationStart(holder);
        try
        {
            Assertions.assertThatThrownBy(cfs::truncateBlocking)
                    .as("Unable to stop compaction. Usually retrying truncate will work")
                    .isInstanceOf(TruncateException.class);

            assertRows(execute("SELECT * FROM %s WHERE id = 1"), row(1, "a"));
            assertRows(execute("SELECT * FROM %s WHERE id = 2"), row(2, "b"));
            assertRows(execute("SELECT * FROM %s WHERE id = 3"), row(3, "c"));
            assertFalse("SSTables should still be present after truncation failure",
                    cfs.getLiveSSTables().isEmpty());

        }
        finally
        {
            holderCloseable.close();
        }
    }

    @Test
    @BMRule(name = "no-op waitForCessation",
            targetClass = "org.apache.cassandra.db.compaction.CompactionManager",
            targetMethod = "waitForCessation",
            action = "return;")
    public void testTruncateFailsWhenCompactionsDoNotStopInTime()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v text)");

        execute("INSERT INTO %s (id, v) VALUES (1, 'a')");
        execute("INSERT INTO %s (id, v) VALUES (2, 'b')");
        execute("INSERT INTO %s (id, v) VALUES (3, 'c')");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        // Mark the sstable as compacting directly in the tracker, without registering a
        // table operation. There is nothing for interruptCompactionForCFs to stop, so
        // runWithCompactionsDisabled falls through to waitForCessation, then finds the sstable
        // still in the compacting set and returns null.
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstable, OperationType.ANTICOMPACTION))
        {
            assertNotNull("Unable to mark sstable compacting", txn);

            Assertions.assertThatThrownBy(cfs::truncateBlocking)
                    .as("Unable to stop compaction. Usually retrying truncate will work")
                    .isInstanceOf(TruncateException.class);

            assertRows(execute("SELECT * FROM %s WHERE id = 1"), row(1, "a"));
            assertRows(execute("SELECT * FROM %s WHERE id = 2"), row(2, "b"));
            assertRows(execute("SELECT * FROM %s WHERE id = 3"), row(3, "c"));
            assertFalse("SSTables should still be present after truncation failure",
                    cfs.getLiveSSTables().isEmpty());
        }
    }

    @Test
    public void testRebuildOnFailedScrubReturnsFalseWhenTruncateFails()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v text)");
        // rebuildOnFailedScrub only applies to indexes with their own backing table
        createIndex("CREATE INDEX ON %s (v) USING 'legacy_local_table'");

        execute("INSERT INTO %s (id, v) VALUES (1, 'a')");
        flush();

        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        ColumnFamilyStore indexCfs = baseCfs.indexManager.getAllIndexColumnFamilyStores().iterator().next();

        // Register a P0-priority compaction holder for the index cfs to force truncateBlocking to fail.
        AbstractTableOperation holder = new AbstractTableOperation()
        {
            public OperationProgress getProgress()
            {
                return new OperationProgress(indexCfs.metadata(),
                        OperationType.P0,
                        0,
                        100,
                        nextTimeUUID(),
                        Collections.emptySet());
            }

            public boolean isGlobal()
            {
                return false;
            }
        };

        NonThrowingCloseable holderCloseable = CompactionManager.instance.active.onOperationStart(holder);
        try
        {
            RuntimeException scrubFailure = new RuntimeException("original scrub failure");
            // rebuildOnFailedScrub should report the rebuild as unsuccessful
            assertFalse("rebuildOnFailedScrub should return false when it can't truncate the index",
                    indexCfs.rebuildOnFailedScrub(scrubFailure));
        }
        finally
        {
            holderCloseable.close();
        }
    }

    @Test
    public void testMutateSSTableRepairedStateThrowsWhenCompactionsCannotBeDisabled()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v text)");

        execute("INSERT INTO %s (id, v) VALUES (1, 'a')");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        // Register a P0-priority compaction holder for this table to force runWithCompactionsDisabled
        // to return null
        AbstractTableOperation holder = new AbstractTableOperation()
        {
            public OperationProgress getProgress()
            {
                return new OperationProgress(cfs.metadata(),
                        OperationType.P0,
                        0,
                        100,
                        nextTimeUUID(),
                        Collections.emptySet());
            }

            public boolean isGlobal()
            {
                return false;
            }
        };

        NonThrowingCloseable holderCloseable = CompactionManager.instance.active.onOperationStart(holder);
        try
        {
            // mutateSSTableRepairedState should report the null runWithCompactionsDisabled result as a
            // failure to the caller
            Assertions.assertThatThrownBy(() -> StorageService.instance.mutateSSTableRepairedState(true, false, keyspace(), Collections.singletonList(currentTable())))
                    .as("Unable to cancel in-progress compactions. Usually retrying will work")
                    .isInstanceOf(RuntimeException.class);
        }
        finally
        {
            holderCloseable.close();
        }
    }
}