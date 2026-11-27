/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.index.sai.cql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * An SAI indexer that fails on a write after the memtable applied it: the update stays applied, the write fails, and
 * the memtable accounts for the update as for any other write, so that it can be flushed and its minimum timestamp
 * covers the row.
 */
public class IndexFailureAfterApplyTest extends SAITester
{
    private static final String[] MEMTABLES = { "skiplist", "skiplist_sharded", "skiplist_sharded_locking", "trie" };

    private Injection failure;

    /// The storage compatibility mode the system tables were created in.
    private static StorageCompatibilityMode initialMode;

    @BeforeClass
    public static void allowShardedMemtables()
    {
        initialMode = DatabaseDescriptor.getStorageCompatibilityMode();
        // The sharded memtable configurations cannot be written to the schema in the CC4-compatible storage
        // compatibility modes (HCD_1, the test default, and CASSANDRA_4), see MemtableParams.asSchemaValueMap
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
    }

    @Before
    public void injectFailure() throws Throwable
    {
        failure = Injections.newCustom("sai_insert_fails")
                            .add(InvokePointBuilder.newInvokePoint()
                                                   .onClass("org.apache.cassandra.index.sai.StorageAttachedIndex$UpdateIndexer")
                                                   .onMethod("insertRow")
                                                   .atEntry())
                            .add(ActionBuilder.newActionBuilder().actions().doThrow(RuntimeException.class, "\"Injected indexer failure\""))
                            .build();
        Injections.inject(failure);
        failure.disable();
    }

    private void failingWrite(String query)
    {
        failingWrite(failure, query);
    }

    private void failingWrite(Injection failure, String query)
    {
        failure.enable();
        try
        {
            execute(query);
            fail("expected the injected failure");
        }
        catch (RuntimeException e)
        {
            // expected
        }
        finally
        {
            failure.disable();
        }
    }

    /** The failed write is the only write of the memtable, with an old timestamp. */
    @Test
    public void testOnlyWriteOfTheMemtable()
    {
        for (String memtable : MEMTABLES)
        {
            createTable("CREATE TABLE %s (pk int, ck int, v int, x text, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
            disableCompaction();
            createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

            failingWrite("INSERT INTO %s (pk, ck, v, x) VALUES (0, 1, 1, 'abc') USING TIMESTAMP 5");

            Memtable mt = cfs.getCurrentMemtable();
            assertRows(execute("SELECT ck, v, x FROM %s WHERE pk = 0"), row(1, 1, "abc"));
            assertEquals(memtable, 5, mt.getMinTimestamp());
            assertEquals(memtable, 1, mt.operationCount());
            assertTrue(memtable, mt.getLiveDataSize() > 0);

            flush();
            assertEquals(memtable, 1, cfs.getLiveSSTables().size());
            for (SSTableReader sstable : cfs.getLiveSSTables())
                assertEquals(memtable, 5, sstable.getMinTimestamp());
            assertRows(execute("SELECT ck, v, x FROM %s WHERE pk = 0"), row(1, 1, "abc"));
        }
    }

    /** The graph insert of the new vector of a write fails, after the memtable released the partition. */
    @Test
    public void testVectorGraphInsertFailure() throws Throwable
    {
        Injection graphInsertFailure = Injections.newCustom("graph_insert_fails")
                                                 .add(InvokePointBuilder.newInvokePoint()
                                                                        .onClass("io.github.jbellis.jvector.graph.GraphIndexBuilder")
                                                                        .onMethod("addGraphNode")
                                                                        .atExit())
                                                 .add(ActionBuilder.newActionBuilder().actions().doThrow(RuntimeException.class, "\"Injected graph insert failure\""))
                                                 .build();
        Injections.inject(graphInsertFailure);
        graphInsertFailure.disable();
        for (String memtable : MEMTABLES)
        {
            createTable("CREATE TABLE %s (pk int, ck int, v vector<float, 2>, x text, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
            disableCompaction();
            createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

            failingWrite(graphInsertFailure, "INSERT INTO %s (pk, ck, v, x) VALUES (0, 1, [1, 2], 'abc') USING TIMESTAMP 5");

            Memtable mt = cfs.getCurrentMemtable();
            assertRows(execute("SELECT ck, v, x FROM %s WHERE pk = 0"), row(1, vector(1f, 2f), "abc"));
            assertEquals(memtable, 5, mt.getMinTimestamp());
            assertEquals(memtable, 1, mt.operationCount());
            assertTrue(memtable, mt.getLiveDataSize() > 0);

            flush();
            for (SSTableReader sstable : cfs.getLiveSSTables())
                assertEquals(memtable, 5, sstable.getMinTimestamp());
            assertRows(execute("SELECT ck FROM %s ORDER BY v ANN OF [1, 2] LIMIT 1"), row(1));
        }
    }

    /**
     * The graph insert of the first new vector of a write ends with an OutOfMemoryError: the write fails with it at
     * once, and the flush does the insert of its other new vector.
     */
    @Test
    public void testVectorGraphInsertOutOfMemoryError() throws Throwable
    {
        Injection outOfMemory = Injections.newCustom("graph_insert_oom")
                                          .add(InvokePointBuilder.newInvokePoint()
                                                                 .onClass(CassandraOnHeapGraph.class)
                                                                 .onMethod("insertIntoGraph")
                                                                 .atExit())
                                          .add(ActionBuilder.newActionBuilder().actions().doThrow(OutOfMemoryError.class, "\"Injected graph insert OOM\""))
                                          .build();
        Injections.Counter inserts = Injections.newCounter("graph_inserts")
                                               .add(InvokePointBuilder.newInvokePoint()
                                                                      .onClass(CassandraOnHeapGraph.class)
                                                                      .onMethod("insertIntoGraph"))
                                               .build();
        Injections.inject(outOfMemory, inserts);
        outOfMemory.disable();
        for (String memtable : MEMTABLES)
        {
            createTable("CREATE TABLE %s (pk int, ck int, v vector<float, 2>, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
            disableCompaction();
            createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

            inserts.reset();
            outOfMemory.enable();
            try
            {
                // locally, as through a coordinator the error stays on the replica and the write times out
                QueryProcessor.executeInternal(formatQuery("BEGIN UNLOGGED BATCH " +
                                                           "INSERT INTO %1$s (pk, ck, v) VALUES (0, 1, [1, 2]); " +
                                                           "INSERT INTO %1$s (pk, ck, v) VALUES (0, 2, [3, 4]); " +
                                                           "APPLY BATCH"));
                fail("expected the injected failure");
            }
            catch (OutOfMemoryError e)
            {
                // expected
            }
            finally
            {
                outOfMemory.disable();
            }
            assertEquals(memtable, 1, inserts.get());

            flush();
            assertEquals(memtable, 2, inserts.get());
            assertRowsIgnoringOrder(execute("SELECT ck FROM %s ORDER BY v ANN OF [1, 2] LIMIT 2"), row(1), row(2));
        }
    }

    /** A compaction keeps the tombstone that shadows the failed write: the memtable's minimum timestamp covers it. */
    @Test
    public void testCompactionKeepsTheTombstoneOfTheFailedWrite() throws Throwable
    {
        for (String memtable : MEMTABLES)
        {
            for (String deletion : new String[]{ "DELETE FROM %s USING TIMESTAMP 100 WHERE pk = 0 AND ck = 1",
                                                 "DELETE FROM %s USING TIMESTAMP 100 WHERE pk = 0" })
            {
                createTable("CREATE TABLE %s (pk int, ck int, v int, x text, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "' " +
                            "AND gc_grace_seconds = 0");
                disableCompaction();
                createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
                execute(deletion);
                flush();

                failingWrite("INSERT INTO %s (pk, ck, v, x) VALUES (0, 1, 1, 'abc') USING TIMESTAMP 5");

                String context = memtable + ", " + deletion;
                Memtable mt = getCurrentColumnFamilyStore().getCurrentMemtable();
                assertEquals(context, 0, execute("SELECT ck, v FROM %s WHERE pk = 0").size());
                // past the tombstone's local deletion time, so that it can be purged with gc_grace_seconds 0
                Thread.sleep(2100);
                // compaction writes system.compaction_history, whose columns depend on the mode it was created in
                DatabaseDescriptor.setStorageCompatibilityMode(initialMode);
                try
                {
                    compact();
                }
                finally
                {
                    DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
                }
                assertEquals(context + ", after compaction: the deleted row is back", 0, execute("SELECT ck, v FROM %s WHERE pk = 0").size());
                assertEquals(context, 5, mt.getMinTimestamp());
            }
        }
    }

    /** A column added to the table, first written by the failed write. */
    @Test
    public void testNewColumnWrittenByTheFailedWrite()
    {
        for (String memtable : MEMTABLES)
        {
            createTable("CREATE TABLE %s (pk int, ck int, v int, x text, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
            disableCompaction();
            createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
            execute("INSERT INTO %s (pk, ck, v, x) VALUES (0, 0, 0, 'old') USING TIMESTAMP 100");
            alterTable("ALTER TABLE %s ADD y text");

            failingWrite("INSERT INTO %s (pk, ck, v, y) VALUES (0, 1, 1, 'new') USING TIMESTAMP 200");

            flush();
            assertRows(execute("SELECT ck, v, x, y FROM %s WHERE pk = 0"), row(0, 0, "old", null), row(1, 1, null, "new"));
        }
    }

    /** A rebuild flushes the memtable first, so that it indexes the row of the failed write the index missed. */
    @Test
    public void testRebuildRepairsTheFailedWriteInTheMemtable()
    {
        List<String> missing = new ArrayList<>();
        for (String memtable : MEMTABLES)
        {
            createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
            disableCompaction();
            String index = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
            execute("INSERT INTO %s (pk, ck, v) VALUES (0, 0, 1)");

            failingWrite("INSERT INTO %s (pk, ck, v) VALUES (0, 1, 1)");

            // the write is applied, and the index misses it (so the failure was injected where it matters)
            assertRows(execute("SELECT ck, v FROM %s WHERE pk = 0"), row(0, 1), row(1, 1));
            assertRows(execute("SELECT ck FROM %s WHERE v = 1"), row(0));
            assertEquals(memtable, 0, getCurrentColumnFamilyStore().getLiveSSTables().size());

            rebuildIndexes(index);
            if (execute("SELECT ck FROM %s WHERE v = 1").size() != 2)
                missing.add(memtable + " after the rebuild");

            flush();
            if (execute("SELECT ck FROM %s WHERE v = 1").size() != 2)
                missing.add(memtable + " after the flush that followed the rebuild");
            assertRowsIgnoringOrder(execute("SELECT ck FROM %s WHERE pk = 0"), row(0), row(1));
        }
        assertEquals("the index misses the row of the failed write", Collections.emptyList(), missing);
    }

    /** The partition is in the row cache when a write of a new row of it fails after it was applied. */
    @Test
    public void testRowCacheInvalidated()
    {
        for (String memtable : MEMTABLES)
        {
            createTable("CREATE TABLE %s (pk int, ck int, v int, x text, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "' " +
                        "AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}");
            disableCompaction();
            createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            assertTrue(memtable, cfs.isRowCacheEnabled());
            execute("INSERT INTO %s (pk, ck, v, x) VALUES (0, 0, 0, 'old') USING TIMESTAMP 100");
            flush();
            // the first read caches the partition, the second reads it from the cache
            assertRows(execute("SELECT ck, v, x FROM %s WHERE pk = 0"), row(0, 0, "old"));
            assertRows(execute("SELECT ck, v, x FROM %s WHERE pk = 0"), row(0, 0, "old"));
            assertTrue(memtable, cfs.containsCachedParition(Util.dk(Int32Type.instance.decompose(0))));

            failingWrite("INSERT INTO %s (pk, ck, v, x) VALUES (0, 1, 1, 'new') USING TIMESTAMP 200");

            assertRows(execute("SELECT ck, v, x FROM %s WHERE pk = 0"), row(0, 0, "old"), row(1, 1, "new"));
        }
    }
}
