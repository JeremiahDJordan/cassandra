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

package org.apache.cassandra.db.memtable;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.btree.BTreeSet;

import static org.apache.cassandra.inject.Expression.expr;
import static org.apache.cassandra.inject.Expression.quote;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Reads of memtable data after a schema change of the table, from a memtable created before it or after it, report
 * the table's current columns: an LWT's not-applied result lists the columns of the partition's metadata.
 */
@RunWith(Parameterized.class)
public class MemtableReadDuringSchemaChangeTest extends CQLTester
{
    private static final AtomicInteger barriers = new AtomicInteger();

    @Parameterized.Parameter
    public String memtable;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object> parameters()
    {
        return Arrays.asList("skiplist", "trie", "trie_stage1", "trie_stage2", "trie_stage3");
    }

    private void createTable()
    {
        createTable("CREATE TABLE %s (pk int, ck int, s int static, b int, d int, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + '\'');
    }

    @Test
    public void testReadAfterAnAdd() throws Throwable
    {
        createTable();
        execute("INSERT INTO %s (pk, ck, s, b, d) VALUES (0, 1, 3, 2, 4)");

        afterSchemaChange("ALTER TABLE %s ADD a int", () -> {
            assertRows(execute("INSERT INTO %s (pk, ck, d) VALUES (0, 1, 5) IF NOT EXISTS"), row(false, 0, 1, null, null, 2, 4));
            assertRowsNet(executeNet("INSERT INTO %s (pk, ck, d) VALUES (0, 1, 5) IF NOT EXISTS"), row(false, 0, 1, null, null, 2, 4));
            assertRows(execute("SELECT * FROM %s"), row(0, 1, 3, null, 2, 4));
            assertLocalReadsReportTheCurrentTable();
        });

        flush();
        assertRows(execute("SELECT * FROM %s"), row(0, 1, 3, null, 2, 4));
    }

    @Test
    public void testReadAfterADrop() throws Throwable
    {
        createTable();
        execute("INSERT INTO %s (pk, ck, s, b, d) VALUES (0, 1, 3, 2, 4)");

        afterSchemaChange("ALTER TABLE %s DROP b", () -> {
            assertRows(execute("INSERT INTO %s (pk, ck, d) VALUES (0, 1, 5) IF NOT EXISTS"), row(false, 0, 1, null, 4));
            assertRowsNet(executeNet("INSERT INTO %s (pk, ck, d) VALUES (0, 1, 5) IF NOT EXISTS"), row(false, 0, 1, null, 4));
            assertRows(execute("SELECT * FROM %s"), row(0, 1, 3, 4));
            assertLocalReadsReportTheCurrentTable();
        });

        flush();
        assertRows(execute("SELECT * FROM %s"), row(0, 1, 3, 4));

        // a memtable created after the DROP
        execute("INSERT INTO %s (pk, ck, d) VALUES (0, 2, 6)");
        assertRows(execute("INSERT INTO %s (pk, ck, d) VALUES (0, 2, 7) IF NOT EXISTS"), row(false, 0, 2, null, 6));
        assertLocalReadsReportTheCurrentTable();
    }

    /** The clustering bounds of the reads are values of the type with the new field. */
    @Test
    public void testReadOfAClusteringAfterAFieldIsAddedToItsType() throws Throwable
    {
        String udt = createType("CREATE TYPE %s (a int, b int)");
        createTable("CREATE TABLE %s (pk int, ck frozen<" + udt + ">, x int, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + '\'');
        execute("INSERT INTO %s (pk, ck, x) VALUES (0, {a: 1, b: 2}, 0)");

        afterSchemaChange("ALTER TYPE " + KEYSPACE + '.' + udt + " ADD c int", () -> {
            execute("INSERT INTO %s (pk, ck, x) VALUES (0, {a: 1, b: 2, c: 3}, 1)");
            assertRows(execute("SELECT x FROM %s WHERE pk = 0 AND ck = {a: 1, b: 2}"), row(0));
            assertRows(execute("SELECT x FROM %s WHERE pk = 0 AND ck = {a: 1, b: 2, c: 3}"), row(1));
            assertRows(execute("SELECT x FROM %s WHERE pk = 0 AND ck > {a: 1, b: 2}"), row(1));
            assertRows(execute("UPDATE %s SET x = 2 WHERE pk = 0 AND ck = {a: 1, b: 2} IF x = 0"), row(true));
        });

        assertRows(execute("SELECT x FROM %s"), row(2), row(1));
    }

    /** SAI looks up the cells of the rows it orders by their keys, which can be values of the new type. */
    @Test
    public void testCellLookupAfterAFieldIsAddedToTheTypeOfTheClustering() throws Throwable
    {
        String udt = createType("CREATE TYPE %s (a int, b int)");
        createTable("CREATE TABLE %s (pk int, ck frozen<" + udt + ">, x int, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + '\'');
        execute("INSERT INTO %s (pk, ck, x) VALUES (0, {a: 1, b: 2}, 0)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Memtable before = cfs.getTracker().getView().getCurrentMemtable();

        afterSchemaChange("ALTER TYPE " + KEYSPACE + '.' + udt + " ADD c int", () -> {
            TableMetadata metadata = cfs.metadata();
            DecoratedKey key = Util.dk(Int32Type.instance.decompose(0));
            ColumnMetadata x = metadata.getColumn(ColumnIdentifier.getInterned("x", false));
            ByteBuffer one = Int32Type.instance.decompose(1);
            ByteBuffer two = Int32Type.instance.decompose(2);
            Cell<?> old = before.getCellForKey(key, Clustering.make(TupleType.buildValue(one, two, null)), x);
            assertEquals(Int32Type.instance.decompose(0), old.buffer());
            assertNull(before.getCellForKey(key, Clustering.make(TupleType.buildValue(one, two, Int32Type.instance.decompose(3))), x));
        });
    }

    /** A range read and a read of the static row only, which reads no slice of the partition. */
    private void assertLocalReadsReportTheCurrentTable()
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        TableMetadata metadata = cfs.metadata();
        ReadCommand range = Util.cmd(cfs).build();
        ReadCommand staticRow = SinglePartitionReadCommand.create(metadata,
                                                                  FBUtilities.nowInSeconds(),
                                                                  Util.makeKey(metadata, 0),
                                                                  ColumnFilter.all(metadata),
                                                                  new ClusteringIndexNamesFilter(BTreeSet.empty(metadata.comparator), false));
        for (ReadCommand command : Arrays.asList(range, staticRow))
        {
            try (ReadExecutionController controller = command.executionController();
                 UnfilteredPartitionIterator partitions = command.executeLocally(controller);
                 UnfilteredRowIterator partition = partitions.next())
            {
                assertEquals(metadata, partitions.metadata());
                assertEquals(metadata, partition.metadata());
            }
        }
    }

    private interface Reads
    {
        void run() throws Throwable;
    }

    /**
     * Runs the given schema change of the current table and then the given reads. The schema change switches the
     * memtable, and the flush of the old one is held back until the reads are done, so that they merge it.
     */
    private void afterSchemaChange(String schemaChange, Reads reads) throws Throwable
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Memtable before = cfs.getTracker().getView().getCurrentMemtable();
        Injections.Barrier flush = Injections.newBarrier("flush_" + barriers.incrementAndGet(), 2, false)
                                             .add(InvokePointBuilder.newInvokePoint().onClass(ColumnFamilyStore.class, "Flush").onMethod("flushMemtable"))
                                             .withLastActionBuilder(action -> action.conditions().when(expr("$1.name").method("equals").args(quote(currentTable()))))
                                             .build();
        Injections.inject(flush);
        try
        {
            execute(schemaChange);
            await().atMost(30, TimeUnit.SECONDS).until(() -> flush.getCount() == 1);
            assertTrue(cfs.getTracker().getView().flushingMemtables.contains(before));
            reads.run();
            assertTrue(cfs.getTracker().getView().flushingMemtables.contains(before));
        }
        finally
        {
            flush.countDown();
            flush.disable();
        }
    }
}
