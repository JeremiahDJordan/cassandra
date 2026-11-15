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
package org.apache.cassandra.db.memtable;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

/**
 * A write in flight during a schema change can be built with another version of the table's metadata than the
 * memtable it is applied to: it is decoded (or built by its statement) before the change and applied after the
 * memtable switch, or decoded after the table's metadata reference was updated and applied before the switch that
 * {@code ColumnFamilyStore.reload} does next. Each cell has to end up in its own column whatever the memtable.
 * <p>
 * Every schema change switches the memtable, so the memtable from before a change only receives writes in that
 * window. The first four tests emulate it by building the other version of the metadata by hand and applying the
 * write without changing the table. The other six hold {@code ColumnFamilyStore.reload} back and write through the
 * real window.
 */
@RunWith(Parameterized.class)
public class MemtableWriteDuringSchemaChangeTest extends CQLTester
{
    private static final AtomicInteger barriers = new AtomicInteger();

    @Parameterized.Parameter
    public String memtable;

    @Parameterized.Parameters(name = "memtable={0}")
    public static Collection<Object[]> parameters()
    {
        return Arrays.asList(new Object[][]{ { "skiplist" }, { "trie" }, { "trie_stage1" }, { "trie_stage2" }, { "trie_stage3" } });
    }

    private TableMetadata createTable()
    {
        createTable("CREATE TABLE %s (pk int, ck int, b int, d int, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + '\'');
        return currentTableMetadata();
    }

    /** Applies b = 2 and d = 4 to (pk 0, ck 1), with the columns of the given version of the table's metadata. */
    private static void write(TableMetadata metadata, boolean b, boolean d)
    {
        long timestamp = FBUtilities.timestampMicros() + 1000;
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.make(Int32Type.instance.decompose(1)));
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, FBUtilities.nowInSeconds()));
        if (b)
            builder.addCell(BufferCell.live(column(metadata, "b"), timestamp, Int32Type.instance.decompose(2)));
        if (d)
            builder.addCell(BufferCell.live(column(metadata, "d"), timestamp, Int32Type.instance.decompose(4)));
        new Mutation(PartitionUpdate.singleRowUpdate(metadata, Util.makeKey(metadata, 0), builder.build())).apply();
    }

    private static ColumnMetadata column(TableMetadata metadata, String name)
    {
        return metadata.getColumn(ColumnIdentifier.getInterned(name, false));
    }

    @Test
    public void testWriteBuiltBeforeAnAddAndAppliedAfter() throws Throwable
    {
        TableMetadata before = createTable();
        execute("ALTER TABLE %s ADD a int");
        write(before, true, true);

        assertRows(execute("SELECT pk, ck, a, b, d FROM %s"), row(0, 1, null, 2, 4));
        flush();
        assertRows(execute("SELECT pk, ck, a, b, d FROM %s"), row(0, 1, null, 2, 4));
    }

    @Test
    public void testWriteBuiltBeforeADropAndAppliedAfter() throws Throwable
    {
        TableMetadata before = createTable();
        execute("ALTER TABLE %s DROP b");
        write(before, true, true);

        assertRows(execute("SELECT pk, ck, d FROM %s"), row(0, 1, 4));
        flush();
        assertRows(execute("SELECT pk, ck, d FROM %s"), row(0, 1, 4));
    }

    @Test
    public void testWriteBuiltAfterAnAddAndAppliedBeforeTheMemtableSwitch() throws Throwable
    {
        TableMetadata before = createTable();
        TableMetadata after = before.unbuild().addRegularColumn("a", Int32Type.instance).build();
        write(after, true, true);

        assertRows(execute("SELECT pk, ck, b, d FROM %s"), row(0, 1, 2, 4));
        flush();
        assertRows(execute("SELECT pk, ck, b, d FROM %s"), row(0, 1, 2, 4));
    }

    @Test
    public void testWriteBuiltAfterADropAndAppliedBeforeTheMemtableSwitch() throws Throwable
    {
        TableMetadata before = createTable();
        TableMetadata after = before.unbuild()
                                    .removeRegularOrStaticColumn(column(before, "b").name)
                                    .recordColumnDrop(column(before, "b"), FBUtilities.timestampMicros())
                                    .build();
        write(after, false, true);

        assertRows(execute("SELECT pk, ck, b, d FROM %s"), row(0, 1, null, 4));
        flush();
        assertRows(execute("SELECT pk, ck, b, d FROM %s"), row(0, 1, null, 4));
    }

    /**
     * A write to a column that is being added: built once the table's metadata reference has the column, applied
     * before {@code ColumnFamilyStore.reload} has switched the memtable, which is held back here.
     */
    @Test
    public void testWriteToTheAddedColumnBeforeTheMemtableSwitch() throws Throwable
    {
        testWriteToTheAddedColumnBeforeTheMemtableSwitch("int", "1", 1, false);
        testWriteToTheAddedColumnBeforeTheMemtableSwitch("int static", "1", 1, true);
        testWriteToTheAddedColumnBeforeTheMemtableSwitch("set<int>", "{1}", set(1), false);
    }

    private void testWriteToTheAddedColumnBeforeTheMemtableSwitch(String type, String literal, Object value, boolean isStatic) throws Throwable
    {
        createTable();
        execute("INSERT INTO %s (pk, ck, b, d) VALUES (0, 0, 20, 40)");

        beforeTheMemtableSwitchOf("ALTER TABLE %s ADD a " + type,
                                  () -> execute("INSERT INTO %s (pk, ck, a, b, d) VALUES (0, 1, " + literal + ", 2, 4)"));

        assertRows(execute("SELECT pk, ck, a, b, d FROM %s"), row(0, 0, isStatic ? value : null, 20, 40), row(0, 1, value, 2, 4));
        flush();
        assertRows(execute("SELECT pk, ck, a, b, d FROM %s"), row(0, 0, isStatic ? value : null, 20, 40), row(0, 1, value, 2, 4));
    }

    /**
     * A write to a column that is being added again after a drop. The memtable was created after the drop, so the
     * column is a dropped column for it and not one it has not heard of.
     */
    @Test
    public void testWriteToTheReAddedColumnBeforeTheMemtableSwitch() throws Throwable
    {
        testWriteToTheReAddedColumnBeforeTheMemtableSwitch("int", "1", 1, false);
        testWriteToTheReAddedColumnBeforeTheMemtableSwitch("int static", "1", 1, true);
        testWriteToTheReAddedColumnBeforeTheMemtableSwitch("set<int>", "{1}", set(1), false);
    }

    private void testWriteToTheReAddedColumnBeforeTheMemtableSwitch(String type, String literal, Object value, boolean isStatic) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a " + type + ", b int, d int, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + '\'');
        execute("INSERT INTO %s (pk, ck, a, b, d) VALUES (0, 0, " + literal + ", 20, 40)");
        execute("ALTER TABLE %s DROP a");
        execute("INSERT INTO %s (pk, ck, b, d) VALUES (0, 2, 22, 42)");

        beforeTheMemtableSwitchOf("ALTER TABLE %s ADD a " + type,
                                  () -> execute("INSERT INTO %s (pk, ck, a, b, d) VALUES (0, 1, " + literal + ", 2, 4)"));

        // the value from before the drop stays dropped
        Object[][] expected = { row(0, 0, isStatic ? value : null, 20, 40), row(0, 1, value, 2, 4), row(0, 2, isStatic ? value : null, 22, 42) };
        assertRows(execute("SELECT pk, ck, a, b, d FROM %s"), expected);
        flush();
        assertRows(execute("SELECT pk, ck, a, b, d FROM %s"), expected);
    }

    /**
     * A write to a column that this node gets added and dropped in one schema change, because it was behind on the
     * schema, from a coordinator that had the ADD. The memtable from before that schema change does not know the
     * column, not even as a dropped one.
     */
    @Test
    public void testWriteToAColumnAddedAndDroppedInOneSchemaChangeBeforeTheMemtableSwitch() throws Throwable
    {
        TableMetadata before = createTable();
        execute("INSERT INTO %s (pk, ck, b, d) VALUES (0, 0, 20, 40)");
        TableMetadata added = before.unbuild().addRegularColumn("a", Int32Type.instance).build();
        TableMetadata dropped = before.unbuild().recordColumnDrop(column(added, "a"), FBUtilities.timestampMicros()).build();

        beforeTheMemtableSwitchOf(() -> SchemaTestUtil.announceTableUpdate(dropped), () -> {
            long timestamp = FBUtilities.timestampMicros() + 1000;
            Row.Builder builder = BTreeRow.unsortedBuilder();
            builder.newRow(Clustering.make(Int32Type.instance.decompose(1)));
            builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, FBUtilities.nowInSeconds()));
            builder.addCell(BufferCell.live(column(added, "a"), timestamp, Int32Type.instance.decompose(1)));
            builder.addCell(BufferCell.live(column(added, "b"), timestamp, Int32Type.instance.decompose(2)));
            builder.addCell(BufferCell.live(column(added, "d"), timestamp, Int32Type.instance.decompose(4)));
            new Mutation(PartitionUpdate.singleRowUpdate(added, Util.makeKey(added, 0), builder.build())).apply();
        });

        assertRows(execute("SELECT pk, ck, b, d FROM %s"), row(0, 0, 20, 40), row(0, 1, 2, 4));
        flush();
        assertRows(execute("SELECT pk, ck, b, d FROM %s"), row(0, 0, 20, 40), row(0, 1, 2, 4));
    }

    /** One mutation with updates of two tables, of which one is having a column added. Both updates are applied. */
    @Test
    public void testBatchOfTwoTablesBeforeTheMemtableSwitch() throws Throwable
    {
        String other = createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + '\'');
        createTable();

        beforeTheMemtableSwitchOf("ALTER TABLE %s ADD a int",
                                  () -> execute("BEGIN UNLOGGED BATCH " +
                                                "INSERT INTO " + KEYSPACE + '.' + other + " (pk, ck, v) VALUES (0, 1, 7); " +
                                                "INSERT INTO %s (pk, ck, a, b, d) VALUES (0, 1, 1, 2, 4); " +
                                                "APPLY BATCH"));

        assertRows(execute("SELECT pk, ck, a, b, d FROM %s"), row(0, 1, 1, 2, 4));
        assertRows(execute("SELECT pk, ck, v FROM " + KEYSPACE + '.' + other), row(0, 1, 7));
        flush();
        assertRows(execute("SELECT pk, ck, a, b, d FROM %s"), row(0, 1, 1, 2, 4));
    }

    /** A counter update goes through {@code CounterMutation}, which applies the mutation it builds while it holds its locks. */
    @Test
    public void testCounterUpdateBeforeTheMemtableSwitch() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, c1 counter, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + '\'');
        execute("UPDATE %s SET c1 = c1 + 1 WHERE pk = 0 AND ck = 0");

        beforeTheMemtableSwitchOf("ALTER TABLE %s ADD c2 counter", () -> {
            execute("UPDATE %s SET c1 = c1 + 1, c2 = c2 + 5 WHERE pk = 0 AND ck = 0");
            execute("UPDATE %s SET c1 = c1 + 1, c2 = c2 + 5 WHERE pk = 0 AND ck = 0");
        });

        assertRows(execute("SELECT pk, ck, c1, c2 FROM %s"), row(0, 0, 3L, 10L));
        flush();
        assertRows(execute("SELECT pk, ck, c1, c2 FROM %s"), row(0, 0, 3L, 10L));
    }

    /** Many writes to the added column in one window cause one memtable switch, the one that {@code reload} then does not need to do. */
    @Test
    public void testWritesBeforeTheMemtableSwitchCauseOneSwitch() throws Throwable
    {
        createTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Memtable before = cfs.getTracker().getView().getCurrentMemtable();
        Memtable[] afterTheFirstWrite = new Memtable[1];

        beforeTheMemtableSwitchOf("ALTER TABLE %s ADD a int", () -> {
            execute("INSERT INTO %s (pk, ck, a, b, d) VALUES (0, 0, 1, 2, 4)");
            afterTheFirstWrite[0] = cfs.getTracker().getView().getCurrentMemtable();
            for (int ck = 1; ck < 100; ck++)
                execute("INSERT INTO %s (pk, ck, a, b, d) VALUES (0, ?, 1, 2, 4)", ck);
            assertSame(afterTheFirstWrite[0], cfs.getTracker().getView().getCurrentMemtable());
        });

        // only the memtable that keys its cells by the columns it was created with has to be switched by the write
        if (memtable.equals("trie"))
            assertNotSame(before, afterTheFirstWrite[0]);
        // reload switches the memtable unless a write has done it already
        assertNotSame(before, cfs.getTracker().getView().getCurrentMemtable());
        if (memtable.equals("trie"))
            assertSame(afterTheFirstWrite[0], cfs.getTracker().getView().getCurrentMemtable());
        assertEquals(100, execute("SELECT * FROM %s WHERE a = 1 ALLOW FILTERING").size());
    }

    private interface Write
    {
        void run() throws Throwable;
    }

    /**
     * Runs the given schema change of the current table and, once it has updated the table's metadata reference and is
     * about to reload the table, which is held back, the given write.
     */
    private void beforeTheMemtableSwitchOf(String alter, Write write) throws Throwable
    {
        beforeTheMemtableSwitchOf(() -> execute(alter), write);
    }

    private void beforeTheMemtableSwitchOf(Write schemaChange, Write write) throws Throwable
    {
        Injections.Barrier beforeReload = Injections.newBarrier("before_reload_" + barriers.incrementAndGet(), 2, false)
                                                    .add(InvokePointBuilder.newInvokePoint().onClass(ColumnFamilyStore.class).onMethod("reload"))
                                                    .build();
        Injections.inject(beforeReload);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            try
            {
                schemaChange.run();
            }
            catch (Throwable t)
            {
                failure.set(t);
            }
        });
        try
        {
            thread.start();
            // wait for the ALTER to have updated the metadata reference and to be about to reload the table
            for (int i = 0; i < 3000 && beforeReload.getCount() != 1; i++)
                Thread.sleep(10);
            assertEquals(1, beforeReload.getCount());

            write.run();
        }
        finally
        {
            beforeReload.countDown();
            thread.join();
            beforeReload.disable();
        }
        if (failure.get() != null)
            throw failure.get();
    }
}
