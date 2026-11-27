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

package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Predicate;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IScrubber;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.ALLOW_CURSOR_COMPACTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Compaction and reads of a collection column dropped and re-added with a type that this version considers unrelated
 * to the original one, {@code map<int, frozen<map<int, text>>>} then {@code map<int, frozen<map<blob, text>>>}, which
 * earlier releases allowed. This version's {@code ALTER TABLE} rejects that re-add, so the tests apply it directly to
 * the schema. The values of the re-added column are not valid for the original type, so a compaction that stores them
 * under that type loses them to {@code scrub}.
 * <p>
 * Each test runs with cursor and iterator compaction, and with the sstables kept open from before the schema changes
 * or reopened after them, as after a restart.
 */
@RunWith(Parameterized.class)
public class CompactionDroppedColumnVersionsTest extends CQLTester
{
    private static final ColumnIdentifier COLUMN_C = ColumnIdentifier.getInterned("c", false);
    private static final AbstractType<?> OLD_TYPE = mapOfFrozenMaps(Int32Type.instance);
    private static final AbstractType<?> NEW_TYPE = mapOfFrozenMaps(BytesType.instance);
    // Cells of the re-added column whose values are not valid for OLD_TYPE (see newTypeValues for what they read as).
    private static final String NEW_TYPE_CELLS = "c[1] = {0x01: 'x'}, c[2] = {0x0203: 'y'}, c[3] = {0x04: 'z'}";

    @Parameterized.Parameter(0)
    public boolean useCursors;

    @Parameterized.Parameter(1)
    public boolean reopenSSTables;

    private WithProperties properties;

    @Parameterized.Parameters(name = "cursors={0}, reopen={1}")
    public static Collection<Object[]> parameters()
    {
        return Arrays.asList(new Object[][]{ { true, false }, { true, true }, { false, false }, { false, true } });
    }

    @Before
    public void setCompactionPath()
    {
        properties = new WithProperties().set(ALLOW_CURSOR_COMPACTION, useCursors);
    }

    @After
    public void resetCompactionPath()
    {
        properties.close();
    }

    /**
     * {@code ALTER TYPE ... ADD} on the type of a live column, with the sstables kept open from before the change. The
     * sstables written before it are compacted together first, so they end up with the highest id, and their header
     * still has the earlier type. The final compaction must store the column under the current type, as the newest
     * values have the added field.
     */
    @Test
    public void testAlterTypeAddOnLiveColumn() throws Throwable
    {
        for (boolean frozen : new boolean[]{ false, true })
        {
            String type = createTableWithUserTypeColumn(frozen);
            execute("ALTER TYPE " + KEYSPACE + '.' + type + " ADD x text");
            execute("INSERT INTO %s (pk, ck, v, c) VALUES (2, 1, 2, {a: 5, b: 6, x: 'new'})");
            execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 1, 2, {a: 7, b: 8, x: 'newer'})");
            flush();
            checkUserTypeColumnAfterCompaction();
        }
    }

    /**
     * As above, with a second {@code ALTER TYPE ... ADD} before any sstable has the type of the first one: no sstable
     * has the current type, so it must come from the schema.
     */
    @Test
    public void testAlterTypeAddTwiceOnLiveColumn() throws Throwable
    {
        for (boolean frozen : new boolean[]{ false, true })
        {
            String type = createTableWithUserTypeColumn(frozen);
            execute("ALTER TYPE " + KEYSPACE + '.' + type + " ADD x text");
            execute("INSERT INTO %s (pk, ck, v, c) VALUES (2, 1, 2, {a: 5, b: 6, x: 'new'})");
            execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 1, 2, {a: 7, b: 8, x: 'newer'})");
            flush();
            execute("ALTER TYPE " + KEYSPACE + '.' + type + " ADD y text");
            checkUserTypeColumnAfterCompaction();
        }
    }

    /**
     * A {@code text} column is dropped, re-added as {@code blob}, written to, and dropped again, with the second drop
     * older than that write. That cell is not dropped, and is not valid {@code text}. As in
     * {@link #testSurvivingDataKeepsTypeOfLastDrop}, but with a history that this version allows.
     */
    @Test
    public void testTextReAddedAsBlobAndDroppedAgain() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, c text, PRIMARY KEY (pk, ck))");
        disableCompaction();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 1, 1, 'a')");
        flush();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 2, 1, 'b')");
        flush();
        execute("ALTER TABLE %s DROP c");
        execute("ALTER TABLE %s ADD c blob");
        long writeTimestamp = FBUtilities.timestampMicros();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 1, 2, 0xff00fe) USING TIMESTAMP " + writeTimestamp);
        flush();
        execute("ALTER TABLE %s DROP c USING TIMESTAMP " + (writeTimestamp - 1));
        maybeReopenSSTables();

        rewriteEachAlone(sstable -> typeOfC(sstable).equals(UTF8Type.instance));
        assertEquals(UTF8Type.instance, typeOfC(newestSSTable()));
        compactAll();
        assertTypeOfCInAllSSTables(BytesType.instance);
        assertRowsSurviveScrub();

        execute("ALTER TABLE %s ADD c blob");
        assertRows(execute("SELECT pk, ck, v, c FROM %s"), row(1, 1, 2, bytes(0xff, 0x00, 0xfe)), row(1, 2, 1, null));
    }

    /**
     * A regular column is dropped, and a clustering column is then renamed to its name, which earlier versions of
     * {@code ALTER TABLE} allowed. The sstables that have the dropped column must still open and compact, with the
     * dropped column among the regular columns of the output header, and the clustering column kept out of them.
     */
    @Test
    public void testClusteringColumnRenamedToDroppedColumnName() throws Throwable
    {
        for (AbstractType<?> type : Arrays.asList(Int32Type.instance, UTF8Type.instance))
        {
            createTable("CREATE TABLE %s (pk int, ck int, v int, w int, c " + type.asCQL3Type() +
                        ", PRIMARY KEY (pk, ck))");
            disableCompaction();
            String value = type == Int32Type.instance ? "7" : "'a'";
            execute("INSERT INTO %s (pk, ck, v, w, c) VALUES (1, 1, 1, 10, " + value + ')');
            execute("INSERT INTO %s (pk, ck, v) VALUES (2, 1, 1)");
            flush();
            execute("INSERT INTO %s (pk, ck, v, w, c) VALUES (1, 2, 1, 20, " + value + ')');
            execute("INSERT INTO %s (pk, ck, w) VALUES (3, 1, 30)");
            flush();
            execute("ALTER TABLE %s DROP c");
            SchemaTestUtil.announceTableUpdate(currentTableMetadata().unbuild()
                                                                     .renamePrimaryKeyColumn(ColumnIdentifier.getInterned("ck", false),
                                                                                             ColumnIdentifier.getInterned("c", false))
                                                                     .build());
            maybeReopenSSTables();

            Object[][] expected = { row(1, 1, 1, 10), row(1, 2, 1, 20), row(2, 1, 1, null), row(3, 1, null, 30) };
            assertRowsIgnoringOrder(execute("SELECT pk, c, v, w FROM %s"), expected);
            compactAll();
            assertTypeOfCInAllSSTables(type);
            assertRowsIgnoringOrder(execute("SELECT pk, c, v, w FROM %s"), expected);
            maybeReopenSSTables();
            assertRowsIgnoringOrder(execute("SELECT pk, c, v, w FROM %s"), expected);
        }
    }

    /**
     * The column is dropped, re-added with an unrelated type, written to, and dropped again, with the second drop older
     * than that write: as for a write that reached a replica before the drop did, or that has a client-supplied
     * timestamp. Those cells are not dropped.
     * <p>
     * The sstables of the original column are rewritten on their own first, as by {@code upgradesstables}, so they end
     * up with the highest ids and keep the original type. The final compaction must still store the surviving cells
     * under the type the column was last dropped with.
     */
    @Test
    public void testSurvivingDataKeepsTypeOfLastDrop() throws Throwable
    {
        createTableWithUnrelatedColumnVersions();
        long writeTimestamp = FBUtilities.timestampMicros();
        execute("UPDATE %s USING TIMESTAMP " + writeTimestamp +
                " SET v = 2, " + NEW_TYPE_CELLS + " WHERE pk = 1 AND ck = 1");
        flush();
        execute("ALTER TABLE %s DROP c USING TIMESTAMP " + (writeTimestamp - 1));
        maybeReopenSSTables();

        rewriteEachAlone(sstable -> typeOfC(sstable).equals(OLD_TYPE));
        assertEquals(OLD_TYPE, typeOfC(newestSSTable()));
        compactAll();
        assertTypeOfCInAllSSTables(NEW_TYPE);
        assertRowsSurviveScrub();

        execute("ALTER TABLE %s ADD c " + NEW_TYPE.asCQL3Type());
        assertRows(execute("SELECT pk, ck, v, c FROM %s"),
                   row(1, 1, 2, newTypeValues()),
                   row(1, 2, 1, null));
    }

    /**
     * As above, with the surviving cells in two sstables. The newest sstable of the original column, which has the
     * highest id once rewritten, is then compacted with one of them only. Both sstables with surviving cells must then
     * use the type of the last drop: if one used the original type, both versions would have data, and reads and
     * compactions would fail.
     */
    @Test
    public void testSurvivingDataInSeveralSSTablesKeepsTypeOfLastDrop() throws Throwable
    {
        createTableWithUnrelatedColumnVersions();
        long writeTimestamp = FBUtilities.timestampMicros();
        execute("UPDATE %s USING TIMESTAMP " + writeTimestamp +
                " SET v = 2, c[1] = {0x01: 'x'}, c[2] = {0x0203: 'y'} WHERE pk = 1 AND ck = 1");
        flush();
        String laterWrite = "UPDATE %s USING TIMESTAMP " + (writeTimestamp + 1);
        execute(laterWrite + " SET c[3] = {0x04: 'z'} WHERE pk = 1 AND ck = 1");
        execute(laterWrite + " SET c[1] = {0x05: 'w'} WHERE pk = 1 AND ck = 2");
        flush();
        execute("ALTER TABLE %s DROP c USING TIMESTAMP " + (writeTimestamp - 1));
        maybeReopenSSTables();

        rewriteEachAlone(sstable -> typeOfC(sstable).equals(OLD_TYPE));
        List<SSTableReader> sstables = new ArrayList<>(getCurrentColumnFamilyStore().getLiveSSTables());
        sstables.sort(CompactionSSTable.idComparator);
        assertEquals(4, sstables.size());
        SSTableReader firstOfNewType = sstables.get(0);
        SSTableReader secondOfNewType = sstables.get(1);
        SSTableReader ofOldType = sstables.get(3);
        assertEquals(NEW_TYPE, typeOfC(firstOfNewType));
        assertEquals(NEW_TYPE, typeOfC(secondOfNewType));
        assertEquals(OLD_TYPE, typeOfC(sstables.get(2)));
        assertEquals(OLD_TYPE, typeOfC(ofOldType));
        compactOnly(sstable -> sstable == firstOfNewType || sstable == ofOldType);
        assertEquals(NEW_TYPE, typeOfC(newestSSTable()));
        assertEquals(NEW_TYPE, typeOfC(secondOfNewType));
        assertRows(execute("SELECT pk, ck, v FROM %s"), row(1, 1, 2), row(1, 2, 1));

        compactAll();
        assertTypeOfCInAllSSTables(NEW_TYPE);
        assertRowsSurviveScrub();

        execute("ALTER TABLE %s ADD c " + NEW_TYPE.asCQL3Type());
        assertRows(execute("SELECT pk, ck, v, c FROM %s"),
                   row(1, 1, 2, newTypeValues()),
                   row(1, 2, 1, map(1, map(bytes(0x05), "w"))));
    }

    /**
     * A frozen user type gets a field, a cell uses it, the type gets another field, and the column is dropped, with
     * the drop older than that cell. No sstable has the type of the drop, which has both fields, and the sstables of
     * the original type are rewritten on their own, so they have the highest ids. The final compaction must store the
     * column under the type of the drop: under the original type, {@code scrub} finds the cells that use the first
     * field invalid, and removes all the rows of their partitions.
     */
    @Test
    public void testDroppedColumnWithTwoAlterTypeAdds() throws Throwable
    {
        String type = createTableWithUserTypeColumn(true);
        execute("ALTER TYPE " + KEYSPACE + '.' + type + " ADD x text");
        long writeTimestamp = FBUtilities.timestampMicros();
        String using = " USING TIMESTAMP " + writeTimestamp;
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (2, 1, 2, {a: 5, b: 6, x: 'new'})" + using);
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 1, 2, {a: 7, b: 8, x: 'newer'})" + using);
        flush();
        execute("ALTER TYPE " + KEYSPACE + '.' + type + " ADD y text");
        execute("ALTER TABLE %s DROP c USING TIMESTAMP " + (writeTimestamp - 1));
        AbstractType<?> typeOfDrop = currentTableMetadata().getDroppedColumn(COLUMN_C.bytes).type;
        assertEquals(4, typeOfDrop.subTypes().size());
        maybeReopenSSTables();

        rewriteEachAlone(sstable -> typeOfC(sstable).subTypes().size() == 2);
        assertEquals(2, typeOfC(newestSSTable()).subTypes().size());
        compactAll();
        assertRowsSurviveScrub(row(1, 1, 2), row(1, 2, 1), row(2, 1, 2));
        assertTypeOfCInAllSSTables(typeOfDrop);
    }

    /**
     * As above, with an {@code ascii} column dropped and re-added as {@code text}, a cell that is not valid
     * {@code ascii}, and the column dropped, re-added as {@code blob} and dropped again, both drops older than that
     * cell. The final compaction must store the column as {@code blob}, not {@code ascii}.
     */
    @Test
    public void testDroppedColumnReAddedTwice() throws Throwable
    {
        checkDroppedColumnReAddedTwice("", "%s", new String[]{ "'a'", "'b'", "'\u00e9'", "'\u00e9\u00e8'" },
                                       bytes(0xc3, 0xa9, 0xc3, 0xa8), null, bytes(0xc3, 0xa9));
    }

    /**
     * As above, on a static column, with another static column that stays.
     */
    @Test
    public void testStaticDroppedColumnReAddedTwice() throws Throwable
    {
        checkDroppedColumnReAddedTwice("s2 int static, ", "%s static",
                                       new String[]{ "'a'", "'b'", "'\u00e9'", "'\u00e9\u00e8'" },
                                       bytes(0xc3, 0xa9, 0xc3, 0xa8), bytes(0xc3, 0xa9, 0xc3, 0xa8), bytes(0xc3, 0xa9));
    }

    /**
     * As above, on a non-frozen set, {@code set<ascii>} then {@code set<text>} then {@code set<blob>}.
     */
    @Test
    public void testCollectionDroppedColumnReAddedTwice() throws Throwable
    {
        checkDroppedColumnReAddedTwice("", "set<%s>",
                                       new String[]{ "{'a'}", "{'b'}", "{'\u00e9'}", "{'\u00e9\u00e8'}" },
                                       set(bytes(0xc3, 0xa9, 0xc3, 0xa8)), null, set(bytes(0xc3, 0xa9)));
    }

    /**
     * As {@link #testDroppedColumnWithTwoAlterTypeAdds}, on a non-frozen user type. Its drop record is a multi-cell
     * tuple, which the final compaction must use. Only the header and the rows are checked, not {@code scrub}: scrub
     * validates a cell of a multi-cell tuple column only once tuple columns are validated field by field.
     */
    @Test
    public void testNonFrozenUserTypeDroppedColumnWithTwoAlterTypeAdds() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int)");
        createTable("CREATE TABLE %s (pk int, ck int, v int, c " + type + ", PRIMARY KEY (pk, ck))");
        disableCompaction();
        long writeTimestamp = FBUtilities.timestampMicros();
        execute("UPDATE %s USING TIMESTAMP " + writeTimestamp + " SET v = 1, c.a = 1 WHERE pk = 1 AND ck = 1");
        flush();
        execute("ALTER TYPE " + KEYSPACE + '.' + type + " ADD b text");
        execute("UPDATE %s USING TIMESTAMP " + writeTimestamp + " SET v = 2, c.b = 'x' WHERE pk = 2 AND ck = 1");
        flush();
        execute("ALTER TYPE " + KEYSPACE + '.' + type + " ADD d int");
        execute("ALTER TABLE %s DROP c USING TIMESTAMP " + (writeTimestamp - 1));
        AbstractType<?> typeOfDrop = currentTableMetadata().getDroppedColumn(COLUMN_C.bytes).type;
        assertTrue(typeOfDrop.isMultiCell());
        assertEquals(3, typeOfDrop.subTypes().size());
        maybeReopenSSTables();

        rewriteEachAlone(sstable -> typeOfC(sstable).subTypes().size() == 1);
        assertEquals(1, typeOfC(newestSSTable()).subTypes().size());
        compactAll();
        assertTypeOfCInAllSSTables(typeOfDrop);
        assertRows(execute("SELECT pk, ck, v FROM %s"), row(1, 1, 1), row(2, 1, 2));
    }

    /**
     * A drop record whose complexity differs from the sstables' column, as an earlier release could leave: a frozen
     * tuple with one more field, for a non-frozen user type column. The output header must keep the sstables' column.
     * Only with the sstables kept open: reopened sstables take {@code c} as frozen from this drop record.
     */
    @Test
    public void testDropRecordOfOtherComplexityIsNotUsed() throws Throwable
    {
        Assume.assumeFalse(reopenSSTables);
        String type = createType("CREATE TYPE %s (a int, b int)");
        createTable("CREATE TABLE %s (pk int, ck int, v int, c " + type + ", PRIMARY KEY (pk, ck))");
        disableCompaction();
        long writeTimestamp = FBUtilities.timestampMicros();
        execute("UPDATE %s USING TIMESTAMP " + writeTimestamp + " SET v = 1, c.a = 1 WHERE pk = 1 AND ck = 1");
        flush();
        execute("UPDATE %s USING TIMESTAMP " + writeTimestamp + " SET v = 2, c.b = 2 WHERE pk = 2 AND ck = 1");
        flush();
        TableMetadata current = currentTableMetadata();
        AbstractType<?> frozenTuple = new TupleType(Arrays.asList(Int32Type.instance, Int32Type.instance,
                                                                  UTF8Type.instance), false);
        ColumnMetadata record = ColumnMetadata.regularColumn(current.keyspace, current.name, "c", frozenTuple);
        SchemaTestUtil.announceTableUpdate(current.unbuild()
                                                  .removeRegularOrStaticColumn(COLUMN_C)
                                                  .recordColumnDrop(record, writeTimestamp - 1)
                                                  .build());
        assertFalse(currentTableMetadata().getDroppedColumn(COLUMN_C.bytes).isComplex());

        compactAll();
        List<ColumnMetadata> columnsNamedC = new ArrayList<>();
        for (ColumnMetadata column : newestSSTable().header.columns())
        {
            if (column.name.equals(COLUMN_C))
                columnsNamedC.add(column);
        }
        assertRows(execute("SELECT pk, ck, v FROM %s"), row(1, 1, 1), row(2, 1, 2));
        reopenSSTables();
        assertRows(execute("SELECT pk, ck, v FROM %s"), row(1, 1, 1), row(2, 1, 2));
        assertEquals(columnsNamedC.toString(), 1, columnsNamedC.size());
        assertTrue(columnsNamedC.get(0).isComplex());
    }

    /**
     * The column of {@link #createTableWithUnrelatedColumnVersions} is dropped again, and only the sstables of the
     * original column are compacted, together. Their output has the type of the last drop, as a compaction with an
     * sstable of the re-added column would give it. Nothing is lost, even when cells of the original column are newer
     * than both drops, as with a client-supplied timestamp: the new type can read the values of the original one.
     */
    @Test
    public void testOriginalColumnCompactedAloneGetsTypeOfLastDrop() throws Throwable
    {
        long future = FBUtilities.timestampMicros() + 3_600_000_000L;
        createTableWithUnrelatedColumnVersions(" USING TIMESTAMP " + future);
        execute("ALTER TABLE %s DROP c");
        maybeReopenSSTables();

        compactOnly(sstable -> typeOfC(sstable).equals(OLD_TYPE));
        assertTypeOfCInAllSSTables(NEW_TYPE);
        assertRowsSurviveScrub(row(1, 1, 1), row(1, 2, 1));

        execute("ALTER TABLE %s ADD c " + NEW_TYPE.asCQL3Type());
        assertRows(execute("SELECT pk, ck, v, c FROM %s"),
                   row(1, 1, 1, map(1, map(bytes(0, 0, 0, 1), "a"), 2, map(bytes(0, 0, 0, 2), "b"))),
                   row(1, 2, 1, map(1, map(bytes(0, 0, 0, 1), "a"), 3, map(bytes(0, 0, 0, 3), "c"))));
    }

    /**
     * The column is dropped and re-added with an unrelated type, and stays live. The sstables of the original column
     * are compacted together first, so they end up with the highest id; if they were open before the re-add, the
     * result keeps their header, with the original type. The final compaction must still store the column under its
     * current type.
     */
    @Test
    public void testLiveColumnKeepsCurrentType() throws Throwable
    {
        createTableWithUnrelatedColumnVersions();
        execute("UPDATE %s SET v = 2, " + NEW_TYPE_CELLS + " WHERE pk = 1 AND ck = 1");
        flush();
        maybeReopenSSTables();

        compactOnly(sstable -> typeOfC(sstable).equals(OLD_TYPE));
        compactAll();
        assertTypeOfCInAllSSTables(NEW_TYPE);
        assertRowsSurviveScrub();

        assertRows(execute("SELECT pk, ck, v, c FROM %s"),
                   row(1, 1, 2, newTypeValues()),
                   row(1, 2, 1, null));
    }

    /**
     * The column is dropped, re-added with an unrelated type, written to, and dropped again, so that none of its data
     * survives. Compaction must neither fail nor bring any of that data back.
     */
    @Test
    public void testDroppedColumnWithUnrelatedVersions() throws Throwable
    {
        createTableWithUnrelatedColumnVersions();
        execute("UPDATE %s SET v = 2, " + NEW_TYPE_CELLS + " WHERE pk = 1 AND ck = 1");
        flush();
        execute("ALTER TABLE %s DROP c");
        maybeReopenSSTables();

        assertRows(execute("SELECT * FROM %s"), row(1, 1, 2), row(1, 2, 1));
        compactAll();
        assertTypeOfCInAllSSTables(NEW_TYPE);
        assertRowsSurviveScrub();

        // Re-adding the column with the type it was last dropped with is legal here: none of the dropped data returns.
        execute("ALTER TABLE %s ADD c " + NEW_TYPE.asCQL3Type());
        assertRows(execute("SELECT pk, ck, v, c FROM %s"), row(1, 1, 2, null), row(1, 2, 1, null));
    }

    /**
     * As above, but the second drop is older than the writes to the re-added column, as for writes that reached a
     * replica before the drop did, or that have a client-supplied timestamp. Those cells are not dropped; compaction
     * must keep them under the type of the last drop, which is the type they were written with, and still discard
     * the data of the original column.
     */
    @Test
    public void testDroppedColumnWithDataWrittenAfterDrop() throws Throwable
    {
        createTableWithUnrelatedColumnVersions();
        long writeTimestamp = FBUtilities.timestampMicros();
        execute("UPDATE %s USING TIMESTAMP " + writeTimestamp +
                " SET v = 2, " + NEW_TYPE_CELLS + " WHERE pk = 1 AND ck = 1");
        flush();
        execute("ALTER TABLE %s DROP c USING TIMESTAMP " + (writeTimestamp - 1));
        maybeReopenSSTables();

        assertRows(execute("SELECT pk, ck, v FROM %s"), row(1, 1, 2), row(1, 2, 1));
        compactAll();
        assertTypeOfCInAllSSTables(NEW_TYPE);
        assertRowsSurviveScrub();

        // The surviving cells come back when the column is re-added, and the cells from before the drops do not.
        execute("ALTER TABLE %s ADD c " + NEW_TYPE.asCQL3Type());
        assertRows(execute("SELECT pk, ck, v, c FROM %s"),
                   row(1, 1, 2, newTypeValues()),
                   row(1, 2, 1, null));
    }

    /**
     * The column is dropped and re-added with an unrelated type, and stays live. The data of the original column is
     * hidden by the drop, and the data of the re-added column must survive compaction under its type, also when the
     * sstables of the original column, kept open, still have its type.
     */
    @Test
    public void testLiveColumnWithUnrelatedOldVersion() throws Throwable
    {
        createTableWithUnrelatedColumnVersions();
        execute("UPDATE %s SET v = 2, " + NEW_TYPE_CELLS + " WHERE pk = 1 AND ck = 1");
        flush();
        maybeReopenSSTables();

        Object expected = newTypeValues();
        assertRows(execute("SELECT pk, ck, v, c FROM %s"), row(1, 1, 2, expected), row(1, 2, 1, null));
        compactAll();
        assertTypeOfCInAllSSTables(NEW_TYPE);
        assertRowsSurviveScrub();
        assertRows(execute("SELECT pk, ck, v, c FROM %s"), row(1, 1, 2, expected), row(1, 2, 1, null));
    }

    /**
     * Dropped complex columns, regular and static, whose cells are partly older and partly newer than the drop, with
     * live complex columns after them, and a row where nothing of the dropped column survives. Cursor compaction skips
     * a dropped column that has no cell newer than the drop (SkipEmptyDataCursor on the input). Both compaction paths
     * must keep exactly the cells newer than the drop.
     */
    @Test
    public void testDroppedColumnsWithCellsOnBothSidesOfDrop() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, a set<int>, c map<int, int>, e set<int>, " +
                    "sc map<int, int> static, se set<int> static, PRIMARY KEY (pk, ck))");
        disableCompaction();
        // The cell timestamps, for each sstable, of the entries 0 to 5 of c and sc; the columns are dropped at 1000.
        long[][] timestamps = { { 990, 995, 1005, 990, 1006, 1000 }, { 1000, 1000, 999, 1001, 998, 1002 } };
        for (int sstable = 0; sstable < 2; sstable++)
        {
            for (int ck = 1; ck <= 3; ck++)
            {
                for (int k = 0; k < timestamps[sstable].length; k++)
                {
                    long timestamp = timestamps[sstable][k];
                    if (ck == 2 && timestamp > 1000)
                        continue; // nothing of c survives in row 2
                    String update = "UPDATE %s USING TIMESTAMP " + timestamp;
                    int value = 100 * sstable + k;
                    execute(update + " SET c[" + k + "] = " + value + " WHERE pk = 1 AND ck = " + ck);
                    execute(update + " SET sc[" + k + "] = " + value + " WHERE pk = 1");
                }
                execute("UPDATE %s USING TIMESTAMP 5 SET a = a + {" + sstable + "}, e = e + {" + (10 + sstable) +
                        "}, v = " + ck + " WHERE pk = 1 AND ck = " + ck);
            }
            execute("UPDATE %s USING TIMESTAMP 5 SET se = se + {" + sstable + "} WHERE pk = 1");
            flush();
        }
        execute("ALTER TABLE %s DROP c USING TIMESTAMP 1000");
        execute("ALTER TABLE %s DROP sc USING TIMESTAMP 1000");
        maybeReopenSSTables();

        Object[][] expected = { row(1, 1, 1, set(0, 1), set(10, 11), set(0, 1)),
                                row(1, 2, 2, set(0, 1), set(10, 11), set(0, 1)),
                                row(1, 3, 3, set(0, 1), set(10, 11), set(0, 1)) };
        assertRows(execute("SELECT pk, ck, v, a, e, se FROM %s"), expected);
        compactAll();
        assertRows(execute("SELECT pk, ck, v, a, e, se FROM %s"), expected);

        execute("ALTER TABLE %s ADD c map<int, int>");
        execute("ALTER TABLE %s ADD sc map<int, int> static");
        Object surviving = map(2, 2, 3, 103, 4, 4, 5, 105);
        assertRows(execute("SELECT pk, ck, c, sc FROM %s"),
                   row(1, 1, surviving, surviving),
                   row(1, 2, null, surviving),
                   row(1, 3, surviving, surviving));
    }

    /**
     * Creates column {@code c} of type {@code typeFormat} applied to {@code ascii}, with {@code otherColumns}, and
     * writes {@code values[0]} and {@code values[1]} to it, in rows (1, 1) and (1, 2) of two sstables. Then drops it,
     * re-adds it with {@code text}, writes {@code values[2]} and {@code values[3]}, in rows (2, 1) and (1, 1) of a
     * third sstable, and drops it, re-adds it with {@code blob} and drops it again, both drops older than those two
     * writes. Rewrites the sstables of the {@code ascii} type alone, so that they have the highest ids, compacts all
     * the sstables, and checks {@code scrub}, the {@code blob} type in the header, and, once the column is re-added
     * with {@code blob}, its values in rows (1, 1), (1, 2) and (2, 1).
     */
    private void checkDroppedColumnReAddedTwice(String otherColumns, String typeFormat, String[] values,
                                                Object... expected) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, " + otherColumns + "c " +
                    String.format(typeFormat, "ascii") + ", PRIMARY KEY (pk, ck))");
        disableCompaction();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 1, 1, " + values[0] + ')');
        flush();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 2, 1, " + values[1] + ')');
        flush();
        execute("ALTER TABLE %s DROP c");
        execute("ALTER TABLE %s ADD c " + String.format(typeFormat, "text"));
        long writeTimestamp = FBUtilities.timestampMicros();
        String using = ") USING TIMESTAMP " + writeTimestamp;
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (2, 1, 2, " + values[2] + using);
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 1, 2, " + values[3] + using);
        flush();
        execute("ALTER TABLE %s DROP c USING TIMESTAMP " + (writeTimestamp - 2));
        execute("ALTER TABLE %s ADD c " + String.format(typeFormat, "blob"));
        execute("ALTER TABLE %s DROP c USING TIMESTAMP " + (writeTimestamp - 1));
        AbstractType<?> typeOfDrop = currentTableMetadata().getDroppedColumn(COLUMN_C.bytes).type;
        String originalType = String.format(typeFormat.replace(" static", ""), "ascii");
        maybeReopenSSTables();

        rewriteEachAlone(sstable -> typeOfC(sstable).asCQL3Type().toString().equals(originalType));
        assertEquals(originalType, typeOfC(newestSSTable()).asCQL3Type().toString());
        compactAll();
        assertRowsSurviveScrub(row(1, 1, 2), row(1, 2, 1), row(2, 1, 2));
        assertTypeOfCInAllSSTables(typeOfDrop);
        assertEquals(String.format(typeFormat.replace(" static", ""), "blob"), typeOfDrop.asCQL3Type().toString());

        execute("ALTER TABLE %s ADD c " + String.format(typeFormat, "blob"));
        assertRows(execute("SELECT pk, ck, v, c FROM %s"),
                   row(1, 1, 2, expected[0]),
                   row(1, 2, 1, expected[1]),
                   row(2, 1, 2, expected[2]));
    }

    /**
     * Creates the table with column {@code c} of a user type with fields {@code a} and {@code b}, frozen or not, and
     * writes two rows to it, in two sstables.
     *
     * @return the name of the user type
     */
    private String createTableWithUserTypeColumn(boolean frozen)
    {
        String type = createType("CREATE TYPE %s (a int, b int)");
        createTable("CREATE TABLE %s (pk int, ck int, v int, c " + (frozen ? "frozen<" + type + '>' : type) +
                    ", PRIMARY KEY (pk, ck))");
        disableCompaction();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 1, 1, {a: 1, b: 2})");
        flush();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 2, 1, {a: 3, b: 4})");
        flush();
        return type;
    }

    /**
     * For a table made by {@link #createTableWithUserTypeColumn} whose type got the field {@code x} (and maybe more),
     * then three more rows: compacts the sstables with the original type, then all of them, and checks the type of
     * {@code c}, the rows and {@code scrub}.
     */
    private void checkUserTypeColumnAfterCompaction() throws Throwable
    {
        maybeReopenSSTables();
        compactOnly(sstable -> typeOfC(sstable).subTypes().size() == 2);
        compactAll();
        assertTypeOfCInAllSSTables(currentTableMetadata().getColumn(COLUMN_C).type);
        String query = "SELECT pk, ck, v, c.a, c.b, c.x FROM %s";
        Object[][] expected = { row(1, 1, 2, 7, 8, "newer"), row(1, 2, 1, 3, 4, null), row(2, 1, 2, 5, 6, "new") };
        assertRows(execute(query), expected);
        assertEquals(CompactionManager.AllSSTableOpStatus.SUCCESSFUL,
                     getCurrentColumnFamilyStore().scrub(true, IScrubber.options().checkData().build(), 1));
        assertRows(execute(query), expected);
    }

    /**
     * Creates the table with column {@code c} of {@link #OLD_TYPE} and writes two rows to it, in two sstables. Then
     * drops the column and re-adds it as {@link #NEW_TYPE}, as earlier releases allowed.
     */
    private void createTableWithUnrelatedColumnVersions() throws Throwable
    {
        createTableWithUnrelatedColumnVersions("");
    }

    /**
     * As {@link #createTableWithUnrelatedColumnVersions()}, with {@code writeOptions}, such as a timestamp, for the
     * writes.
     */
    private void createTableWithUnrelatedColumnVersions(String writeOptions) throws Throwable
    {
        assertFalse(NEW_TYPE.isValueCompatibleWith(OLD_TYPE));
        assertFalse(OLD_TYPE.isValueCompatibleWith(NEW_TYPE));

        createTable("CREATE TABLE %s (pk int, ck int, v int, c " + OLD_TYPE.asCQL3Type() +
                    ", PRIMARY KEY (pk, ck))");
        disableCompaction();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 1, 1, {1: {1: 'a'}, 2: {2: 'b'}})" + writeOptions);
        flush();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 2, 1, {1: {1: 'a'}, 3: {3: 'c'}})" + writeOptions);
        flush();
        execute("ALTER TABLE %s DROP c");
        assertInvalidMessage("incompatible with previously dropped column 'c'",
                             "ALTER TABLE %s ADD c " + NEW_TYPE.asCQL3Type());
        SchemaTestUtil.announceTableUpdate(currentTableMetadata().unbuild().addRegularColumn("c", NEW_TYPE).build());
    }

    private void maybeReopenSSTables()
    {
        if (reopenSSTables)
            reopenSSTables();
    }

    private void reopenSSTables()
    {
        // The obsolete inputs of a compaction are deleted asynchronously; loadNewSSTables must not find them.
        LifecycleTransaction.waitForDeletions();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        int count = cfs.getLiveSSTables().size();
        cfs.clearUnsafe();
        cfs.loadNewSSTables();
        assertEquals(count, cfs.getLiveSSTables().size());
    }

    static AbstractType<?> typeOfC(SSTableReader sstable)
    {
        for (ColumnMetadata column : sstable.header.columns())
        {
            if (column.name.toString().equals("c"))
                return sstable.header.getType(column);
        }
        throw new AssertionError("No column c in " + sstable);
    }

    private void assertTypeOfCInAllSSTables(AbstractType<?> expected)
    {
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
            assertEquals("Type of c in " + sstable, expected, typeOfC(sstable));
    }

    /**
     * Compacts the sstables that match {@code predicate}, of which there must be at least one, into one sstable, and
     * checks that compaction took the path that the test is for.
     */
    private void compactOnly(Predicate<SSTableReader> predicate) throws Throwable
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Collection<Descriptor> descriptors = new ArrayList<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            if (predicate.test(sstable))
                descriptors.add(sstable.descriptor);
        }
        assertFalse(descriptors.isEmpty());
        int expected = cfs.getLiveSSTables().size() - descriptors.size() + 1;
        withCompactionPathCheck(() -> {
            Future<?> future = CompactionManager.instance.submitUserDefined(cfs, descriptors, CompactionManager.NO_GC);
            FBUtilities.waitOnFuture(future);
        });
        assertEquals(expected, cfs.getLiveSSTables().size());
    }

    /**
     * Compacts each sstable that matches {@code predicate}, of which there must be at least one, on its own, as
     * {@code upgradesstables} or a single-sstable tombstone compaction does. It then has a higher id than the other
     * sstables, and keeps its header: a compaction of one sstable writes the columns of its header as they are.
     */
    private void rewriteEachAlone(Predicate<SSTableReader> predicate) throws Throwable
    {
        List<SSTableReader> matching = new ArrayList<>();
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
        {
            if (predicate.test(sstable))
                matching.add(sstable);
        }
        assertFalse(matching.isEmpty());
        for (SSTableReader sstable : matching)
            compactOnly(candidate -> candidate == sstable);
    }

    private SSTableReader newestSSTable()
    {
        return Collections.max(getCurrentColumnFamilyStore().getLiveSSTables(), CompactionSSTable.idComparator);
    }

    /**
     * Compacts all the sstables, of which there must be several, into one, and checks that compaction took the path
     * that the test is for.
     */
    private void compactAll() throws Throwable
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertTrue(cfs.getLiveSSTables().size() > 1);
        withCompactionPathCheck(this::compact);
        assertEquals(1, cfs.getLiveSSTables().size());
    }

    /**
     * Runs {@code compaction} and checks that it took the path that the test is for.
     */
    private void withCompactionPathCheck(Runnable compaction) throws Throwable
    {
        withLogAppender(CompactionTask.class, appender -> {
            compaction.run();
            String path = useCursors ? " by cursors" : " by iterators";
            assertTrue("Compaction did not run" + path,
                       appender.list.stream().anyMatch(event -> event.getFormattedMessage().contains(path)));
        });
    }

    /**
     * Checks that {@code scrub}, validating the data, keeps both rows: it discards a partition that has data invalid
     * for the type that its sstable's header gives.
     */
    private void assertRowsSurviveScrub() throws Throwable
    {
        assertRowsSurviveScrub(row(1, 1, 2), row(1, 2, 1));
    }

    /**
     * Checks that {@code scrub}, validating the data, keeps the {@code expected} rows, as {@code (pk, ck, v)}.
     */
    private void assertRowsSurviveScrub(Object[]... expected) throws Throwable
    {
        assertRows(execute("SELECT pk, ck, v FROM %s"), expected);
        getCurrentColumnFamilyStore().scrub(true, IScrubber.options().checkData().build(), 1);
        assertRows(execute("SELECT pk, ck, v FROM %s"), expected);
    }

    private Object newTypeValues()
    {
        return map(1, map(bytes(0x01), "x"), 2, map(bytes(0x02, 0x03), "y"), 3, map(bytes(0x04), "z"));
    }

    private static AbstractType<?> mapOfFrozenMaps(AbstractType<?> innerKeys)
    {
        return MapType.getInstance(Int32Type.instance, MapType.getInstance(innerKeys, UTF8Type.instance, false), true);
    }

    private static ByteBuffer bytes(int... values)
    {
        ByteBuffer buffer = ByteBuffer.allocate(values.length);
        for (int value : values)
            buffer.put((byte) value);
        buffer.flip();
        return buffer;
    }
}
