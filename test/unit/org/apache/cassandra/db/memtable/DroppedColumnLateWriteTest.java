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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A write that carries data of a column dropped before the write arrived (from a coordinator that did not have the
 * drop yet, from a coordinator that already has a re-add of the column this node has not received yet, or on commit
 * log replay) is applied, whatever the memtable.
 * <p>
 * Sstables and every memtable keep the data of the dropped column that is newer than the drop: it is not visible
 * while the column is dropped, and is visible again once the column is re-added. {@code TrieMemtable} gives the
 * dropped columns an id for this, see {@code TrieMemtable.withDroppedColumns}.
 */
@RunWith(Parameterized.class)
public class DroppedColumnLateWriteTest extends CQLTester
{
    private static final String SELECT_WITHOUT_DROPPED = "SELECT pk, ck, v, s FROM %s";
    private static final String SELECT_WITH_READDED = "SELECT pk, ck, v, s, c FROM %s";

    public enum Kind
    {
        SIMPLE("int", false), COMPLEX("set<int>", false), STATIC_SIMPLE("int", true), STATIC_COMPLEX("set<int>", true);

        final String type;
        final boolean isStatic;

        Kind(String type, boolean isStatic)
        {
            this.type = type;
            this.isStatic = isStatic;
        }

        boolean isComplex()
        {
            return type.startsWith("set");
        }
    }

    @Parameterized.Parameter(0)
    public String memtable;

    @Parameterized.Parameter(1)
    public Kind kind;

    @Parameterized.Parameters(name = "memtable={0}, kind={1}")
    public static Collection<Object[]> parameters()
    {
        List<Object[]> parameters = new ArrayList<>();
        for (String memtable : new String[]{ "skiplist", "trie", "trie_stage1", "trie_stage2", "trie_stage3" })
            for (Kind kind : Kind.values())
                parameters.add(new Object[]{ memtable, kind });
        return parameters;
    }

    private TableMetadata createTableAndDropColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, s int static, c " + kind.type + (kind.isStatic ? " static" : "") +
                    ", PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + '\'');
        disableCompaction();
        TableMetadata before = currentTableMetadata();
        Thread.sleep(5);
        execute("ALTER TABLE %s DROP c");
        Thread.sleep(5); // timestamps taken from here on are newer than the drop
        return before;
    }

    private void reAddColumn() throws Throwable
    {
        execute("ALTER TABLE %s ADD c " + kind.type + (kind.isStatic ? " static" : ""));
    }

    /** The value of the static column s in the rows of partition 0, if the write included the other columns. */
    private Integer expectedS()
    {
        return kind.isStatic ? 7 : null;
    }

    /** The value of c written by {@link #lateWrite}, once the column is re-added. */
    private Object expectedC()
    {
        return kind.isComplex() ? set(3) : 3;
    }

    private static ColumnMetadata column(TableMetadata metadata, String name)
    {
        return metadata.getColumn(ColumnIdentifier.getInterned(name, false));
    }

    private void addDroppedColumnData(Row.Builder builder, ColumnMetadata c, long timestamp, boolean complexDeletion, boolean cells)
    {
        if (complexDeletion)
            builder.addComplexDeletion(c, DeletionTime.build(timestamp - 1, FBUtilities.nowInSeconds()));
        if (!cells)
            return;
        if (kind.isComplex())
            builder.addCell(BufferCell.live(c, timestamp, ByteBufferUtil.EMPTY_BYTE_BUFFER, CellPath.create(Int32Type.instance.decompose(3))));
        else
            builder.addCell(BufferCell.live(c, timestamp, Int32Type.instance.decompose(3)));
    }

    /**
     * Applies a write to (pk 0, ck 1) built with the metadata from before the drop, as a replica that has the drop
     * receives it (or reads it from its commit log).
     *
     * @param otherColumns whether the write also has a row marker and v = 2 (and s = 7 if c is static)
     * @param complexDeletion whether c is overwritten or deleted, rather than added to
     * @param cells whether the write has cells of c: 3 or {3}
     */
    private void lateWrite(TableMetadata before, long timestamp, boolean otherColumns, boolean complexDeletion, boolean cells) throws IOException
    {
        PartitionUpdate.Builder update = PartitionUpdate.builder(before, Util.makeKey(before, 0), before.regularAndStaticColumns(), 2);
        if (kind.isStatic)
        {
            Row.Builder builder = BTreeRow.unsortedBuilder();
            builder.newRow(Clustering.STATIC_CLUSTERING);
            if (otherColumns)
                builder.addCell(BufferCell.live(column(before, "s"), timestamp, Int32Type.instance.decompose(7)));
            addDroppedColumnData(builder, column(before, "c"), timestamp, complexDeletion, cells);
            update.add(builder.build());
        }
        if (otherColumns || !kind.isStatic)
        {
            Row.Builder builder = BTreeRow.unsortedBuilder();
            builder.newRow(Clustering.make(Int32Type.instance.decompose(1)));
            if (otherColumns)
            {
                builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, FBUtilities.nowInSeconds()));
                builder.addCell(BufferCell.live(column(before, "v"), timestamp, Int32Type.instance.decompose(2)));
            }
            if (!kind.isStatic)
                addDroppedColumnData(builder, column(before, "c"), timestamp, complexDeletion, cells);
            update.add(builder.build());
        }

        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Mutation.serializer.serialize(new Mutation(update.build()), out, MessagingService.current_version);
            Mutation.serializer.deserialize(new DataInputBuffer(out.buffer(), true), MessagingService.current_version).apply();
        }
    }

    private void addSecondSSTableAndCompact() throws Throwable
    {
        execute("INSERT INTO %s (pk, ck, v) VALUES (0, 2, 9)");
        flush();
        compact();
    }

    @Test
    public void testLateWrite() throws Throwable
    {
        TableMetadata before = createTableAndDropColumn();
        lateWrite(before, FBUtilities.timestampMicros(), true, false, true);

        assertRows(execute(SELECT_WITHOUT_DROPPED), row(0, 1, 2, expectedS()));
        flush();
        assertRows(execute(SELECT_WITHOUT_DROPPED), row(0, 1, 2, expectedS()));
        addSecondSSTableAndCompact();
        assertRows(execute(SELECT_WITHOUT_DROPPED), row(0, 1, 2, expectedS()), row(0, 2, 9, expectedS()));

        reAddColumn();
        assertRows(execute(SELECT_WITH_READDED), row(0, 1, 2, expectedS(), expectedC()), row(0, 2, 9, expectedS(), kind.isStatic ? expectedC() : null));
    }

    @Test
    public void testLateWriteAndReAddBeforeFlush() throws Throwable
    {
        TableMetadata before = createTableAndDropColumn();
        lateWrite(before, FBUtilities.timestampMicros(), true, false, true);

        reAddColumn();
        assertRows(execute(SELECT_WITH_READDED), row(0, 1, 2, expectedS(), expectedC()));
        flush();
        assertRows(execute(SELECT_WITH_READDED), row(0, 1, 2, expectedS(), expectedC()));
        addSecondSSTableAndCompact();
        assertRows(execute(SELECT_WITH_READDED), row(0, 1, 2, expectedS(), expectedC()), row(0, 2, 9, expectedS(), kind.isStatic ? expectedC() : null));
    }

    @Test
    public void testWriteOlderThanTheDrop() throws Throwable
    {
        long timestamp = FBUtilities.timestampMicros();
        TableMetadata before = createTableAndDropColumn();
        lateWrite(before, timestamp, true, false, true);

        assertRows(execute(SELECT_WITHOUT_DROPPED), row(0, 1, 2, expectedS()));
        reAddColumn();
        assertRows(execute(SELECT_WITH_READDED), row(0, 1, 2, expectedS(), null));
        flush();
        assertRows(execute(SELECT_WITH_READDED), row(0, 1, 2, expectedS(), null));
    }

    @Test
    public void testLateWriteOfOnlyTheDroppedColumn() throws Throwable
    {
        TableMetadata before = createTableAndDropColumn();
        lateWrite(before, FBUtilities.timestampMicros(), false, false, true);

        // Only SELECT * before a flush: a selection of some columns, or a read from an sstable, returns a row of nulls
        // for a row with only a dropped column's cell.
        assertEmpty(execute("SELECT * FROM %s"));

        reAddColumn();
        Object[] expected = kind.isStatic ? row(0, null, null, null, expectedC()) : row(0, 1, null, null, expectedC());
        assertRows(execute(SELECT_WITH_READDED), expected);
        flush();
        assertRows(execute(SELECT_WITH_READDED), expected);
    }

    /** The complex deletion of an overwrite (c = {3}) is kept with its cells: it shadows older elements after a re-add. */
    @Test
    public void testLateOverwriteOfComplexColumn() throws Throwable
    {
        checkLateComplexDeletion(true);
    }

    /** As {@link #testLateOverwriteOfComplexColumn} for a write that only deletes the column (c = {}, or DELETE c). */
    @Test
    public void testLateDeletionOfComplexColumn() throws Throwable
    {
        checkLateComplexDeletion(false);
    }

    private void checkLateComplexDeletion(boolean cells) throws Throwable
    {
        Assume.assumeTrue(kind.isComplex());
        TableMetadata before = createTableAndDropColumn();
        long timestamp = FBUtilities.timestampMicros();
        lateWrite(before, timestamp, true, true, cells);
        assertRows(execute(SELECT_WITHOUT_DROPPED), row(0, 1, 2, expectedS()));

        reAddColumn();
        // older than the late write
        execute("UPDATE %s USING TIMESTAMP ? SET c = c + {5} WHERE pk = 0" + (kind.isStatic ? "" : " AND ck = 1"), timestamp - 2);
        // the complex deletion of the late write shadows the older element
        Object expected = cells ? expectedC() : null;
        assertRows(execute(SELECT_WITH_READDED), row(0, 1, 2, expectedS(), expected));
        flush();
        assertRows(execute(SELECT_WITH_READDED), row(0, 1, 2, expectedS(), expected));
    }
}
