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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A write in flight during a schema change can be built with another version of the table's metadata than the
 * memtable it is applied to: it is decoded (or built by its statement) before the change and applied after the
 * memtable switch, or decoded after the table's metadata reference was updated and applied before the switch that
 * {@code ColumnFamilyStore.reload} does next. Each cell has to end up in its own column whatever the memtable.
 * <p>
 * Every schema change switches the memtable, so the memtable from before a change only receives writes in that
 * window. The tests that apply a write before the memtable switch emulate the window by building the newer version
 * of the metadata by hand and applying the write without changing the table.
 */
@RunWith(Parameterized.class)
public class MemtableWriteDuringSchemaChangeTest extends CQLTester
{
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
}
