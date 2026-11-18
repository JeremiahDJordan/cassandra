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

import java.util.function.Consumer;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

/**
 * Tests {@link ReadCommand#withoutSkippableCells} on merged rows.
 */
public class ReadCommandSkippableCellsTest extends CQLTester
{
    private ColumnMetadata r, t, x, l, m;

    @Test
    public void testWithoutSkippableCells() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, r int, t int, x int, l set<int>, m map<text, int>, PRIMARY KEY (k, c))");
        r = column("r");
        t = column("t");
        x = column("x");
        l = column("l");
        m = column("m");

        ReadCommand command = readCommand(formatQuery("SELECT k, c, x, m['b'] FROM %s WHERE k = 1 AND c = 1"));
        long nowInSec = command.nowInSec();
        LivenessInfo liveMarker = LivenessInfo.create(10, nowInSec);
        LivenessInfo expiredMarker = LivenessInfo.create(10, 1, nowInSec - 100);

        // A row marker that is live: the cells of the not queried columns (and not queried cells of queried columns)
        // that are older than it are removed, except the live cells of a not queried collection.
        Row row = apply(command, olderCells(liveMarker, nowInSec));
        assertNull(row.getCell(r));
        assertNull(row.getCell(t));
        assertNotNull(row.getCell(l, setPath(1)));
        assertNull(row.getCell(l, setPath(2)));
        assertNull(row.getCell(m, mapPath("a")));
        assertNotNull(row.getCell(m, mapPath("b")));
        assertNull(row.getCell(m, mapPath("c")));

        // A row marker that has expired: only the tombstones older than it are removed.
        row = apply(command, olderCells(expiredMarker, nowInSec));
        assertNotNull(row.getCell(r));
        assertNull(row.getCell(t));
        assertNotNull(row.getCell(l, setPath(1)));
        assertNull(row.getCell(l, setPath(2)));
        assertNotNull(row.getCell(m, mapPath("a")));
        assertNotNull(row.getCell(m, mapPath("b")));
        assertNull(row.getCell(m, mapPath("c")));

        // Nothing to remove: the row is returned as it is.
        Row unchanged = build(liveMarker, b -> {
            b.addCell(BufferCell.live(r, 10, ByteBufferUtil.bytes(7)));
            b.addCell(BufferCell.live(x, 1, ByteBufferUtil.bytes(8)));
            b.addCell(BufferCell.live(l, 1, ByteBufferUtil.EMPTY_BYTE_BUFFER, setPath(1)));
            b.addCell(BufferCell.live(m, 1, ByteBufferUtil.bytes(2), mapPath("b")));
        });
        assertSame(unchanged, apply(command, unchanged));

        // No cell older than the row marker: the row is returned as it is.
        Row notOlder = build(liveMarker, b -> {
            b.addCell(BufferCell.live(r, 10, ByteBufferUtil.bytes(7)));
            b.addCell(BufferCell.tombstone(t, 10, nowInSec));
            b.addCell(BufferCell.live(l, 10, ByteBufferUtil.EMPTY_BYTE_BUFFER, setPath(1)));
            b.addCell(BufferCell.live(m, 11, ByteBufferUtil.bytes(1), mapPath("a")));
        });
        assertSame(notOlder, apply(command, notOlder));
    }

    private static Row build(LivenessInfo liveness, Consumer<Row.Builder> cells)
    {
        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(Clustering.make(ByteBufferUtil.bytes(1)));
        builder.addPrimaryKeyLivenessInfo(liveness);
        cells.accept(builder);
        return builder.build();
    }

    /**
     * A row with the given row marker and older cells: a live cell of the not queried column r, a tombstone of the
     * not queried column t, a live cell and a tombstone of the not queried collection l, and in the queried map m a
     * live cell outside the selection, a live cell in the selection and a tombstone outside the selection.
     */
    private Row olderCells(LivenessInfo liveness, long nowInSec)
    {
        return build(liveness, b -> {
            b.addCell(BufferCell.live(r, 1, ByteBufferUtil.bytes(7)));
            b.addCell(BufferCell.tombstone(t, 2, nowInSec));
            b.addCell(BufferCell.live(l, 1, ByteBufferUtil.EMPTY_BYTE_BUFFER, setPath(1)));
            b.addCell(BufferCell.tombstone(l, 2, nowInSec, setPath(2)));
            b.addCell(BufferCell.live(m, 1, ByteBufferUtil.bytes(1), mapPath("a")));
            b.addCell(BufferCell.live(m, 1, ByteBufferUtil.bytes(2), mapPath("b")));
            b.addCell(BufferCell.tombstone(m, 2, nowInSec, mapPath("c")));
        });
    }

    private Row apply(ReadCommand command, Row row)
    {
        DecoratedKey key = command.metadata().partitioner.decorateKey(Int32Type.instance.decompose(1));
        UnfilteredRowIterator partition = UnfilteredRowIterators.singleton(row, command.metadata(), key, DeletionTime.LIVE,
                                                                           command.metadata().regularAndStaticColumns(),
                                                                           Rows.EMPTY_STATIC_ROW, false,
                                                                           EncodingStats.NO_STATS);
        try (UnfilteredPartitionIterator iterator = command.withoutSkippableCells(new SingletonUnfilteredPartitionIterator(partition));
             UnfilteredRowIterator result = iterator.next())
        {
            return (Row) result.next();
        }
    }

    private ColumnMetadata column(String name)
    {
        return currentTableMetadata().getColumn(ColumnIdentifier.getInterned(name, false));
    }

    private static CellPath setPath(int element)
    {
        return CellPath.create(Int32Type.instance.decompose(element));
    }

    private static CellPath mapPath(String key)
    {
        return CellPath.create(ByteBufferUtil.bytes(key));
    }

    private static ReadCommand readCommand(String query)
    {
        SelectStatement statement = (SelectStatement) QueryProcessor.parseStatement(query).prepare(ClientState.forInternalCalls());
        return (ReadCommand) ((SinglePartitionReadQuery.Group<?>) statement.getQuery(QueryOptions.DEFAULT, FBUtilities.nowInSeconds())).queries.get(0);
    }
}
