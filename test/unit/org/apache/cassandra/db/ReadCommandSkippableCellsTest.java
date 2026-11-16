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
import org.apache.cassandra.db.rows.TrieBackedRow;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.index.sai.utils.RowWithSource;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

/**
 * Tests {@link ReadCommand#withoutSkippableCells} on the row implementations: {@link BTreeRow}, which the read path
 * gives it, and {@link TrieBackedRow} and SAI's {@link RowWithSource}, which it converts to a {@link BTreeRow} when
 * it has a cell to remove.
 */
public class ReadCommandSkippableCellsTest extends CQLTester
{
    private static final CellSourceIdentifier SOURCE = new SequenceBasedSSTableId(1);

    private TableMetadata metadata;
    private ColumnMetadata r, t, x, l, m;

    @Test
    public void testRowImplementations() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, r int, t int, x int, l set<int>, m map<text, int>, PRIMARY KEY (k, c))");
        metadata = currentTableMetadata();
        r = column("r");
        t = column("t");
        x = column("x");
        l = column("l");
        m = column("m");

        ReadCommand command = readCommand(formatQuery("SELECT k, c, x, m['b'] FROM %s WHERE k = 1 AND c = 1"));
        long nowInSec = command.nowInSec();
        LivenessInfo liveMarker = LivenessInfo.create(10, nowInSec);
        LivenessInfo expiredMarker = LivenessInfo.create(10, 1, nowInSec - 100);

        for (RowType type : RowType.values())
        {
            // A row marker that is live: the cells of the not queried columns (and not queried cells of queried
            // columns) that are older than it are removed, except the live cells of a not queried collection.
            Row row = apply(command, olderCells(type, liveMarker, nowInSec));
            String message = type + ", live marker";
            assertNull(message, row.getCell(r));
            assertNull(message, row.getCell(t));
            assertNotNull(message, row.getCell(l, setPath(1)));
            assertNull(message, row.getCell(l, setPath(2)));
            assertNull(message, row.getCell(m, mapPath("a")));
            assertNotNull(message, row.getCell(m, mapPath("b")));
            assertNull(message, row.getCell(m, mapPath("c")));

            // A row marker that has expired: only the tombstones older than it are removed.
            row = apply(command, olderCells(type, expiredMarker, nowInSec));
            message = type + ", expired marker";
            assertNotNull(message, row.getCell(r));
            assertNull(message, row.getCell(t));
            assertNotNull(message, row.getCell(l, setPath(1)));
            assertNull(message, row.getCell(l, setPath(2)));
            assertNotNull(message, row.getCell(m, mapPath("a")));
            assertNotNull(message, row.getCell(m, mapPath("b")));
            assertNull(message, row.getCell(m, mapPath("c")));

            // Nothing to remove: the row is returned as it is.
            Row unchanged = type.build(this, liveMarker, b -> {
                b.addCell(BufferCell.live(r, 10, ByteBufferUtil.bytes(7)));
                b.addCell(BufferCell.live(x, 1, ByteBufferUtil.bytes(8)));
                b.addCell(BufferCell.live(l, 1, ByteBufferUtil.EMPTY_BYTE_BUFFER, setPath(1)));
                b.addCell(BufferCell.live(m, 1, ByteBufferUtil.bytes(2), mapPath("b")));
            });
            assertSame(type.toString(), unchanged, apply(command, unchanged));

            // No cell older than the row marker: the row is returned as it is.
            Row notOlder = type.build(this, liveMarker, b -> {
                b.addCell(BufferCell.live(r, 10, ByteBufferUtil.bytes(7)));
                b.addCell(BufferCell.tombstone(t, 10, nowInSec));
                b.addCell(BufferCell.live(l, 10, ByteBufferUtil.EMPTY_BYTE_BUFFER, setPath(1)));
                b.addCell(BufferCell.live(m, 11, ByteBufferUtil.bytes(1), mapPath("a")));
            });
            assertSame(type.toString(), notOlder, apply(command, notOlder));
        }
    }

    private enum RowType
    {
        BTREE_ROW
        {
            Row build(Row.Builder builder)
            {
                return builder.build();
            }
        },
        TRIE_BACKED_ROW
        {
            Row.Builder builder(TableMetadata metadata)
            {
                return TrieBackedRow.builder(metadata.regularColumns());
            }

            Row build(Row.Builder builder)
            {
                return builder.build();
            }
        },
        ROW_WITH_SOURCE
        {
            Row build(Row.Builder builder)
            {
                return new RowWithSource(builder.build(), SOURCE);
            }
        };

        Row.Builder builder(TableMetadata metadata)
        {
            return BTreeRow.sortedBuilder();
        }

        abstract Row build(Row.Builder builder);

        Row build(ReadCommandSkippableCellsTest test, LivenessInfo liveness, Consumer<Row.Builder> cells)
        {
            Row.Builder builder = builder(test.metadata);
            builder.newRow(Clustering.make(ByteBufferUtil.bytes(1)));
            builder.addPrimaryKeyLivenessInfo(liveness);
            cells.accept(builder);
            return build(builder);
        }
    }

    /**
     * A row with the given row marker and older cells: a live cell of the not queried column r, a tombstone of the
     * not queried column t, a live cell and a tombstone of the not queried collection l, and in the queried map m a
     * live cell outside the selection, a live cell in the selection and a tombstone outside the selection.
     */
    private Row olderCells(RowType type, LivenessInfo liveness, long nowInSec)
    {
        return type.build(this, liveness, b -> {
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
        DecoratedKey key = metadata.partitioner.decorateKey(Int32Type.instance.decompose(1));
        UnfilteredRowIterator partition = UnfilteredRowIterators.singleton(row, metadata, key, DeletionTime.LIVE,
                                                                           metadata.regularAndStaticColumns(),
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
        return metadata.getColumn(ColumnIdentifier.getInterned(name, false));
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
