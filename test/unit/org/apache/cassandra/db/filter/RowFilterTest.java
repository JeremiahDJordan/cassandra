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

package org.apache.cassandra.db.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.btree.BTree;

public class RowFilterTest
{
    @Test
    public void testRowFilterClose()
    {
        // CASSANDRA-15126
        TableMetadata metadata = TableMetadata.builder("testks", "testcf")
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addStaticColumn("s", Int32Type.instance)
                                              .addRegularColumn("r", Int32Type.instance)
                                              .build();
        ColumnMetadata s = metadata.getColumn(new ColumnIdentifier("s", true));
        ColumnMetadata r = metadata.getColumn(new ColumnIdentifier("r", true));

        ByteBuffer one = Int32Type.instance.decompose(1);
        RowFilter.Builder filter = RowFilter.builder();
        filter.add(s, Operator.NEQ, one);
        AtomicBoolean closed = new AtomicBoolean();
        UnfilteredPartitionIterator iter = filter.build().filter(new SingletonUnfilteredPartitionIterator(new UnfilteredRowIterator()
        {
            public DeletionTime partitionLevelDeletion() { return null; }
            public EncodingStats stats() { return null; }
            public TableMetadata metadata() { return metadata; }
            public boolean isReverseOrder() { return false; }
            public RegularAndStaticColumns columns() { return null; }
            public DecoratedKey partitionKey() { return null; }
            public boolean hasNext() { return false; }
            public Unfiltered next() { return null; }
            public Row staticRow()
            {
                return BTreeRow.create(Clustering.STATIC_CLUSTERING,
                                       LivenessInfo.EMPTY,
                                       Row.Deletion.LIVE,
                                       BTree.singleton(new BufferCell(s, 1, Cell.NO_TTL, Cell.NO_DELETION_TIME, one, null)));
            }
            public void close()
            {
                closed.set(true);
            }
        }), 1);
        Assert.assertFalse(iter.hasNext());
        Assert.assertTrue(closed.get());

        filter = RowFilter.builder();
        filter.add(r, Operator.NEQ, one);
        closed.set(false);
        iter = filter.build().filter(new SingletonUnfilteredPartitionIterator(new UnfilteredRowIterator()
        {
            boolean hasNext = true;
            public DeletionTime partitionLevelDeletion() { return null; }
            public EncodingStats stats() { return null; }
            public TableMetadata metadata() { return metadata; }
            public boolean isReverseOrder() { return false; }
            public RegularAndStaticColumns columns() { return null; }
            public DecoratedKey partitionKey() { return null; }
            public Row staticRow() { return Rows.EMPTY_STATIC_ROW; }
            public boolean hasNext()
            {
                boolean r = hasNext;
                hasNext = false;
                return r;
            }
            public Unfiltered next()
            {
                return BTreeRow.create(Clustering.EMPTY,
                                       LivenessInfo.EMPTY,
                                       Row.Deletion.LIVE,
                                       BTree.singleton(new BufferCell(r, 1, Cell.NO_TTL, Cell.NO_DELETION_TIME, one, null)));
            }
            public void close()
            {
                closed.set(true);
            }
        }), 1);
        Assert.assertFalse(iter.hasNext());
        Assert.assertTrue(closed.get());
    }

    /**
     * Disjunctions can mix conditions on static and regular columns, so the filter must evaluate each row against the
     * whole tree, taking the values of static columns from the static row, instead of requiring the partition-level
     * and row-level parts of the tree to be satisfied separately.
     */
    @Test
    public void testStaticAndRegularColumnDisjunctions()
    {
        TableMetadata metadata = TableMetadata.builder("testks", "testcf")
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addClusteringColumn("ck", Int32Type.instance)
                                              .addStaticColumn("s", Int32Type.instance)
                                              .addRegularColumn("a", Int32Type.instance)
                                              .addRegularColumn("b", Int32Type.instance)
                                              .build();
        ColumnMetadata s = metadata.getColumn(new ColumnIdentifier("s", true));
        ColumnMetadata a = metadata.getColumn(new ColumnIdentifier("a", true));
        ColumnMetadata b = metadata.getColumn(new ColumnIdentifier("b", true));

        // (s = 1 AND a = 1) OR (s = 2 AND b = 2): neither branch matches s = 1, a = 0, b = 2
        RowFilter filter = filter(or(and(eq(s, 1), eq(a, 1)), and(eq(s, 2), eq(b, 2))));
        Assert.assertEquals(0, matchingRows(filter, metadata, s, 1, a, b, 0, 2));
        Assert.assertEquals(1, matchingRows(filter, metadata, s, 1, a, b, 1, 2));
        Assert.assertEquals(1, matchingRows(filter, metadata, s, 2, a, b, 0, 2));
        Assert.assertEquals(0, matchingRows(filter, metadata, s, 3, a, b, 1, 2));

        // (s = 1 OR a = 1) AND b = 2: the regular column satisfies the disjunction although the static one doesn't
        filter = filter(and(or(eq(s, 1), eq(a, 1)), eq(b, 2)));
        Assert.assertEquals(1, matchingRows(filter, metadata, s, 0, a, b, 1, 2));
        Assert.assertEquals(1, matchingRows(filter, metadata, s, 1, a, b, 0, 2));
        Assert.assertEquals(0, matchingRows(filter, metadata, s, 0, a, b, 0, 2));
        Assert.assertEquals(0, matchingRows(filter, metadata, s, 1, a, b, 1, 3));

        // s = 1 AND (a = 1 OR b = 2): the static condition is necessary, and the disjunction is evaluated per row
        filter = filter(and(eq(s, 1), or(eq(a, 1), eq(b, 2))));
        Assert.assertEquals(1, matchingRows(filter, metadata, s, 1, a, b, 0, 2));
        Assert.assertEquals(0, matchingRows(filter, metadata, s, 2, a, b, 1, 2));
        Assert.assertEquals(0, matchingRows(filter, metadata, s, 1, a, b, 0, 3));

        // s = 1 OR s = 2: only static conditions, so rows only depend on their partition
        filter = filter(or(eq(s, 1), eq(s, 2)));
        Assert.assertEquals(1, matchingRows(filter, metadata, s, 2, a, b, 0, 0));
        Assert.assertEquals(0, matchingRows(filter, metadata, s, 3, a, b, 0, 0));
    }

    /**
     * A partition with only a static row matches a filter with only partition-level conditions, and none whose
     * condition on a regular column is needed for the static row to match. A disjunction mixing static and regular
     * columns that the static row alone satisfies, such as {@code s = 1 AND (t = 2 OR a = 1)}, returns it, as SAI's
     * FilterTree does on the replicas.
     */
    @Test
    public void testPartitionsWithOnlyStaticRow()
    {
        TableMetadata metadata = TableMetadata.builder("testks", "testcf")
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addClusteringColumn("ck", Int32Type.instance)
                                              .addStaticColumn("s", Int32Type.instance)
                                              .addStaticColumn("t", Int32Type.instance)
                                              .addRegularColumn("a", Int32Type.instance)
                                              .build();
        ColumnMetadata s = metadata.getColumn(new ColumnIdentifier("s", true));
        ColumnMetadata t = metadata.getColumn(new ColumnIdentifier("t", true));
        ColumnMetadata a = metadata.getColumn(new ColumnIdentifier("a", true));

        // only static conditions, with or without nested groups
        Assert.assertTrue(returnsStaticOnlyPartition(filter(and(eq(s, 1), eq(t, 2))), metadata, s, t));
        Assert.assertTrue(returnsStaticOnlyPartition(filter(and(eq(s, 1), or(eq(t, 0), eq(t, 2)))), metadata, s, t));
        Assert.assertTrue(returnsStaticOnlyPartition(filter(or(and(eq(s, 1), eq(t, 2)), and(eq(s, 3), eq(t, 3)))),
                                                     metadata, s, t));
        Assert.assertFalse(returnsStaticOnlyPartition(filter(and(eq(s, 1), or(eq(t, 0), eq(t, 3)))), metadata, s, t));

        // a condition on a regular column needs a row
        Assert.assertFalse(returnsStaticOnlyPartition(filter(and(eq(s, 1), eq(a, 1))), metadata, s, t));
        Assert.assertFalse(returnsStaticOnlyPartition(filter(and(eq(s, 1), or(eq(t, 0), eq(a, 1)))), metadata, s, t));
        // ...unless the static row alone satisfies a disjunction mixing static and regular columns
        Assert.assertTrue(returnsStaticOnlyPartition(filter(and(eq(s, 1), or(eq(t, 2), eq(a, 1)))), metadata, s, t));
    }

    /**
     * A partition is returned with the rows that pass a filter on a regular column, and with the range tombstone
     * markers around them, whether its first content is a row or a marker. A partition without such rows is dropped.
     */
    @Test
    public void testPartitionsStartingWithRowOrMarker()
    {
        TableMetadata metadata = TableMetadata.builder("testks", "testcf")
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addClusteringColumn("ck", Int32Type.instance)
                                              .addRegularColumn("a", Int32Type.instance)
                                              .build();
        ColumnMetadata a = metadata.getColumn(new ColumnIdentifier("a", true));
        RowFilter filter = filter(and(eq(a, 1)));
        DeletionTime deletion = new DeletionTime(1, 1);
        Unfiltered open = new RangeTombstoneBoundMarker(ClusteringBound.inclusiveStartOf(clustering(0)), deletion);
        Unfiltered close = new RangeTombstoneBoundMarker(ClusteringBound.inclusiveEndOf(clustering(9)), deletion);
        Unfiltered match1 = row(a, 1, 1);
        Unfiltered match2 = row(a, 4, 1);

        Assert.assertEquals(List.of(match1, match2),
                            filtered(filter, metadata, row(a, 0, 0), match1, row(a, 3, 0), match2));
        Assert.assertEquals(List.of(match1), filtered(filter, metadata, match1));
        Assert.assertEquals(List.of(open, match1, close),
                            filtered(filter, metadata, open, row(a, 0, 0), match1, close));
        Assert.assertNull(filtered(filter, metadata, open, row(a, 0, 0), close));
        Assert.assertNull(filtered(filter, metadata, row(a, 0, 0)));
    }

    private static Clustering<?> clustering(int ck)
    {
        return Clustering.make(Int32Type.instance.decompose(ck));
    }

    private static Row row(ColumnMetadata column, int ck, int value)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(clustering(ck));
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(2, 1));
        builder.addCell(BufferCell.live(column, 2, Int32Type.instance.decompose(value)));
        return builder.build();
    }

    /** The contents of the given partition that the filter returns, or null if it drops the partition. */
    private static List<Unfiltered> filtered(RowFilter filter, TableMetadata metadata, Unfiltered... contents)
    {
        DecoratedKey key = Murmur3Partitioner.instance.decorateKey(Int32Type.instance.decompose(0));
        UnfilteredRowIterator partition = new AbstractUnfilteredRowIterator(metadata, key, DeletionTime.LIVE,
                                                                            metadata.regularAndStaticColumns(),
                                                                            Rows.EMPTY_STATIC_ROW, false, EncodingStats.NO_STATS)
        {
            private int next = 0;

            @Override
            protected Unfiltered computeNext()
            {
                return next < contents.length ? contents[next++] : endOfData();
            }
        };

        try (UnfilteredPartitionIterator partitions = filter.filter(new SingletonUnfilteredPartitionIterator(partition), 1))
        {
            if (!partitions.hasNext())
                return null;
            List<Unfiltered> returned = new ArrayList<>();
            try (UnfilteredRowIterator rows = partitions.next())
            {
                rows.forEachRemaining(returned::add);
            }
            Assert.assertFalse(partitions.hasNext());
            return returned;
        }
    }

    /** Whether the filter returns a partition with only a static row, with s = 1 and t = 2. */
    private static boolean returnsStaticOnlyPartition(RowFilter filter, TableMetadata metadata, ColumnMetadata s, ColumnMetadata t)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.STATIC_CLUSTERING);
        builder.addCell(BufferCell.live(s, 1, Int32Type.instance.decompose(1)));
        builder.addCell(BufferCell.live(t, 1, Int32Type.instance.decompose(2)));
        Row staticRow = builder.build();
        DecoratedKey key = Murmur3Partitioner.instance.decorateKey(Int32Type.instance.decompose(0));
        UnfilteredRowIterator partition = new AbstractUnfilteredRowIterator(metadata, key, DeletionTime.LIVE,
                                                                            metadata.regularAndStaticColumns(),
                                                                            staticRow, false, EncodingStats.NO_STATS)
        {
            @Override
            protected Unfiltered computeNext()
            {
                return endOfData();
            }
        };

        try (UnfilteredPartitionIterator partitions = filter.filter(new SingletonUnfilteredPartitionIterator(partition), 1))
        {
            if (!partitions.hasNext())
                return false;
            try (UnfilteredRowIterator returned = partitions.next())
            {
                Assert.assertFalse(returned.staticRow().isEmpty());
                Assert.assertFalse(returned.hasNext());
            }
            Assert.assertFalse(partitions.hasNext());
            return true;
        }
    }

    private static RowFilter filter(RowFilter.FilterElement root)
    {
        return new RowFilter(root, IndexHints.NONE);
    }

    private static RowFilter.FilterElement or(Object... members)
    {
        return element(true, members);
    }

    private static RowFilter.FilterElement and(Object... members)
    {
        return element(false, members);
    }

    private static RowFilter.FilterElement element(boolean isDisjunction, Object... members)
    {
        List<RowFilter.Expression> expressions = new ArrayList<>();
        List<RowFilter.FilterElement> children = new ArrayList<>();
        for (Object member : members)
        {
            if (member instanceof RowFilter.Expression)
                expressions.add((RowFilter.Expression) member);
            else
                children.add((RowFilter.FilterElement) member);
        }
        return new RowFilter.FilterElement(isDisjunction, expressions, children);
    }

    private static RowFilter.Expression eq(ColumnMetadata column, int value)
    {
        return new RowFilter.SimpleExpression(column, Operator.EQ, Int32Type.instance.decompose(value), null, null);
    }

    /** The number of rows of a one-row partition with the given static and regular values that pass the filter. */
    private static int matchingRows(RowFilter filter, TableMetadata metadata,
                                    ColumnMetadata s, int sValue,
                                    ColumnMetadata a, ColumnMetadata b, int aValue, int bValue)
    {
        Row staticRow = BTreeRow.create(Clustering.STATIC_CLUSTERING,
                                        LivenessInfo.EMPTY,
                                        Row.Deletion.LIVE,
                                        BTree.singleton(BufferCell.live(s, 1, Int32Type.instance.decompose(sValue))));
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.make(Int32Type.instance.decompose(0)));
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(1, 1));
        builder.addCell(BufferCell.live(a, 1, Int32Type.instance.decompose(aValue)));
        builder.addCell(BufferCell.live(b, 1, Int32Type.instance.decompose(bValue)));
        Row row = builder.build();
        DecoratedKey key = Murmur3Partitioner.instance.decorateKey(Int32Type.instance.decompose(0));

        UnfilteredRowIterator partition = new AbstractUnfilteredRowIterator(metadata, key, DeletionTime.LIVE,
                                                                            metadata.regularAndStaticColumns(),
                                                                            staticRow, false, EncodingStats.NO_STATS)
        {
            private boolean returned = false;

            @Override
            protected Unfiltered computeNext()
            {
                if (returned)
                    return endOfData();
                returned = true;
                return row;
            }
        };

        int count = 0;
        try (UnfilteredPartitionIterator partitions = filter.filter(new SingletonUnfilteredPartitionIterator(partition), 1))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator rows = partitions.next())
                {
                    while (rows.hasNext())
                    {
                        if (rows.next().isRow())
                            count++;
                    }
                }
            }
        }
        return count;
    }
}
