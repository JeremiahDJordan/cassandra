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

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidPatternException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.BTree;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
        RowFilter.Builder filter = RowFilter.builder(false);
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
        assertFalse(iter.hasNext());
        assertTrue(closed.get());

        filter = RowFilter.builder(false);
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
        assertFalse(iter.hasNext());
        assertTrue(closed.get());
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
        assertEquals(0, matchingRows(filter, metadata, s, 1, a, b, 0, 2));
        assertEquals(1, matchingRows(filter, metadata, s, 1, a, b, 1, 2));
        assertEquals(1, matchingRows(filter, metadata, s, 2, a, b, 0, 2));
        assertEquals(0, matchingRows(filter, metadata, s, 3, a, b, 1, 2));

        // (s = 1 OR a = 1) AND b = 2: the regular column satisfies the disjunction although the static one doesn't
        filter = filter(and(or(eq(s, 1), eq(a, 1)), eq(b, 2)));
        assertEquals(1, matchingRows(filter, metadata, s, 0, a, b, 1, 2));
        assertEquals(1, matchingRows(filter, metadata, s, 1, a, b, 0, 2));
        assertEquals(0, matchingRows(filter, metadata, s, 0, a, b, 0, 2));
        assertEquals(0, matchingRows(filter, metadata, s, 1, a, b, 1, 3));

        // s = 1 AND (a = 1 OR b = 2): the static condition is necessary, and the disjunction is evaluated per row
        filter = filter(and(eq(s, 1), or(eq(a, 1), eq(b, 2))));
        assertEquals(1, matchingRows(filter, metadata, s, 1, a, b, 0, 2));
        assertEquals(0, matchingRows(filter, metadata, s, 2, a, b, 1, 2));
        assertEquals(0, matchingRows(filter, metadata, s, 1, a, b, 0, 3));

        // s = 1 OR s = 2: only static conditions, so rows only depend on their partition
        filter = filter(or(eq(s, 1), eq(s, 2)));
        assertEquals(1, matchingRows(filter, metadata, s, 2, a, b, 0, 0));
        assertEquals(0, matchingRows(filter, metadata, s, 3, a, b, 0, 0));
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
        assertTrue(returnsStaticOnlyPartition(filter(and(eq(s, 1), eq(t, 2))), metadata, s, t));
        assertTrue(returnsStaticOnlyPartition(filter(and(eq(s, 1), or(eq(t, 0), eq(t, 2)))), metadata, s, t));
        assertTrue(returnsStaticOnlyPartition(filter(or(and(eq(s, 1), eq(t, 2)), and(eq(s, 3), eq(t, 3)))), metadata, s, t));
        assertFalse(returnsStaticOnlyPartition(filter(and(eq(s, 1), or(eq(t, 0), eq(t, 3)))), metadata, s, t));

        // a condition on a regular column needs a row
        assertFalse(returnsStaticOnlyPartition(filter(and(eq(s, 1), eq(a, 1))), metadata, s, t));
        assertFalse(returnsStaticOnlyPartition(filter(and(eq(s, 1), or(eq(t, 0), eq(a, 1)))), metadata, s, t));
        // ...unless the static row alone satisfies a disjunction mixing static and regular columns
        assertTrue(returnsStaticOnlyPartition(filter(and(eq(s, 1), or(eq(t, 2), eq(a, 1)))), metadata, s, t));
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
        Unfiltered open = new RangeTombstoneBoundMarker(ClusteringBound.inclusiveStartOf(clustering(0)), DeletionTime.build(1, 1));
        Unfiltered close = new RangeTombstoneBoundMarker(ClusteringBound.inclusiveEndOf(clustering(9)), DeletionTime.build(1, 1));
        Unfiltered match1 = row(a, 1, 1);
        Unfiltered match2 = row(a, 4, 1);

        assertEquals(List.of(match1, match2), filtered(filter, metadata, row(a, 0, 0), match1, row(a, 3, 0), match2));
        assertEquals(List.of(match1), filtered(filter, metadata, match1));
        assertEquals(List.of(open, match1, close), filtered(filter, metadata, open, row(a, 0, 0), match1, close));
        assertNull(filtered(filter, metadata, open, row(a, 0, 0), close));
        assertNull(filtered(filter, metadata, row(a, 0, 0)));
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
            assertFalse(partitions.hasNext());
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
                assertFalse(returned.staticRow().isEmpty());
                assertFalse(returned.hasNext());
            }
            assertFalse(partitions.hasNext());
            return true;
        }
    }

    private static RowFilter filter(RowFilter.FilterElement root)
    {
        return new RowFilter(root, false, IndexHints.NONE, OptimizerOptions.NONE);
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

    @Test
    public void testMutableIntersections()
    {
        TableMetadata metadata = TableMetadata.builder("testks", "testcf")
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addRegularColumn("r", Int32Type.instance)
                                              .addRegularColumn("t", UTF8Type.instance)
                                              .build();

        // Empty filter - no expressions
        RowFilter filter = RowFilter.builder(false).build();
        assertFalse(filter.isMutableIntersection());

        ColumnMetadata r = metadata.getColumn(new ColumnIdentifier("r", true));

        // Single expression on one column
        RowFilter.Builder builder = RowFilter.builder(false);
        builder.add(r, Operator.GT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        filter = builder.build();
        assertFalse(filter.isMutableIntersection());

        // Two expressions on the same column
        builder = RowFilter.builder(false);
        builder.add(r, Operator.GT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        builder.add(r, Operator.LT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        filter = builder.build();
        assertFalse(filter.isMutableIntersection());

        // Three expressions on two different columns - this is a mutable intersection
        ColumnMetadata t = metadata.getColumn(new ColumnIdentifier("t", true));
        builder = RowFilter.builder(false);
        builder.add(r, Operator.GT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        builder.add(r, Operator.LT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        builder.add(t, Operator.EQ, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        filter = builder.build();
        assertTrue(filter.isMutableIntersection());
    }

    @Test
    public void testHasStaticExpression()
    {
        TableMetadata metadata = TableMetadata.builder("testks", "testcf")
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addClusteringColumn("ck", Int32Type.instance)
                                              .addStaticColumn("s", Int32Type.instance)
                                              .addRegularColumn("r", Int32Type.instance)
                                              .build();
        ColumnMetadata s = metadata.getColumn(new ColumnIdentifier("s", true));
        ColumnMetadata r = metadata.getColumn(new ColumnIdentifier("r", true));
        ByteBuffer one = Int32Type.instance.decompose(1);

        RowFilter filter = RowFilter.builder(false).build();
        assertFalse(filter.hasStaticExpression());
        assertFalse(filter.hasStaticExpression());

        RowFilter.Builder builder = RowFilter.builder(false);
        builder.add(r, Operator.EQ, one);
        filter = builder.build();
        assertFalse(filter.hasStaticExpression());
        assertFalse(filter.hasStaticExpression());

        builder = RowFilter.builder(false);
        builder.add(r, Operator.EQ, one);
        RowFilter.Expression staticExpression = builder.add(s, Operator.EQ, one);
        filter = builder.build();
        assertTrue(filter.hasStaticExpression());
        assertTrue(filter.hasStaticExpression());
        assertFalse(filter.without(staticExpression).hasStaticExpression());

        // a static expression in a nested disjunction
        RowFilter.Expression regularExpression = new RowFilter.SimpleExpression(r, Operator.EQ, one, null, null);
        RowFilter.FilterElement disjunction = new RowFilter.FilterElement(true, List.of(staticExpression, regularExpression), List.of());
        RowFilter.FilterElement root = new RowFilter.FilterElement(false, List.of(), List.of(disjunction));
        filter = new RowFilter(root, false, IndexHints.NONE, OptimizerOptions.NONE);
        assertTrue(filter.hasStaticExpression());
        assertTrue(filter.hasStaticExpression());
    }

    /**
     * A LIKE pattern that can't be compiled into an automaton fails the filtering with an invalid pattern error naming
     * the restricted column, which the operator evaluating the pattern doesn't know.
     */
    @Test
    public void testInvalidLikePatternNamesColumn()
    {
        TableMetadata metadata = TableMetadata.builder("testks", "testcf")
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addRegularColumn("v", UTF8Type.instance)
                                              .build();
        ColumnMetadata v = metadata.getColumn(new ColumnIdentifier("v", true));

        // a generic pattern too complex to determinize
        StringBuilder pattern = new StringBuilder();
        for (int i = 0; i < 1000; i++)
            pattern.append("a%");
        pattern.append('a');

        RowFilter.Builder builder = RowFilter.builder(false);
        builder.add(v, Operator.LIKE_MATCHES, UTF8Type.instance.decompose(pattern.toString()));
        RowFilter filter = builder.build();
        Row row = BTreeRow.create(Clustering.EMPTY,
                                  LivenessInfo.EMPTY,
                                  Row.Deletion.LIVE,
                                  BTree.singleton(new BufferCell(v, 1, Cell.NO_TTL, Cell.NO_DELETION_TIME, UTF8Type.instance.decompose("abc"), null)));
        DecoratedKey key = Murmur3Partitioner.instance.decorateKey(Int32Type.instance.decompose(1));

        assertThatThrownBy(() -> filter.isSatisfiedBy(metadata, key, row, 1))
            .isInstanceOf(InvalidPatternException.class)
            .hasMessageStartingWith("Invalid pattern for LIKE '<term>' restriction on column v: ");
    }

    @Test
    public void testCheckOnce() throws Exception
    {
        RowFilter.Builder builder = new RowFilter.Builder(false, null, IndexHints.NONE);
        AtomicInteger runs = new AtomicInteger();
        builder.checkOnce("a", runs::incrementAndGet);
        builder.checkOnce("a", runs::incrementAndGet);
        builder.checkOnce("b", runs::incrementAndGet);
        assertEquals(2, runs.get());

        // under a disjunction, each conjunction is built by a sub-builder that shares the checks of the filter
        Field current = RowFilter.Builder.class.getDeclaredField("current");
        current.setAccessible(true);
        current.set(builder, new RowFilter.FilterElement.Builder(true));
        builder.addAllAsConjunction(b -> b.checkOnce("a", runs::incrementAndGet));
        builder.addAllAsConjunction(b -> b.checkOnce("c", runs::incrementAndGet));
        builder.addAllAsConjunction(b -> b.checkOnce("c", runs::incrementAndGet));
        builder.checkOnce("c", runs::incrementAndGet);
        assertEquals(3, runs.get());

        // the first check of a filter can run in a sub-builder
        builder = new RowFilter.Builder(false, null, IndexHints.NONE);
        current.set(builder, new RowFilter.FilterElement.Builder(true));
        builder.addAllAsConjunction(b -> b.checkOnce("a", runs::incrementAndGet));
        builder.addAllAsConjunction(b -> b.checkOnce("a", runs::incrementAndGet));
        builder.checkOnce("a", runs::incrementAndGet);
        assertEquals(4, runs.get());
    }

    @Test
    public void testRowFilterAll()
    {
        // RowFilter.ALL is the singleton unsatisfiable filter
        assertTrue(RowFilter.REJECT_ALL.isUnsatisfiable());
        assertTrue(RowFilter.REJECT_ALL.isEmpty());

        // isSatisfiedBy always returns false
        TableMetadata metadata = TableMetadata.builder("k", "t")
                                              .addPartitionKeyColumn("k", Int32Type.instance)
                                              .addRegularColumn("r", Int32Type.instance)
                                              .build();
        ColumnMetadata r = metadata.getColumn(new ColumnIdentifier("r", true));
        ByteBuffer one = Int32Type.instance.decompose(1);
        Row row = BTreeRow.create(Clustering.EMPTY,
                                  LivenessInfo.EMPTY,
                                  Row.Deletion.LIVE,
                                  BTree.singleton(new BufferCell(r, 1, Cell.NO_TTL, Cell.NO_DELETION_TIME, one, null)));
        assertFalse(RowFilter.REJECT_ALL.isSatisfiedBy(metadata, null, row, 1));

        // filter(UnfilteredPartitionIterator) closes the non-empty input and returns an empty iterator
        AtomicBoolean closed = new AtomicBoolean();
        UnfilteredPartitionIterator unfiltereds = new SingletonUnfilteredPartitionIterator(new UnfilteredRowIterator()
        {
            public DeletionTime partitionLevelDeletion() { return DeletionTime.LIVE; }
            public EncodingStats stats() { return EncodingStats.NO_STATS; }
            public TableMetadata metadata() { return metadata; }
            public boolean isReverseOrder() { return false; }
            public RegularAndStaticColumns columns() { return metadata.regularAndStaticColumns(); }
            public DecoratedKey partitionKey() { return null; }
            public Row staticRow() { return Rows.EMPTY_STATIC_ROW; }
            public boolean hasNext() { return true; }   // non-empty partition
            public Unfiltered next() { return row; }
            public void close() { closed.set(true); }
        });
        assertTrue(unfiltereds.hasNext());
        assertFalse(RowFilter.REJECT_ALL.filter(unfiltereds, 1).hasNext());
        assertTrue(closed.get());

        // filter(PartitionIterator) closes the non-empty input and returns an empty iterator
        closed.set(false);
        PartitionIterator partitions = new PartitionIterator()
        {
            boolean hasNext = true;
            public boolean hasNext() { return hasNext; }
            public RowIterator next() {hasNext = false; return null; }
            public void close() { closed.set(true); }
        };
        assertTrue(partitions.hasNext());
        assertFalse(RowFilter.REJECT_ALL.filter(partitions, metadata, 1).hasNext());
        assertTrue(closed.get());
    }
}
