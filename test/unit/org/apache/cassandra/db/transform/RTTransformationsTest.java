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
package org.apache.cassandra.db.transform;

import java.nio.ByteBuffer;
import java.util.*;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterators;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.ClusteringPrefix.Kind;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.RTBoundValidator.Stage;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.transform.RTBoundCloser.close;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.apache.cassandra.db.transform.RTBoundValidator.validate;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public final class RTTransformationsTest
{
    private static final String KEYSPACE = "RTBoundCloserTest";
    private static final String TABLE = "table";

    private final int nowInSec = FBUtilities.nowInSeconds();

    private TableMetadata metadata;
    private DecoratedKey key;

    @Before
    public void setUp()
    {
        metadata =
            TableMetadata.builder(KEYSPACE, TABLE)
                         .addPartitionKeyColumn("pk", UTF8Type.instance)
                         .addClusteringColumn("ck0", UTF8Type.instance)
                         .addClusteringColumn("ck1", UTF8Type.instance)
                         .addClusteringColumn("ck2", UTF8Type.instance)
                         .partitioner(Murmur3Partitioner.instance)
                         .build();
        key = Murmur3Partitioner.instance.decorateKey(bytes("key"));
    }

    @Test
    public void testAddsNothingWhenAlreadyClosed()
    {
        UnfilteredPartitionIterator original = iter(false
        , bound(Kind.INCL_START_BOUND, 0, "a", "1")
        , row(1, "a", "1", "")
        , bound(Kind.INCL_END_BOUND, 0, "a", "1")
        );

        UnfilteredPartitionIterator extended = iter(false
        , bound(Kind.INCL_START_BOUND, 0, "a", "1")
        , row(1, "a", "1", "")
        , bound(Kind.INCL_END_BOUND, 0, "a", "1")
        );
        assertIteratorsEqual(original, close(extended));
    }

    @Test
    public void testAddsNothingWhenAlreadyClosedInReverseOrder()
    {
        UnfilteredPartitionIterator original = iter(true
        , bound(Kind.INCL_END_BOUND, 0, "a", "1")
        , row(1, "a", "1", "")
        , bound(Kind.INCL_START_BOUND, 0, "a", "1")
        );

        UnfilteredPartitionIterator extended = iter(true
        , bound(Kind.INCL_END_BOUND, 0, "a", "1")
        , row(1, "a", "1", "")
        , bound(Kind.INCL_START_BOUND, 0, "a", "1")
        );
        assertIteratorsEqual(original, close(extended));
    }

    @Test
    public void testClosesUnclosedBound()
    {
        UnfilteredPartitionIterator original = iter(false
        , bound(Kind.INCL_START_BOUND, 0, "a", "1")
        , row(1, "a", "1", "")
        );
        UnfilteredPartitionIterator extended = close(original);

        UnfilteredPartitionIterator expected = iter(false
        , bound(Kind.INCL_START_BOUND, 0, "a", "1")
        , row(1, "a", "1", "")
        , bound(Kind.INCL_END_BOUND, 0, "a", "1", "")
        );
        assertIteratorsEqual(expected, extended);
    }

    @Test
    public void testClosesUnclosedBoundary()
    {
        UnfilteredPartitionIterator original = iter(false
        , bound(Kind.INCL_START_BOUND, 0, "a")
        , boundary(Kind.EXCL_END_INCL_START_BOUNDARY, 0, 1, "a", "0")
        , row(2, "a", "1", "")
        );
        UnfilteredPartitionIterator extended = close(original);

        UnfilteredPartitionIterator expected = iter(false
        , bound(Kind.INCL_START_BOUND, 0, "a")
        , boundary(Kind.EXCL_END_INCL_START_BOUNDARY, 0, 1, "a", "0")
        , row(2, "a", "1", "")
        , bound(Kind.INCL_END_BOUND, 1, "a", "1", "")
        );
        assertIteratorsEqual(expected, extended);
    }

    @Test
    public void testClosesUnclosedBoundInReverseOrder()
    {
        UnfilteredPartitionIterator original = iter(true
        , bound(Kind.INCL_END_BOUND, 0, "a", "1")
        , row(1, "a", "1", "")
        );
        UnfilteredPartitionIterator extended = close(original);

        UnfilteredPartitionIterator expected = iter(true
        , bound(Kind.INCL_END_BOUND, 0, "a", "1")
        , row(1, "a", "1", "")
        , bound(Kind.INCL_START_BOUND, 0, "a", "1", "")
        );
        assertIteratorsEqual(expected, extended);
    }

    @Test
    public void testClosesUnclosedBoundaryInReverseOrder()
    {
        UnfilteredPartitionIterator original = iter(true
        , bound(Kind.INCL_END_BOUND, 0, "a")
        , boundary(Kind.INCL_END_EXCL_START_BOUNDARY, 1, 0, "a", "1")
        , row(2, "a", "0", "")
        );
        UnfilteredPartitionIterator extended = close(original);

        UnfilteredPartitionIterator expected = iter(true
        , bound(Kind.INCL_END_BOUND, 0, "a")
        , boundary(Kind.INCL_END_EXCL_START_BOUNDARY, 1, 0, "a", "1")
        , row(2, "a", "0", "")
        , bound(Kind.INCL_START_BOUND, 1, "a", "0", "")
        );

        assertIteratorsEqual(expected, extended);
    }

    @Test
    public void testFailsWithoutSeeingRows()
    {
        UnfilteredPartitionIterator iterator = iter(false
        , bound(Kind.INCL_START_BOUND, 0, "a")
        );
        assertThrowsISEIterated(close(iterator));
    }

    @Test
    public void testRemovesUnclosedBoundAfterLastRow()
    {
        UnfilteredPartitionIterator original = iter(false
        , row(1, "a", "0", "")
        , bound(Kind.INCL_START_BOUND, 0, "a", "1")
        );
        UnfilteredPartitionIterator extended = closeWithoutReturning(original);

        UnfilteredPartitionIterator expected = iter(false
        , row(1, "a", "0", "")
        );
        assertIteratorsEqual(expected, extended);
    }

    @Test
    public void testRemovesUnclosedBoundAfterLastRowInReverseOrder()
    {
        UnfilteredPartitionIterator original = iter(true
        , row(1, "a", "1", "")
        , bound(Kind.INCL_END_BOUND, 0, "a", "0")
        );
        UnfilteredPartitionIterator extended = closeWithoutReturning(original);

        UnfilteredPartitionIterator expected = iter(true
        , row(1, "a", "1", "")
        );
        assertIteratorsEqual(expected, extended);
    }

    @Test
    public void testClosesUnclosedBoundaryAfterLastRow()
    {
        UnfilteredPartitionIterator original = iter(false
        , bound(Kind.INCL_START_BOUND, 0, "a")
        , row(2, "a", "0", "")
        , boundary(Kind.EXCL_END_INCL_START_BOUNDARY, 0, 1, "a", "1")
        );
        UnfilteredPartitionIterator extended = closeWithoutReturning(original);

        UnfilteredPartitionIterator expected = iter(false
        , bound(Kind.INCL_START_BOUND, 0, "a")
        , row(2, "a", "0", "")
        , bound(Kind.EXCL_END_BOUND, 0, "a", "1")
        );
        assertIteratorsEqual(expected, extended);
    }

    @Test
    public void testClosesUnclosedBoundBeforeUnreturnedRow()
    {
        UnfilteredPartitionIterator original = iter(false
        , bound(Kind.INCL_START_BOUND, 0, "a")
        , row(1, "a", "0", "")
        , row(1, "a", "1", "")
        );
        UnfilteredPartitionIterator extended = closeWithoutReturning(original, "a", "1", "");

        UnfilteredPartitionIterator expected = iter(false
        , bound(Kind.INCL_START_BOUND, 0, "a")
        , row(1, "a", "0", "")
        , bound(Kind.EXCL_END_BOUND, 0, "a", "1", "")
        );
        assertIteratorsEqual(expected, extended);
    }

    @Test
    public void testClosesUnclosedBoundBeforeUnreturnedRowInReverseOrder()
    {
        UnfilteredPartitionIterator original = iter(true
        , bound(Kind.INCL_END_BOUND, 0, "a")
        , row(1, "a", "1", "")
        , row(1, "a", "0", "")
        );
        UnfilteredPartitionIterator extended = closeWithoutReturning(original, "a", "0", "");

        UnfilteredPartitionIterator expected = iter(true
        , bound(Kind.INCL_END_BOUND, 0, "a")
        , row(1, "a", "1", "")
        , bound(Kind.EXCL_START_BOUND, 0, "a", "0", "")
        );
        assertIteratorsEqual(expected, extended);
    }

    @Test
    public void testKeepsUnclosedBoundBeforeUnreturnedRow()
    {
        UnfilteredPartitionIterator original = iter(false
        , row(1, "a", "0", "")
        , bound(Kind.INCL_START_BOUND, 0, "a", "1")
        , row(1, "a", "1", "")
        );
        UnfilteredPartitionIterator extended = closeWithoutReturning(original, "a", "1", "");

        UnfilteredPartitionIterator expected = iter(false
        , row(1, "a", "0", "")
        , bound(Kind.INCL_START_BOUND, 0, "a", "1")
        , bound(Kind.EXCL_END_BOUND, 0, "a", "1", "")
        );
        assertIteratorsEqual(expected, extended);
    }

    @Test
    public void testKeepsUnclosedBoundBeforeUnreturnedRowInReverseOrder()
    {
        UnfilteredPartitionIterator original = iter(true
        , row(1, "a", "1", "")
        , bound(Kind.INCL_END_BOUND, 0, "a", "0")
        , row(1, "a", "0", "")
        );
        UnfilteredPartitionIterator extended = closeWithoutReturning(original, "a", "0", "");

        UnfilteredPartitionIterator expected = iter(true
        , row(1, "a", "1", "")
        , bound(Kind.INCL_END_BOUND, 0, "a", "0")
        , bound(Kind.EXCL_START_BOUND, 0, "a", "0", "")
        );
        assertIteratorsEqual(expected, extended);
    }

    @Test
    public void testValidatesLegalBounds()
    {
        UnfilteredPartitionIterator iterator = iter(false
        , bound(Kind.INCL_START_BOUND, 0, "a", "1")
        , row(1, "a", "1", "")
        , bound(Kind.INCL_END_BOUND, 0, "a", "1")

        , bound(Kind.INCL_START_BOUND, 0, "a", "2")
        , row(1, "a", "2", "")
        , bound(Kind.INCL_END_BOUND, 0, "a", "2")
        );
        iterator = validate(iterator, Stage.PROCESSED, true);
        drain(iterator);
    }

    @Test
    public void testValidatesLegalBoundsInReverseOrder()
    {
        UnfilteredPartitionIterator iterator = iter(true
        , bound(Kind.INCL_END_BOUND, 0, "a", "2")
        , row(1, "a", "2", "")
        , bound(Kind.INCL_START_BOUND, 0, "a", "2")

        , bound(Kind.INCL_END_BOUND, 0, "a", "1")
        , row(1, "a", "1", "")
        , bound(Kind.INCL_START_BOUND, 0, "a", "1")
        );
        iterator = validate(iterator, Stage.PROCESSED, true);
        drain(iterator);
    }

    @Test
    public void testValidatesLegalBoundaries()
    {
        UnfilteredPartitionIterator iterator = iter(false
        , bound(Kind.INCL_START_BOUND, 0, "a")

        , boundary(Kind.EXCL_END_INCL_START_BOUNDARY, 0, 1, "a", "1")
        , row(2, "a", "1", "")
        , boundary(Kind.INCL_END_EXCL_START_BOUNDARY, 1, 0, "a", "1")

        , boundary(Kind.EXCL_END_INCL_START_BOUNDARY, 0, 2, "a", "2")
        , row(3, "a", "2", "")
        , boundary(Kind.INCL_END_EXCL_START_BOUNDARY, 2, 0, "a", "2")

        , boundary(Kind.EXCL_END_INCL_START_BOUNDARY, 0, 3, "a", "3")
        , row(4, "a", "3", "")
        , boundary(Kind.INCL_END_EXCL_START_BOUNDARY, 3, 0, "a", "3")

        , bound(Kind.INCL_END_BOUND, 0, "a")
        );
        iterator = validate(iterator, Stage.PROCESSED, true);
        drain(iterator);
    }

    @Test
    public void testValidatesLegalBoundariesInReverseOrder()
    {
        UnfilteredPartitionIterator iterator = iter(true
        , bound(Kind.INCL_END_BOUND, 0, "a")

        , boundary(Kind.INCL_END_EXCL_START_BOUNDARY, 3, 0, "a", "3")
        , row(4, "a", "3", "")
        , boundary(Kind.EXCL_END_INCL_START_BOUNDARY, 0, 3, "a", "3")

        , boundary(Kind.INCL_END_EXCL_START_BOUNDARY, 2, 0, "a", "2")
        , row(3, "a", "2", "")
        , boundary(Kind.EXCL_END_INCL_START_BOUNDARY, 0, 2, "a", "2")

        , boundary(Kind.INCL_END_EXCL_START_BOUNDARY, 1, 0, "a", "1")
        , row(2, "a", "1", "")
        , boundary(Kind.EXCL_END_INCL_START_BOUNDARY, 0, 1, "a", "1")

        , bound(Kind.INCL_START_BOUND, 0, "a")
        );
        iterator = validate(iterator, Stage.PROCESSED, true);
        drain(iterator);
    }

    @Test
    public void testComplainsAboutMismatchedTimestamps()
    {
        UnfilteredPartitionIterator iterator = iter(false
        , bound(Kind.INCL_START_BOUND, 0, "a", "1")
        , row(1, "a", "1", "")
        , bound(Kind.INCL_END_BOUND, 1, "a", "1")
        );
        iterator = validate(iterator, Stage.PROCESSED, true);
        assertThrowsISEIterated(iterator);
    }

    @Test
    public void testComplainsAboutMismatchedTimestampsInReverseOrder()
    {
        UnfilteredPartitionIterator iterator = iter(true
        , bound(Kind.INCL_END_BOUND, 0, "a", "1")
        , row(1, "a", "1", "")
        , bound(Kind.INCL_START_BOUND, 1, "a", "1")
        );
        iterator = validate(iterator, Stage.PROCESSED, true);
        assertThrowsISEIterated(iterator);
    }

    @Test
    public void testComplainsAboutInvalidSequence()
    {
        // duplicated start bound
        UnfilteredPartitionIterator iterator = iter(false
        , bound(Kind.INCL_START_BOUND, 0, "a", "1")
        , bound(Kind.INCL_START_BOUND, 0, "a", "1")
        , row(1, "a", "1", "")
        , bound(Kind.INCL_END_BOUND, 0, "a", "1")
        );
        iterator = validate(iterator, Stage.PROCESSED, true);
        assertThrowsISEIterated(iterator);

        // duplicated end bound
        iterator = iter(false
        , bound(Kind.INCL_START_BOUND, 0, "a", "1")
        , row(1, "a", "1", "")
        , bound(Kind.INCL_END_BOUND, 0, "a", "1")
        , bound(Kind.INCL_END_BOUND, 0, "a", "1")
        );
        iterator = validate(iterator, Stage.PROCESSED, true);
        assertThrowsISEIterated(iterator);

        // absent open bound
        iterator = iter(false
        , row(1, "a", "1", "")
        , bound(Kind.INCL_END_BOUND, 0, "a", "1")
        );
        iterator = validate(iterator, Stage.PROCESSED, true);
        assertThrowsISEIterated(iterator);

        // absent end bound
        iterator = iter(false
        , bound(Kind.INCL_START_BOUND, 0, "a", "1")
        , row(1, "a", "1", "")
        );
        iterator = validate(iterator, Stage.PROCESSED, true);
        assertThrowsISEIterated(iterator);
    }

    @Test
    public void testComplainsAboutInvalidSequenceInReveseOrder()
    {
        // duplicated start bound
        UnfilteredPartitionIterator iterator = iter(true
        , bound(Kind.INCL_END_BOUND, 0, "a", "1")
        , row(1, "a", "1", "")
        , bound(Kind.INCL_START_BOUND, 0, "a", "1")
        , bound(Kind.INCL_START_BOUND, 0, "a", "1")
        );
        iterator = validate(iterator, Stage.PROCESSED, true);
        assertThrowsISEIterated(iterator);

        // duplicated end bound
        iterator = iter(true
        , bound(Kind.INCL_END_BOUND, 0, "a", "1")
        , bound(Kind.INCL_END_BOUND, 0, "a", "1")
        , row(1, "a", "1", "")
        , bound(Kind.INCL_START_BOUND, 0, "a", "1")
        );
        iterator = validate(iterator, Stage.PROCESSED, true);
        assertThrowsISEIterated(iterator);

        // absent open bound
        iterator = iter(true
        , bound(Kind.INCL_END_BOUND, 0, "a", "1")
        , row(1, "a", "1", "")
        );
        iterator = validate(iterator, Stage.PROCESSED, true);
        assertThrowsISEIterated(iterator);

        // absent end bound
        iterator = iter(true
        , row(1, "a", "1", "")
        , bound(Kind.INCL_START_BOUND, 0, "a", "1")
        );
        iterator = validate(iterator, Stage.PROCESSED, true);
        assertThrowsISEIterated(iterator);
    }

    private RangeTombstoneBoundMarker bound(ClusteringPrefix.Kind kind, long timestamp, Object... clusteringValues)
    {
        ByteBuffer[] clusteringByteBuffers = new ByteBuffer[clusteringValues.length];
        for (int i = 0; i < clusteringValues.length; i++)
            clusteringByteBuffers[i] = decompose(metadata.clusteringColumns().get(i).type, clusteringValues[i]);

        return new RangeTombstoneBoundMarker(BufferClusteringBound.create(kind, clusteringByteBuffers), new DeletionTime(timestamp, nowInSec));
    }

    private RangeTombstoneBoundaryMarker boundary(ClusteringPrefix.Kind kind, long closeTimestamp, long openTimestamp, Object... clusteringValues)
    {
        ByteBuffer[] clusteringByteBuffers = new ByteBuffer[clusteringValues.length];
        for (int i = 0; i < clusteringValues.length; i++)
            clusteringByteBuffers[i] = decompose(metadata.clusteringColumns().get(i).type, clusteringValues[i]);

        return new RangeTombstoneBoundaryMarker(BufferClusteringBoundary.create(kind, clusteringByteBuffers),
                                                new DeletionTime(closeTimestamp, nowInSec),
                                                new DeletionTime(openTimestamp, nowInSec));
    }

    private Row row(long timestamp, Object... clusteringValues)
    {
        ByteBuffer[] clusteringByteBuffers = new ByteBuffer[clusteringValues.length];
        for (int i = 0; i < clusteringValues.length; i++)
            clusteringByteBuffers[i] = decompose(metadata.clusteringColumns().get(i).type, clusteringValues[i]);

        return BTreeRow.noCellLiveRow(Clustering.make(clusteringByteBuffers), LivenessInfo.create(timestamp, nowInSec));
    }

    /**
     * Closes the iterator after limits that stop at the row with the given clustering, if any, without returning it.
     */
    private UnfilteredPartitionIterator closeWithoutReturning(UnfilteredPartitionIterator iterator, Object... clusteringValues)
    {
        Clustering<?> unreturned = clusteringValues.length == 0 ? null : row(0, clusteringValues).clustering();
        RTBoundCloser.LastReadRow lastReadRow = new RTBoundCloser.LastReadRow();
        iterator = Transformation.apply(iterator, lastReadRow);
        iterator = Transformation.apply(iterator, new StoppingTransformation<UnfilteredRowIterator>()
        {
            @Override
            protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
            {
                return Transformation.apply(partition, this);
            }

            @Override
            protected Row applyToRow(Row row)
            {
                if (!row.clustering().equals(unreturned))
                    return row;

                stopInPartition();
                return null;
            }
        });
        return close(iterator, lastReadRow);
    }

    @SuppressWarnings("unchecked")
    private static <T> ByteBuffer decompose(AbstractType<?> type, T value)
    {
        return ((AbstractType<T>) type).decompose(value);
    }

    private UnfilteredPartitionIterator iter(boolean isReversedOrder, Unfiltered... unfiltereds)
    {
        Iterator<Unfiltered> iterator = Iterators.forArray(unfiltereds);

        UnfilteredRowIterator rowIter =
            new AbstractUnfilteredRowIterator(metadata,
                                              key,
                                              DeletionTime.LIVE,
                                              metadata.regularAndStaticColumns(),
                                              Rows.EMPTY_STATIC_ROW,
                                              isReversedOrder,
                                              EncodingStats.NO_STATS)
        {
            protected Unfiltered computeNext()
            {
                return iterator.hasNext() ? iterator.next() : endOfData();
            }
        };

        return new SingletonUnfilteredPartitionIterator(rowIter);
    }

    private void assertIteratorsEqual(UnfilteredPartitionIterator iter1, UnfilteredPartitionIterator iter2)
    {
        while (iter1.hasNext())
        {
            assertTrue(iter2.hasNext());

            try (UnfilteredRowIterator partition1 = iter1.next())
            {
                try (UnfilteredRowIterator partition2 = iter2.next())
                {
                    assertIteratorsEqual(partition1, partition2);
                }
            }
        }
        assertFalse(iter2.hasNext());
    }

    private void assertIteratorsEqual(UnfilteredRowIterator iter1, UnfilteredRowIterator iter2)
    {
        while (iter1.hasNext())
        {
            assertTrue(iter2.hasNext());

            assertEquals(iter1.next(), iter2.next());
        }
        assertFalse(iter2.hasNext());
    }

    private void assertThrowsISEIterated(UnfilteredPartitionIterator iterator)
    {
        Throwable t = null;
        try
        {
            drain(iterator);
        }
        catch (Throwable e)
        {
            t = e;
        }
        assertTrue(t instanceof IllegalStateException);
    }

    private void drain(UnfilteredPartitionIterator iter)
    {
        while (iter.hasNext())
        {
            try (UnfilteredRowIterator partition = iter.next())
            {
                while (partition.hasNext())
                    partition.next();
            }
        }
    }
}
