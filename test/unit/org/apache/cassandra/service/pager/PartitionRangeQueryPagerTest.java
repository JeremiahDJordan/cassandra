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

package org.apache.cassandra.service.pager;

import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the page queries built by {@link PartitionRangeQueryPager}.
 */
public class PartitionRangeQueryPagerTest
{
    private static final ProtocolVersion PROTOCOL_VERSION = ProtocolVersion.CURRENT;

    private static TableMetadata metadata;

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        metadata = TableMetadata.builder("ks", "tbl")
                                .partitioner(Murmur3Partitioner.instance)
                                .addPartitionKeyColumn("k", Int32Type.instance)
                                .addClusteringColumn("c", Int32Type.instance)
                                .addRegularColumn("v", Int32Type.instance)
                                .build();
    }

    /**
     * Partition-restricted index queries that replicas can't strictly filter are range reads on a single partition,
     * [k, k]. Paging them must stop once it doesn't want more rows from that partition, instead of going on with the
     * range (k, k], which wraps around the whole ring.
     */
    @Test
    public void testSinglePartitionRange()
    {
        DecoratedKey key = key(0);
        PartitionRangeReadCommand command = command(new Bounds<>(key, key));
        assertThat(command.isLimitedToOnePartition()).isTrue();

        // the per-partition limit has been reached
        assertThat(nextPage(command, key, row(1), 0)).isNull();

        // the partition only had a static row, or no clustering columns
        assertThat(nextPage(command, key, null, 0)).isNull();
        assertThat(nextPage(command, key, null, 3)).isNull();

        // the partition can still have rows after the last returned one
        PartitionRangeReadCommand next = nextPage(command, key, row(1), 3);
        assertThat(next).isNotNull();
        assertThat(next.dataRange().keyRange()).isEqualTo(new Bounds<>(key, key));
        assertThat(next.isLimitedToOnePartition()).isTrue();
    }

    /**
     * Paging states written by a {@link SinglePartitionPager} have no partition key. Coordinators of DataStax releases
     * before 5.0.10, and all coordinators at ONE/LOCAL_ONE, page partition-restricted index queries that way, and a
     * client can hand such a state to a coordinator that pages the same query as a range read of the partition. That
     * read goes on in that partition, after the last returned row.
     */
    @Test
    public void testPagingStateOfSinglePartitionPager()
    {
        DecoratedKey key = key(0);
        PartitionRangeReadCommand command = command(new Bounds<>(key, key));

        for (ProtocolVersion version : new ProtocolVersion[]{ ProtocolVersion.V4, ProtocolVersion.V5, PROTOCOL_VERSION })
        {
            // what SinglePartitionPager#state() writes: no partition key, and the mark of the last returned row
            PagingState.RowMark rowMark = PagingState.RowMark.create(metadata, row(1), version);
            PagingState state = PagingState.deserialize(new PagingState(null, rowMark, 8, 2).serialize(version), version);
            assertThat(state.partitionKey).isNull();

            PartitionRangeQueryPager pager = (PartitionRangeQueryPager) command.getPager(state, version);
            PartitionRangeReadCommand next = (PartitionRangeReadCommand) pager.nextPageReadQuery(PageSize.inRows(1), command.limits());
            assertThat(next).isNotNull();
            assertThat(next.dataRange().keyRange()).isEqualTo(new Bounds<>(key, key));
            assertThat(next.dataRange().isPaging()).isTrue();
            assertThat(((DataRange.Paging) next.dataRange()).getLastReturned()).isEqualTo(row(1).clustering());
            assertThat(pager.state().partitionKey).isEqualTo(key.getKey());
        }
    }

    /**
     * Paging a range of several partitions goes on after the last returned partition.
     */
    @Test
    public void testMultiplePartitionRange()
    {
        DecoratedKey left = key(0);
        DecoratedKey last = key(1);
        DecoratedKey right = key(2);
        if (left.compareTo(right) > 0)
        {
            DecoratedKey tmp = left;
            left = right;
            right = tmp;
        }
        PartitionRangeReadCommand command = command(new Bounds<>(left, right));

        // the per-partition limit has been reached in a partition that isn't the last one of the range
        PartitionRangeReadCommand next = nextPage(command, last, row(1), 0);
        assertThat(next).isNotNull();
        assertThat(next.dataRange().keyRange()).isEqualTo(new Range<>(last, right));

        // the per-partition limit has been reached in the last partition of the range
        assertThat(nextPage(command, right, row(1), 0)).isNull();
    }

    /**
     * Ranges that exclude their end never return it, so paging them goes on after the last returned partition.
     */
    @Test
    public void testRangeExcludingEnd()
    {
        DecoratedKey[] keys = sortedKeys(3);
        DecoratedKey left = keys[0];
        DecoratedKey last = keys[1];
        DecoratedKey right = keys[2];

        for (AbstractBounds<PartitionPosition> bounds : Arrays.<AbstractBounds<PartitionPosition>>asList(new IncludingExcludingBounds<>(left, right),
                                                                                                         new ExcludingBounds<>(left, right)))
        {
            PartitionRangeReadCommand command = command(bounds);
            assertThat(nextPage(command, last, row(1), 0).dataRange().keyRange()).isEqualTo(new ExcludingBounds<>(last, right));
            assertThat(nextPage(command, last, row(1), 3).dataRange().keyRange()).isEqualTo(new IncludingExcludingBounds<>(last, right));
        }
    }

    /**
     * Token-restricted reads end at a token bound, not at a key: the last returned key is never the end of the range,
     * and the next page is the rest of the range up to that bound, which doesn't wrap around the ring.
     */
    @Test
    public void testTokenRange()
    {
        DecoratedKey key = key(0);
        Token token = key.getToken();
        Token minimum = Murmur3Partitioner.instance.getMinimumToken();

        // token(k) = token(0), and token(k) >= token(0), which goes on to the end of the ring
        for (PartitionPosition right : Arrays.<PartitionPosition>asList(token.maxKeyBound(), minimum.maxKeyBound()))
        {
            PartitionRangeReadCommand command = command(new Range<>(token.minKeyBound(), right));
            PartitionRangeReadCommand next = nextPage(command, key, row(1), 0);
            assertThat(next).isNotNull();
            assertThat(next.dataRange().keyRange()).isEqualTo(new Range<>(key, right));
            assertThat(next.dataRange().keyRange().contains(key)).isFalse();
            // the token of k = 2 is after the token of k = 0
            assertThat(next.dataRange().keyRange().contains(key(2))).isEqualTo(right.isMinimum());
        }
    }

    private static DecoratedKey[] sortedKeys(int n)
    {
        DecoratedKey[] keys = new DecoratedKey[n];
        for (int i = 0; i < n; i++)
            keys[i] = key(i);
        Arrays.sort(keys);
        return keys;
    }

    private static PartitionRangeReadCommand nextPage(PartitionRangeReadCommand command,
                                                      DecoratedKey lastReturnedKey,
                                                      BTreeRow lastReturnedRow,
                                                      int remainingInPartition)
    {
        PagingState.RowMark rowMark = lastReturnedRow == null ? null : PagingState.RowMark.create(metadata, lastReturnedRow, PROTOCOL_VERSION);
        PartitionRangeQueryPager pager = new PartitionRangeQueryPager(command, PROTOCOL_VERSION, lastReturnedKey, rowMark, 10, remainingInPartition);
        return (PartitionRangeReadCommand) pager.nextPageReadQuery(PageSize.inRows(1), command.limits());
    }

    private static PartitionRangeReadCommand command(AbstractBounds<PartitionPosition> bounds)
    {
        DataRange dataRange = new DataRange(bounds, new ClusteringIndexSliceFilter(Slices.ALL, false));
        return PartitionRangeReadCommand.create(metadata, 0, ColumnFilter.all(metadata), RowFilter.none(), DataLimits.cqlLimits(10, 3), dataRange);
    }

    private static DecoratedKey key(int k)
    {
        return Murmur3Partitioner.instance.decorateKey(Int32Type.instance.decompose(k));
    }

    private static BTreeRow row(int c)
    {
        return BTreeRow.emptyRow(Clustering.make(Int32Type.instance.decompose(c)));
    }
}
