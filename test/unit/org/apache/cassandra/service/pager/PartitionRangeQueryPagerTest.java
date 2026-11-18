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
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
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
