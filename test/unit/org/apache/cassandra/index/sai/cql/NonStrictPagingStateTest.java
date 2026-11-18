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

package org.apache.cassandra.index.sai.cql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Paging states of partition-restricted SAI queries whose row filter replicas can't strictly apply (CASSANDRA-19018),
 * across the ways coordinators build these queries.
 */
public class NonStrictPagingStateTest extends SAITester
{
    private boolean previousNonStrict;

    @Before
    public void setup()
    {
        previousNonStrict = DatabaseDescriptor.getSAINonStrictReplicaFilteringEnabled();
        DatabaseDescriptor.setSAINonStrictReplicaFilteringEnabled(true);
    }

    @After
    public void restoreNonStrictFiltering()
    {
        DatabaseDescriptor.setSAINonStrictReplicaFilteringEnabled(previousNonStrict);
    }

    /**
     * A client can page a partition-restricted query on coordinators that build it differently: as a single-partition
     * read with its per-partition limit (at ONE, and on coordinators without this folding), as a range read of
     * the partition with the per-partition limit folded into the row limit (this coordinator at QUORUM), or as a range
     * read with the limits as written (Apache Cassandra coordinators). The paging state of each must continue on the
     * others with the right rows: the remaining rows in the partition, which the state keeps, still bound the folded
     * reads and the unfolded ones. A coordinator also changes from the single-partition read to the range read, and
     * back, if non-strict replica filtering is enabled or disabled between two pages.
     */
    @Test
    public void testPagingStatesAcrossFoldedLimits()
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'}");
        createTable(keyspace, "CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY (k, c))");
        createIndex(keyspace, "CREATE INDEX ON %s(a) USING 'sai'");
        createIndex(keyspace, "CREATE INDEX ON %s(b) USING 'sai'");
        String table = keyspace + '.' + currentTable();
        for (int c = 0; c < 20; c++)
            execute("INSERT INTO " + table + " (k, c, a, b) VALUES (0, ?, 1, 2)", c);
        long nowInSec = FBUtilities.nowInSeconds();
        ProtocolVersion version = ProtocolVersion.CURRENT;

        // the limits, and how many rows the client must get
        Object[][] cases = {
            { " PER PARTITION LIMIT 5", 5 },
            { " PER PARTITION LIMIT 5 LIMIT 100", 5 },
            { " PER PARTITION LIMIT 5 LIMIT 3", 3 },
            { " PER PARTITION LIMIT 12", 12 },
            { " LIMIT 5", 5 },
            { "", 20 },
        };
        for (Object[] testCase : cases)
        {
            String cql = "SELECT c FROM " + table + " WHERE k = 0 AND a = 1 AND b = 2" + testCase[0];
            List<Integer> expected = new ArrayList<>();
            for (int c = 0; c < (Integer) testCase[1]; c++)
                expected.add(c);

            ReadQuery single = query(cql, ConsistencyLevel.ONE, nowInSec);
            ReadQuery folded = query(cql, ConsistencyLevel.QUORUM, nowInSec);
            assertThat(single).isInstanceOf(SinglePartitionReadQuery.Group.class);
            assertThat(folded).isInstanceOf(PartitionRangeReadCommand.class);

            // with non-strict filtering disabled, the coordinator reads the query as a single-partition read
            DatabaseDescriptor.setSAINonStrictReplicaFilteringEnabled(false);
            assertThat(query(cql, ConsistencyLevel.QUORUM, nowInSec)).isInstanceOf(SinglePartitionReadQuery.Group.class);
            DatabaseDescriptor.setSAINonStrictReplicaFilteringEnabled(true);

            ReadQuery unfolded = ((PartitionRangeReadCommand) folded).withUpdatedLimit(single.limits());
            ReadQuery[] queries = { single, folded, unfolded };

            for (int pageSize : new int[]{ 1, 2, 4 })
            {
                for (ReadQuery first : queries)
                {
                    for (ReadQuery rest : queries)
                    {
                        QueryPager pager = first.getPager(null, version);
                        List<Integer> rows = new ArrayList<>(fetchPage(pager, pageSize));
                        if (!pager.isExhausted())
                        {
                            PagingState state = PagingState.deserialize(pager.state().serialize(version), version);
                            QueryPager next = rest.getPager(state, version);
                            for (int pages = 0; !next.isExhausted() && pages < 100; pages++)
                                rows.addAll(fetchPage(next, pageSize));
                        }
                        assertThat(rows).as("%s, page size %d, first page with %s limits [%s], next with [%s]",
                                            cql, pageSize, first.getClass().getSimpleName(), first.limits(), rest.limits())
                                        .isEqualTo(expected);
                    }
                }
            }
        }
    }

    private static ReadQuery query(String cql, ConsistencyLevel consistency, long nowInSec)
    {
        SelectStatement statement = (SelectStatement) QueryProcessor.prepareInternal(cql).statement;
        return statement.getQuery(QueryOptions.forInternalCalls(consistency, Collections.emptyList()), nowInSec);
    }

    /** Reads a page locally, and returns its clusterings. */
    private static List<Integer> fetchPage(QueryPager pager, int pageSize)
    {
        List<Integer> rows = new ArrayList<>();
        try (ReadExecutionController controller = pager.executionController();
             PartitionIterator partitions = pager.fetchPageInternal(PageSize.inRows(pageSize), controller))
        {
            while (partitions.hasNext())
            {
                try (RowIterator partition = partitions.next())
                {
                    while (partition.hasNext())
                        rows.add(Int32Type.instance.compose(partition.next().clustering().bufferAt(0)));
                }
            }
        }
        return rows;
    }
}
