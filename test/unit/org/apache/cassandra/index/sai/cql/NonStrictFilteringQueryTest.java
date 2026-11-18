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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.filter.DataLimits.NO_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests how the coordinator builds the read commands of SAI queries whose row filter can't be strictly applied on
 * replicas, because they need reconciliation and intersect several mutable columns (CASSANDRA-19018), with
 * non-strict replica filtering enabled and disabled.
 */
@RunWith(Parameterized.class)
public class NonStrictFilteringQueryTest extends SAITester
{
    @Parameterized.Parameter
    public boolean nonStrict;

    @Parameterized.Parameters(name = "nonStrict={0}")
    public static Collection<Object[]> parameters()
    {
        return Arrays.asList(new Object[]{ false }, new Object[]{ true });
    }

    private String keyspace;
    private boolean previousNonStrict;

    @Before
    public void setup()
    {
        previousNonStrict = DatabaseDescriptor.getSAINonStrictReplicaFilteringEnabled();
        DatabaseDescriptor.setSAINonStrictReplicaFilteringEnabled(nonStrict);

        // With more than one replica, reads at QUORUM need reconciliation at the coordinator
        keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'}");
        createTable(keyspace, "CREATE TABLE %s (k int, c int, a int, b int, v int, PRIMARY KEY (k, c))");
        createIndex(keyspace, "CREATE INDEX ON %s(a) USING 'sai'");
        createIndex(keyspace, "CREATE INDEX ON %s(b) USING 'sai'");
        createIndex(keyspace, "CREATE INDEX ON %s(v) USING 'sai'");
    }

    @After
    public void restoreNonStrictFiltering()
    {
        DatabaseDescriptor.setSAINonStrictReplicaFilteringEnabled(previousNonStrict);
    }

    /**
     * Partition-restricted queries that the replicas don't strictly filter must be range reads, so that the
     * coordinator reconciles and filters the candidate rows of all the replicas, instead of comparing digests. With
     * non-strict filtering disabled, they are single-partition reads that the replicas filter strictly.
     */
    @Test
    public void testPartitionRestrictedQueries()
    {
        // intersections of mutable columns that need reconciliation are relaxed if non-strict filtering is enabled...
        assertPartitionRestrictedRead("k = 0 AND a = 1 AND b = 2", ConsistencyLevel.QUORUM);
        assertPartitionRestrictedRead("k = 0 AND a = 1 AND b = 2", ConsistencyLevel.ALL);
        assertPartitionRestrictedRead("k = 0 AND a = 1 AND (b = 2 OR v = 3)", ConsistencyLevel.QUORUM);

        // ...not those that don't need reconciliation
        assertSinglePartitionRead("k = 0 AND a = 1 AND b = 2", ConsistencyLevel.ONE);
        assertSinglePartitionRead("k = 0 AND a = 1 AND b = 2", ConsistencyLevel.LOCAL_ONE);

        // restrictions on a single mutable column can be strictly filtered
        assertSinglePartitionRead("k = 0 AND a = 1", ConsistencyLevel.QUORUM);
        assertSinglePartitionRead("k = 0 AND c = 1 AND a = 1", ConsistencyLevel.QUORUM);

        // serial reads need reconciliation too: relaxed, they are range reads, which don't run Paxos
        assertPartitionRestrictedRead("k = 0 AND a = 1 AND b = 2", ConsistencyLevel.SERIAL);
        assertPartitionRestrictedRead("k = 0 AND a = 1 AND b = 2", ConsistencyLevel.LOCAL_SERIAL);
        assertSinglePartitionRead("k = 0 AND a = 1", ConsistencyLevel.SERIAL);
    }

    /**
     * Reads of full primary keys skip the index if the replicas filter them strictly; otherwise they use it, and a read
     * of a single partition is a range read.
     */
    @Test
    public void testFullPrimaryKeyQueries()
    {
        for (ConsistencyLevel consistency : new ConsistencyLevel[]{ ConsistencyLevel.QUORUM, ConsistencyLevel.ALL })
        {
            ReadQuery query = query("k = 0 AND c = 1 AND a = 1 AND b = 2", options(consistency));
            if (nonStrict)
            {
                assertThat(query).isInstanceOf(PartitionRangeReadCommand.class);
                assertThat(query.indexQueryPlan()).isNotNull();
            }
            else
            {
                assertThat(query).isInstanceOf(SinglePartitionReadQuery.Group.class);
                SinglePartitionReadCommand command = (SinglePartitionReadCommand) ((SinglePartitionReadQuery.Group<?>) query).queries.get(0);
                assertThat(command.indexQueryPlan()).isNull();
                assertThat(command.rowFilter().isStrict()).isTrue();
            }
        }

        // reads at ONE skip the index either way
        ReadQuery query = query("k = 0 AND c = 1 AND a = 1 AND b = 2", options(ConsistencyLevel.ONE));
        assertThat(((SinglePartitionReadQuery.Group<?>) query).queries.get(0).indexQueryPlan()).isNull();
    }

    /**
     * Reads of several partitions stay single-partition reads, which read data from every replica they contact if
     * the replicas may return candidate rows, and compare digests otherwise.
     */
    @Test
    public void testMultiplePartitionQueries()
    {
        ReadQuery query = query("k IN (0, 1) AND ((a = 1 AND b = 2) OR a = 3)", options(ConsistencyLevel.QUORUM));
        assertThat(query).isInstanceOf(SinglePartitionReadQuery.Group.class);
        List<? extends SinglePartitionReadQuery> queries = ((SinglePartitionReadQuery.Group<?>) query).queries;
        assertThat(queries).hasSize(2);
        for (SinglePartitionReadQuery partitionQuery : queries)
        {
            SinglePartitionReadCommand command = (SinglePartitionReadCommand) partitionQuery;
            assertThat(command.indexQueryPlan()).isNotNull();
            assertThat(command.rowFilter().isStrict()).isEqualTo(!nonStrict);
            assertThat(command.returnsCandidateRows()).isEqualTo(nonStrict);
        }
    }

    /**
     * The per-partition limit of a partition-restricted query that is read as a range read is folded into its row
     * limit, so that replica filtering protection doesn't read the whole partition. The
     * byte limit of the page weight guardrail is kept. GROUP BY limits, and the limits of reads of several partitions or
     * of single-partition reads, are left as they are.
     */
    @Test
    public void testPerPartitionLimitOfPartitionRestrictedQueries()
    {
        if (!nonStrict)
        {
            // single-partition reads keep their limits
            assertLimits("k = 0 AND a = 1 AND b = 2 PER PARTITION LIMIT 2", ConsistencyLevel.QUORUM, NO_LIMIT, 2);
            assertLimits("k = 0 AND a = 1 AND b = 2 PER PARTITION LIMIT 5 LIMIT 3", ConsistencyLevel.QUORUM, 3, 5);
            assertLimits("a = 1 AND b = 2 PER PARTITION LIMIT 2 LIMIT 10", ConsistencyLevel.QUORUM, 10, 2);
            return;
        }

        assertLimits("k = 0 AND a = 1 AND b = 2 PER PARTITION LIMIT 2", ConsistencyLevel.QUORUM, 2, NO_LIMIT);
        assertLimits("k = 0 AND a = 1 AND b = 2 PER PARTITION LIMIT 2 LIMIT 10", ConsistencyLevel.ALL, 2, NO_LIMIT);
        assertLimits("k = 0 AND a = 1 AND b = 2 PER PARTITION LIMIT 5 LIMIT 3", ConsistencyLevel.QUORUM, 3, NO_LIMIT);
        assertLimits("k = 0 AND a = 1 AND b = 2 LIMIT 3", ConsistencyLevel.QUORUM, 3, NO_LIMIT);
        assertLimits("k = 0 AND a = 1 AND b = 2 PER PARTITION LIMIT 2", ConsistencyLevel.SERIAL, 2, NO_LIMIT);

        // not read as a single-partition range read
        assertLimits("k = 0 AND a = 1 AND b = 2 PER PARTITION LIMIT 2", ConsistencyLevel.ONE, NO_LIMIT, 2);
        assertLimits("k = 0 AND a = 1 PER PARTITION LIMIT 2", ConsistencyLevel.QUORUM, NO_LIMIT, 2);
        assertLimits("a = 1 AND b = 2 PER PARTITION LIMIT 2 LIMIT 10", ConsistencyLevel.QUORUM, 10, 2);

        // GROUP BY limits count groups
        ReadQuery groupBy = query("k = 0 AND a = 1 AND b = 2 GROUP BY k, c PER PARTITION LIMIT 2", options(ConsistencyLevel.QUORUM));
        assertThat(groupBy).isInstanceOf(PartitionRangeReadCommand.class);
        assertThat(groupBy.limits().isGroupByLimit()).isTrue();
        assertThat(groupBy.limits().perPartitionCount()).isEqualTo(2);

        // the page weight guardrail adds a byte limit, which is kept
        String warnThreshold = Guardrails.instance.getPageWeightWarnThreshold();
        String failThreshold = Guardrails.instance.getPageWeightFailThreshold();
        Guardrails.instance.setPageWeightThreshold(null, "1MiB");
        try
        {
            String table = keyspace + '.' + currentTable();
            SelectStatement select = (SelectStatement) parseStatement("SELECT * FROM " + table + " WHERE k = 0 AND a = 1 AND b = 2 PER PARTITION LIMIT 2 LIMIT 10");
            QueryOptions options = options(ConsistencyLevel.QUORUM);
            ReadQuery query = externalQuery(select, options);
            assertThat(query).isInstanceOf(PartitionRangeReadCommand.class);
            assertThat(query.limits().count()).isEqualTo(2);
            assertThat(query.limits().perPartitionCount()).isEqualTo(NO_LIMIT);
            assertThat(query.limits().bytes()).isEqualTo(1024 * 1024);
        }
        finally
        {
            Guardrails.instance.setPageWeightThreshold(warnThreshold, failThreshold);
        }
    }

    /**
     * Coordinators tell replicas with a custom message parameter that they filter the reconciled rows of non-strict
     * reads; replicas relax their filtering only for those coordinators.
     */
    @Test
    public void testNonStrictFilteringParam()
    {
        assertNonStrictFilteringParam("k = 0 AND a = 1 AND b = 2", ConsistencyLevel.QUORUM, nonStrict);
        assertNonStrictFilteringParam("a = 1 AND b = 2", ConsistencyLevel.ALL, nonStrict);
        assertNonStrictFilteringParam("k = 0 AND a = 1 AND b = 2", ConsistencyLevel.ONE, false);
        assertNonStrictFilteringParam("a = 1 AND b = 2", ConsistencyLevel.LOCAL_ONE, false);
        assertNonStrictFilteringParam("k = 0 AND a = 1", ConsistencyLevel.QUORUM, false);
        assertNonStrictFilteringParam("a = 1", ConsistencyLevel.QUORUM, false);
    }

    private void assertNonStrictFilteringParam(String where, ConsistencyLevel consistency, boolean expected)
    {
        ReadQuery query = query(where, options(consistency));
        ReadCommand command = query instanceof ReadCommand ? (ReadCommand) query : (ReadCommand) ((SinglePartitionReadQuery.Group<?>) query).queries.get(0);
        assertThat(command.rowFilter().isStrict()).isEqualTo(!expected);
        Map<String, byte[]> customParams = command.createMessage(false, Dispatcher.RequestTime.forImmediateExecution()).header.customParams();
        assertThat(customParams != null && customParams.containsKey(ReadCommand.NON_STRICT_FILTERING_PARAM)).isEqualTo(expected);
    }

    /**
     * Top-K queries are executed at ONE/LOCAL_ONE, so their row filters must not ask replicas to relax their filtering,
     * as their results are not filtered at the coordinator.
     */
    @Test
    public void testTopKQueries()
    {
        for (String where : new String[]{ "a = 1 AND b = 2", "k = 0 AND a = 1 AND b = 2" })
        {
            QueryOptions options = options(ConsistencyLevel.QUORUM);
            ReadQuery query = query(where + " ORDER BY v LIMIT 10", options);
            assertThat(query.isTopK()).isTrue();
            assertThat(options.getConsistency()).isEqualTo(ConsistencyLevel.ONE);
            assertThat(query.rowFilter().needsReconciliation()).isFalse();
            assertThat(query.rowFilter().isStrict()).isTrue();

            options = options(ConsistencyLevel.LOCAL_QUORUM);
            query = query(where + " ORDER BY v LIMIT 10", options);
            assertThat(options.getConsistency()).isEqualTo(ConsistencyLevel.LOCAL_ONE);
            assertThat(query.rowFilter().needsReconciliation()).isFalse();
        }

        // partition-restricted top-K queries can still be single-partition reads
        assertThat(query("k = 0 AND a = 1 AND b = 2 ORDER BY v LIMIT 10", options(ConsistencyLevel.QUORUM)))
        .isInstanceOf(SinglePartitionReadQuery.Group.class);
    }

    /**
     * Downgrading the consistency level of top-K queries doesn't build their row filter twice, which would repeat its
     * validations and guardrail warnings.
     */
    @Test
    public void testTopKQueriesBuildTheirRowFilterOnce()
    {
        int warnThreshold = Guardrails.instance.getQueryFiltersWarnThreshold();
        int failThreshold = Guardrails.instance.getQueryFiltersFailThreshold();
        Guardrails.instance.setQueryFiltersThreshold(1, -1);
        try
        {
            for (ConsistencyLevel consistency : new ConsistencyLevel[]{ ConsistencyLevel.ONE, ConsistencyLevel.QUORUM })
            {
                ClientWarn.instance.captureWarnings();
                try
                {
                    String table = keyspace + '.' + currentTable();
                    SelectStatement select = (SelectStatement) parseStatement("SELECT * FROM " + table + " WHERE a = 1 AND b = 2 ORDER BY v LIMIT 10");
                    QueryOptions options = options(consistency);
                    ReadQuery query = externalQuery(select, options);
                    assertThat(query.isTopK()).isTrue();
                    assertThat(query.rowFilter().needsReconciliation()).isFalse();

                    List<String> warnings = ClientWarn.instance.getWarnings();
                    assertThat(warnings).isNotNull();
                    assertThat(warnings.stream().filter(w -> w.contains("column value filters"))).hasSize(1);
                }
                finally
                {
                    ClientWarn.instance.resetWarnings();
                }
            }
        }
        finally
        {
            Guardrails.instance.setQueryFiltersThreshold(warnThreshold, failThreshold);
        }
    }

    /** A range read of the partition if the replicas may relax the filter, a single-partition read otherwise. */
    private void assertPartitionRestrictedRead(String where, ConsistencyLevel consistency)
    {
        if (nonStrict)
            assertSinglePartitionRangeRead(where, consistency);
        else
            assertSinglePartitionRead(where, consistency);
    }

    private void assertSinglePartitionRangeRead(String where, ConsistencyLevel consistency)
    {
        ReadQuery query = query(where, options(consistency));
        assertThat(query).isInstanceOf(PartitionRangeReadCommand.class);
        PartitionRangeReadCommand command = (PartitionRangeReadCommand) query;
        assertThat(command.rowFilter().isStrict()).isFalse();
        assertThat(command.isSinglePartition()).isTrue();
        assertThat(Int32Type.instance.compose(((DecoratedKey) command.dataRange().startKey()).getKey())).isZero();
    }

    private void assertLimits(String where, ConsistencyLevel consistency, int count, int perPartitionCount)
    {
        ReadQuery query = query(where, options(consistency));
        assertThat(query.limits().count()).isEqualTo(count);
        assertThat(query.limits().perPartitionCount()).isEqualTo(perPartitionCount);
        assertThat(query.limits().bytes()).isEqualTo(NO_LIMIT);
    }

    private void assertSinglePartitionRead(String where, ConsistencyLevel consistency)
    {
        ReadQuery query = query(where, options(consistency));
        assertThat(query).isInstanceOf(SinglePartitionReadQuery.Group.class);
        SinglePartitionReadQuery.Group<?> group = (SinglePartitionReadQuery.Group<?>) query;
        assertThat(group.queries).hasSize(1);
        assertThat(group.queries.get(0)).isInstanceOf(SinglePartitionReadCommand.class);
        assertThat(group.rowFilter().isStrict()).isTrue();
    }

    private static QueryOptions options(ConsistencyLevel consistency)
    {
        return QueryOptions.forInternalCalls(consistency, Collections.emptyList());
    }

    private ReadQuery query(String where, QueryOptions options)
    {
        String table = keyspace + '.' + currentTable();
        SelectStatement select = (SelectStatement) parseStatement("SELECT * FROM " + table + " WHERE " + where);
        return select.getQuery(options, FBUtilities.nowInSeconds());
    }

    private static ReadQuery externalQuery(SelectStatement select, QueryOptions options)
    {
        ClientState state = ClientState.forExternalCalls(new InetSocketAddress(InetAddress.getLoopbackAddress(), 9042));
        return select.getQuery(options,
                               state,
                               select.getSelection().newSelectors(options).getColumnFilter(),
                               FBUtilities.nowInSeconds(),
                               select.getLimit(options),
                               select.getPerPartitionLimit(options),
                               select.getOffset(options),
                               select.getAggregationSpec(options));
    }
}
