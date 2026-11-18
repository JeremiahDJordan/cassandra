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

package org.apache.cassandra.distributed.test.sai;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.QueryInfoTracker;
import org.apache.cassandra.service.reads.AbstractReadExecutor;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the reads that the coordinator sends for partition-restricted SAI queries that restrict two indexed regular
 * columns at QUORUM with RF 3, with non-strict replica filtering (CASSANDRA-19018) disabled, its default, and enabled.
 * <p>
 * Disabled, these queries are single-partition reads, with data from one replica and a digest from the other, a read
 * executor that can speculate, and the index skipped for full primary keys. Enabled, a read of a single partition is a
 * range read, reads of several partitions get data from every replica they contact, and full primary keys use the
 * index, all with the parameter that lets the replicas relax their filtering.
 * <p>
 * The coordinator, node 1, isn't a replica of the queried partitions, so that all the replica reads are messages. The
 * table's speculative retry threshold is longer than the read timeout, so that no read actually speculates.
 */
public class NonStrictReadPathTest extends TestBaseImpl
{
    private static final String RF3_KEYSPACE = "rf3_keyspace";
    private static final String TABLE = RF3_KEYSPACE + ".t";
    private static final String NESTED = "((a = 1 AND b = 2) OR (a = 7 AND b = 7))";

    private static Cluster CLUSTER;

    /** Two partitions that node 1 doesn't replicate */
    private static int key0, key1;

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CLUSTER = init(Cluster.build(4).withConfig(config -> config.set("hinted_handoff_enabled", false).with(GOSSIP).with(NETWORK)).start());
        CLUSTER.schemaChange("CREATE KEYSPACE " + RF3_KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
        CLUSTER.schemaChange("CREATE TABLE " + TABLE + " (k int, c int, a int, b int, PRIMARY KEY (k, c)) " +
                             "WITH read_repair = 'NONE' AND speculative_retry = '60000ms'");
        CLUSTER.schemaChange("CREATE INDEX ON " + TABLE + "(a) USING 'sai'");
        CLUSTER.schemaChange("CREATE INDEX ON " + TABLE + "(b) USING 'sai'");
        SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, RF3_KEYSPACE);

        List<Integer> keys = new ArrayList<>();
        String coordinator = CLUSTER.get(1).broadcastAddress().getAddress().getHostAddress();
        for (int k = 0; keys.size() < 2; k++)
        {
            if (!SAIUtil.endpoints(CLUSTER, RF3_KEYSPACE, "t", k).contains(coordinator))
                keys.add(k);
        }
        key0 = keys.get(0);
        key1 = keys.get(1);

        for (int k : keys)
        {
            CLUSTER.coordinator(1).execute("INSERT INTO " + TABLE + " (k, c, a, b) VALUES (?, 0, 1, 5)", ConsistencyLevel.ALL, k);
            CLUSTER.coordinator(1).execute("INSERT INTO " + TABLE + " (k, c, a, b) VALUES (?, 1, 1, 2)", ConsistencyLevel.ALL, k);
            CLUSTER.coordinator(1).execute("INSERT INTO " + TABLE + " (k, c, a, b) VALUES (?, 2, 3, 2)", ConsistencyLevel.ALL, k);
        }
    }

    @AfterClass
    public static void shutDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Before
    public void disableNonStrictFiltering()
    {
        SAIUtil.setNonStrictReplicaFiltering(CLUSTER.get(1), false);
    }

    @Test
    public void testSinglePartition()
    {
        String query = "SELECT k, c FROM " + TABLE + " WHERE k = " + key0 + " AND a = 1 AND b = 2";

        assertThat(reads(query, row(key0, 1))).containsExactly("READ_REQ data index", "READ_REQ digest index");
        assertThat(executors(query)).containsExactly("SpeculatingReadExecutor");

        SAIUtil.setNonStrictReplicaFiltering(CLUSTER.get(1), true);
        assertThat(reads(query, row(key0, 1))).containsExactly("RANGE_REQ data index nonstrict", "RANGE_REQ data index nonstrict");
        assertThat(executors(query)).containsExactly("range read");
    }

    @Test
    public void testSeveralPartitions()
    {
        String query = "SELECT k, c FROM " + TABLE + " WHERE k IN (" + key0 + ", " + key1 + ") AND " + NESTED;
        Object[][] rows = key0 < key1 ? new Object[][]{ row(key0, 1), row(key1, 1) } : new Object[][]{ row(key1, 1), row(key0, 1) };

        assertThat(reads(query, rows)).containsExactly("READ_REQ data index", "READ_REQ data index",
                                                       "READ_REQ digest index", "READ_REQ digest index");
        assertThat(executors(query)).containsExactly("SpeculatingReadExecutor", "SpeculatingReadExecutor");

        SAIUtil.setNonStrictReplicaFiltering(CLUSTER.get(1), true);
        assertThat(reads(query, rows)).containsExactly("READ_REQ data index nonstrict", "READ_REQ data index nonstrict",
                                                       "READ_REQ data index nonstrict", "READ_REQ data index nonstrict");
        assertThat(executors(query)).containsExactly("SpeculatingReadExecutor", "SpeculatingReadExecutor");
    }

    @Test
    public void testFullPrimaryKey()
    {
        String query = "SELECT k, c FROM " + TABLE + " WHERE k = " + key0 + " AND c IN (0, 1) AND a = 1 AND b = 2";

        assertThat(reads(query, row(key0, 1))).containsExactly("READ_REQ data", "READ_REQ digest");
        assertThat(executors(query)).containsExactly("SpeculatingReadExecutor");

        SAIUtil.setNonStrictReplicaFiltering(CLUSTER.get(1), true);
        assertThat(reads(query, row(key0, 1))).containsExactly("RANGE_REQ data index nonstrict", "RANGE_REQ data index nonstrict");
        assertThat(executors(query)).containsExactly("range read");
    }

    /**
     * Each page takes the setting when it starts, and switching between the single-partition read and the range read of
     * a partition keeps paging through it.
     */
    @Test
    public void testSettingChangedBetweenPages()
    {
        String table = RF3_KEYSPACE + ".paged";
        CLUSTER.schemaChange("CREATE TABLE " + table + " (k int, c int, a int, b int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'");
        CLUSTER.schemaChange("CREATE INDEX ON " + table + "(a) USING 'sai'");
        CLUSTER.schemaChange("CREATE INDEX ON " + table + "(b) USING 'sai'");
        SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, RF3_KEYSPACE);
        List<Object[]> expected = new ArrayList<>();
        for (int c = 0; c < 10; c++)
        {
            CLUSTER.coordinator(1).execute("INSERT INTO " + table + " (k, c, a, b) VALUES (?, ?, 1, 2)", ConsistencyLevel.ALL, key0, c);
            CLUSTER.coordinator(1).execute("INSERT INTO " + table + " (k, c, a, b) VALUES (?, ?, 1, 2)", ConsistencyLevel.ALL, key1, c);
            expected.add(row(key0, c));
        }

        String query = "SELECT k, c FROM " + table + " WHERE k = " + key0 + " AND a = 1 AND b = 2";
        for (boolean nonStrictFirst : new boolean[]{ false, true })
        {
            for (int pageSize : new int[]{ 1, 3, 4 })
            {
                SAIUtil.setNonStrictReplicaFiltering(CLUSTER.get(1), nonStrictFirst);
                Iterator<Object[]> pages = CLUSTER.coordinator(1).executeWithPaging(query, ConsistencyLevel.QUORUM, pageSize);
                List<Object[]> rows = new ArrayList<>();
                while (pages.hasNext())
                {
                    rows.add(pages.next());
                    // the next page is read by the next call to hasNext()
                    if (rows.size() % pageSize == 0)
                        SAIUtil.setNonStrictReplicaFiltering(CLUSTER.get(1), !getNonStrictFiltering());
                }
                assertRows(rows.toArray(new Object[0][]), expected.toArray(new Object[0][]));
            }
        }
    }

    private static boolean getNonStrictFiltering()
    {
        return CLUSTER.get(1).callOnInstance(DatabaseDescriptor::getSAINonStrictReplicaFilteringEnabled);
    }

    /**
     * Executes the query at QUORUM on node 1, checks its rows, and returns the sorted reads that node 1 sent, as
     * "VERB data|digest [index] [nonstrict]".
     */
    private static List<String> reads(String query, Object[]... expectedRows)
    {
        List<String> reads = new CopyOnWriteArrayList<>();
        IMessageFilters.Filter filter = CLUSTER.filters()
                                               .outbound()
                                               .verbs(Verb.READ_REQ.id, Verb.RANGE_REQ.id)
                                               .from(1)
                                               .messagesMatching((from, to, message) -> {
                                                   reads.add(CLUSTER.get(1).callsOnInstance((IIsolatedExecutor.SerializableCallable<String>) () -> describe(Instance.deserializeMessage(message))).call());
                                                   return false;
                                               })
                                               .drop();
        try
        {
            assertRows(CLUSTER.coordinator(1).execute(query, ConsistencyLevel.QUORUM), expectedRows);
        }
        finally
        {
            filter.off();
        }
        List<String> sorted = new ArrayList<>(reads);
        Collections.sort(sorted);
        return sorted;
    }

    @SuppressWarnings("unchecked")
    private static String describe(Message<?> message)
    {
        ReadCommand command = ((Message<ReadCommand>) message).payload;
        Map<String, byte[]> params = message.header.customParams();
        return message.verb() +
               (command.isDigestQuery() ? " digest" : " data") +
               (command.indexQueryPlan() != null ? " index" : "") +
               (params != null && params.containsKey(ReadCommand.NON_STRICT_FILTERING_PARAM) ? " nonstrict" : "");
    }

    /**
     * @return the read executor that node 1 uses at QUORUM for each single-partition read of the query, or
     * "range read"
     */
    private static List<String> executors(String query)
    {
        return CLUSTER.get(1).callOnInstance(() -> {
            SelectStatement statement = (SelectStatement) QueryProcessor.prepareInternal(query).statement;
            QueryOptions options = QueryOptions.forInternalCalls(org.apache.cassandra.db.ConsistencyLevel.QUORUM, Collections.emptyList());
            ReadQuery read = statement.getQuery(options, FBUtilities.nowInSeconds());
            List<String> executors = new ArrayList<>();
            if (!(read instanceof SinglePartitionReadQuery.Group))
            {
                executors.add("range read");
                return executors;
            }
            for (SinglePartitionReadQuery partitionRead : ((SinglePartitionReadQuery.Group<?>) read).queries)
            {
                AbstractReadExecutor executor = AbstractReadExecutor.getReadExecutor((SinglePartitionReadCommand) partitionRead,
                                                                                     org.apache.cassandra.db.ConsistencyLevel.QUORUM,
                                                                                     Dispatcher.RequestTime.forImmediateExecution(),
                                                                                     QueryInfoTracker.ReadTracker.NOOP);
                executors.add(executor.getClass().getSimpleName());
            }
            return executors;
        });
    }
}
