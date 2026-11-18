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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.sai.SAIUtil.toStrings;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Short read protection on SAI queries whose replicas return candidate rows, because they need reconciliation and
 * intersect several mutable columns (CASSANDRA-19018). The coordinator filters out most of the candidate rows, so
 * short read protection asks the replicas for more, in growing requests. The data is the same on all the replicas,
 * so every consistency level must return the rows that ONE returns.
 */
public class NonStrictShortReadProtectionTest extends TestBaseImpl
{
    private static final ConsistencyLevel[] CONSISTENCY_LEVELS = { ConsistencyLevel.QUORUM, ConsistencyLevel.ALL };
    private static final int[] PAGE_SIZES = { 1, 2, 3, 5, 100 };

    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CLUSTER = init(Cluster.build(3).withConfig(config -> SAIUtil.withNonStrictReplicaFiltering(config, true)
                                                                    .set("hinted_handoff_enabled", false)
                                                                    .with(GOSSIP).with(NETWORK)).start());
    }

    @AfterClass
    public static void shutDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    /**
     * Partitions whose first candidate rows are filtered out at the coordinator, with per-partition limits. Row-level
     * short read protection requests keep the per-partition limit, so a replica never returns more rows than that.
     */
    @Test
    public void testPerPartitionLimit()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.ppl (k int, c int, a int, b int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.ppl(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.ppl(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, KEYSPACE);

        // a = 1 on all the rows, b = 2 only on the rows with c >= 10
        for (int k = 0; k < 4; k++)
            for (int c = 0; c < 20; c++)
                CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.ppl (k, c, a, b) VALUES (?, ?, 1, ?)"), ConsistencyLevel.ALL, k, c, c >= 10 ? 2 : 3);

        String[] queries = {
            "SELECT k, c FROM %s.ppl WHERE k = 1 AND a = 1 AND b = 2 PER PARTITION LIMIT 1",
            "SELECT k, c FROM %s.ppl WHERE k = 1 AND a = 1 AND b = 2 PER PARTITION LIMIT 2",
            "SELECT k, c FROM %s.ppl WHERE k = 1 AND a = 1 AND b = 2 PER PARTITION LIMIT 3",
            "SELECT k, c FROM %s.ppl WHERE k = 1 AND a = 1 AND b = 2 PER PARTITION LIMIT 2 LIMIT 10",
            "SELECT k, c FROM %s.ppl WHERE k = 1 AND a = 1 AND b = 2 PER PARTITION LIMIT 2 LIMIT 2",
            "SELECT k, c FROM %s.ppl WHERE k = 1 AND a = 1 AND b = 2 LIMIT 2",
            "SELECT k, c FROM %s.ppl WHERE k = 1 AND a = 1 AND b = 2 LIMIT 1",
            "SELECT k, c FROM %s.ppl WHERE a = 1 AND b = 2 PER PARTITION LIMIT 1",
            "SELECT k, c FROM %s.ppl WHERE a = 1 AND b = 2 PER PARTITION LIMIT 2",
            "SELECT k, c FROM %s.ppl WHERE a = 1 AND b = 2 PER PARTITION LIMIT 2 LIMIT 5",
            "SELECT k, c FROM %s.ppl WHERE a = 1 AND b = 2 PER PARTITION LIMIT 3 LIMIT 100",
            "SELECT k, c FROM %s.ppl WHERE a = 1 AND b = 2 LIMIT 3",
            "SELECT k, c FROM %s.ppl WHERE a = 1 AND b = 2 LIMIT 15",
            "SELECT k, c FROM %s.ppl WHERE a = 1 AND b = 2",
            "SELECT k, count(*) FROM %s.ppl WHERE a = 1 AND b = 2 GROUP BY k",
            "SELECT k, count(*) FROM %s.ppl WHERE a = 1 AND b = 2 GROUP BY k LIMIT 2",
            "SELECT k, c FROM %s.ppl WHERE a = 1 AND b = 2 GROUP BY k, c PER PARTITION LIMIT 2",
        };

        List<String> mismatches = new ArrayList<>();
        for (boolean flush : new boolean[]{ false, true })
        {
            if (flush)
                CLUSTER.forEach(i -> i.flush(KEYSPACE));

            for (String query : queries)
                checkAllConsistencyLevels(query, flush, mismatches);
        }
        assertThat(mismatches).isEmpty();
    }

    /**
     * Partitions without clustering columns, with many partitions between the matches.
     */
    @Test
    public void testSkinnyPartitions()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.skinny (k int PRIMARY KEY, a int, b int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.skinny(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.skinny(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, KEYSPACE);

        // a = 1 on all the rows, b = 2 on 5 of them
        for (int k = 0; k < 300; k++)
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.skinny (k, a, b) VALUES (?, 1, ?)"), ConsistencyLevel.ALL, k, k % 60 == 7 ? 2 : 3);

        String[] queries = {
            "SELECT k FROM %s.skinny WHERE a = 1 AND b = 2",
            "SELECT k FROM %s.skinny WHERE a = 1 AND b = 2 LIMIT 1",
            "SELECT k FROM %s.skinny WHERE a = 1 AND b = 2 LIMIT 3",
            "SELECT k FROM %s.skinny WHERE a = 1 AND b = 2 LIMIT 100",
            "SELECT k FROM %s.skinny WHERE a = 1 AND b = 2 PER PARTITION LIMIT 1",
            "SELECT k FROM %s.skinny WHERE a = 1 AND b = 2 PER PARTITION LIMIT 1 LIMIT 3",
            "SELECT count(*) FROM %s.skinny WHERE a = 1 AND b = 2",
        };

        List<String> mismatches = new ArrayList<>();
        for (String query : queries)
            checkAllConsistencyLevels(query, false, mismatches);
        assertThat(mismatches).isEmpty();
    }

    /**
     * With the page weight guardrail, reads have a byte limit, which short read protection requests keep, so replicas
     * can return fewer rows than requested without having reached the end of the partition. Short read protection must
     * then ask for more rows instead of ending the partition. Guardrails don't apply to the internal client state of
     * the in-JVM coordinator API, so the queries run as an anonymous external client.
     */
    @Test
    public void testBytesLimit()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.bytes_limit (k int, c int, a int, b int, v text, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.bytes_limit(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.bytes_limit(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, KEYSPACE);

        // rows of about 1 KiB, a = 1 on all of them, b = 2 only on the rows with c >= 150
        char[] chars = new char[1000];
        Arrays.fill(chars, 'x');
        String value = new String(chars);
        for (int c = 0; c < 200; c++)
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.bytes_limit (k, c, a, b, v) VALUES (0, ?, 1, ?, ?)"), ConsistencyLevel.ALL, c, c >= 150 ? 2 : 3, value);

        // reads of the coordinator are limited to about 10 rows
        CLUSTER.get(1).runOnInstance(() -> Guardrails.instance.setPageWeightThreshold(null, "10KiB"));
        try
        {
            String[] queries = {
                "SELECT k, c, v FROM %s.bytes_limit WHERE k = 0 AND a = 1 AND b = 2 LIMIT 5",
                "SELECT k, c, v FROM %s.bytes_limit WHERE k = 0 AND a = 1 AND b = 2 PER PARTITION LIMIT 5",
                "SELECT k, c, v FROM %s.bytes_limit WHERE a = 1 AND b = 2 LIMIT 5",
                "SELECT k, c, v FROM %s.bytes_limit WHERE a = 1 AND b = 2 PER PARTITION LIMIT 5",
            };
            // the byte limit is in effect: a read without a limit stops after about 10 rows
            assertThat(executeAsExternalClient(withKeyspace("SELECT k, c, v FROM %s.bytes_limit WHERE k = 0"), ConsistencyLevel.ONE, 0)).hasSizeBetween(1, 20);

            List<String> expected = new ArrayList<>();
            for (int c = 150; c < 155; c++)
                expected.add(Arrays.toString(new Object[]{ 0, c }));
            List<String> mismatches = new ArrayList<>();
            for (String query : queries)
            {
                String select = withKeyspace(query);
                for (ConsistencyLevel cl : new ConsistencyLevel[]{ ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL })
                {
                    check(select, cl.name(), expected, executeAsExternalClient(select, cl, 0), mismatches);
                    for (int pageSize : PAGE_SIZES)
                        check(select, cl + " page size " + pageSize, expected, executeAsExternalClient(select, cl, pageSize), mismatches);
                }
            }
            assertThat(mismatches).isEmpty();
        }
        finally
        {
            CLUSTER.get(1).runOnInstance(() -> Guardrails.instance.setPageWeightThreshold(null, null));
        }
    }

    /**
     * Executes a query whose first two selected columns are ints on node 1, as an anonymous external client, to which
     * guardrails apply.
     *
     * @param pageSize the page size in rows, or 0 to read all the rows at once
     * @return the rows
     */
    private static List<String> executeAsExternalClient(String query, ConsistencyLevel consistencyLevel, int pageSize)
    {
        String consistency = consistencyLevel.name();
        return CLUSTER.get(1).callOnInstance(() -> {
            ClientState clientState = ClientState.forExternalCalls(new InetSocketAddress(FBUtilities.getJustLocalAddress(), 9042));
            CQLStatement statement = QueryProcessor.getStatement(query, clientState);
            statement.validate(clientState);
            List<String> rows = new ArrayList<>();
            PagingState pagingState = null;
            do
            {
                QueryOptions options = QueryOptions.create(org.apache.cassandra.db.ConsistencyLevel.valueOf(consistency),
                                                           Collections.emptyList(),
                                                           false,
                                                           pageSize > 0 ? PageSize.inRows(pageSize) : PageSize.NONE,
                                                           pagingState,
                                                           org.apache.cassandra.db.ConsistencyLevel.SERIAL,
                                                           ProtocolVersion.CURRENT,
                                                           null);
                ResultMessage.Rows result = (ResultMessage.Rows) statement.execute(new QueryState(clientState), options, Dispatcher.RequestTime.forImmediateExecution());
                for (List<ByteBuffer> row : result.result.rows)
                    rows.add(Arrays.toString(new Object[]{ Int32Type.instance.compose(row.get(0)), Int32Type.instance.compose(row.get(1)) }));
                pagingState = result.result.metadata.getPagingState();
            }
            while (pageSize > 0 && pagingState != null);
            return rows;
        });
    }

    /**
     * Runs the query at QUORUM and ALL, unpaged and paged, and records the results that differ from the result at ONE.
     */
    private static void checkAllConsistencyLevels(String query, boolean flushed, List<String> mismatches)
    {
        String select = withKeyspace(query);
        List<String> expected = toStrings(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ONE));
        String name = query + (flushed ? " after flush" : "");
        for (ConsistencyLevel cl : CONSISTENCY_LEVELS)
        {
            check(name, cl.name(), expected, toStrings(CLUSTER.coordinator(1).execute(select, cl)), mismatches);
            for (int pageSize : PAGE_SIZES)
                check(name, cl + " page size " + pageSize, expected, toStrings(CLUSTER.coordinator(1).executeWithPaging(select, cl, pageSize)), mismatches);
        }
    }

    private static void check(String query, String how, List<String> expected, List<String> actual, List<String> mismatches)
    {
        if (!expected.equals(actual))
            mismatches.add(query + " at " + how + ": expected " + expected + " but got " + actual);
    }
}
