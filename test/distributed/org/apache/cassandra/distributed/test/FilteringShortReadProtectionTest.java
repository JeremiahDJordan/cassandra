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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.test.sai.SAIUtil;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.sai.SAIUtil.toStrings;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Short read protection on filtering queries whose rows are shadowed by the tombstones of another replica, without an
 * index, with a legacy index and with SAI, with non-strict replica filtering disabled and enabled. When it is
 * enabled, the row filters of queries on two regular columns that need reconciliation aren't strict (see
 * RowFilter#isStrict), whatever the index, so short read protection grows its requests for all of them.
 */
@RunWith(Parameterized.class)
public class FilteringShortReadProtectionTest extends TestBaseImpl
{
    private static final String[] KINDS = { "none", "legacy", "sai" };

    private static Cluster CLUSTER;

    @Parameterized.Parameter
    public boolean nonStrict;

    @Parameterized.Parameters(name = "nonStrict={0}")
    public static Collection<Object[]> parameters()
    {
        return SAIUtil.nonStrictReplicaFilteringModes();
    }

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CLUSTER = init(Cluster.build(3).withConfig(config -> config.set("hinted_handoff_enabled", false).with(GOSSIP).with(NETWORK)).start());
    }

    @Before
    public void setUp()
    {
        SAIUtil.setNonStrictReplicaFiltering(CLUSTER, nonStrict);
        CLUSTER.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
        init(CLUSTER, 3);
    }

    @AfterClass
    public static void shutDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    /**
     * Partitions 0 and 1 have rows c = 0..11 with a = 1 and b = 2 on nodes 1 and 3. Node 2 only has the rows c = 0..7,
     * and newer deletions of them. So the live rows are c = 8..11, on nodes 1 and 3 only, which return the shadowed
     * rows first.
     */
    @Test
    public void testShadowedRows()
    {
        for (String kind : KINDS)
        {
            String table = "shadowed_" + kind;
            CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (k int, c int, a int, b int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
            if (kind.equals("legacy"))
                CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + "(a)"));
            if (kind.equals("sai"))
            {
                CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + "(a) USING 'sai'"));
                CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + "(b) USING 'sai'"));
            }
        }
        SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, KEYSPACE);

        List<String> mismatches = new ArrayList<>();
        for (String kind : KINDS)
        {
            String table = withKeyspace("%s.shadowed_" + kind);
            for (int node = 1; node <= 3; node++)
                for (int k = 0; k < 2; k++)
                    for (int c = 0; c < (node == 2 ? 8 : 12); c++)
                        CLUSTER.get(node).executeInternal("INSERT INTO " + table + " (k, c, a, b) VALUES (?, ?, 1, 2) USING TIMESTAMP 1", k, c);
            for (int k = 0; k < 2; k++)
                for (int c = 0; c < 8; c++)
                    CLUSTER.get(2).executeInternal("DELETE FROM " + table + " USING TIMESTAMP 2 WHERE k = ? AND c = ?", k, c);

            // the query, and its result at ALL
            String[][] queries = {
                { "SELECT c FROM " + table + " WHERE k = 0 AND a = 1 AND b = 2 PER PARTITION LIMIT 1 ALLOW FILTERING", "[[8]]" },
                { "SELECT c FROM " + table + " WHERE k = 0 AND a = 1 AND b = 2 PER PARTITION LIMIT 2 ALLOW FILTERING", "[[8], [9]]" },
                { "SELECT c FROM " + table + " WHERE k = 0 AND a = 1 AND b = 2 LIMIT 2 ALLOW FILTERING", "[[8], [9]]" },
                { "SELECT c FROM " + table + " WHERE k = 0 AND a = 1 AND b = 2 LIMIT 1 ALLOW FILTERING", "[[8]]" },
                { "SELECT c FROM " + table + " WHERE k = 0 AND a = 1 AND b = 2 ALLOW FILTERING", "[[8], [9], [10], [11]]" },
                { "SELECT c FROM " + table + " WHERE k = 0 AND a = 1 PER PARTITION LIMIT 2 ALLOW FILTERING", "[[8], [9]]" },
                { "SELECT c FROM " + table + " WHERE k = 0 PER PARTITION LIMIT 2", "[[8], [9]]" },
                { "SELECT k, c FROM " + table + " WHERE a = 1 AND b = 2 PER PARTITION LIMIT 2 ALLOW FILTERING", "[[1, 8], [1, 9], [0, 8], [0, 9]]" },
                { "SELECT k, c FROM " + table + " WHERE a = 1 AND b = 2 LIMIT 3 ALLOW FILTERING", "[[1, 8], [1, 9], [1, 10]]" },
                { "SELECT k, c FROM " + table + " WHERE a = 1 PER PARTITION LIMIT 2 ALLOW FILTERING", "[[1, 8], [1, 9], [0, 8], [0, 9]]" },
                { "SELECT k, c FROM " + table + " PER PARTITION LIMIT 2", "[[1, 8], [1, 9], [0, 8], [0, 9]]" },
            };
            for (String[] query : queries)
            {
                // QUORUM might not read node 2, so only ALL has a known result
                for (int coordinator : new int[]{ 1, 2 })
                {
                    ICoordinator node = CLUSTER.coordinator(coordinator);
                    String all = toStrings(node.execute(query[0], ConsistencyLevel.ALL)).toString();
                    check(query[0], "node " + coordinator, query[1], all, mismatches);
                    String paged = toStrings(node.executeWithPaging(query[0], ConsistencyLevel.ALL, 3)).toString();
                    check(query[0], "node " + coordinator + " page size 3", query[1], paged, mismatches);
                }
            }
        }
        assertThat(mismatches).isEmpty();
    }

    private static void check(String query, String how, String expected, String actual, List<String> mismatches)
    {
        if (!expected.equals(actual))
            mismatches.add(query + " at ALL on " + how + ": expected " + expected + " but got " + actual);
    }
}
