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
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A LIKE suffix or contains literal too repetitive for an automaton, on an indexed column in a disjunction with an
 * unindexed column, is filtered by its bytes: the replicas don't search its index, also when they look for matches in
 * unrepaired data for a non-strict read. A disjunction whose other restriction is an IN that the replicas search as
 * equalities to its elements, or as nothing without elements, searches the index, so the query is rejected.
 */
public class NonStrictLongLikeLiteralTest extends TestBaseImpl
{
    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CLUSTER = init(Cluster.build(2)
                              .withConfig(config -> SAIUtil.withNonStrictReplicaFiltering(config, true)
                                                           .with(GOSSIP).with(NETWORK))
                              .start());
    }

    @AfterClass
    public static void shutDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void testLongLiteralInDisjunctionWithUnindexedColumn()
    {
        String table = KEYSPACE + ".t";
        CLUSTER.schemaChange("CREATE TABLE " + table + " (k int PRIMARY KEY, v text, i int, x int, z int, f frozen<list<int>>)");
        CLUSTER.schemaChange("CREATE INDEX ON " + table + "(v) USING 'sai'");
        CLUSTER.schemaChange("CREATE INDEX ON " + table + "(i) USING 'sai'");
        CLUSTER.schemaChange("CREATE INDEX ON " + table + "(full(f)) USING 'sai'");
        SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, KEYSPACE);

        String repeated = "a".repeat(500);
        CLUSTER.coordinator(1).execute("INSERT INTO " + table + " (k, v, i, x, z) VALUES (1, ?, 1, 0, 1)", ConsistencyLevel.ALL, "x" + repeated + "x");
        CLUSTER.coordinator(1).execute("INSERT INTO " + table + " (k, v, i, x, z) VALUES (2, 'abc', 1, 0, 1)", ConsistencyLevel.ALL);
        String pattern = '%' + repeated + '%';

        // i = 2 has no match, so the replicas look for unrepaired matches of v
        String[][] queries = {
            { "(x = 1 OR v LIKE ?) AND i = 1", "[1]" },
            { "(x = 1 OR v LIKE ?) AND i = 2", "[]" },
            { "(x = 1 OR (z = 1 AND v LIKE ?)) AND i = 2", "[]" },
            { "(v LIKE ? OR (x = 1 AND z = 1)) AND i = 2", "[]" },
        };
        for (boolean flush : new boolean[]{ false, true })
        {
            if (flush)
                CLUSTER.forEach(node -> node.flush(KEYSPACE));
            for (String[] query : queries)
            {
                for (int coordinator = 1; coordinator <= 2; coordinator++)
                {
                    List<Object> keys = new ArrayList<>();
                    for (Object[] row : CLUSTER.coordinator(coordinator).execute("SELECT k FROM " + table + " WHERE " + query[0] + " ALLOW FILTERING",
                                                                                ConsistencyLevel.ALL, pattern))
                        keys.add(row[0]);
                    assertThat(keys.toString()).as("%s on node %d, flushed: %s", query[0], coordinator, flush).isEqualTo(query[1]);
                }
            }
            for (String where : new String[]{ "(x IN () OR v LIKE ?) AND i = 2", "(f IN ([1], [2]) OR v LIKE ?) AND i = 2" })
            {
                for (int coordinator = 1; coordinator <= 2; coordinator++)
                {
                    int node = coordinator;
                    assertThatThrownBy(() -> CLUSTER.coordinator(node).execute("SELECT k FROM " + table + " WHERE " + where + " ALLOW FILTERING",
                                                                               ConsistencyLevel.ALL, pattern))
                    .as("%s on node %d, flushed: %s", where, coordinator, flush)
                    .hasMessageContaining("Invalid pattern for LIKE '%<term>%' restriction on column v");
                }
            }
        }
    }
}
