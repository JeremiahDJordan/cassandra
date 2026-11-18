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
import java.util.Arrays;
import java.util.List;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.StubMethod;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.sai.IndexContext;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Replicas relax the filtering of non-strict reads only if they have matches in memtables or unrepaired SSTables, which
 * they detect by searching each indexed expression there. Indexes with stale entries (rows written again after a
 * partition deletion, kept in SSTables until compaction) must not make them filter strictly. These tests emulate the
 * stale entries by making {@link IndexContext#removeRowData} do nothing.
 */
public class NonStrictStaleIndexEntriesTest extends TestBaseImpl
{
    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CLUSTER = init(Cluster.build(2)
                              .withInstanceInitializer(BBHelper::install)
                              .withConfig(config -> SAIUtil.withNonStrictReplicaFiltering(config, true)
                                                           .set("hinted_handoff_enabled", false)
                                                           .with(GOSSIP).with(NETWORK))
                              .start());
    }

    @AfterClass
    public static void shutDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    /**
     * Rows whose columns are split across the two replicas, written before with a = 1 and b = 2 and deleted with their
     * partition: the index of a keeps a = 1 for them, and that of b keeps b = 2.
     */
    @Test
    public void testNotEqualWithStaleEntries()
    {
        List<String> mismatches = new ArrayList<>();
        for (boolean flush : new boolean[]{ false, true })
        {
            for (boolean rewritten : new boolean[]{ false, true })
            {
                String table = KEYSPACE + ".stale_" + (flush ? "sstable" : "memtable") + (rewritten ? "_rewritten" : "");
                CLUSTER.schemaChange("CREATE TABLE " + table + " (k int PRIMARY KEY, a int, b int, u int) WITH read_repair = 'NONE'");
                CLUSTER.schemaChange("CREATE INDEX ON " + table + "(a) USING 'sai'");
                CLUSTER.schemaChange("CREATE INDEX ON " + table + "(b) USING 'sai'");
                SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, KEYSPACE);

                if (rewritten)
                {
                    for (int node = 1; node <= 2; node++)
                    {
                        for (int k = 0; k < 2; k++)
                        {
                            CLUSTER.get(node).executeInternal("INSERT INTO " + table + " (k, a, b, u) VALUES (?, 1, 2, 0) USING TIMESTAMP 1", k);
                            CLUSTER.get(node).executeInternal("DELETE FROM " + table + " USING TIMESTAMP 2 WHERE k = ?", k);
                        }
                    }
                }
                // row 0: a on node 1, b on node 2; row 1: a on node 1, the unindexed u on node 2
                CLUSTER.get(1).executeInternal("INSERT INTO " + table + " (k, a) VALUES (0, 5) USING TIMESTAMP 3");
                CLUSTER.get(2).executeInternal("INSERT INTO " + table + " (k, b) VALUES (0, 7) USING TIMESTAMP 4");
                CLUSTER.get(1).executeInternal("INSERT INTO " + table + " (k, a) VALUES (1, 5) USING TIMESTAMP 5");
                CLUSTER.get(2).executeInternal("INSERT INTO " + table + " (k, u) VALUES (1, 3) USING TIMESTAMP 6");
                if (flush)
                    CLUSTER.forEach(node -> node.flush(KEYSPACE));

                String[][] queries = {
                    { "a != 1 AND b != 2", "[[0, 5, 7, null]]" },
                    { "k = 0 AND a != 1 AND b != 2", "[[0, 5, 7, null]]" },
                    { "a > 1 AND b > 2", "[[0, 5, 7, null]]" },
                    { "a != 1 AND u = 3", "[[1, 5, null, 3]]" },
                    { "k = 1 AND a != 1 AND u = 3", "[[1, 5, null, 3]]" },
                    { "a NOT IN (1, 2) AND u = 3", "[[1, 5, null, 3]]" },
                    { "a != 1", "[[0, 5, 7, null], [1, 5, null, 3]]" },
                };
                for (String[] query : queries)
                {
                    String select = "SELECT k, a, b, u FROM " + table + " WHERE " + query[0] + " ALLOW FILTERING";
                    for (int coordinator = 1; coordinator <= 2; coordinator++)
                    {
                        List<String> rows = new ArrayList<>();
                        for (Object[] row : CLUSTER.coordinator(coordinator).execute(select, ConsistencyLevel.ALL))
                            rows.add(Arrays.toString(row));
                        rows.sort(String::compareTo);
                        if (!rows.toString().equals(query[1]))
                            mismatches.add(select + " on node " + coordinator + ": expected " + query[1] + " but got " + rows);
                    }
                }
            }
        }
        assertThat(mismatches).isEmpty();
    }

    /**
     * Makes {@link IndexContext#removeRowData} do nothing, so that the memtable indexes keep the values of the rows
     * that a partition deletion removes.
     */
    public static class BBHelper
    {
        @SuppressWarnings("resource")
        public static void install(ClassLoader classLoader, Integer node)
        {
            new ByteBuddy().rebase(IndexContext.class)
                           .method(named("removeRowData"))
                           .intercept(StubMethod.INSTANCE)
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }
    }
}
