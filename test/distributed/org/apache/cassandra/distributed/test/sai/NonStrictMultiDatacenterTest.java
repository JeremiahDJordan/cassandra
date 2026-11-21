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

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.distributed.shared.NetworkTopology.networkTopology;
import static org.apache.cassandra.distributed.test.sai.SAIUtil.check;
import static org.apache.cassandra.distributed.test.sai.SAIUtil.toStrings;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Non-strict SAI reads in two datacenters. The row filter of a read isn't strict when its consistency level needs
 * reconciliation and the keyspace has more than one replica (see RowFilter#isStrict), so the replicas return candidate
 * rows that the coordinator filters. LOCAL_QUORUM in a datacenter with a single replica reads that replica only, whose
 * candidate rows stop at the limit or the page size: short read protection must ask it for more rows although there
 * is nothing to reconcile.
 */
public class NonStrictMultiDatacenterTest extends TestBaseImpl
{
    private static final String DC1 = "dc1";
    private static final String DC2 = "dc2";

    /** The replication factors in dc1 and dc2 */
    private static final int[][] REPLICATION = { { 1, 1 }, { 2, 1 }, { 3, 1 } };

    /** A coordinator in each datacenter */
    private static final int[] COORDINATORS = { 1, 4 };

    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CLUSTER = Cluster.build(5)
                         .withNodeIdTopology(networkTopology(5, node -> node <= 3 ? dcAndRack(DC1, "rack1") : dcAndRack(DC2, "rack1")))
                         .withConfig(config -> SAIUtil.withNonStrictReplicaFiltering(config, true)
                                                      .set("hinted_handoff_enabled", false)
                                                      .with(GOSSIP).with(NETWORK))
                         .start();
    }

    @AfterClass
    public static void shutDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    /**
     * The same rows on every replica, 10 per partition, 2 of which match "a = 1 AND b = 2", after 6 rows that only
     * match "a = 1". The table without indexes, whose replicas filter strictly, gives the expected rows, also at
     * LOCAL_QUORUM in a datacenter with one replica, with a limit or a page size.
     */
    @Test
    public void testLimitsAndPagingAtEveryConsistencyLevel()
    {
        int[][] rows = { { 0, 1, 9 }, { 1, 1, 9 }, { 2, 1, 9 }, { 3, 1, 9 }, { 4, 1, 9 }, { 5, 1, 9 }, { 6, 1, 2 }, { 7, 3, 9 }, { 8, 9, 2 }, { 9, 1, 2 } };
        for (int[] replication : REPLICATION)
        {
            String keyspace = keyspace(replication);
            CLUSTER.schemaChange(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', '%s': %d, '%s': %d}",
                                               keyspace, DC1, replication[0], DC2, replication[1]));
            for (String table : new String[]{ "indexed", "reference" })
                CLUSTER.schemaChange("CREATE TABLE " + keyspace + '.' + table + " (k int, c int, a int, b int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'");
            CLUSTER.schemaChange("CREATE INDEX ON " + keyspace + ".indexed(a) USING 'sai'");
            CLUSTER.schemaChange("CREATE INDEX ON " + keyspace + ".indexed(b) USING 'sai'");
            SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, keyspace);

            for (String table : new String[]{ "indexed", "reference" })
                for (int k = 0; k < 4; k++)
                    for (int[] row : rows)
                        CLUSTER.coordinator(1).execute("INSERT INTO " + keyspace + '.' + table + " (k, c, a, b) VALUES (?, ?, ?, ?)", ConsistencyLevel.ALL, k, row[0], row[1], row[2]);
        }

        String[] filters = {
            "a = 1 AND b = 2",
            "k = 0 AND a = 1 AND b = 2",
            "k = 0 AND ((a = 1 AND b = 2) OR a = 3)",
            "k IN (0, 1) AND ((a = 1 AND b = 2) OR a = 3)",
            "k IN (1, 3, 0) AND ((a = 1 AND b = 2) OR (a = 3 AND b = 9))",
            "(a = 1 AND b = 2) OR a = 3",
        };
        String[] limits = { "", " LIMIT 1", " LIMIT 2", " PER PARTITION LIMIT 1", " PER PARTITION LIMIT 2 LIMIT 3" };
        ConsistencyLevel[] levels = { ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.QUORUM };

        List<String> mismatches = new ArrayList<>();
        for (boolean flush : new boolean[]{ false, true })
        {
            if (flush)
                for (int[] replication : REPLICATION)
                    CLUSTER.forEach(node -> node.flush(keyspace(replication)));

            for (int[] replication : REPLICATION)
            {
                String keyspace = keyspace(replication);
                for (String filter : filters)
                {
                    for (String limit : limits)
                    {
                        String where = " WHERE " + filter + limit + " ALLOW FILTERING";
                        String select = "SELECT k, c, a, b FROM " + keyspace + ".indexed" + where;
                        String reference = "SELECT k, c, a, b FROM " + keyspace + ".reference" + where;
                        for (ConsistencyLevel cl : levels)
                        {
                            for (int coordinator : COORDINATORS)
                            {
                                String context = select + (flush ? " after flush" : "") + " at " + cl + " from " + (coordinator <= 3 ? DC1 : DC2);
                                List<String> expected = toStrings(CLUSTER.coordinator(coordinator).execute(reference, cl));
                                check(context, expected, () -> toStrings(CLUSTER.coordinator(coordinator).execute(select, cl)), mismatches);
                                for (int pageSize : new int[]{ 1, 2 })
                                    check(context + " page size " + pageSize, expected,
                                          () -> toStrings(CLUSTER.coordinator(coordinator).executeWithPaging(select, cl, pageSize)), mismatches);
                            }
                        }
                    }
                }
            }
        }
        assertThat(mismatches).isEmpty();
    }

    /**
     * Filters on a static column and another column, and aggregates, at LOCAL_QUORUM in a datacenter with one replica.
     * The aggregates page internally by 1 KiB, and every row is larger, so a candidate row that doesn't match can fill
     * an internal page of the aggregate over all partitions, which then needs short read protection.
     */
    @Test
    public void testStaticsAndAggregates()
    {
        String keyspace = "statics_rf_1_1";
        CLUSTER.schemaChange(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', '%s': 1, '%s': 1}",
                                           keyspace, DC1, DC2));
        for (String table : new String[]{ "indexed", "reference" })
            CLUSTER.schemaChange("CREATE TABLE " + keyspace + '.' + table + " (k int, c int, s int static, a int, b int, p text, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'");
        for (String column : new String[]{ "s", "a", "b" })
            CLUSTER.schemaChange("CREATE INDEX ON " + keyspace + ".indexed(" + column + ") USING 'sai'");
        SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, keyspace);

        // s = 1 in partitions 1 and 3, a = 1 on rows 8 and 9, b = 2 on the even rows
        String payload = "x".repeat(2048);
        for (String table : new String[]{ "indexed", "reference" })
            for (int k = 0; k < 4; k++)
            {
                CLUSTER.coordinator(1).execute("INSERT INTO " + keyspace + '.' + table + " (k, s) VALUES (?, ?)", ConsistencyLevel.ALL, k, k % 2);
                for (int c = 0; c < 10; c++)
                    CLUSTER.coordinator(1).execute("INSERT INTO " + keyspace + '.' + table + " (k, c, a, b, p) VALUES (?, ?, ?, ?, ?)", ConsistencyLevel.ALL,
                                                   k, c, c >= 8 ? 1 : 0, c % 2 == 0 ? 2 : 0, payload);
            }
        CLUSTER.forEach(node -> node.runOnInstance(() -> DatabaseDescriptor.getRawConfig().aggregation_subpage_size = new DataStorageSpec.IntKibibytesBound(1)));

        String[] queries = {
            "SELECT k, c, s FROM %s WHERE s = 1 AND a = 1 ALLOW FILTERING",
            "SELECT k, c, s FROM %s WHERE s = 1 AND a = 1 LIMIT 2 ALLOW FILTERING",
            "SELECT count(*) FROM %s WHERE a = 1 AND b = 2 ALLOW FILTERING",
            "SELECT k, count(*) FROM %s WHERE k = 1 AND a = 1 AND b = 2 ALLOW FILTERING",
        };

        List<String> mismatches = new ArrayList<>();
        for (String query : queries)
        {
            String select = String.format(query, keyspace + ".indexed");
            String reference = String.format(query, keyspace + ".reference");
            for (int coordinator : COORDINATORS)
            {
                String context = select + " at LOCAL_QUORUM from " + (coordinator <= 3 ? DC1 : DC2);
                List<String> expected = toStrings(CLUSTER.coordinator(coordinator).execute(reference, ConsistencyLevel.LOCAL_QUORUM));
                assertThat(expected).as(reference).isNotEmpty();
                long shortReadRequests = shortReadProtectionRequests(coordinator, keyspace);
                check(context, expected, () -> toStrings(CLUSTER.coordinator(coordinator).execute(select, ConsistencyLevel.LOCAL_QUORUM)), mismatches);
                if (query.startsWith("SELECT count(*)") && shortReadProtectionRequests(coordinator, keyspace) == shortReadRequests)
                    mismatches.add(context + ": no short read protection request");
                for (int pageSize : new int[]{ 1, 2 })
                    check(context + " page size " + pageSize, expected,
                          () -> toStrings(CLUSTER.coordinator(coordinator).executeWithPaging(select, ConsistencyLevel.LOCAL_QUORUM, pageSize)), mismatches);
            }
        }
        assertThat(mismatches).isEmpty();
    }

    private static long shortReadProtectionRequests(int node, String keyspace)
    {
        return CLUSTER.get(node).callOnInstance(() -> Keyspace.open(keyspace).getColumnFamilyStore("indexed").metric.shortReadProtectionRequests.getCount());
    }

    private static String keyspace(int[] replication)
    {
        return "rf_" + replication[0] + '_' + replication[1];
    }
}
