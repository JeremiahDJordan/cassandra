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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.test.sai.SAIUtil.check;
import static org.apache.cassandra.distributed.test.sai.SAIUtil.toStrings;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests SAI queries whose row filter replicas can't strictly apply, because they need reconciliation and intersect
 * several mutable columns (CASSANDRA-19018). If they are partition-restricted, the coordinator reads them as range
 * reads on a single partition, which must behave as the single-partition reads they would otherwise be: paging stays
 * in the partition, and read repair is honored. The coordinator also filters their candidate rows, which must evaluate
 * the original Boolean tree, including disjunctions of static and regular columns. Reads of several partitions stay
 * single-partition reads, which read data from all the replicas and filter it at the coordinator instead of comparing
 * digests.
 */
public class NonStrictPartitionReadTest extends TestBaseImpl
{
    private static final ConsistencyLevel[] CONSISTENCY_LEVELS = { ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL };

    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        // 3 nodes with RF=3, so that QUORUM reads don't contact all the replicas
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
     * Partition-restricted queries that the replicas can't strictly filter are range reads on a single partition.
     * Paging them must never leave that partition, even when the page ends after the last row allowed in it.
     */
    @Test
    public void testPartitionRestrictedPaging()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partition_restricted_ppl (k int, c int, a int, b int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partition_restricted_ppl(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partition_restricted_ppl(b) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partition_restricted_skinny (k int PRIMARY KEY, a int, b int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partition_restricted_skinny(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partition_restricted_skinny(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        for (int k = 0; k < 4; k++)
        {
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.partition_restricted_skinny(k, a, b) VALUES (?, 1, 2)"), ConsistencyLevel.ALL, k);
            for (int c = 0; c < 3; c++)
                CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.partition_restricted_ppl(k, c, a, b) VALUES (?, ?, 1, 2)"), ConsistencyLevel.ALL, k, c);
        }

        for (ConsistencyLevel cl : CONSISTENCY_LEVELS)
        {
            for (int pageSize : new int[]{ 1, 2, 3, 10 })
            {
                String select = withKeyspace("SELECT k, c, a, b FROM %s.partition_restricted_ppl WHERE k = 0 AND a = 1 AND b = 2");
                assertRows(CLUSTER.coordinator(1).executeWithPaging(select + " PER PARTITION LIMIT 1 LIMIT 4", cl, pageSize),
                           row(0, 0, 1, 2));
                assertRows(CLUSTER.coordinator(1).executeWithPaging(select + " PER PARTITION LIMIT 2", cl, pageSize),
                           row(0, 0, 1, 2), row(0, 1, 1, 2));
                assertRows(CLUSTER.coordinator(1).executeWithPaging(select, cl, pageSize),
                           row(0, 0, 1, 2), row(0, 1, 1, 2), row(0, 2, 1, 2));

                select = withKeyspace("SELECT k, a, b FROM %s.partition_restricted_skinny WHERE k = 0 AND a = 1 AND b = 2");
                assertRows(CLUSTER.coordinator(1).executeWithPaging(select + " LIMIT 4", cl, pageSize), row(0, 1, 2));
                assertRows(CLUSTER.coordinator(1).executeWithPaging(select, cl, pageSize), row(0, 1, 2));
            }
        }
    }

    /**
     * The per-partition limit applies to the whole partition, with or without paging and LIMIT.
     */
    @Test
    public void testPartitionRestrictedPerPartitionLimit()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partition_restricted_ppl_limit (k int, c int, a int, b int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partition_restricted_ppl_limit(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partition_restricted_ppl_limit(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        // a = 1 on all the rows, b = 2 on the even clusterings
        for (int c = 0; c < 6; c++)
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.partition_restricted_ppl_limit(k, c, a, b) VALUES (3, ?, 1, ?)"), ConsistencyLevel.ALL, c, c % 2 == 0 ? 2 : 0);

        String select = withKeyspace("SELECT k, c FROM %s.partition_restricted_ppl_limit WHERE k = 3 AND a = 1 AND b = 2");
        for (ConsistencyLevel cl : CONSISTENCY_LEVELS)
        {
            assertRows(CLUSTER.coordinator(1).execute(select + " PER PARTITION LIMIT 1 LIMIT 3", cl), row(3, 0));
            assertRows(CLUSTER.coordinator(1).execute(select + " PER PARTITION LIMIT 2 LIMIT 3", cl), row(3, 0), row(3, 2));
            assertRows(CLUSTER.coordinator(1).execute(select + " LIMIT 2", cl), row(3, 0), row(3, 2));
            for (int pageSize : new int[]{ 1, 2, 3, 4 })
            {
                assertRows(CLUSTER.coordinator(1).executeWithPaging(select + " PER PARTITION LIMIT 1", cl, pageSize), row(3, 0));
                assertRows(CLUSTER.coordinator(1).executeWithPaging(select + " PER PARTITION LIMIT 2", cl, pageSize), row(3, 0), row(3, 2));
                assertRows(CLUSTER.coordinator(1).executeWithPaging(select + " PER PARTITION LIMIT 2 LIMIT 1", cl, pageSize), row(3, 0));
                assertRows(CLUSTER.coordinator(1).executeWithPaging(select, cl, pageSize), row(3, 0), row(3, 2), row(3, 4));
            }
        }
    }

    /**
     * Partition-restricted queries with a per-partition limit on a partition with many more candidate rows than
     * matches. Their per-partition limit is folded into their row limit, so replica filtering protection reads the
     * partition in pages.
     */
    @Test
    public void testPartitionRestrictedPerPartitionLimitOnWidePartition()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.wide_partition (k int, c int, a int, b int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.wide_partition(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.wide_partition(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, KEYSPACE);

        // a = 1 on all the rows of the partition, b = 2 on every 1000th of them, starting with the first one
        int rows = 12000;
        for (int node = 1; node <= 3; node++)
            for (int c = 0; c < rows; c++)
                CLUSTER.get(node).executeInternal(withKeyspace("INSERT INTO %s.wide_partition(k, c, a, b) VALUES (0, ?, 1, ?) USING TIMESTAMP 1"), c, c % 1000 == 0 ? 2 : 0);
        Object[][] matches = new Object[rows / 1000][];
        for (int i = 0; i < matches.length; i++)
            matches[i] = row(i * 1000);

        String select = withKeyspace("SELECT c FROM %s.wide_partition WHERE k = 0 AND a = 1 AND b = 2");
        for (boolean flush : new boolean[]{ false, true })
        {
            if (flush)
                CLUSTER.forEach(i -> i.flush(KEYSPACE));

            for (ConsistencyLevel cl : CONSISTENCY_LEVELS)
            {
                assertRows(CLUSTER.coordinator(1).execute(select + " PER PARTITION LIMIT 1", cl), row(0));
                assertRows(CLUSTER.coordinator(1).execute(select + " PER PARTITION LIMIT 1 LIMIT 1", cl), row(0));
                assertRows(CLUSTER.coordinator(1).execute(select + " PER PARTITION LIMIT 3", cl), row(0), row(1000), row(2000));
                assertRows(CLUSTER.coordinator(1).execute(select + " PER PARTITION LIMIT 2 LIMIT 3", cl), row(0), row(1000));
                assertRows(CLUSTER.coordinator(1).execute(select + " PER PARTITION LIMIT 100", cl), matches);
                assertRows(CLUSTER.coordinator(1).executeWithPaging(select + " PER PARTITION LIMIT 5", cl, 2), row(0), row(1000), row(2000), row(3000), row(4000));
                assertRows(CLUSTER.coordinator(1).executeWithPaging(select + " PER PARTITION LIMIT 100", cl, 5), matches);
            }
        }
    }

    /**
     * SERIAL reads need reconciliation, so partition-restricted queries that the replicas can't strictly filter are
     * range reads at SERIAL too. Range reads don't run Paxos, so they read at QUORUM without the linearizability of a
     * SERIAL read; the results are correct for settled data.
     */
    @Test
    public void testPartitionRestrictedSerialRead()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partition_restricted_serial (k int, c int, a int, b int, PRIMARY KEY (k, c))"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partition_restricted_serial(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partition_restricted_serial(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.partition_restricted_serial(k, c, a, b) VALUES (0, 0, 1, 5)"), ConsistencyLevel.ALL);
        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.partition_restricted_serial(k, c, a, b) VALUES (0, 1, 1, 2)"), ConsistencyLevel.ALL);
        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.partition_restricted_serial(k, c, a, b) VALUES (0, 2, 3, 2)"), ConsistencyLevel.ALL);

        for (ConsistencyLevel cl : new ConsistencyLevel[]{ ConsistencyLevel.SERIAL, ConsistencyLevel.LOCAL_SERIAL })
        {
            assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT k, c FROM %s.partition_restricted_serial WHERE k = 0 AND a = 1 AND b = 2"), cl),
                       row(0, 1));
            assertRows(CLUSTER.coordinator(1).executeWithPaging(withKeyspace("SELECT k, c FROM %s.partition_restricted_serial WHERE k = 0 AND a = 1 AND b = 2"), cl, 1),
                       row(0, 1));

            // strict queries are still single-partition reads, with Paxos
            assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT k, c FROM %s.partition_restricted_serial WHERE k = 0 AND a = 1"), cl),
                       row(0, 0), row(0, 1));
        }
    }

    /**
     * Index restrictions nested in a disjunction, with every clustering column restricted by "=" or "IN": reads of such
     * a set of primary keys skip the indexes (see CassandraRelevantProperties#SKIP_INDEXES_ON_FULL_PRIMARY_KEYS), and
     * must still find row 0 of each partition, whose "a" is on one replica and "b" on another; row 1 is on all the
     * replicas. A table without clustering columns always reads a set of primary keys: partitions 0 and 1 of the skinny
     * table have their columns split across the replicas, and partition 2 is on all the replicas.
     */
    @Test
    public void testFullPrimaryKeysWithNestedIndexRestrictions()
    {
        String table = withKeyspace("%s.full_primary_keys");
        CLUSTER.schemaChange("CREATE TABLE " + table + " (k int, c int, a int, b int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'");
        CLUSTER.schemaChange("CREATE INDEX ON " + table + "(a) USING 'sai'");
        CLUSTER.schemaChange("CREATE INDEX ON " + table + "(b) USING 'sai'");
        SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, KEYSPACE);
        for (int k = 0; k < 2; k++)
        {
            CLUSTER.get(1).executeInternal("INSERT INTO " + table + " (k, c, a) VALUES (?, 0, 1) USING TIMESTAMP 1", k);
            CLUSTER.get(2).executeInternal("INSERT INTO " + table + " (k, c, b) VALUES (?, 0, 2) USING TIMESTAMP 1", k);
            CLUSTER.coordinator(1).execute("INSERT INTO " + table + " (k, c, a, b) VALUES (?, 1, 1, 2) USING TIMESTAMP 1", ConsistencyLevel.ALL, k);
        }

        String skinny = withKeyspace("%s.full_primary_keys_skinny");
        CLUSTER.schemaChange("CREATE TABLE " + skinny + " (k int PRIMARY KEY, a int, b int) WITH read_repair = 'NONE'");
        CLUSTER.schemaChange("CREATE INDEX ON " + skinny + "(a) USING 'sai'");
        CLUSTER.schemaChange("CREATE INDEX ON " + skinny + "(b) USING 'sai'");
        SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, KEYSPACE);
        for (int k = 0; k < 2; k++)
        {
            CLUSTER.get(1).executeInternal("INSERT INTO " + skinny + " (k, a) VALUES (?, 1) USING TIMESTAMP 1", k);
            CLUSTER.get(2).executeInternal("INSERT INTO " + skinny + " (k, b) VALUES (?, 2) USING TIMESTAMP 1", k);
        }
        CLUSTER.coordinator(1).execute("INSERT INTO " + skinny + " (k, a, b) VALUES (2, 1, 2) USING TIMESTAMP 1", ConsistencyLevel.ALL);

        String nested = "((a = 1 AND b = 2) OR (a = 7 AND b = 7))";
        String[][] skinnyQueries = {
            { "k = 0 AND " + nested, "[0]" },
            { "k IN (0, 1, 2) AND ((a = 1 AND b = 2) OR a = 7)", "[0]", "[1]", "[2]" },
            { "k IN (1, 2) AND " + nested, "[1]", "[2]" },
            { "k = 1 AND a = 1 AND b = 2", "[1]" },
        };
        String[][] queries = {
            { "k = 0 AND c = 0 AND " + nested, "[0, 0]" },
            { "k = 0 AND c IN (0, 1) AND " + nested, "[0, 0]", "[0, 1]" },
            { "k IN (0, 1) AND c IN (0, 1) AND " + nested, "[0, 0]", "[0, 1]", "[1, 0]", "[1, 1]" },
            { "k IN (0, 1) AND c = 0 AND ((a = 1 AND b = 2) OR a = 7)", "[0, 0]", "[1, 0]" },
            { "k = 0 AND c IN (0, 1) AND a = 1 AND b = 2", "[0, 0]", "[0, 1]" },
            { "k = 0 AND c >= 0 AND " + nested, "[0, 0]", "[0, 1]" },
        };
        List<String> mismatches = new ArrayList<>();
        for (boolean flush : new boolean[]{ false, true })
        {
            if (flush)
                CLUSTER.forEach(i -> i.flush(KEYSPACE));
            for (boolean isSkinny : new boolean[]{ false, true })
            {
                for (String[] query : isSkinny ? skinnyQueries : queries)
                {
                    String select = isSkinny ? "SELECT k FROM " + skinny + " WHERE " + query[0] + " ALLOW FILTERING"
                                             : "SELECT k, c FROM " + table + " WHERE " + query[0] + " ALLOW FILTERING";
                    List<String> expected = Arrays.asList(Arrays.copyOfRange(query, 1, query.length));
                    String when = (flush ? " after flush" : "") + " at ALL";
                    check(select + when, expected, () -> sorted(toStrings(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL))), mismatches);
                    check(select + when + " page size 1", expected,
                          () -> sorted(toStrings(CLUSTER.coordinator(1).executeWithPaging(select, ConsistencyLevel.ALL, 1))), mismatches);
                }
            }
        }
        assertThat(mismatches).isEmpty();
    }

    /**
     * Partition-restricted queries that the replicas can't strictly filter do blocking read repair.
     */
    @Test
    public void testPartitionRestrictedReadRepair()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partition_restricted_repair (k int PRIMARY KEY, a int, b int) WITH read_repair = 'BLOCKING'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partition_restricted_repair(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partition_restricted_repair(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.partition_restricted_repair(k, a) VALUES (0, 1) USING TIMESTAMP 1"), ConsistencyLevel.ALL);
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s.partition_restricted_repair USING TIMESTAMP 2 SET b = 2 WHERE k = 0"));

        String select = withKeyspace("SELECT k, a, b FROM %s.partition_restricted_repair WHERE k = 0 AND a = 1 AND b = 2");
        assertRows(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL), row(0, 1, 2));

        // the read above has completed, so the blocking read repair must have reached all the replicas
        for (int node = 1; node <= CLUSTER.size(); node++)
            assertRows(CLUSTER.get(node).executeInternal(withKeyspace("SELECT k, a, b FROM %s.partition_restricted_repair WHERE k = 0")), row(0, 1, 2));

        // without read repair, the replica missing the update keeps missing it
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partition_restricted_no_repair (k int PRIMARY KEY, a int, b int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partition_restricted_no_repair(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partition_restricted_no_repair(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.partition_restricted_no_repair(k, a) VALUES (0, 1) USING TIMESTAMP 1"), ConsistencyLevel.ALL);
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s.partition_restricted_no_repair USING TIMESTAMP 2 SET b = 2 WHERE k = 0"));

        select = withKeyspace("SELECT k, a, b FROM %s.partition_restricted_no_repair WHERE k = 0 AND a = 1 AND b = 2");
        assertRows(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL), row(0, 1, 2));
        assertRows(CLUSTER.get(1).executeInternal(withKeyspace("SELECT k, a, b FROM %s.partition_restricted_no_repair WHERE k = 0")), row(0, 1, null));
    }

    /**
     * Disjunctions mixing static and regular columns must be evaluated as written at the coordinator, which filters
     * the candidate rows of replicas that can't strictly filter them.
     */
    @Test
    public void testStaticAndRegularColumnDisjunctions()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.static_disjunctions (k int, c int, s int static, a int, b int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.static_disjunctions(s) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.static_disjunctions(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.static_disjunctions(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.static_disjunctions(k, c, s, a, b) VALUES (0, 0, 1, 0, 2)"), ConsistencyLevel.ALL);
        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.static_disjunctions(k, c, s, a, b) VALUES (1, 0, 0, 1, 2)"), ConsistencyLevel.ALL);

        for (boolean flush : new boolean[]{ false, true })
        {
            if (flush)
                CLUSTER.forEach(i -> i.flush(KEYSPACE));

            for (ConsistencyLevel cl : CONSISTENCY_LEVELS)
            {
                // neither branch matches, although each restriction matches in some branch
                String select = withKeyspace("SELECT k, c, s, a, b FROM %s.static_disjunctions WHERE (s = 1 AND a = 1) OR (s = 2 AND b = 2)");
                assertRows(CLUSTER.coordinator(1).execute(select, cl));
                assertRows(CLUSTER.coordinator(1).executeWithPaging(select, cl, 1));

                // the regular column satisfies the disjunction, although the static one doesn't
                select = withKeyspace("SELECT k, c, s, a, b FROM %s.static_disjunctions WHERE k = 1 AND (s = 1 OR a = 1) AND b = 2");
                assertRows(CLUSTER.coordinator(1).execute(select, cl), row(1, 0, 0, 1, 2));
                assertRows(CLUSTER.coordinator(1).executeWithPaging(select, cl, 1), row(1, 0, 0, 1, 2));

                select = withKeyspace("SELECT k, c, s, a, b FROM %s.static_disjunctions WHERE (s = 1 OR a = 1) AND b = 2");
                assertRows(CLUSTER.coordinator(1).execute(select, cl), row(1, 0, 0, 1, 2), row(0, 0, 1, 0, 2));
            }
        }
    }

    /**
     * Index restrictions nested in conjunctions and disjunctions make the row filter non-strict as much as top-level
     * ones do. The replicas hold the same rows, so their responses match. Partition-restricted reads are range reads on
     * a single partition, whose candidate rows the coordinator filters; reads of several partitions with "IN" are
     * single-partition reads, which read data from all the replicas and filter it at the coordinator. The table without
     * indexes gives the expected results. Partition 3 has rows split across the replicas, so only ALL has a known
     * result for it, when the replicas can use the index for the whole filter.
     */
    @Test
    public void testNestedIndexRestrictions()
    {
        String table = withKeyspace("%s.nested");
        String reference = withKeyspace("%s.nested_reference");
        for (String t : new String[]{ table, reference })
            CLUSTER.schemaChange("CREATE TABLE " + t + " (k int, c int, a int, b int, x int, s int static, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'");
        for (String column : new String[]{ "a", "b", "s" })
            CLUSTER.schemaChange("CREATE INDEX ON " + table + '(' + column + ") USING 'sai'");
        SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, KEYSPACE);

        for (String t : new String[]{ table, reference })
        {
            for (int node = 1; node <= CLUSTER.size(); node++)
            {
                for (int k = 0; k < 3; k++)
                {
                    CLUSTER.get(node).executeInternal("INSERT INTO " + t + " (k, s) VALUES (?, 1) USING TIMESTAMP 1", k);
                    int c = 0;
                    for (int a : new int[]{ 1, 3, 9 })
                        for (int b : new int[]{ 2, 4, 9 })
                            CLUSTER.get(node).executeInternal("INSERT INTO " + t + " (k, c, a, b, x) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP 1", k, c, a, b, c++ % 2);
                }
            }
            // partition 3: the rows of node 1 have a, those of node 2 have b, and node 3 has none
            for (int c = 0; c < 4; c++)
            {
                if (t.equals(reference))
                {
                    CLUSTER.coordinator(1).execute("INSERT INTO " + t + " (k, c, a, b, x) VALUES (3, ?, ?, ?, 0) USING TIMESTAMP 1", ConsistencyLevel.ALL, c, c % 2 == 0 ? 1 : 3, c < 2 ? 2 : 4);
                    continue;
                }
                CLUSTER.get(1).executeInternal("INSERT INTO " + t + " (k, c, a, x) VALUES (3, ?, ?, 0) USING TIMESTAMP 1", c, c % 2 == 0 ? 1 : 3);
                CLUSTER.get(2).executeInternal("INSERT INTO " + t + " (k, c, b) VALUES (3, ?, ?) USING TIMESTAMP 1", c, c < 2 ? 2 : 4);
            }
        }

        String[] filters = {
            "(a = 1 AND b = 2) OR (a = 3 AND b = 4)",
            "(a = 1 AND b = 2)",
            "(a = 1 AND b = 2) OR a = 3",
            "(a = 1 AND b = 2) OR (a = 3 AND b = 4) OR (a = 9 AND b = 9)",
            "x = 1 AND ((a = 1 AND b = 2) OR (a = 3 AND b = 4))",
            "c >= 0 AND ((a = 1 AND b = 2) OR (a = 3 AND b = 4))",
            "x = 1 OR (a = 1 AND b = 2)",
            "c = 8 OR (a = 1 AND b = 2)",
            "a = 1 OR (b = 2 AND (a = 3 OR b = 4))",
            "(s = 1 AND a = 1) OR (s = 2 AND b = 2)",
            "(a = 1 OR a = 3) AND (b = 2 OR b = 4)",
            "((a = 1 OR a = 3) AND b = 2) OR ((a = 9 OR a = 3) AND b = 4)",
        };
        String[] partitions = { "k = 0", "k IN (0)", "k IN (0, 1)", "k IN (2, 0, 1)", "k IN (0, 3)", "k = 3" };

        List<String> mismatches = new ArrayList<>();
        for (boolean flush : new boolean[]{ false, true })
        {
            if (flush)
                CLUSTER.forEach(i -> i.flush(KEYSPACE));

            for (String filter : filters)
            {
                for (String partition : partitions)
                {
                    // top-level index restrictions are rejected with "IN" on several partitions
                    if (partition.contains(",") && filter.equals("(a = 1 AND b = 2)"))
                        continue;
                    // the replicas only relax the filter when they use the index for all of it, so only then can they
                    // find the rows split across them
                    if (partition.contains("3") && (filter.startsWith("x = 1 OR") || filter.startsWith("c = 8 OR")))
                        continue;

                    for (String limit : new String[]{ "", " LIMIT 3" })
                    {
                        String where = " WHERE " + partition + " AND (" + filter + ')' + limit + " ALLOW FILTERING";
                        String select = "SELECT k, c, a, b, x, s FROM " + table + where;
                        List<String> expected = toStrings(CLUSTER.coordinator(1).execute("SELECT k, c, a, b, x, s FROM " + reference + where, ConsistencyLevel.ONE));
                        for (ConsistencyLevel cl : partition.contains("3") ? new ConsistencyLevel[]{ ConsistencyLevel.ALL } : CONSISTENCY_LEVELS)
                        {
                            String when = (flush ? " after flush" : "") + " at " + cl;
                            check(select + when, expected, () -> toStrings(CLUSTER.coordinator(1).execute(select, cl)), mismatches);
                            check(select + when + " page size 2", expected, () -> toStrings(CLUSTER.coordinator(2).executeWithPaging(select, cl, 2)), mismatches);
                        }
                    }
                }
            }
        }
        assertThat(mismatches).isEmpty();
    }

    /**
     * Reads of several partitions whose replicas may return candidate rows read data from all the replicas they
     * contact, including those they speculate on, and still do read repair when the replicas disagree.
     */
    @Test
    public void testSeveralPartitionsWithSpeculativeRetryAndReadRepair()
    {
        String nested = "((a = 1 AND b = 2) OR (a = 7 AND b = 7))";

        // the coordinator always speculates on a third replica at QUORUM
        String speculating = withKeyspace("%s.speculating");
        CLUSTER.schemaChange("CREATE TABLE " + speculating + " (k int, c int, a int, b int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE' AND speculative_retry = 'ALWAYS'");
        CLUSTER.schemaChange("CREATE INDEX ON " + speculating + "(a) USING 'sai'");
        CLUSTER.schemaChange("CREATE INDEX ON " + speculating + "(b) USING 'sai'");
        SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, KEYSPACE);
        for (int k = 0; k < 2; k++)
        {
            CLUSTER.coordinator(1).execute("INSERT INTO " + speculating + " (k, c, a, b) VALUES (?, 0, 1, 2)", ConsistencyLevel.ALL, k);
            CLUSTER.coordinator(1).execute("INSERT INTO " + speculating + " (k, c, a, b) VALUES (?, 1, 1, 5)", ConsistencyLevel.ALL, k);
            CLUSTER.coordinator(1).execute("INSERT INTO " + speculating + " (k, c, a, b) VALUES (?, 2, 3, 2)", ConsistencyLevel.ALL, k);
        }
        String select = "SELECT k, c FROM " + speculating + " WHERE k IN (0, 1) AND " + nested;
        for (ConsistencyLevel cl : new ConsistencyLevel[]{ ConsistencyLevel.QUORUM, ConsistencyLevel.ALL })
        {
            assertThat(toStrings(CLUSTER.coordinator(1).execute(select, cl))).as(select).containsExactlyInAnyOrder("[0, 0]", "[1, 0]");
            assertThat(toStrings(CLUSTER.coordinator(1).executeWithPaging(select, cl, 1))).as(select).containsExactlyInAnyOrder("[0, 0]", "[1, 0]");
            assertThat(toStrings(CLUSTER.coordinator(1).execute(select + " LIMIT 1", cl))).as(select).hasSize(1);
        }
        assertThat(remoteReadRequests(select, ConsistencyLevel.QUORUM, 4)).as(select).hasSize(4).containsOnly("reading data from");

        // a row split across the replicas, which blocking read repair completes on all of them
        String repaired = withKeyspace("%s.repaired");
        CLUSTER.schemaChange("CREATE TABLE " + repaired + " (k int, c int, a int, b int, PRIMARY KEY (k, c)) WITH read_repair = 'BLOCKING'");
        CLUSTER.schemaChange("CREATE INDEX ON " + repaired + "(a) USING 'sai'");
        CLUSTER.schemaChange("CREATE INDEX ON " + repaired + "(b) USING 'sai'");
        SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, KEYSPACE);
        CLUSTER.coordinator(1).execute("INSERT INTO " + repaired + " (k, c, a, b) VALUES (0, 0, 1, 5) USING TIMESTAMP 1", ConsistencyLevel.ALL);
        CLUSTER.coordinator(1).execute("INSERT INTO " + repaired + " (k, c, a, b) VALUES (1, 0, 1, 2) USING TIMESTAMP 1", ConsistencyLevel.ALL);
        CLUSTER.get(2).executeInternal("UPDATE " + repaired + " USING TIMESTAMP 2 SET b = 2 WHERE k = 0 AND c = 0");

        select = "SELECT k, c, a, b FROM " + repaired + " WHERE k IN (0, 1) AND " + nested;
        assertThat(toStrings(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL))).as(select).containsExactlyInAnyOrder("[0, 0, 1, 2]", "[1, 0, 1, 2]");
        for (int node = 1; node <= CLUSTER.size(); node++)
            assertRows(CLUSTER.get(node).executeInternal("SELECT k, c, a, b FROM " + repaired + " WHERE k = 0"), row(0, 0, 1, 2));
    }

    /**
     * Single-partition reads that need no coordinator filtering, because their row filter is strict or they don't use
     * an index, still read digests from all the replicas but one, while those whose replicas may return candidate rows
     * read data from all of them, to filter it at the coordinator.
     */
    @Test
    public void testDigestRequests()
    {
        String table = withKeyspace("%s.digests");
        CLUSTER.schemaChange("CREATE TABLE " + table + " (k int, c int, a int, b int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE' AND speculative_retry = 'NONE'");
        CLUSTER.schemaChange("CREATE INDEX ON " + table + "(a) USING 'sai'");
        CLUSTER.schemaChange("CREATE INDEX ON " + table + "(b) USING 'sai'");
        SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, KEYSPACE);
        for (int k = 0; k < 2; k++)
        {
            CLUSTER.coordinator(1).execute("INSERT INTO " + table + " (k, c, a, b) VALUES (?, 0, 1, 2)", ConsistencyLevel.ALL, k);
            CLUSTER.coordinator(1).execute("INSERT INTO " + table + " (k, c, a, b) VALUES (?, 1, 1, 5)", ConsistencyLevel.ALL, k);
        }

        // a single replica gets a data request, which is usually the coordinator, so the other replicas get digest requests
        String[] strict = { "k = 0", "k IN (0, 1)", "k = 0 AND a = 1", "k = 0 AND (a = 1 OR a = 5)", "k IN (0, 1) AND (a = 1 OR a = 5)" };
        for (String where : strict)
        {
            String select = "SELECT k, c FROM " + table + " WHERE " + where;
            int partitions = where.contains("IN") ? 2 : 1;
            assertThat(remoteReadRequests(select, ConsistencyLevel.ALL, 2 * partitions)).as(select).hasSize(2 * partitions).contains("reading digest from");
        }

        // a disjunction of two columns isn't strict either (see RowFilter#isMutableIntersection)
        String[][] nonStrict = { { "k IN (0, 1) AND ((a = 1 AND b = 2) OR a = 3)", "[0, 0]", "[1, 0]" },
                                 { "k IN (0, 1) AND ((a = 1 AND b = 2) OR (a = 3 AND b = 4))", "[0, 0]", "[1, 0]" },
                                 { "k IN (0, 1) AND (a = 2 OR b = 2)", "[0, 0]", "[1, 0]" } };
        for (String[] query : nonStrict)
        {
            String select = "SELECT k, c FROM " + table + " WHERE " + query[0];
            assertThat(remoteReadRequests(select, ConsistencyLevel.ALL, 4)).as(select).hasSize(4).containsOnly("reading data from");
            assertThat(toStrings(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL))).as(select)
                                                                                               .containsExactlyInAnyOrder(Arrays.copyOfRange(query, 1, query.length));
        }
    }

    /**
     * Executes the given query with tracing on node 1, and returns the trace events of its requests to other replicas,
     * without their address, once there are as many as expected, or after 30 seconds.
     */
    private static List<String> remoteReadRequests(String select, ConsistencyLevel cl, int expected)
    {
        UUID session = TimeUUID.Generator.nextTimeUUID().asUUID();
        CLUSTER.coordinator(1).executeWithTracing(session, select, cl);
        List<String> requests = new ArrayList<>();
        for (int attempt = 0; attempt < 300 && requests.size() < expected; attempt++)
        {
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            requests.clear();
            for (Object[] event : CLUSTER.coordinator(1).execute("SELECT activity FROM system_traces.events WHERE session_id = ?", ConsistencyLevel.ALL, session))
            {
                String activity = (String) event[0];
                if (activity.startsWith("reading data from") || activity.startsWith("reading digest from"))
                    requests.add(activity.replaceFirst(" from .*", " from"));
            }
        }
        return requests;
    }

    private static List<String> sorted(List<String> rows)
    {
        List<String> sorted = new ArrayList<>(rows);
        sorted.sort(String::compareTo);
        return sorted;
    }
}
