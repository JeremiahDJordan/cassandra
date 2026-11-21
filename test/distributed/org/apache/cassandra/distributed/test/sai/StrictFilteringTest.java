/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.test.sai;

import java.io.IOException;
import java.util.Iterator;

import com.google.common.collect.ImmutableSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.junit.Assert.assertEquals;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * This class contains a small set of hand-crafted tests that document corner cases around how SAI handles
 * resolving partial updates, unrepaired data, and post-index filtering, with non-strict replica filtering enabled
 * unless a test disables it.
 * 
 * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-19018">CASSANDRA-19018</a>
 */
public class StrictFilteringTest extends TestBaseImpl
{
    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CLUSTER = init(Cluster.build(2).withConfig(config -> SAIUtil.withNonStrictReplicaFiltering(config, true)
                                                                    .set("hinted_handoff_enabled", false)
                                                                    .with(GOSSIP).with(NETWORK)).start());
    }

    @Test
    public void shouldDegradeToUnionOnSingleStatic()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.single_static (pk0 int, ck0 int, ck1 int, s0 int static, v0 int, PRIMARY KEY (pk0, ck0, ck1)) " +
                                          "WITH read_repair = 'NONE' AND CLUSTERING ORDER BY (ck0 ASC, ck1 DESC)"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.single_static(ck0) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.single_static(s0) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        // To present the coordinator with enough data to find a row match, both replicas must degrade to OR at query
        // time. The static column match from node 2 and the clustering key match from node 1 must be merged.
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.single_static (pk0, ck0, ck1, s0, v0) VALUES (0, 1, 2, 3, 4)"));
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.single_static SET v0 = 5 WHERE pk0 = 0 AND ck0 = 6 AND ck1 = 7"));

        // A static column predicate and ANY other predicate makes strict filtering impossible, as the static match
        // applies to the entire partition.
        String select = withKeyspace("SELECT * FROM %s.single_static WHERE s0 = 3 AND ck0 = 6");
        assertRows(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL), row(0, 6, 7, 3, 5));
    }

    /**
     * IN restrictions are evaluated in the filter tree as disjunctions, so non-strict reads relax them like any other
     * restriction instead of rejecting the query.
     */
    @Test
    public void testNonStrictIN()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.non_strict_in (k int PRIMARY KEY, a int, b int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.non_strict_in(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.non_strict_in(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        // insert an unrepaired row
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.non_strict_in(k, a) VALUES (0, 1)"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.non_strict_in(k, b) VALUES (0, 2)"));

        String select = withKeyspace("SELECT * FROM %s.non_strict_in WHERE a = 1 AND b IN (2, 3) ALLOW FILTERING");
        assertRows(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL), row(0, 1, 2));

        // repair fixes the split row
        CLUSTER.get(1).nodetoolResult("repair", KEYSPACE).asserts().success();
        assertRows(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ONE), row(0, 1, 2));
        assertRows(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL), row(0, 1, 2));
    }

    /**
     * Replicas that can't strictly filter rows return candidate rows that the coordinator must filter after
     * reconciliation, even when all the replicas agree on those rows. Partition-restricted queries must do that too,
     * instead of returning the unfiltered rows of a single replica when the digests of the responses match.
     */
    @Test
    public void testPartitionRestrictedWithReplicatedRows()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partition_restricted (k int, c int, a int, b int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partition_restricted(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partition_restricted(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        // rows on both replicas, only one of which matches both restrictions
        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.partition_restricted(k, c, a, b) VALUES (0, 0, 1, 5)"), ConsistencyLevel.ALL);
        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.partition_restricted(k, c, a, b) VALUES (0, 1, 1, 2)"), ConsistencyLevel.ALL);
        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.partition_restricted(k, c, a, b) VALUES (0, 2, 3, 2)"), ConsistencyLevel.ALL);

        String select = withKeyspace("SELECT * FROM %s.partition_restricted WHERE k = 0 AND a = 1 AND b = 2");
        assertRows(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL), row(0, 1, 1, 2));
        assertRows(CLUSTER.coordinator(1).execute(select + " LIMIT 1", ConsistencyLevel.ALL), row(0, 1, 1, 2));
        assertRows(CLUSTER.coordinator(1).executeWithPaging(select, ConsistencyLevel.ALL, 1), row(0, 1, 1, 2));

        // insert a split row in the same partition
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partition_restricted(k, c, a) VALUES (0, 3, 1)"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.partition_restricted(k, c, b) VALUES (0, 3, 2)"));

        assertRows(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL), row(0, 1, 1, 2), row(0, 3, 1, 2));
        assertRows(CLUSTER.coordinator(1).executeWithPaging(select, ConsistencyLevel.ALL, 1), row(0, 1, 1, 2), row(0, 3, 1, 2));
    }

    /**
     * DataStax SAI supports OR in queries, so a split row must also be found when an intersection contains a
     * disjunction.
     */
    @Test
    public void testPartialUpdatesWithDisjunction()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partial_updates_or (k int PRIMARY KEY, a int, b int, c int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_or(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_or(b) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_or(c) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        // insert a split row
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_or(k, a) VALUES (0, 1) USING TIMESTAMP 1"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_or(k, b, c) VALUES (0, 2, 4) USING TIMESTAMP 2"));

        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT * FROM %s.partial_updates_or WHERE a = 1 AND (b = 2 OR c = 3)"), ConsistencyLevel.ALL),
                   row(0, 1, 2, 4));
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT * FROM %s.partial_updates_or WHERE a = 1 AND (b = 3 OR c = 4)"), ConsistencyLevel.ALL),
                   row(0, 1, 2, 4));
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT * FROM %s.partial_updates_or WHERE a = 1 AND (b = 3 OR c = 3)"), ConsistencyLevel.ALL));
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT * FROM %s.partial_updates_or WHERE (a = 1 AND b = 2) OR c = 3"), ConsistencyLevel.ALL),
                   row(0, 1, 2, 4));
    }

    /**
     * Nested operations that can't use indexes, such as IN or OR on unindexed columns, don't narrow down the
     * candidates on unrepaired data, but the indexed restrictions still do.
     */
    @Test
    public void testPartialUpdatesWithUnindexedDisjunctions()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partial_updates_unindexed_or (k int PRIMARY KEY, a int, b int, c int, d int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_unindexed_or(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_unindexed_or(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        // insert a split row
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_unindexed_or(k, a) VALUES (0, 1) USING TIMESTAMP 1"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_unindexed_or(k, c) VALUES (0, 7) USING TIMESTAMP 2"));

        Object[] expected = row(0, 1, null, 7, null);
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT * FROM %s.partial_updates_unindexed_or WHERE a = 1 AND c IN (7, 8) ALLOW FILTERING"), ConsistencyLevel.ALL), expected);
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT * FROM %s.partial_updates_unindexed_or WHERE a = 1 AND (c = 7 OR d = 8) ALLOW FILTERING"), ConsistencyLevel.ALL), expected);
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT * FROM %s.partial_updates_unindexed_or WHERE a = 1 AND (b = 2 OR c = 7) ALLOW FILTERING"), ConsistencyLevel.ALL), expected);
    }

    /**
     * A split row must also be found when the intersection contains a negation, which DataStax SAI supports.
     */
    @Test
    public void testPartialUpdatesWithNegation()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partial_updates_not (k int PRIMARY KEY, a int, b int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_not(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_not(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        // insert a split row, where the stale value of b on node 1 doesn't match the negation, and the live one on
        // node 2 does, but node 2 doesn't have the value of a
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_not(k, a, b) VALUES (0, 1, 4) USING TIMESTAMP 1"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_not(k, b) VALUES (0, 2) USING TIMESTAMP 2"));

        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT * FROM %s.partial_updates_not WHERE a = 1 AND b != 4"), ConsistencyLevel.ALL),
                   row(0, 1, 2));
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT * FROM %s.partial_updates_not WHERE a = 1 AND b != 2"), ConsistencyLevel.ALL));
    }

    @Test
    public void testPartialUpdates()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partial_updates (k int PRIMARY KEY, a int, b int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        // insert a split row
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partial_updates(k, a) VALUES (0, 1) USING TIMESTAMP 1"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.partial_updates(k, b) VALUES (0, 2) USING TIMESTAMP 2"));

        String select = withKeyspace("SELECT * FROM %s.partial_updates WHERE a = 1 AND b = 2");
        Object[][] initialRows = CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL);
        assertRows(initialRows, row(0, 1, 2));

        // with non-strict replica filtering disabled on the coordinator, the replicas filter strictly and miss the row
        SAIUtil.setNonStrictReplicaFiltering(CLUSTER.get(1), false);
        try
        {
            assertRows(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL));
            assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT * FROM %s.partial_updates WHERE k = 0 AND a = 1 AND b = 2"), ConsistencyLevel.ALL));
            assertRows(CLUSTER.coordinator(2).execute(select, ConsistencyLevel.ALL), row(0, 1, 2));
        }
        finally
        {
            SAIUtil.setNonStrictReplicaFiltering(CLUSTER.get(1), true);
        }
    }

    @Test
    public void testPartialUpdatesOnNonIndexedColumnsAfterRepair()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partial_updates_non_indexed_columns (k int PRIMARY KEY, a int, b int, c int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_non_indexed_columns(a) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.partial_updates_non_indexed_columns(k, a) VALUES (0, 1) USING TIMESTAMP 1"), ConsistencyLevel.ALL);
        CLUSTER.get(1).nodetoolResult("repair", KEYSPACE).asserts().success();
        SAIUtil.waitForRepairedSSTables(CLUSTER, KEYSPACE, "partial_updates_non_indexed_columns");

        // insert a split row
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_non_indexed_columns(k, b) VALUES (0, 2) USING TIMESTAMP 2"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_non_indexed_columns(k, c) VALUES (0, 3) USING TIMESTAMP 3"));

        String select = withKeyspace("SELECT * FROM %s.partial_updates_non_indexed_columns WHERE a = 1 AND b = 2 AND c = 3 ALLOW FILTERING");
        Object[][] initialRows = CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL);
        assertRows(initialRows, row(0, 1, 2, 3));
    }

    /**
     * Like {@link #testPartialUpdatesOnNonIndexedColumnsAfterRepair()}, with DataStax-only query features: a nested
     * disjunction on unindexed columns, and an indexed column whose index is excluded by the query hints.
     */
    @Test
    public void testPartialUpdatesOnNonIndexedColumnsAfterRepairWithDisjunctionsAndHints()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partial_updates_non_indexed_or (k int PRIMARY KEY, a int, b int, c int, d int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_non_indexed_or(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX b_idx ON %s.partial_updates_non_indexed_or(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.partial_updates_non_indexed_or(k, a) VALUES (0, 1) USING TIMESTAMP 1"), ConsistencyLevel.ALL);
        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.partial_updates_non_indexed_or(k, a) VALUES (1, 1) USING TIMESTAMP 1"), ConsistencyLevel.ALL);
        CLUSTER.get(1).nodetoolResult("repair", KEYSPACE).asserts().success();
        SAIUtil.waitForRepairedSSTables(CLUSTER, KEYSPACE, "partial_updates_non_indexed_or");

        // insert split rows, the first one on unindexed columns...
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_non_indexed_or(k, c) VALUES (0, 3) USING TIMESTAMP 2"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_non_indexed_or(k, d) VALUES (0, 4) USING TIMESTAMP 3"));

        // ...and the second one on an unindexed column and on an indexed column whose index the query excludes
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_non_indexed_or(k, b) VALUES (1, 2) USING TIMESTAMP 2"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_non_indexed_or(k, c) VALUES (1, 3) USING TIMESTAMP 3"));

        String select = withKeyspace("SELECT * FROM %s.partial_updates_non_indexed_or WHERE a = 1 AND c = 3 AND (d = 4 OR d = 5) ALLOW FILTERING");
        assertRows(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL), row(0, 1, null, 3, 4));

        select = withKeyspace("SELECT * FROM %s.partial_updates_non_indexed_or WHERE a = 1 AND b = 2 AND c = 3 ALLOW FILTERING WITH excluded_indexes = {b_idx}");
        assertRows(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL), row(1, 1, 2, 3, null));
    }

    @Test
    public void testPartialUpdatesOnNonIndexedCollectionAfterRepair()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partial_updates_non_indexed_collection (k int PRIMARY KEY, a int, s set<int>) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_non_indexed_collection(a) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.partial_updates_non_indexed_collection(k, a) VALUES (0, 1) USING TIMESTAMP 1"), ConsistencyLevel.ALL);
        CLUSTER.get(1).nodetoolResult("repair", KEYSPACE).asserts().success();
        SAIUtil.waitForRepairedSSTables(CLUSTER, KEYSPACE, "partial_updates_non_indexed_collection");

        // insert a row whose set elements are split between the replicas
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.partial_updates_non_indexed_collection USING TIMESTAMP 2 SET s = s + {2} WHERE k = 0"));
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s.partial_updates_non_indexed_collection USING TIMESTAMP 3 SET s = s + {3} WHERE k = 0"));

        String select = withKeyspace("SELECT * FROM %s.partial_updates_non_indexed_collection WHERE a = 1 AND s CONTAINS 2 AND s CONTAINS 3 ALLOW FILTERING");
        assertRows(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL), row(0, 1, ImmutableSet.of(2, 3)));
    }

    @Test
    public void testPartialUpdateOnNonIndexedColumnAfterRepair()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partial_updates_non_indexed_column (k int PRIMARY KEY, a int, b int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_non_indexed_column(a) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.partial_updates_non_indexed_column(k, a) VALUES (0, 1) USING TIMESTAMP 1"), ConsistencyLevel.ALL);
        CLUSTER.get(1).nodetoolResult("repair", KEYSPACE).asserts().success();
        SAIUtil.waitForRepairedSSTables(CLUSTER, KEYSPACE, "partial_updates_non_indexed_column");

        // insert a split row
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_non_indexed_column(k, b) VALUES (0, 2) USING TIMESTAMP 2"));

        String select = withKeyspace("SELECT * FROM %s.partial_updates_non_indexed_column WHERE a = 1 AND b = 2 ALLOW FILTERING");
        Object[][] initialRows = CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL);
        assertRows(initialRows, row(0, 1, 2));
    }

    @Test
    public void testPartialUpdatesWithDeleteBetween()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partial_updates_delete_between (k int, c int, a int, b int, x int, y int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_delete_between(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_delete_between(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        // insert a split row w/ a range tombstone sandwiched in the middle 
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_delete_between(k, c, a, x) VALUES (0, 1, 1, 100) USING TIMESTAMP 1"));
        CLUSTER.get(2).executeInternal(withKeyspace("DELETE FROM %s.partial_updates_delete_between USING TIMESTAMP 2 WHERE k = 0 AND c > 0"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_delete_between(k, c, b, y) VALUES (0, 1, 2, 200) USING TIMESTAMP 3"));

        String select = withKeyspace("SELECT * FROM %s.partial_updates_delete_between WHERE a = 1 AND b = 2");
        Object[][] initialRows = CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL);
        assertRows(initialRows);
    }

    @Test
    public void testDanglingUnfilteredColumnWithDeleteBetween()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.dangling_unfiltered_delete_between (k int, c int, a int, b int, x int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.dangling_unfiltered_delete_between(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.dangling_unfiltered_delete_between(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        // insert a split row w/ a range tombstone sandwiched in the middle 
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.dangling_unfiltered_delete_between(k, c, a, b, x) VALUES (0, 1, 1, 2, 100) USING TIMESTAMP 1"));
        CLUSTER.get(2).executeInternal(withKeyspace("DELETE FROM %s.dangling_unfiltered_delete_between USING TIMESTAMP 2 WHERE k = 0 AND c > 0"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.dangling_unfiltered_delete_between(k, c, a, b) VALUES (0, 1, 1, 2) USING TIMESTAMP 3"));

        String select = withKeyspace("SELECT * FROM %s.dangling_unfiltered_delete_between WHERE a = 1 AND b = 2");
        Object[][] initialRows = CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL);
        assertRows(initialRows, row(0, 1, 1, 2, null));
    }

    @Test
    public void testPartialUpdatesStaticOnly()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partial_updates_statics (k int, c int, s int static, b int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_statics(s) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        // insert a split row
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_statics(k, s) VALUES (0, 2) USING TIMESTAMP 100"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_statics(k, c, s, b) VALUES (0, 0, 1, 2) USING TIMESTAMP 10"));

        String select = withKeyspace("SELECT * FROM %s.partial_updates_statics WHERE s = 2");
        Object[][] initialRows = CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL);
        assertRows(initialRows, row(0, 0, 2, 2));
    }

    @Test
    public void testShortReadWithRegularColumns()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partial_updates_short_read (k int PRIMARY KEY, a int, b int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_short_read(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_short_read(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        // insert a split row
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_short_read(k, a) VALUES (0, 1) USING TIMESTAMP 1"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_short_read(k, b) VALUES (0, 2) USING TIMESTAMP 2"));

        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_short_read(k, a, b) VALUES (1, 4, 2) USING TIMESTAMP 3"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_short_read(k, a, b) VALUES (1, 1, 4) USING TIMESTAMP 4"));

        String select = withKeyspace("SELECT * FROM %s.partial_updates_short_read WHERE a = 1 AND b = 2 LIMIT 1");
        Iterator<Object[]> initialRows = CLUSTER.coordinator(1).executeWithPaging(select, ConsistencyLevel.ALL, 2);
        assertRows(initialRows, row(0, 1, 2));
    }

    @Test
    public void testNoShortReadAtLimit()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.no_srp_at_limit (k int, c int, a int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.no_srp_at_limit(a) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.no_srp_at_limit(k, c, a) VALUES (0, 2, 1) USING TIMESTAMP 5"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.no_srp_at_limit(k, c, a) VALUES (0, 3, 1) USING TIMESTAMP 6"));

        Long srpRequestsBefore = CLUSTER.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("no_srp_at_limit").metric.shortReadProtectionRequests.getCount());

        String select = withKeyspace("SELECT * FROM %s.no_srp_at_limit WHERE k = 0 AND a = 1 LIMIT 1");
        Object[][] initialRows = CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL);
        assertRows(initialRows, row(0, 2, 1));

        Long srpRequestsAfter = CLUSTER.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("no_srp_at_limit").metric.shortReadProtectionRequests.getCount());
        assertEquals(srpRequestsBefore, srpRequestsAfter);
    }

    @Test
    public void testNecessaryShortRead()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.necessary_short_read (k int, c int, a int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.necessary_short_read(a) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.necessary_short_read(k, c, a) VALUES (0, 2, 1) USING TIMESTAMP 5"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.necessary_short_read(k, c, a) VALUES (0, 2, 2) USING TIMESTAMP 6"));

        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.necessary_short_read(k, c, a) VALUES (0, 3, 1) USING TIMESTAMP 7"));
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.necessary_short_read(k, c, a) VALUES (0, 3, 2) USING TIMESTAMP 8"));

        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.necessary_short_read(k, c, a) VALUES (0, 4, 1) USING TIMESTAMP 9"));

        Long srpRequestsBefore = CLUSTER.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("necessary_short_read").metric.shortReadProtectionRequests.getCount());

        String select = withKeyspace("SELECT * FROM %s.necessary_short_read WHERE k = 0 AND a = 1 LIMIT 1");
        Object[][] initialRows = CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL);
        assertRows(initialRows, row(0, 4, 1));

        Long srpRequestsAfter = CLUSTER.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("necessary_short_read").metric.shortReadProtectionRequests.getCount());
        assertEquals(srpRequestsBefore + 2L, srpRequestsAfter.longValue());
    }

    @Test
    public void testNoShortReadAtLimitOverPartitions()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.no_srp_over_partitions (k int, c int, a int, b int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.no_srp_over_partitions(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.no_srp_over_partitions(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, KEYSPACE);

        // The same rows on both replicas, all of them match
        for (int k = 0; k < 4; k++)
            for (int c = 0; c < 10; c++)
                CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.no_srp_over_partitions(k, c, a, b) VALUES (?, ?, 1, 2)"), ConsistencyLevel.ALL, k, c);

        // The rows both replicas return fill the limit, so neither replica is asked for the rest of the partition,
        // with a filter on one column (a = 1) or two (a = 1 AND b = 2).
        for (String filter : new String[]{ "a = 1", "a = 1 AND b = 2" })
        {
            String select = withKeyspace("SELECT k, c FROM %s.no_srp_over_partitions WHERE " + filter + " LIMIT 2");
            long srpRequestsBefore = shortReadProtectionRequests("no_srp_over_partitions");
            assertRows(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL), row(1, 0), row(1, 1));
            assertEquals(select, srpRequestsBefore, shortReadProtectionRequests("no_srp_over_partitions"));

            select = withKeyspace("SELECT k, c FROM %s.no_srp_over_partitions WHERE " + filter);
            srpRequestsBefore = shortReadProtectionRequests("no_srp_over_partitions");
            Iterator<Object[]> pagedRows = CLUSTER.coordinator(1).executeWithPaging(select, ConsistencyLevel.ALL, 3);
            int rowCount = 0;
            while (pagedRows.hasNext())
            {
                pagedRows.next();
                rowCount++;
            }
            assertEquals(select, 40, rowCount);
            assertEquals(select, srpRequestsBefore, shortReadProtectionRequests("no_srp_over_partitions"));
        }
    }

    @Test
    public void testNecessaryShortReadWithinPartition()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.necessary_short_read_within_partition (k int, c int, a int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.necessary_short_read_within_partition(a) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, KEYSPACE);

        // Node 2 has newer non-matching versions of c = 0 and c = 1
        for (int c = 0; c < 4; c++)
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.necessary_short_read_within_partition(k, c, a) VALUES (0, ?, 1) USING TIMESTAMP 1"), ConsistencyLevel.ALL, c);
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.necessary_short_read_within_partition(k, c, a) VALUES (0, 0, 2) USING TIMESTAMP 2"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.necessary_short_read_within_partition(k, c, a) VALUES (0, 1, 2) USING TIMESTAMP 2"));

        // Node 1 returns c = 0 and c = 1, which node 2 is silent about, so node 1 is asked for more rows once, and
        // returns c = 2 and c = 3, which both nodes returned, and which fill the limit.
        long srpRequestsBefore = shortReadProtectionRequests("necessary_short_read_within_partition");
        String select = withKeyspace("SELECT * FROM %s.necessary_short_read_within_partition WHERE a = 1 LIMIT 2");
        assertRows(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL), row(0, 2, 1), row(0, 3, 1));
        assertEquals(srpRequestsBefore + 1, shortReadProtectionRequests("necessary_short_read_within_partition"));
    }

    private static long shortReadProtectionRequests(String table)
    {
        return CLUSTER.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).metric.shortReadProtectionRequests.getCount());
    }

    @Test
    public void testShortReadWithStaticColumn()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partial_updates_short_read_static (k int, c int, a int, b int static, PRIMARY KEY(k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_short_read_static(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_short_read_static(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        // insert a split row
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_short_read_static(k, c, a) VALUES (0, 0, 1) USING TIMESTAMP 1"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_short_read_static(k, c, b) VALUES (0, 0, 2) USING TIMESTAMP 2"));

        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_short_read_static(k, c, a, b) VALUES (1, 1, 4, 2) USING TIMESTAMP 3"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_short_read_static(k, c, a, b) VALUES (1, 1, 1, 4) USING TIMESTAMP 4"));

        String select = withKeyspace("SELECT k, a, b FROM %s.partial_updates_short_read_static WHERE a = 1 AND b = 2 LIMIT 1");
        Object[][] rows = CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL);
        assertRows(rows, row(0, 1, 2));
    }

    @Test
    public void testTimestampCollision()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.timestamp_collision (k int PRIMARY KEY, a int, b int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.timestamp_collision(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.timestamp_collision(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        // insert a split row
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.timestamp_collision(k, a, b) VALUES (0, 1, 2) USING TIMESTAMP 1"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.timestamp_collision(k, a, b) VALUES (0, 2, 1) USING TIMESTAMP 1"));
        
        String select = withKeyspace("SELECT * FROM %s.timestamp_collision WHERE a = 2 AND b = 2");
        Object[][] initialRows = CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL);
        assertRows(initialRows, row(0, 2, 2));
    }

    @Test
    public void testPartialUpdateOnOneColumn()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.one_column (k int PRIMARY KEY, a int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.one_column(a) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.one_column(k, a) VALUES (0, 1) USING TIMESTAMP 1"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.one_column(k, a) VALUES (0, 100) USING TIMESTAMP 1"));
        
        // resolved via replica filtering protection
        Object[][] initialRows = CLUSTER.coordinator(1).execute(withKeyspace("SELECT * FROM %s.one_column WHERE a = 1"), ConsistencyLevel.ALL);
        assertRows(initialRows);
    }

    @Test
    public void testPagingPartitionRestrictedQueryWithoutClusteringColumns()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.paging_no_clustering (k int PRIMARY KEY, a int, b int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.paging_no_clustering(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.paging_no_clustering(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        for (int k = 0; k < 4; k++)
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.paging_no_clustering(k, a, b) VALUES (?, 1, 2)"), ConsistencyLevel.ALL, k);

        // Without strict filtering, the partition-restricted query is a range read of the single partition k = 0.
        // Paging it must stop at the end of that partition rather than go on with the rest of the ring.
        String select = withKeyspace("SELECT * FROM %s.paging_no_clustering WHERE k = 0 AND a = 1 AND b = 2 LIMIT 10");
        assertRows(CLUSTER.coordinator(1).executeWithPaging(select, ConsistencyLevel.ALL, 1), row(0, 1, 2));
    }

    @Test
    public void testPagingPartitionRestrictedQueryWithPerPartitionLimit()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.paging_per_partition_limit (k int, c int, a int, b int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.paging_per_partition_limit(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.paging_per_partition_limit(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        for (int k = 0; k < 4; k++)
            for (int c = 0; c < 3; c++)
                CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.paging_per_partition_limit(k, c, a, b) VALUES (?, ?, 1, 2)"), ConsistencyLevel.ALL, k, c);

        // Without strict filtering, the partition-restricted query is a range read of the single partition k = 0.
        // Paging it must stop once the per-partition limit is reached rather than go on with the rest of the ring.
        String select = withKeyspace("SELECT * FROM %s.paging_per_partition_limit WHERE k = 0 AND a = 1 AND b = 2 PER PARTITION LIMIT 1 LIMIT 10");
        assertRows(CLUSTER.coordinator(1).executeWithPaging(select, ConsistencyLevel.ALL, 1), row(0, 0, 1, 2));

        select = withKeyspace("SELECT * FROM %s.paging_per_partition_limit WHERE k = 0 AND a = 1 AND b = 2 PER PARTITION LIMIT 2 LIMIT 10");
        assertRows(CLUSTER.coordinator(1).executeWithPaging(select, ConsistencyLevel.ALL, 1), row(0, 0, 1, 2), row(0, 1, 1, 2));
        assertRows(CLUSTER.coordinator(1).executeWithPaging(select, ConsistencyLevel.ALL, 2), row(0, 0, 1, 2), row(0, 1, 1, 2));

        // GROUP BY queries page the same range read through their own pager.
        select = withKeyspace("SELECT k, c, count(*) FROM %s.paging_per_partition_limit WHERE k = 0 AND a = 1 AND b = 2 GROUP BY k, c PER PARTITION LIMIT 1 LIMIT 10");
        assertRows(CLUSTER.coordinator(1).executeWithPaging(select, ConsistencyLevel.ALL, 1), row(0, 0, 1L));
    }

    @Test
    public void testPagingPartitionRestrictedQueryWithStaticColumn()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.paging_static (k int, c int, s int static, v int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.paging_static(s) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.paging_static(v) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.paging_static(k, s) VALUES (0, 5)"), ConsistencyLevel.ALL);
        for (int c = 0; c < 3; c++)
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.paging_static(k, c, v) VALUES (0, ?, 1)"), ConsistencyLevel.ALL, c);

        // An expression on a static column and another expression can't be strictly filtered, so the query is a range
        // read of the single partition k = 0. Paging it must stop once the per-partition limit is reached. As k = 0 is
        // the only partition, going on with the rest of the ring would return the same rows again.
        String select = withKeyspace("SELECT k, c FROM %s.paging_static WHERE k = 0 AND s = 5 AND v = 1 PER PARTITION LIMIT 1 LIMIT 10");
        assertRows(CLUSTER.coordinator(1).executeWithPaging(select, ConsistencyLevel.ALL, 1), row(0, 0));

        select = withKeyspace("SELECT k, c FROM %s.paging_static WHERE k = 0 AND s = 5 AND v = 1 PER PARTITION LIMIT 2 LIMIT 10");
        assertRows(CLUSTER.coordinator(1).executeWithPaging(select, ConsistencyLevel.ALL, 1), row(0, 0), row(0, 1));
        assertRows(CLUSTER.coordinator(1).executeWithPaging(select, ConsistencyLevel.ALL, 2), row(0, 0), row(0, 1));
    }

    @Test
    public void testPagingPartitionRestrictedQueryWithStaticColumnFilter()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.paging_static_filter (k int, c int, s int static, v int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.paging_static_filter(v) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);

        for (int k = 0; k < 3; k++)
        {
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.paging_static_filter(k, s) VALUES (?, 7)"), ConsistencyLevel.ALL, k);
            for (int c = 0; c < 3; c++)
                CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.paging_static_filter(k, c, v) VALUES (?, ?, 1)"), ConsistencyLevel.ALL, k, c);
        }

        // The same with a filter on the static column instead of an index on it, and other partitions that match.
        String select = withKeyspace("SELECT k, c, s FROM %s.paging_static_filter WHERE k = 0 AND s = 7 AND v = 1 PER PARTITION LIMIT 2 LIMIT 10 ALLOW FILTERING");
        assertRows(CLUSTER.coordinator(1).executeWithPaging(select, ConsistencyLevel.ALL, 1), row(0, 0, 7), row(0, 1, 7));
        assertRows(CLUSTER.coordinator(1).executeWithPaging(select, ConsistencyLevel.ALL, 2), row(0, 0, 7), row(0, 1, 7));
    }

    @AfterClass
    public static void shutDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }
}
