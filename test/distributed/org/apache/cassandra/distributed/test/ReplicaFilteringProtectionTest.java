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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.Collections;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;

import static org.apache.cassandra.config.ReplicaFilteringProtectionOptions.DEFAULT_FAIL_THRESHOLD;
import static org.apache.cassandra.config.ReplicaFilteringProtectionOptions.DEFAULT_WARN_THRESHOLD;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertEquals;

/**
 * Exercises the functionality of {@link org.apache.cassandra.service.reads.ReplicaFilteringProtection}, the
 * mechanism that ensures distributed index and filtering queries at read consistency levels > ONE/LOCAL_ONE
 * avoid stale replica results.
 */
public class ReplicaFilteringProtectionTest extends TestBaseImpl
{
    private static final int REPLICAS = 2;
    private static final int ROWS = 3;

    private static Cluster cluster;

    @BeforeClass
    public static void setup() throws IOException
    {
        cluster = init(Cluster.build()
                              .withNodes(REPLICAS)
                              .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                          .set("num_tokens", 1)).start());

        // Make sure we start w/ the correct defaults:
        cluster.get(1).runOnInstance(() -> assertEquals(DEFAULT_WARN_THRESHOLD, StorageService.instance.getCachedReplicaRowsWarnThreshold()));
        cluster.get(1).runOnInstance(() -> assertEquals(DEFAULT_FAIL_THRESHOLD, StorageService.instance.getCachedReplicaRowsFailThreshold()));
    }

    @AfterClass
    public static void teardown()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testMissedUpdatesBelowCachingWarnThreshold()
    {
        String tableName = "missed_updates_no_warning";
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + tableName + " (k int PRIMARY KEY, v text)"));

        // The warning threshold provided is one more than the total number of rows returned
        // to the coordinator from all replicas and therefore should not be triggered.
        testMissedUpdates(tableName, REPLICAS * ROWS, Integer.MAX_VALUE, false);
    }

    @Test
    public void testMissedUpdatesAboveCachingWarnThreshold()
    {
        String tableName = "missed_updates_cache_warn";
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + tableName + " (k int PRIMARY KEY, v text)"));

        // The warning threshold provided is one less than the total number of rows returned
        // to the coordinator from all replicas and therefore should be triggered but not fail the query.
        testMissedUpdates(tableName, REPLICAS * ROWS - 1, Integer.MAX_VALUE, true);
    }

    @Test
    public void testMissedUpdatesAroundCachingFailThreshold()
    {
        String tableName = "missed_updates_cache_fail";
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + tableName + " (k int PRIMARY KEY, v text)"));

        // The failure threshold provided is exactly the total number of rows returned
        // to the coordinator from all replicas and therefore should just warn.
        testMissedUpdates(tableName, 1, REPLICAS * ROWS, true);

        try
        {
            // The failure threshold provided is one less than the total number of rows returned
            // to the coordinator from all replicas and therefore should fail the query.
            testMissedUpdates(tableName, 1, REPLICAS * ROWS - 1, true);
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getClass().getName(), OverloadedException.class.getName());
            Assertions.assertThat(e)
                      .hasMessageContaining("Replica filtering protection has cached " + REPLICAS * ROWS)
                      .hasMessageContaining("cached_replica_rows_fail_threshold");
        }
    }

    /**
     * A row that both replicas return, as it matches on each replica's data, but that fails the filter once
     * reconciled: each node deletes another element of l = [7, 7], so the first row that matches is c = 2.
     */
    @Test
    public void testRowFailingFilterOnceReconciled()
    {
        String table = KEYSPACE + ".row_failing_once_reconciled";
        cluster.schemaChange("CREATE TABLE " + table + " (k int, c int, l list<int>, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'");
        for (int c = 1; c <= 2; c++)
            cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, l) VALUES (0, ?, [7, 7]) USING TIMESTAMP 1", ALL, c);
        cluster.get(1).executeInternal("DELETE l[0] FROM " + table + " USING TIMESTAMP 2 WHERE k = 0 AND c = 1");
        cluster.get(2).executeInternal("DELETE l[1] FROM " + table + " USING TIMESTAMP 2 WHERE k = 0 AND c = 1");

        for (String query : new String[]{ "SELECT k, c FROM " + table + " WHERE l CONTAINS 7 LIMIT 1 ALLOW FILTERING",
                                          "SELECT k, c FROM " + table + " WHERE k = 0 AND l CONTAINS 7 LIMIT 1 ALLOW FILTERING" })
        {
            assertRows(cluster.coordinator(1).execute(query, ALL), row(0, 2));
            for (int pageSize = 1; pageSize <= 2; pageSize++)
                assertRows(cluster.coordinator(1).executeWithPaging(query, ALL, pageSize), row(0, 2));
        }
    }

    /**
     * Replicas that agree return all the rows that a query with a per-partition limit, a filter on a static column, a
     * page in bytes or an aggregate read in pages of bytes needs, so short read protection asks them for no more rows.
     * Without a row limit, a range read with a per-partition limit asks each replica once for the partitions after its
     * last one.
     */
    @Test
    public void testNoShortReadProtectionRequestsOnAgreeingReplicas()
    {
        String table = createAgreeingTable("srp_on_agreeing_replicas");
        setCachedRowsThresholds(DEFAULT_WARN_THRESHOLD, DEFAULT_FAIL_THRESHOLD);
        cluster.get(1).runOnInstance(() -> DatabaseDescriptor.setAggregationSubPageSize(PageSize.inBytes(1024)));
        try
        {
            assertShortReadProtectionRequests(REPLICAS, table, "SELECT k, c FROM %s WHERE v = 7 PER PARTITION LIMIT 2 ALLOW FILTERING",
                                              row(1, 0), row(1, 1), row(0, 0), row(0, 1));
            assertShortReadProtectionRequests(0, table, "SELECT k, c FROM %s WHERE s = 7 LIMIT 5 ALLOW FILTERING",
                                              row(1, 0), row(1, 1), row(1, 2), row(1, 3), row(1, 4));
            assertShortReadProtectionRequests(0, table, "SELECT count(*) FROM %s WHERE v = 7 ALLOW FILTERING",
                                              row(32L));
            assertShortReadProtectionRequests(0, table, "SELECT k, count(*) FROM %s WHERE v = 7 GROUP BY k ALLOW FILTERING",
                                              row(1, 16L), row(0, 16L));

            long requests = shortReadProtectionRequests(table);
            assertEquals(32, rowsInPagesOfBytes("SELECT k, c, t FROM " + KEYSPACE + '.' + table + " WHERE v = 7 ALLOW FILTERING", 256));
            assertEquals(0, shortReadProtectionRequests(table) - requests);
        }
        finally
        {
            cluster.get(1).runOnInstance(() -> DatabaseDescriptor.setAggregationSubPageSize(PageSize.inBytes(2048 * 1024)));
        }
    }

    /**
     * Agreeing replicas return one row of each partition of 16 rows to a query with a per-partition limit of 1, a filter
     * on a static column and a limit of 1, or a page of 256 bytes, so replica filtering protection caches fewer rows
     * than a fail threshold of 8.
     */
    @Test
    public void testAgreeingReplicasBelowCachingFailThreshold()
    {
        String table = createAgreeingTable("agreeing_replicas_cache_fail");
        setCachedRowsThresholds(8, 8);
        try
        {
            assertRows(cluster.coordinator(1).execute("SELECT k, c FROM " + KEYSPACE + '.' + table + " WHERE v = 7 PER PARTITION LIMIT 1 ALLOW FILTERING", ALL),
                       row(1, 0), row(0, 0));
            assertRows(cluster.coordinator(1).execute("SELECT k, c FROM " + KEYSPACE + '.' + table + " WHERE s = 7 LIMIT 1 ALLOW FILTERING", ALL),
                       row(1, 0));
            assertEquals(32, rowsInPagesOfBytes("SELECT k, c, t FROM " + KEYSPACE + '.' + table + " WHERE v = 7 ALLOW FILTERING", 256));
        }
        finally
        {
            setCachedRowsThresholds(DEFAULT_WARN_THRESHOLD, DEFAULT_FAIL_THRESHOLD);
        }
    }

    /**
     * Rows that both replicas return after the rows that fill the per-partition limit don't count towards the limits.
     * Partitions 1, 0, 2 and 3 come in that order. Partition 1 has c = 0 to 3 matching on each node, of which c = 0
     * doesn't once reconciled, partition 0 has such a row c = 0 only, and partitions 2 and 3 a row that matches.
     */
    @Test
    public void testRowsAfterPerPartitionLimit()
    {
        String table = KEYSPACE + ".rows_after_per_partition_limit";
        cluster.schemaChange("CREATE TABLE " + table + " (k int, c int, l list<int>, t text, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'");
        String text = "x".repeat(500);
        for (int c = 0; c <= 3; c++)
            cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, l, t) VALUES (1, ?, [7, 7], ?) USING TIMESTAMP 1", ALL, c, text);
        for (int k : new int[]{ 0, 2, 3 })
            cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, l, t) VALUES (?, 0, [7, 7], ?) USING TIMESTAMP 1", ALL, k, text);
        for (int k = 0; k <= 1; k++)
        {
            cluster.get(1).executeInternal("DELETE l[0] FROM " + table + " USING TIMESTAMP 2 WHERE k = ? AND c = 0", k);
            cluster.get(2).executeInternal("DELETE l[1] FROM " + table + " USING TIMESTAMP 2 WHERE k = ? AND c = 0", k);
        }

        String query = "SELECT k, c FROM " + table + " WHERE l CONTAINS 7 PER PARTITION LIMIT 2 LIMIT 3 ALLOW FILTERING";
        assertRows(cluster.coordinator(1).execute(query, ALL), row(1, 1), row(1, 2), row(2, 0));
        assertRows(cluster.coordinator(1).executeWithPaging(query, ALL, 3), row(1, 1), row(1, 2), row(2, 0));

        String groupByQuery = "SELECT k, c, count(*) FROM " + table + " WHERE l CONTAINS 7 GROUP BY k, c PER PARTITION LIMIT %d LIMIT %d ALLOW FILTERING";
        assertRows(cluster.coordinator(1).execute(String.format(groupByQuery, 2, 3), ALL), row(1, 1, 1L), row(1, 2, 1L), row(2, 0, 1L));
        assertRows(cluster.coordinator(1).execute(String.format(groupByQuery, 1, 2), ALL), row(1, 1, 1L), row(2, 0, 1L));

        assertEquals(4, rowsInPagesOfBytes("SELECT k, c, t FROM " + table + " WHERE l CONTAINS 7 PER PARTITION LIMIT 2 ALLOW FILTERING", 1500));
    }

    /**
     * Rows read past the per-partition limit don't count towards the limits, before partitions whose replicas differ.
     * Partitions 5, 1 and 0 come in that order. Partition 5 has c = 1, 3 and 4 matching on both nodes, and c = 2
     * matching on each node but not once reconciled. Partition 1 matches on node 1 only, and is overwritten on node 2.
     * Partition 0 has c = 1 on node 2 only, deleted on node 1, and c = 2 on node 1 only.
     */
    @Test
    public void testRowsPastPerPartitionLimitBeforeDivergentPartitions()
    {
        String table = KEYSPACE + ".rows_past_per_partition_limit";
        cluster.schemaChange("CREATE TABLE " + table + " (k int, c int, l list<int>, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'");
        for (int c : new int[]{ 1, 3, 4 })
            cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, l) VALUES (5, ?, [7]) USING TIMESTAMP 1", ALL, c);
        cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, l) VALUES (5, 2, [7, 7]) USING TIMESTAMP 1", ALL);
        cluster.get(1).executeInternal("DELETE l[0] FROM " + table + " USING TIMESTAMP 2 WHERE k = 5 AND c = 2");
        cluster.get(2).executeInternal("DELETE l[1] FROM " + table + " USING TIMESTAMP 2 WHERE k = 5 AND c = 2");
        cluster.get(1).executeInternal("INSERT INTO " + table + " (k, c, l) VALUES (1, 1, [7]) USING TIMESTAMP 1");
        cluster.get(2).executeInternal("INSERT INTO " + table + " (k, c, l) VALUES (1, 1, [8]) USING TIMESTAMP 2");
        cluster.get(2).executeInternal("INSERT INTO " + table + " (k, c, l) VALUES (0, 1, [7]) USING TIMESTAMP 1");
        cluster.get(1).executeInternal("DELETE FROM " + table + " USING TIMESTAMP 2 WHERE k = 0 AND c = 1");
        cluster.get(1).executeInternal("INSERT INTO " + table + " (k, c, l) VALUES (0, 2, [7]) USING TIMESTAMP 1");

        String query = "SELECT k, c FROM " + table + " WHERE l CONTAINS 7 PER PARTITION LIMIT 2%s ALLOW FILTERING";
        String groupByQuery = "SELECT k, c, count(*) FROM " + table + " WHERE l CONTAINS 7 GROUP BY k, c PER PARTITION LIMIT 2 LIMIT 3 ALLOW FILTERING";
        for (String limit : new String[]{ " LIMIT 3", "" })
        {
            assertRows(cluster.coordinator(1).execute(String.format(query, limit), ALL), row(5, 1), row(5, 3), row(0, 2));
            for (int pageSize = 1; pageSize <= 3; pageSize++)
                assertRows(cluster.coordinator(1).executeWithPaging(String.format(query, limit), ALL, pageSize), row(5, 1), row(5, 3), row(0, 2));
        }
        assertRows(cluster.coordinator(1).execute(groupByQuery, ALL), row(5, 1, 1L), row(5, 3, 1L), row(0, 2, 1L));
        for (int pageSize = 1; pageSize <= 3; pageSize++)
            assertRows(cluster.coordinator(1).executeWithPaging(groupByQuery, ALL, pageSize), row(5, 1, 1L), row(5, 3, 1L), row(0, 2, 1L));
    }

    /**
     * A small row of partition 1 on node 1 only comes before a row of 500 bytes on both nodes, and the second phase
     * returns it in place of that row for PER PARTITION LIMIT 1. Node 1 stops its page of 256 bytes at a row of
     * partition 0 that node 2 has a newer non-match of, so the row of partition 2 needs a short read protection request.
     */
    @Test
    public void testRowOnOneReplicaBeforePerPartitionLimit()
    {
        String table = KEYSPACE + ".row_on_one_replica_before_per_partition_limit";
        cluster.schemaChange("CREATE TABLE " + table + " (k int, c int, v int, t text, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'");
        String text = "x".repeat(500);
        cluster.get(1).executeInternal("INSERT INTO " + table + " (k, c, v, t) VALUES (1, 0, 1, 'a') USING TIMESTAMP 1");
        cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, v, t) VALUES (1, 1, 1, ?) USING TIMESTAMP 1", ALL, text);
        cluster.get(1).executeInternal("INSERT INTO " + table + " (k, c, v, t) VALUES (0, 0, 1, ?) USING TIMESTAMP 1", text);
        cluster.get(2).executeInternal("INSERT INTO " + table + " (k, c, v, t) VALUES (0, 0, 0, ?) USING TIMESTAMP 2", text);
        cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, v, t) VALUES (2, 0, 1, 'z') USING TIMESTAMP 1", ALL);

        assertEquals(2, rowsInPagesOfBytes("SELECT k, c, t FROM " + table + " WHERE v = 1 PER PARTITION LIMIT 1 ALLOW FILTERING", 256));
    }

    /**
     * Partition 1 has a static row of 1200 bytes on both nodes and a row on each node only, so the second phase returns
     * its two rows rather than its static row. Each node stops its page of 1 KiB at a static row that the other node has
     * a newer non-match of, so the row of partition 4 needs a short read protection request.
     */
    @Test
    public void testStaticRowOfPartitionWithRowsOnOneReplica()
    {
        String table = KEYSPACE + ".static_row_with_rows_on_one_replica";
        cluster.schemaChange("CREATE TABLE " + table + " (k int, c int, s text static, v int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'");
        String text = "s".repeat(1200);
        cluster.coordinator(1).execute("INSERT INTO " + table + " (k, s) VALUES (1, ?) USING TIMESTAMP 1", ALL, text);
        cluster.get(1).executeInternal("INSERT INTO " + table + " (k, c, v) VALUES (1, 0, 1) USING TIMESTAMP 1");
        cluster.get(2).executeInternal("INSERT INTO " + table + " (k, c, v) VALUES (1, 1, 1) USING TIMESTAMP 1");
        cluster.get(1).executeInternal("INSERT INTO " + table + " (k, s) VALUES (0, ?) USING TIMESTAMP 1", text);
        cluster.get(2).executeInternal("INSERT INTO " + table + " (k, s) VALUES (0, 'other') USING TIMESTAMP 2");
        cluster.get(2).executeInternal("INSERT INTO " + table + " (k, s) VALUES (2, ?) USING TIMESTAMP 1", text);
        cluster.get(1).executeInternal("INSERT INTO " + table + " (k, s) VALUES (2, 'other') USING TIMESTAMP 2");
        cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, s, v) VALUES (4, 0, ?, 1) USING TIMESTAMP 1", ALL, text);

        assertEquals(3, rowsInPagesOfBytes("SELECT k, c, v FROM " + table + " WHERE s = '" + text + "' ALLOW FILTERING", 1024));
    }

    /**
     * The static list of partition 1 matches on each node but not once reconciled: each node deletes another element of
     * [7, 7, 8]. Its row c = 0 doesn't count, so the row of partition 0, which comes next and is on node 1 only, is
     * returned.
     */
    @Test
    public void testRowsOfPartitionWhoseStaticRowFailsFilterOnceReconciled()
    {
        String table = KEYSPACE + ".static_row_failing_once_reconciled";
        cluster.schemaChange("CREATE TABLE " + table + " (k int, c int, sl list<int> static, v int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'");
        cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, sl, v) VALUES (1, 0, [7, 7, 8], 1) USING TIMESTAMP 1", ALL);
        cluster.get(1).executeInternal("DELETE sl[0] FROM " + table + " USING TIMESTAMP 2 WHERE k = 1");
        cluster.get(2).executeInternal("DELETE sl[1] FROM " + table + " USING TIMESTAMP 2 WHERE k = 1");
        cluster.get(1).executeInternal("INSERT INTO " + table + " (k, c, sl, v) VALUES (0, 0, [7], 1) USING TIMESTAMP 1");

        String query = "SELECT k, c FROM " + table + " WHERE sl CONTAINS 7 LIMIT 1 ALLOW FILTERING";
        assertRows(cluster.coordinator(1).execute(query, ALL), row(0, 0));
        assertRows(cluster.coordinator(1).executeWithPaging(query, ALL, 1), row(0, 0));
        assertRows(cluster.coordinator(1).execute("SELECT k, c, count(*) FROM " + table + " WHERE sl CONTAINS 7 GROUP BY k, c LIMIT 1 ALLOW FILTERING", ALL),
                   row(0, 0, 1L));
    }

    /**
     * Partitions 5, 1 and 0 have only a static list that matches on each node but not once reconciled, so they don't
     * count, and the row of partition 2, which comes next and is on node 1 only, is returned.
     */
    @Test
    public void testStaticOnlyPartitionsFailingFilterOnceReconciled()
    {
        String table = KEYSPACE + ".static_only_failing_once_reconciled";
        cluster.schemaChange("CREATE TABLE " + table + " (k int, c int, sl list<int> static, v int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'");
        for (int k : new int[]{ 5, 1, 0 })
        {
            cluster.coordinator(1).execute("INSERT INTO " + table + " (k, sl) VALUES (?, [7, 7, 8]) USING TIMESTAMP 1", ALL, k);
            cluster.get(1).executeInternal("DELETE sl[0] FROM " + table + " USING TIMESTAMP 2 WHERE k = ?", k);
            cluster.get(2).executeInternal("DELETE sl[1] FROM " + table + " USING TIMESTAMP 2 WHERE k = ?", k);
        }
        cluster.get(1).executeInternal("INSERT INTO " + table + " (k, c, sl, v) VALUES (2, 0, [7], 1) USING TIMESTAMP 1");

        for (int limit = 1; limit <= 2; limit++)
            assertRows(cluster.coordinator(1).execute("SELECT k, c FROM " + table + " WHERE sl CONTAINS 7 LIMIT " + limit + " ALLOW FILTERING", ALL), row(2, 0));
        assertRows(cluster.coordinator(1).execute("SELECT k, count(*) FROM " + table + " WHERE sl CONTAINS 7 GROUP BY k LIMIT 2 ALLOW FILTERING", ALL),
                   row(2, 1L));
    }

    /**
     * Agreeing replicas return the partitions with only a static row that a filter on a static column matches, so short
     * read protection asks them for no more rows, and replica filtering protection caches fewer rows than a fail
     * threshold of 8 for LIMIT 5. Partitions 1, 0, 2 and 4 have only a static row, and partition 3, which comes after
     * them, 16 rows.
     */
    @Test
    public void testStaticOnlyPartitionsOnAgreeingReplicas()
    {
        String table = "static_only_partitions";
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + table + " (k int, c int, s int static, v int, t text, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'");
        for (int k : new int[]{ 0, 1, 2, 4 })
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + table + " (k, s) VALUES (?, 7)", ALL, k);
        for (int c = 0; c < 16; c++)
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + table + " (k, c, s, v, t) VALUES (3, ?, 7, 7, ?)", ALL, c, "x".repeat(500));

        setCachedRowsThresholds(DEFAULT_WARN_THRESHOLD, DEFAULT_FAIL_THRESHOLD);
        assertShortReadProtectionRequests(0, table, "SELECT k, count(*) FROM %s WHERE s = 7 GROUP BY k LIMIT 5 ALLOW FILTERING",
                                          row(1, 1L), row(0, 1L), row(2, 1L), row(4, 1L), row(3, 16L));

        setCachedRowsThresholds(8, 8);
        try
        {
            assertShortReadProtectionRequests(0, table, "SELECT k, c, s FROM %s WHERE s = 7 LIMIT 5 ALLOW FILTERING",
                                              row(1, null, 7), row(0, null, 7), row(2, null, 7), row(4, null, 7), row(3, 0, 7));
        }
        finally
        {
            setCachedRowsThresholds(DEFAULT_WARN_THRESHOLD, DEFAULT_FAIL_THRESHOLD);
        }
    }

    /**
     * Creates a table with partitions 0 and 1 of 16 rows of about 512 bytes each, written to both replicas.
     */
    private String createAgreeingTable(String table)
    {
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + table + " (k int, c int, s int static, v int, t text, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'");
        String text = "x".repeat(500);
        for (int k = 0; k <= 1; k++)
        {
            for (int c = 0; c < 16; c++)
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + table + " (k, c, s, v, t) VALUES (?, ?, 7, 7, ?)", ALL, k, c, text);
        }
        return table;
    }

    /**
     * Reads all the pages of the query on node 1 at ALL, in pages of the given size in bytes, and returns the number of
     * rows.
     */
    private static int rowsInPagesOfBytes(String query, int pageSizeInBytes)
    {
        return cluster.get(1).callOnInstance(() -> {
            SelectStatement statement = (SelectStatement) QueryProcessor.getStatement(query, ClientState.forInternalCalls());
            int rows = 0;
            PagingState pagingState = null;
            do
            {
                QueryOptions options = QueryOptions.create(ConsistencyLevel.ALL, Collections.emptyList(), false, PageSize.inBytes(pageSizeInBytes),
                                                           pagingState, null, ProtocolVersion.CURRENT, null);
                ResultMessage.Rows page = statement.execute(QueryState.forInternalCalls(), options, System.nanoTime());
                rows += page.result.rows.size();
                pagingState = page.result.metadata.getPagingState();
            }
            while (pagingState != null);
            return rows;
        });
    }

    private void setCachedRowsThresholds(int warnThreshold, int failThreshold)
    {
        cluster.get(1).runOnInstance(() -> StorageService.instance.setCachedReplicaRowsWarnThreshold(warnThreshold));
        cluster.get(1).runOnInstance(() -> StorageService.instance.setCachedReplicaRowsFailThreshold(failThreshold));
    }

    private void assertShortReadProtectionRequests(long expected, String table, String query, Object[]... rows)
    {
        String formattedQuery = String.format(query, KEYSPACE + '.' + table);
        long requests = shortReadProtectionRequests(table);
        assertRows(cluster.coordinator(1).execute(formattedQuery, ALL), rows);
        assertEquals(formattedQuery, expected, shortReadProtectionRequests(table) - requests);
    }

    private long shortReadProtectionRequests(String tableName)
    {
        return cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE)
                                                           .getColumnFamilyStore(tableName)
                                                           .metric.shortReadProtectionRequests.getCount());
    }

    private void testMissedUpdates(String tableName, int warnThreshold, int failThreshold, boolean shouldWarn)
    {
        cluster.get(1).runOnInstance(() -> StorageService.instance.setCachedReplicaRowsWarnThreshold(warnThreshold));
        cluster.get(1).runOnInstance(() -> StorageService.instance.setCachedReplicaRowsFailThreshold(failThreshold));

        String fullTableName = KEYSPACE + '.' + tableName;

        // Case 1: Insert and query rows at ALL to verify baseline.
        for (int i = 0; i < ROWS; i++)
        {
            cluster.coordinator(1).execute("INSERT INTO " + fullTableName + "(k, v) VALUES (?, 'old')", ALL, i);
        }

        long histogramSampleCount = rowsCachedPerQueryCount(cluster.get(1), tableName);

        String query = "SELECT * FROM " + fullTableName + " WHERE v = ? LIMIT ? ALLOW FILTERING";

        Object[][] initialRows = cluster.coordinator(1).execute(query, ALL, "old", ROWS);
        assertRows(initialRows, row(1, "old"), row(0, "old"), row(2, "old"));

        // Make sure only one sample was recorded for the query.
        assertEquals(histogramSampleCount + 1, rowsCachedPerQueryCount(cluster.get(1), tableName));

        // Case 2: Update all rows on only one replica, leaving the entire dataset of the remaining replica out-of-date.
        updateAllRowsOn(1, fullTableName, "new");

        // The replica that missed the results creates a mismatch at every row, and we therefore cache a version
        // of that row for all replicas.
        SimpleQueryResult oldResult = cluster.coordinator(1).executeWithResult(query, ALL, "old", ROWS);
        assertRows(oldResult.toObjectArrays());
        verifyWarningState(shouldWarn, ROWS * REPLICAS, oldResult);

        // We should have made 3 row "completion" requests.
        assertEquals(ROWS, protectionQueryCount(cluster.get(1), tableName));

        // In all cases above, the queries should be caching 1 row per partition per replica, but
        // 6 for the whole query, given every row is potentially stale.
        assertEquals(ROWS * REPLICAS, maxRowsCachedPerQuery(cluster.get(1), tableName));

        // Make sure only one more sample was recorded for the query.
        assertEquals(histogramSampleCount + 2, rowsCachedPerQueryCount(cluster.get(1), tableName));

        // Case 3: Observe the effects of blocking read-repair.

        // The previous query peforms a blocking read-repair, which removes replica divergence. This
        // will only warn, therefore, if the warning threshold is actually below the number of replicas.
        // (i.e. The row cache counter is decremented/reset as each partition is consumed.)
        SimpleQueryResult newResult = cluster.coordinator(1).executeWithResult(query, ALL, "new", ROWS);
        assertRows(newResult.toObjectArrays(), row(1, "new"), row(0, "new"), row(2, "new"));
        verifyWarningState(warnThreshold < REPLICAS, REPLICAS, newResult);

        // We still sould only have made 3 row "completion" requests, with no replica divergence in the last query.
        assertEquals(ROWS, protectionQueryCount(cluster.get(1), tableName));

        // With no replica divergence, we only cache a single partition at a time across 2 replicas.
        assertEquals(REPLICAS, minRowsCachedPerQuery(cluster.get(1), tableName));

        // Make sure only one more sample was recorded for the query.
        assertEquals(histogramSampleCount + 3, rowsCachedPerQueryCount(cluster.get(1), tableName));

        // Case 4: Introduce another mismatch by updating all rows on only one replica.

        updateAllRowsOn(1, fullTableName, "future");

        // Another mismatch is introduced, and we once again cache a version of each row during resolution.
        SimpleQueryResult futureResult = cluster.coordinator(1).executeWithResult(query, ALL, "future", ROWS);
        assertRows(futureResult.toObjectArrays(), row(1, "future"), row(0, "future"), row(2, "future"));
        verifyWarningState(shouldWarn, ROWS * REPLICAS, futureResult);

        // We sould have made 3 more row "completion" requests.
        assertEquals(ROWS * 2, protectionQueryCount(cluster.get(1), tableName));

        // In all cases above, the queries should be caching 1 row per partition, but 6 for the
        // whole query, given every row is potentially stale.
        assertEquals(ROWS * REPLICAS, maxRowsCachedPerQuery(cluster.get(1), tableName));

        // Make sure only one more sample was recorded for the query.
        assertEquals(histogramSampleCount + 4, rowsCachedPerQueryCount(cluster.get(1), tableName));
    }

    private void updateAllRowsOn(int node, String table, String value)
    {
        for (int i = 0; i < ROWS; i++)
        {
            cluster.get(node).executeInternal("UPDATE " + table + " SET v = ? WHERE k = ?", value, i);
        }
    }

    private void verifyWarningState(boolean shouldWarn, int cached, SimpleQueryResult result)
    {
        ListAssert<String> warnings = Assertions.assertThat(result.warnings());
        if (shouldWarn)
        {
            warnings.hasSize(1)
                    .anyMatch(w -> w.contains("Replica filtering protection has cached up to " + cached + " rows"))
                    .anyMatch(w -> w.contains("cached_replica_rows_warn_threshold"));
        }
        else
        {
            warnings.isEmpty();
        }
    }

    private long protectionQueryCount(IInvokableInstance instance, String tableName)
    {
        return instance.callOnInstance(() -> Keyspace.open(KEYSPACE)
                                                     .getColumnFamilyStore(tableName)
                                                     .metric.replicaFilteringProtectionRequests.getCount());
    }

    private long maxRowsCachedPerQuery(IInvokableInstance instance, String tableName)
    {
        return instance.callOnInstance(() -> Keyspace.open(KEYSPACE)
                                                     .getColumnFamilyStore(tableName)
                                                     .metric.rfpRowsCachedPerQuery.getSnapshot().getMax());
    }

    private long minRowsCachedPerQuery(IInvokableInstance instance, String tableName)
    {
        return instance.callOnInstance(() -> Keyspace.open(KEYSPACE)
                                                     .getColumnFamilyStore(tableName)
                                                     .metric.rfpRowsCachedPerQuery.getSnapshot().getMin());
    }

    private long rowsCachedPerQueryCount(IInvokableInstance instance, String tableName)
    {
        return instance.callOnInstance(() -> Keyspace.open(KEYSPACE)
                                                     .getColumnFamilyStore(tableName)
                                                     .metric.rfpRowsCachedPerQuery.getCount());
    }
}
