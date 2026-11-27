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
import java.util.List;

import com.google.common.collect.Iterators;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.upgrade.BytePagingReads;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.service.StorageService;
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
    private static final int PARTITIONS = 3;
    private static final int ROWS_PER_PARTITION = 3;

    private static Cluster cluster;

    @BeforeClass
    public static void setup() throws IOException
    {
        cluster = init(Cluster.build()
                              .withNodes(REPLICAS)
                              .withConfig(config -> config.set("hinted_handoff_enabled", false)).start());

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
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + tableName + " (k int, c int, v text, PRIMARY KEY (k, c))"));

        // The warning threshold provided is one more than the total number of rows returned
        // to the coordinator from all replicas and therefore should not be triggered.
        testMissedUpdates(tableName, REPLICAS * ROWS_PER_PARTITION, Integer.MAX_VALUE, false);
    }

    @Test
    public void testMissedUpdatesAboveCachingWarnThreshold()
    {
        String tableName = "missed_updates_cache_warn";
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + tableName + " (k int, c int, v text, PRIMARY KEY (k, c))"));

        // The warning threshold provided is one less than the total number of rows returned
        // to the coordinator from all replicas and therefore should be triggered but not fail the query.
        testMissedUpdates(tableName, REPLICAS * ROWS_PER_PARTITION - 1, Integer.MAX_VALUE, true);
    }

    @Test
    public void testMissedUpdatesAroundCachingFailThreshold()
    {
        String tableName = "missed_updates_cache_fail";
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + tableName + " (k int, c int, v text, PRIMARY KEY (k, c))"));

        // The failure threshold provided is exactly the total number of rows returned
        // to the coordinator from all replicas and therefore should just warn.
        testMissedUpdates(tableName, 1, REPLICAS * ROWS_PER_PARTITION, true);

        try
        {
            // The failure threshold provided is one less than the total number of rows returned
            // to the coordinator from all replicas and therefore should fail the query.
            testMissedUpdates(tableName, 1, REPLICAS * ROWS_PER_PARTITION - 1, true);
        }
        catch (RuntimeException e)
        {
            assertEquals(e.getClass().getName(), OverloadedException.class.getName());
            Assertions.assertThat(e)
                      .hasMessageContaining("Replica filtering protection has cached " + REPLICAS * ROWS_PER_PARTITION)
                      .hasMessageContaining("cached_replica_rows_fail_threshold");
        }
    }

    /**
     * Replica filtering protection splits the partitions of single-partition queries into pages of at most the query's
     * row limit (the page size or the LIMIT). The per-partition limit must still apply to the whole partition, not to
     * each page.
     */
    @Test
    public void testPerPartitionLimitAcrossPages()
    {
        String table = createRangeTombstoneTable("ppl_across_pages");

        // Matches at c = 0 to 3 on both replicas, which disagree on a non-filtered value, so the digests mismatch
        for (int c = 0; c < 4; c++)
        {
            write(1, "INSERT INTO %s (k, c, v) VALUES (0, " + c + ", 1) USING TIMESTAMP 1", table);
            write(2, "INSERT INTO %s (k, c, v) VALUES (0, " + c + ", 1) USING TIMESTAMP 1", table);
        }
        cluster.schemaChange("ALTER TABLE " + table + " ADD x int");
        write(2, "UPDATE %s USING TIMESTAMP 2 SET x = 1 WHERE k = 0 AND c = 0", table);

        for (int pageSize : new int[]{ 1, 2, 3, 4 })
        {
            assertRows(cluster.coordinator(1).executeWithPaging("SELECT c FROM " + table + " WHERE k = 0 AND v = 1 PER PARTITION LIMIT 1 ALLOW FILTERING", ALL, pageSize),
                       row(0));
            assertRows(cluster.coordinator(1).executeWithPaging("SELECT c FROM " + table + " WHERE k = 0 AND v = 1 PER PARTITION LIMIT 2 ALLOW FILTERING", ALL, pageSize),
                       row(0), row(1));
            assertRows(cluster.coordinator(1).executeWithPaging("SELECT c FROM " + table + " WHERE k = 0 AND v = 1 ALLOW FILTERING", ALL, pageSize),
                       row(0), row(1), row(2), row(3));
            assertRows(cluster.coordinator(1).executeWithPaging("SELECT c FROM " + table + " WHERE k = 0 AND v = 1 PER PARTITION LIMIT 1 LIMIT 2 ALLOW FILTERING", ALL, pageSize),
                       row(0));
        }

        // Without paging, a LIMIT above the per-partition limit splits the partition the same way
        assertRows(cluster.coordinator(1).execute("SELECT c FROM " + table + " WHERE k = 0 AND v = 1 PER PARTITION LIMIT 1 LIMIT 2 ALLOW FILTERING", ALL),
                   row(0));
        assertRows(cluster.coordinator(1).execute("SELECT c FROM " + table + " WHERE k = 0 AND v = 1 PER PARTITION LIMIT 2 LIMIT 3 ALLOW FILTERING", ALL),
                   row(0), row(1));
    }

    @Test
    public void testRangeTombstoneAcrossPages()
    {
        String table = createRangeTombstoneTable("rt_across_pages");

        // Node 2 has stale matches at c = 0 and c = 5. Node 1 overwrites c = 0 and deletes c = 5 with a range
        // tombstone that also covers newer live matches at c = 1 and c = 7.
        write(2, "INSERT INTO %s (k, c, v) VALUES (0, 0, 1) USING TIMESTAMP 1", table);
        write(2, "INSERT INTO %s (k, c, v) VALUES (0, 5, 1) USING TIMESTAMP 1", table);
        write(1, "INSERT INTO %s (k, c, v) VALUES (0, 0, 0) USING TIMESTAMP 2", table);
        write(1, "DELETE FROM %s USING TIMESTAMP 2 WHERE k = 0 AND c > 0 AND c <= 10", table);
        write(1, "INSERT INTO %s (k, c, v) VALUES (0, 1, 1) USING TIMESTAMP 3", table);
        write(1, "INSERT INTO %s (k, c, v) VALUES (0, 7, 1) USING TIMESTAMP 3", table);

        // Replica filtering protection's first page of LIMIT rows ends at c = 1, inside the range tombstone, then drops
        // c = 0. The next page, ending at c = 7, must still see the range tombstone that deletes c = 5.
        assertRows(cluster.coordinator(1).execute("SELECT c FROM " + table + " WHERE k = 0 AND v = 1 LIMIT 2 ALLOW FILTERING", ALL),
                   row(1), row(7));
    }

    @Test
    public void testRangeTombstoneAcrossPagesReversed()
    {
        String table = createRangeTombstoneTable("rt_across_pages_reversed");

        write(2, "INSERT INTO %s (k, c, v) VALUES (0, 10, 1) USING TIMESTAMP 1", table);
        write(2, "INSERT INTO %s (k, c, v) VALUES (0, 5, 1) USING TIMESTAMP 1", table);
        write(1, "INSERT INTO %s (k, c, v) VALUES (0, 10, 0) USING TIMESTAMP 2", table);
        write(1, "DELETE FROM %s USING TIMESTAMP 2 WHERE k = 0 AND c > 0 AND c <= 9", table);
        write(1, "INSERT INTO %s (k, c, v) VALUES (0, 8, 1) USING TIMESTAMP 3", table);
        write(1, "INSERT INTO %s (k, c, v) VALUES (0, 2, 1) USING TIMESTAMP 3", table);

        // In descending order the first page ends at c = 8, inside the range tombstone that deletes c = 5.
        assertRows(cluster.coordinator(1).execute("SELECT c FROM " + table + " WHERE k = 0 AND v = 1 ORDER BY c DESC LIMIT 2 ALLOW FILTERING", ALL),
                   row(8), row(2));
    }

    @Test
    public void testRangeTombstoneAcrossPagesWithoutRowsFromDeletingReplica()
    {
        String table = createRangeTombstoneTable("rt_across_pages_without_rows");

        write(2, "INSERT INTO %s (k, c, v) VALUES (0, 0, 1) USING TIMESTAMP 1", table);
        write(2, "INSERT INTO %s (k, c, v) VALUES (0, 4, 1) USING TIMESTAMP 1", table);
        write(1, "INSERT INTO %s (k, c, v) VALUES (0, 0, 0) USING TIMESTAMP 2", table);
        write(1, "DELETE FROM %s USING TIMESTAMP 2 WHERE k = 0 AND c > 0 AND c <= 10", table);
        write(1, "INSERT INTO %s (k, c, v) VALUES (0, 1, 1) USING TIMESTAMP 3", table);
        write(2, "INSERT INTO %s (k, c, v) VALUES (0, 5, 1) USING TIMESTAMP 4", table);
        write(2, "INSERT INTO %s (k, c, v) VALUES (0, 6, 1) USING TIMESTAMP 4", table);

        // The second page holds nothing from node 1, which must still delete c = 4 there.
        assertRows(cluster.coordinator(1).execute("SELECT c FROM " + table + " WHERE k = 0 AND v = 1 LIMIT 2 ALLOW FILTERING", ALL),
                   row(1), row(5));
    }

    @Test
    public void testRangeTombstoneBoundaryAtPageEnd()
    {
        String table = createRangeTombstoneTable("rt_boundary_at_page_end");
        // Blocking read repair also merges the replicas' protected partitions, including the markers closed at page ends.
        cluster.schemaChange("ALTER TABLE " + table + " WITH read_repair = 'BLOCKING'");

        write(2, "INSERT INTO %s (k, c, v) VALUES (0, 0, 1) USING TIMESTAMP 1", table);
        write(2, "INSERT INTO %s (k, c, v) VALUES (0, 5, 1) USING TIMESTAMP 1", table);
        write(1, "INSERT INTO %s (k, c, v) VALUES (0, 0, 0) USING TIMESTAMP 2", table);
        write(1, "DELETE FROM %s USING TIMESTAMP 2 WHERE k = 0 AND c > 0 AND c <= 1", table);
        write(1, "DELETE FROM %s USING TIMESTAMP 4 WHERE k = 0 AND c > 1 AND c <= 10", table);
        write(1, "INSERT INTO %s (k, c, v) VALUES (0, 1, 1) USING TIMESTAMP 3", table);
        write(1, "INSERT INTO %s (k, c, v) VALUES (0, 7, 1) USING TIMESTAMP 5", table);

        // The first page ends at c = 1, where node 1 has a boundary between two range tombstones.
        assertRows(cluster.coordinator(1).execute("SELECT c FROM " + table + " WHERE k = 0 AND v = 1 LIMIT 2 ALLOW FILTERING", ALL),
                   row(1), row(7));
    }

    @Test
    public void testRangeTombstoneBoundaryOpensRangeThatSpansPageEnd()
    {
        String table = createRangeTombstoneTable("rt_boundary_opens_range_across_pages");

        // Node 2 has a stale match at c = 5. Node 1 has two adjacent ranges meeting at a boundary at c = 1: the
        // first closes there, the second opens there and extends to cover the newer live match at c = 7. The
        // boundary itself is consumed within the first page (to reach c = 2), but the range it opens must still
        // be remembered in the second page, where node 1 contributes nothing until c = 7, to shadow node 2's c = 5.
        write(2, "INSERT INTO %s (k, c, v) VALUES (0, 0, 1) USING TIMESTAMP 1", table);
        write(2, "INSERT INTO %s (k, c, v) VALUES (0, 5, 1) USING TIMESTAMP 1", table);
        write(1, "INSERT INTO %s (k, c, v) VALUES (0, 0, 0) USING TIMESTAMP 2", table);
        write(1, "DELETE FROM %s USING TIMESTAMP 2 WHERE k = 0 AND c > 0 AND c <= 1", table);
        write(1, "DELETE FROM %s USING TIMESTAMP 4 WHERE k = 0 AND c > 1 AND c <= 10", table);
        write(1, "INSERT INTO %s (k, c, v) VALUES (0, 1, 1) USING TIMESTAMP 3", table);
        write(1, "INSERT INTO %s (k, c, v) VALUES (0, 2, 1) USING TIMESTAMP 6", table);
        write(1, "INSERT INTO %s (k, c, v) VALUES (0, 7, 1) USING TIMESTAMP 5", table);

        // The first page ends at c = 2, after the boundary at c = 1 opened the range that must shadow c = 5 in
        // the second page.
        assertRows(cluster.coordinator(1).execute("SELECT c FROM " + table + " WHERE k = 0 AND v = 1 LIMIT 3 ALLOW FILTERING", ALL),
                   row(1), row(2), row(7));
    }

    @Test
    public void testPagedPartitionReadWithMissedUpdates()
    {
        String table = createTable("paged_missed_updates", "(k int, c1 int, c2 int, s int static, v int, PRIMARY KEY (k, c1, c2))");

        // Node 2 has two matching rows, node 1 newer versions of both that don't match.
        write(2, "INSERT INTO %s (k, c1, c2, v, s) VALUES (0, 1, 0, 2, 1) USING TIMESTAMP 1", table);
        write(2, "INSERT INTO %s (k, c1, c2, v, s) VALUES (0, 1, 1, 2, 2) USING TIMESTAMP 40", table);
        write(1, "INSERT INTO %s (k, c1, c2, v) VALUES (0, 1, 0, 0) USING TIMESTAMP 59", table);
        write(1, "INSERT INTO %s (k, c1, c2, v, s) VALUES (0, 1, 1, 0, 6) USING TIMESTAMP 184", table);

        // Paged by 1, each page reads the partition with a limit, so it is completed at several checkpoints, and
        // each of them can add a row to read by name from node 1.
        String query = "SELECT c1, c2, v, s FROM " + table + " WHERE k = 0 AND c2 < 3 AND v > 0 ORDER BY c1 ASC ALLOW FILTERING";
        assertRows(cluster.coordinator(1).execute(query, ALL));
        assertRows(Iterators.toArray(cluster.coordinator(1).executeWithPaging(query, ALL, 1), Object[].class));
    }

    @Test
    public void testNoShortReadProtectionWhenReturnedRowsFillLimit()
    {
        String tableName = "no_srp_when_rows_fill_limit";
        String table = createTable(tableName, "(k int, c int, v int, PRIMARY KEY (k, c))");

        // The same rows on both replicas, all of them match
        for (int k = 0; k < PARTITIONS; k++)
            for (int c = 0; c < 10; c++)
                cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, v) VALUES (?, ?, 1)", ALL, k, c);

        // The rows both replicas return fill the limit, so neither replica is asked for the rest of the partition
        assertRows(cluster.coordinator(1).execute("SELECT k, c FROM " + table + " WHERE v = 1 LIMIT 2 ALLOW FILTERING", ALL),
                   row(1, 0), row(1, 1));
        assertEquals(0, shortReadProtectionRequestCount(cluster.get(1), tableName));
        assertEquals(0, protectionQueryCount(cluster.get(1), tableName));
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
        String tableName = "srp_on_agreeing_replicas";
        String table = createAgreeingTable(tableName);
        int subPageSize = cluster.get(1).callOnInstance(() -> DatabaseDescriptor.getAggregationSubPageSize().bytes());
        cluster.get(1).runOnInstance(BytePagingReads.aggregationSubPageBytes(1024));
        try
        {
            assertShortReadProtectionRequests(REPLICAS, tableName, "SELECT k, c FROM " + table + " WHERE v = 7 PER PARTITION LIMIT 2 ALLOW FILTERING",
                                              row(1, 0), row(1, 1), row(0, 0), row(0, 1));
            assertShortReadProtectionRequests(0, tableName, "SELECT k, c FROM " + table + " WHERE s = 7 LIMIT 5 ALLOW FILTERING",
                                              row(1, 0), row(1, 1), row(1, 2), row(1, 3), row(1, 4));
            assertShortReadProtectionRequests(0, tableName, "SELECT count(*) FROM " + table + " WHERE v = 7 ALLOW FILTERING",
                                              row(32L));
            assertShortReadProtectionRequests(0, tableName, "SELECT k, count(*) FROM " + table + " WHERE v = 7 GROUP BY k ALLOW FILTERING",
                                              row(1, 16L), row(0, 16L));

            long requests = shortReadProtectionRequestCount(cluster.get(1), tableName);
            assertEquals(32, rowsInPagesOfBytes("SELECT k, c, t FROM " + table + " WHERE v = 7 ALLOW FILTERING", 256));
            assertEquals(requests, shortReadProtectionRequestCount(cluster.get(1), tableName));
        }
        finally
        {
            cluster.get(1).runOnInstance(BytePagingReads.aggregationSubPageBytes(subPageSize));
        }
    }

    /**
     * A small row of partition 1 on node 1 only comes before a row of 500 bytes on both nodes, and the second phase
     * returns it in place of that row for PER PARTITION LIMIT 1. Node 1 stops its page of 256 bytes at a row of
     * partition 0 that node 2 has a newer non-match of, so the row of partition 2 needs a short read protection request.
     */
    @Test
    public void testRowOnOneReplicaBeforePerPartitionLimit()
    {
        String table = createTable("row_on_one_replica_before_per_partition_limit", "(k int, c int, v int, t text, PRIMARY KEY (k, c))");
        String text = "x".repeat(500);
        write(1, "INSERT INTO %s (k, c, v, t) VALUES (1, 0, 1, 'a') USING TIMESTAMP 1", table);
        cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, v, t) VALUES (1, 1, 1, ?) USING TIMESTAMP 1", ALL, text);
        write(1, "INSERT INTO %s (k, c, v, t) VALUES (0, 0, 1, '" + text + "') USING TIMESTAMP 1", table);
        write(2, "INSERT INTO %s (k, c, v, t) VALUES (0, 0, 0, '" + text + "') USING TIMESTAMP 2", table);
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
        String table = createTable("static_row_with_rows_on_one_replica", "(k int, c int, s text static, v int, PRIMARY KEY (k, c))");
        String text = "s".repeat(1200);
        cluster.coordinator(1).execute("INSERT INTO " + table + " (k, s) VALUES (1, ?) USING TIMESTAMP 1", ALL, text);
        write(1, "INSERT INTO %s (k, c, v) VALUES (1, 0, 1) USING TIMESTAMP 1", table);
        write(2, "INSERT INTO %s (k, c, v) VALUES (1, 1, 1) USING TIMESTAMP 1", table);
        write(1, "INSERT INTO %s (k, s) VALUES (0, '" + text + "') USING TIMESTAMP 1", table);
        write(2, "INSERT INTO %s (k, s) VALUES (0, 'other') USING TIMESTAMP 2", table);
        write(2, "INSERT INTO %s (k, s) VALUES (2, '" + text + "') USING TIMESTAMP 1", table);
        write(1, "INSERT INTO %s (k, s) VALUES (2, 'other') USING TIMESTAMP 2", table);
        cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, s, v) VALUES (4, 0, ?, 1) USING TIMESTAMP 1", ALL, text);

        assertEquals(3, rowsInPagesOfBytes("SELECT k, c, v FROM " + table + " WHERE s = '" + text + "' ALLOW FILTERING", 1024));
    }

    /**
     * Node 2 has an older static row s = 0 of partition 1, so it doesn't return the small row c = 0 that matches s = 7
     * OR v = 1 with the newer s = 7 of node 1, and both nodes return the row c = 1 of 500 bytes. The second phase reads
     * partition 1 from node 2 and returns c = 0 in place of c = 1 for PER PARTITION LIMIT 1, so the row of partition 0
     * needs a short read protection request.
     */
    @Test
    public void testStricterStaticRowBeforePerPartitionLimit()
    {
        String table = createTable("stricter_static_row_before_per_partition_limit", "(k int, c int, s int static, v int, t text, PRIMARY KEY (k, c))");
        write(1, "INSERT INTO %s (k, s) VALUES (1, 7) USING TIMESTAMP 2", table);
        write(2, "INSERT INTO %s (k, s) VALUES (1, 0) USING TIMESTAMP 1", table);
        write(2, "INSERT INTO %s (k, c, v, t) VALUES (1, 0, 0, 'a') USING TIMESTAMP 1", table);
        cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, v, t) VALUES (1, 1, 1, ?) USING TIMESTAMP 1", ALL, "x".repeat(500));
        cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, s, v, t) VALUES (0, 0, 7, 1, 'z') USING TIMESTAMP 1", ALL);

        assertEquals(2, rowsInPagesOfBytes("SELECT k, c, t FROM " + table + " WHERE s = 7 OR v = 1 PER PARTITION LIMIT 1 ALLOW FILTERING", 256));
    }

    /**
     * Partition 1 has a row on both nodes and a static list [7, 7, 8] from which each node deletes another 7, so it
     * matches sl CONTAINS 7 on each node but not once reconciled. Partition 0, next in token order, has a matching row
     * on node 1 only.
     */
    @Test
    public void testRowsOfPartitionWhoseStaticRowFailsFilterOnceReconciled()
    {
        String table = createTable("rows_static_row_failing_once_reconciled", "(k int, c int, sl list<int> static, PRIMARY KEY (k, c))");
        cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, sl) VALUES (1, 0, [7, 7, 8]) USING TIMESTAMP 1", ALL);
        write(1, "DELETE sl[0] FROM %s USING TIMESTAMP 2 WHERE k = 1", table);
        write(2, "DELETE sl[1] FROM %s USING TIMESTAMP 2 WHERE k = 1", table);
        write(1, "INSERT INTO %s (k, c, sl) VALUES (0, 0, [7]) USING TIMESTAMP 1", table);

        String query = "SELECT k, c FROM " + table + " WHERE sl CONTAINS 7 LIMIT 1 ALLOW FILTERING";
        assertRows(cluster.coordinator(1).execute(query, ALL), row(0, 0));
        assertRows(cluster.coordinator(1).executeWithPaging(query, ALL, 1), row(0, 0));
        assertRows(cluster.coordinator(1).execute("SELECT k, c, count(*) FROM " + table + " WHERE sl CONTAINS 7 GROUP BY k, c LIMIT 1 ALLOW FILTERING", ALL),
                   row(0, 0, 1L));
    }

    /**
     * Partitions 1, 0 and 2 have only a static list [7, 7, 8] from which each node deletes another 7, so it matches
     * sl CONTAINS 7 on each node but not once reconciled. Partition 4, next in token order, has a matching row on node 1
     * only.
     */
    @Test
    public void testStaticOnlyPartitionsFailingFilterOnceReconciled()
    {
        String table = createTable("static_only_failing_once_reconciled", "(k int, c int, sl list<int> static, PRIMARY KEY (k, c))");
        for (int k : new int[]{ 1, 0, 2 })
        {
            cluster.coordinator(1).execute("INSERT INTO " + table + " (k, sl) VALUES (?, [7, 7, 8]) USING TIMESTAMP 1", ALL, k);
            write(1, "DELETE sl[0] FROM %s USING TIMESTAMP 2 WHERE k = " + k, table);
            write(2, "DELETE sl[1] FROM %s USING TIMESTAMP 2 WHERE k = " + k, table);
        }
        write(1, "INSERT INTO %s (k, c, sl) VALUES (4, 0, [7]) USING TIMESTAMP 1", table);

        assertRows(cluster.coordinator(1).execute("SELECT k, c FROM " + table + " WHERE sl CONTAINS 7 LIMIT 1 ALLOW FILTERING", ALL), row(4, 0));
        assertRows(cluster.coordinator(1).execute("SELECT k, c FROM " + table + " WHERE sl CONTAINS 7 LIMIT 2 ALLOW FILTERING", ALL), row(4, 0));
        assertRows(cluster.coordinator(1).execute("SELECT k, count(*) FROM " + table + " WHERE sl CONTAINS 7 GROUP BY k LIMIT 2 ALLOW FILTERING", ALL),
                   row(4, 1L));
    }

    /**
     * Rows c = 0 of partitions 1 and 0 have a list [7, 7, 8] from which each node deletes another 7, so they match
     * l CONTAINS 7 on each node but not once reconciled. Partition 1 has c = 1, 2 and 3 on both nodes, and partition 0
     * c = 1, so short read protection reads c = 2 and c = 3 of partition 1 from both nodes.
     */
    @Test
    public void testRowsAfterPerPartitionLimitMatchingOnEachNode()
    {
        String table = createTable("rows_after_ppl_matching_on_each_node", "(k int, c int, l list<int>, PRIMARY KEY (k, c))");
        for (int k : new int[]{ 1, 0 })
        {
            cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, l) VALUES (?, 0, [7, 7, 8]) USING TIMESTAMP 1", ALL, k);
            write(1, "DELETE l[0] FROM %s USING TIMESTAMP 2 WHERE k = " + k + " AND c = 0", table);
            write(2, "DELETE l[1] FROM %s USING TIMESTAMP 2 WHERE k = " + k + " AND c = 0", table);
        }
        for (int c = 1; c <= 3; c++)
            cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, l) VALUES (1, ?, [7]) USING TIMESTAMP 1", ALL, c);
        cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, l) VALUES (0, 1, [7]) USING TIMESTAMP 1", ALL);

        assertRows(cluster.coordinator(1).execute("SELECT k, c FROM " + table + " WHERE l CONTAINS 7 PER PARTITION LIMIT 2 LIMIT 3 ALLOW FILTERING", ALL),
                   row(1, 1), row(1, 2), row(0, 1));
        assertRows(cluster.coordinator(1).execute("SELECT k, c, count(*) FROM " + table + " WHERE l CONTAINS 7 GROUP BY k, c PER PARTITION LIMIT 2 LIMIT 3 ALLOW FILTERING", ALL),
                   row(1, 1, 1L), row(1, 2, 1L), row(0, 1, 1L));
    }

    /**
     * Partition 1 has a static row s = 7 and c = 0 and 1 on both nodes, c = 2 on node 1 only and c = 3 on node 2 only,
     * each deleted later on the other node, and partition 0 has s = 7 and c = 0 and 1 on both nodes. Each node stops
     * at LIMIT 4 before the last row of partition 0, and c = 2 and c = 3 are reconciled away, so short read protection
     * must read that row.
     */
    @Test
    public void testStaticRowOfPartitionWithCountedRows()
    {
        String table = createTable("static_row_with_counted_rows", "(k int, c int, s int static, v int, PRIMARY KEY (k, c))");
        for (int k : new int[]{ 1, 0 })
            for (int c = 0; c < 2; c++)
                cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, s, v) VALUES (?, ?, 7, 1) USING TIMESTAMP 1", ALL, k, c);
        write(1, "INSERT INTO %s (k, c, v) VALUES (1, 2, 1) USING TIMESTAMP 1", table);
        write(2, "DELETE FROM %s USING TIMESTAMP 2 WHERE k = 1 AND c = 2", table);
        write(2, "INSERT INTO %s (k, c, v) VALUES (1, 3, 1) USING TIMESTAMP 1", table);
        write(1, "DELETE FROM %s USING TIMESTAMP 2 WHERE k = 1 AND c = 3", table);

        assertRows(cluster.coordinator(1).execute("SELECT k, c FROM " + table + " WHERE s = 7 LIMIT 4 ALLOW FILTERING", ALL),
                   row(1, 0), row(1, 1), row(0, 0), row(0, 1));
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
        cluster.get(1).runOnInstance(() -> StorageService.instance.setCachedReplicaRowsFailThreshold(8));
        assertRows(cluster.coordinator(1).execute("SELECT k, c FROM " + table + " WHERE v = 7 PER PARTITION LIMIT 1 ALLOW FILTERING", ALL),
                   row(1, 0), row(0, 0));
        assertRows(cluster.coordinator(1).execute("SELECT k, c FROM " + table + " WHERE s = 7 LIMIT 1 ALLOW FILTERING", ALL),
                   row(1, 0));
        assertEquals(32, rowsInPagesOfBytes("SELECT k, c, t FROM " + table + " WHERE v = 7 ALLOW FILTERING", 256));
    }

    /**
     * Partitions 0 to 3 have only a static row, and partition 4 has 16 rows, on both replicas. The partitions without
     * rows whose static row matches the filter count towards the limit, so short read protection asks the replicas
     * for no more rows.
     */
    @Test
    public void testNoShortReadProtectionRequestsOnStaticOnlyPartitions()
    {
        String tableName = "srp_on_static_only_partitions";
        String table = createTable(tableName, "(k int, c int, s int static, v int, PRIMARY KEY (k, c))");
        for (int k = 0; k < 4; k++)
            cluster.coordinator(1).execute("INSERT INTO " + table + " (k, s) VALUES (?, 7)", ALL, k);
        for (int c = 0; c < 16; c++)
            cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, s, v) VALUES (4, ?, 7, 7)", ALL, c);

        assertShortReadProtectionRequests(0, tableName, "SELECT k, c FROM " + table + " WHERE s = 7 LIMIT 5 ALLOW FILTERING",
                                          row(1, null), row(0, null), row(2, null), row(4, 0), row(4, 1));
        assertShortReadProtectionRequests(0, tableName, "SELECT k, c FROM " + table + " WHERE s = 7 OR v = 8 LIMIT 5 ALLOW FILTERING",
                                          row(1, null), row(0, null), row(2, null), row(4, 0), row(4, 1));
    }

    /**
     * Rows that both replicas return after the rows that fill the per-partition limit don't count towards the limit.
     * Partition 1 has c = 0, 3, 4 and 5 on both nodes, and c = 1 and c = 2 each match on one node and not on the other,
     * so that short read protection reads c = 3 and c = 4 from both. Partition 0, next in token order, has such rows
     * c = 0 and c = 1 before c = 2 and c = 3.
     */
    @Test
    public void testRowsAfterPerPartitionLimit()
    {
        String table = createTable("rows_after_per_partition_limit", "(k int, c int, v int, PRIMARY KEY (k, c))");
        for (int c : new int[]{ 0, 3, 4, 5 })
            cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, v) VALUES (1, ?, 1) USING TIMESTAMP 1", ALL, c);
        for (int c : new int[]{ 2, 3 })
            cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, v) VALUES (0, ?, 1) USING TIMESTAMP 1", ALL, c);
        for (int k : new int[]{ 1, 0 })
        {
            int c = k == 1 ? 1 : 0;
            write(1, "INSERT INTO %s (k, c, v) VALUES (" + k + ", " + c + ", 1) USING TIMESTAMP 1", table);
            write(2, "INSERT INTO %s (k, c, v) VALUES (" + k + ", " + c + ", 0) USING TIMESTAMP 2", table);
            write(2, "INSERT INTO %s (k, c, v) VALUES (" + k + ", " + (c + 1) + ", 1) USING TIMESTAMP 1", table);
            write(1, "INSERT INTO %s (k, c, v) VALUES (" + k + ", " + (c + 1) + ", 0) USING TIMESTAMP 2", table);
        }

        String query = "SELECT k, c FROM " + table + " WHERE v = 1 PER PARTITION LIMIT 2 LIMIT 3 ALLOW FILTERING";
        String groupByQuery = "SELECT k, c, count(*) FROM " + table + " WHERE v = 1 GROUP BY k, c PER PARTITION LIMIT 2 LIMIT 3 ALLOW FILTERING";
        assertRows(cluster.coordinator(1).execute(query, ALL), row(1, 0), row(1, 3), row(0, 2));
        assertRows(cluster.coordinator(1).execute(groupByQuery, ALL), row(1, 0, 1L), row(1, 3, 1L), row(0, 2, 1L));
        assertRows(cluster.coordinator(1).executeWithPaging(query, ALL, 3), row(1, 0), row(1, 3), row(0, 2));
        assertRows(cluster.coordinator(1).executeWithPaging(groupByQuery, ALL, 3), row(1, 0, 1L), row(1, 3, 1L), row(0, 2, 1L));
    }

    /**
     * Creates a table with partitions 0 and 1 of 16 rows of about 512 bytes each, written to both replicas.
     */
    private String createAgreeingTable(String name)
    {
        String table = createTable(name, "(k int, c int, s int static, v int, t text, PRIMARY KEY (k, c))");
        String text = "x".repeat(500);
        for (int k = 0; k <= 1; k++)
            for (int c = 0; c < 16; c++)
                cluster.coordinator(1).execute("INSERT INTO " + table + " (k, c, s, v, t) VALUES (?, ?, 7, 7, ?)", ALL, k, c, text);
        return table;
    }

    /**
     * Reads all the pages of the query on node 1 at ALL, in pages of the given size in bytes, and returns the number of
     * rows.
     */
    private static int rowsInPagesOfBytes(String query, int pageSizeInBytes)
    {
        return cluster.get(1).callOnInstance(BytePagingReads.read(query, "ALL", pageSizeInBytes, true)).stream().mapToInt(List::size).sum();
    }

    private void assertShortReadProtectionRequests(long expected, String tableName, String query, Object[]... rows)
    {
        long requests = shortReadProtectionRequestCount(cluster.get(1), tableName);
        assertRows(cluster.coordinator(1).execute(query, ALL), rows);
        assertEquals(query, expected, shortReadProtectionRequestCount(cluster.get(1), tableName) - requests);
    }

    private String createRangeTombstoneTable(String name)
    {
        return createTable(name, "(k int, c int, v int, PRIMARY KEY (k, c))");
    }

    private String createTable(String name, String columns)
    {
        cluster.get(1).runOnInstance(() -> StorageService.instance.setCachedReplicaRowsWarnThreshold(DEFAULT_WARN_THRESHOLD));
        cluster.get(1).runOnInstance(() -> StorageService.instance.setCachedReplicaRowsFailThreshold(DEFAULT_FAIL_THRESHOLD));
        String table = KEYSPACE + '.' + name;
        cluster.schemaChange("CREATE TABLE " + table + ' ' + columns + " WITH read_repair = 'NONE'");
        return table;
    }

    private void write(int node, String cql, String table)
    {
        cluster.get(node).executeInternal(String.format(cql, table));
    }

    private void testMissedUpdates(String tableName, int warnThreshold, int failThreshold, boolean shouldWarn)
    {
        cluster.get(1).runOnInstance(() -> StorageService.instance.setCachedReplicaRowsWarnThreshold(warnThreshold));
        cluster.get(1).runOnInstance(() -> StorageService.instance.setCachedReplicaRowsFailThreshold(failThreshold));

        String fullTableName = KEYSPACE + '.' + tableName;

        // Case 1: Insert and query rows at ALL to verify baseline.
        for (int i = 0; i < PARTITIONS; i++)
            for (int j = 0; j < ROWS_PER_PARTITION; j++)
                cluster.coordinator(1).execute("INSERT INTO " + fullTableName + "(k, c, v) VALUES (?, ?, 'old')", ALL, i, j);

        long histogramSampleCount = rowsCachedPerQueryCount(cluster.get(1), tableName);

        String query = "SELECT * FROM " + fullTableName + " WHERE v = ? LIMIT ? ALLOW FILTERING";

        Object[][] initialRows = cluster.coordinator(1).execute(query, ALL, "old", PARTITIONS * ROWS_PER_PARTITION);
        assertRows(initialRows,
                   row(1, 0, "old"), row(1, 1, "old"), row(1, 2, "old"),
                   row(0, 0, "old"), row(0, 1, "old"), row(0, 2, "old"),
                   row(2, 0, "old"), row(2, 1, "old"), row(2, 2, "old"));

        // Make sure only one sample was recorded for the query.
        assertEquals(histogramSampleCount + 1, rowsCachedPerQueryCount(cluster.get(1), tableName));

        // Case 2: Update all rows on only one replica, leaving the entire dataset of the remaining replica out-of-date.
        updateAllRowsOn(1, fullTableName, "new");

        // The replica that missed the results creates a mismatch at every row, and we therefore cache a version
        // of that row for all replicas.
        SimpleQueryResult oldResult = cluster.coordinator(1).executeWithResult(query, ALL, "old", PARTITIONS * ROWS_PER_PARTITION);
        assertRows(oldResult.toObjectArrays());
        verifyWarningState(shouldWarn, ROWS_PER_PARTITION * REPLICAS, oldResult);
//        verifyWarningState(shouldWarn, ROWS_PER_PARTITION * REPLICAS, oldResult);

        // We should have made 3 row "completion" requests.
        assertEquals(PARTITIONS, protectionQueryCount(cluster.get(1), tableName));

        // Queries should be caching 3 rows per partition per replica at any given time.
        assertEquals(PARTITIONS * REPLICAS, maxRowsCachedPerQuery(cluster.get(1), tableName));

        // Make sure only one more sample was recorded for the query.
        assertEquals(histogramSampleCount + 2, rowsCachedPerQueryCount(cluster.get(1), tableName));

        // Case 3: Observe the effects of blocking read-repair.

        // The previous query peforms a blocking read-repair, which removes replica divergence. This
        // will only warn, therefore, if the warning threshold is actually below the number of replicas.
        // (i.e. The row cache counter is decremented/reset as each partition is consumed.)
        SimpleQueryResult newResult = cluster.coordinator(1).executeWithResult(query, ALL, "new", PARTITIONS * ROWS_PER_PARTITION);
        Object[][] newRows = newResult.toObjectArrays();
        assertRows(newRows,
                   row(1, 0, "new"), row(1, 1, "new"), row(1, 2, "new"),
                   row(0, 0, "new"), row(0, 1, "new"), row(0, 2, "new"),
                   row(2, 0, "new"), row(2, 1, "new"), row(2, 2, "new"));

        verifyWarningState(warnThreshold < REPLICAS * ROWS_PER_PARTITION, REPLICAS * ROWS_PER_PARTITION, newResult);

        // We still sould only have made 3 row "completion" requests, with no replica divergence in the last query.
        assertEquals(PARTITIONS, protectionQueryCount(cluster.get(1), tableName));

        // Queries should be caching 3 rows per partition per replica at any given time.
        assertEquals(REPLICAS * ROWS_PER_PARTITION, minRowsCachedPerQuery(cluster.get(1), tableName));

        // Make sure only one more sample was recorded for the query.
        assertEquals(histogramSampleCount + 3, rowsCachedPerQueryCount(cluster.get(1), tableName));

        // Case 4: Introduce another mismatch by updating all rows on only one replica.

        updateAllRowsOn(1, fullTableName, "future");

        // Another mismatch is introduced, and we once again cache a version of each row during resolution.
        SimpleQueryResult futureResult = cluster.coordinator(1).executeWithResult(query, ALL, "future", PARTITIONS * ROWS_PER_PARTITION);
        Object[][] futureRows = futureResult.toObjectArrays();
        assertRows(futureRows,
                   row(1, 0, "future"), row(1, 1, "future"), row(1, 2, "future"),
                   row(0, 0, "future"), row(0, 1, "future"), row(0, 2, "future"),
                   row(2, 0, "future"), row(2, 1, "future"), row(2, 2, "future"));

        verifyWarningState(shouldWarn, ROWS_PER_PARTITION * REPLICAS, futureResult);

        // We sould have made 3 more row "completion" requests.
        assertEquals(PARTITIONS * 2, protectionQueryCount(cluster.get(1), tableName));

        // Queries should be caching 3 rows per partition per replica at any given time.
        assertEquals(PARTITIONS * REPLICAS, maxRowsCachedPerQuery(cluster.get(1), tableName));

        // Make sure only one more sample was recorded for the query.
        assertEquals(histogramSampleCount + 4, rowsCachedPerQueryCount(cluster.get(1), tableName));
    }

    private void updateAllRowsOn(int node, String table, String value)
    {
        for (int i = 0; i < PARTITIONS; i++)
            for (int j = 0; j < ROWS_PER_PARTITION; j++)
                cluster.get(node).executeInternal("UPDATE " + table + " SET v = ? WHERE k = ? and c = ?", value, i, j);
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

    private long shortReadProtectionRequestCount(IInvokableInstance instance, String tableName)
    {
        return instance.callOnInstance(() -> Keyspace.open(KEYSPACE)
                                                     .getColumnFamilyStore(tableName)
                                                     .metric.shortReadProtectionRequests.getCount());
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
