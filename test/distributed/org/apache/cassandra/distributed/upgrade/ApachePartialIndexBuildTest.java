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

package org.apache.cassandra.distributed.upgrade;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.shared.Versions.Version;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertFalse;

/**
 * An Apache index build that reuses the per-SSTable index files of a flush numbers rows by their liveness at build
 * time, while the flush numbered every row live at flush time and every non-empty static row. Rows after one that
 * expired between flush and build, or after a static row with only tombstones, get the wrong primary keys, until
 * {@code nodetool upgradesstables -a} rewrites the SSTables.
 */
public class ApachePartialIndexBuildTest extends ApacheMixedClusterTestBase
{
    private static final int ROWS = 10;
    private static final int TTL_SECONDS = 5;

    // the last Apache Cassandra version known to number the rows of such builds differently from its flush
    private static final String KNOWN_MISNUMBERING_VERSION = "5.0.9";

    private static Version apache;
    private static UpgradeableCluster cluster;

    @BeforeClass
    public static void setUpCluster() throws Throwable
    {
        apache = apacheVersion();
        cluster = startCluster(1, config -> {}, apache);
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"));
    }

    @AfterClass
    public static void tearDownCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testRowExpiredBetweenFlushAndBuild()
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.expired (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))"));
        cluster.schemaChange(withKeyspace("CREATE INDEX expired_a ON %s.expired (a) USING 'sai'"));
        waitForIndexQueryable("expired_a");

        for (int ck = 1; ck < ROWS; ck++)
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.expired (pk, ck, a, b) VALUES (0, ?, ?, ?)"), ONE, ck, ck, ck);
        cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.expired (pk, ck, a, b) VALUES (0, 0, 0, 0) USING TTL " + TTL_SECONDS), ONE);
        cluster.get(1).nodetoolResult("flush", KEYSPACE, "expired").asserts().success();

        // the row expiring first in clustering order was live, and indexed, when the SSTable was flushed
        String expiringRowQuery = withKeyspace("SELECT ck FROM %s.expired WHERE a = 0");
        assertRows(cluster.coordinator(1).execute(expiringRowQuery, ONE), row(0));
        await().atMost(1, TimeUnit.MINUTES).until(() -> cluster.coordinator(1).execute(expiringRowQuery, ONE).length == 0);

        assertBuildMisnumbersRowsUntilRewrite("expired", 1);
    }

    @Test
    public void testStaticRowWithOnlyTombstones()
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.static_tombstone (pk int, ck int, s int static, a int, b int, PRIMARY KEY (pk, ck))"));
        cluster.schemaChange(withKeyspace("CREATE INDEX static_tombstone_a ON %s.static_tombstone (a) USING 'sai'"));
        waitForIndexQueryable("static_tombstone_a");

        for (int ck = 0; ck < ROWS; ck++)
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.static_tombstone (pk, ck, a, b) VALUES (0, ?, ?, ?)"), ONE, ck, ck, ck);
        cluster.coordinator(1).execute(withKeyspace("DELETE s FROM %s.static_tombstone WHERE pk = 0"), ONE);
        cluster.get(1).nodetoolResult("flush", KEYSPACE, "static_tombstone").asserts().success();

        assertBuildMisnumbersRowsUntilRewrite("static_tombstone", 0);
    }

    /**
     * Creates an index on {@code b} over the flushed SSTable, whose rows {@code firstLive..ROWS-1} are live with
     * {@code a = b = ck}, then checks a query of each index for each row before and after the rewrite.
     */
    private static void assertBuildMisnumbersRowsUntilRewrite(String table, int firstLive)
    {
        for (int ck = firstLive; ck < ROWS; ck++)
            assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT ck FROM %s." + table + " WHERE a = ?"), ONE, ck), row(ck));

        cluster.schemaChange(withKeyspace("CREATE INDEX " + table + "_b ON %s." + table + " (b) USING 'sai'"));
        waitForIndexQueryable(table + "_b");

        List<Integer> wrong = new ArrayList<>();
        for (int ck = firstLive; ck < ROWS; ck++)
        {
            Object[][] rows = cluster.coordinator(1).execute(withKeyspace("SELECT ck FROM %s." + table + " WHERE b = ?"), ONE, ck);
            if (rows.length != 1 || !Integer.valueOf(ck).equals(rows[0][0]))
                wrong.add(ck);
        }
        if (apache.version.isLowerThanOrEqualTo(KNOWN_MISNUMBERING_VERSION))
            assertFalse("No row missed by the index built over the flushed SSTable on " + apache.version, wrong.isEmpty());
        else
            Assume.assumeFalse("Partial index builds number rows as flush does on " + apache.version, wrong.isEmpty());

        cluster.get(1).nodetoolResult("upgradesstables", "-a", KEYSPACE, table).asserts().success();
        for (int ck = firstLive; ck < ROWS; ck++)
        {
            assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT ck FROM %s." + table + " WHERE b = ?"), ONE, ck), row(ck));
            assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT ck FROM %s." + table + " WHERE a = ?"), ONE, ck), row(ck));
        }
    }

    private static void waitForIndexQueryable(String index)
    {
        await().atMost(1, TimeUnit.MINUTES).until(() -> {
            for (Object[] row : cluster.get(1).executeInternal("SELECT index_name, is_queryable FROM system_views.sai_column_indexes WHERE keyspace_name = ?", KEYSPACE))
            {
                if (index.equals(row[0]))
                    return Boolean.TRUE.equals(row[1]);
            }
            return false;
        });
    }
}
