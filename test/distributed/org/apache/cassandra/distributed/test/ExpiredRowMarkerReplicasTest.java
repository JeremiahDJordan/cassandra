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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Reads at ALL of a row whose row marker and older cells are on different replicas, or in different sources of a
 * replica: whether the row exists depends on the cells of every replica, whatever the selection.
 */
public class ExpiredRowMarkerReplicasTest extends TestBaseImpl
{
    private static final String[] SELECTIONS = { "k, c", "k, c, x", "*" };

    /**
     * Restrictions selecting row (1, 1): a slice and a read by name.
     */
    private static final String[] RESTRICTIONS = { " WHERE k = 1", " WHERE k = 1 AND c = 1" };

    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CLUSTER = init(Cluster.build(2).start());
    }

    @AfterClass
    public static void shutDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    /**
     * Node 1 has a deletion of r in an sstable and a newer row marker, expired, in its memtable; node 2 has an older
     * value of r. The deletion shadows that value, so the row has no live data.
     */
    @Test
    public void testDeletionOlderThanExpiredMarkerShadowsOtherReplica() throws Throwable
    {
        createTable("deleted_cell");
        CLUSTER.get(1).executeInternal(withKeyspace("DELETE r FROM %s.deleted_cell USING TIMESTAMP 2 WHERE k = 1 AND c = 1"));
        CLUSTER.get(1).flush(KEYSPACE);
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.deleted_cell (k, c) VALUES (1, 1) USING TIMESTAMP 10 AND TTL 1"));
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s.deleted_cell USING TIMESTAMP 1 SET r = 7 WHERE k = 1 AND c = 1"));
        Thread.sleep(2500);

        assertNoRow("deleted_cell");
    }

    /**
     * Node 1 has a value of r in an sstable and a newer live row marker in its memtable; node 2 has an even newer row
     * marker, expired. The row is alive by r.
     */
    @Test
    public void testCellOlderThanLiveMarkerKeepsRowWithExpiredMarkerOnOtherReplica() throws Throwable
    {
        createTable("live_cell");
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.live_cell USING TIMESTAMP 1 SET r = 7 WHERE k = 1 AND c = 1"));
        CLUSTER.get(1).flush(KEYSPACE);
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.live_cell (k, c) VALUES (1, 1) USING TIMESTAMP 10"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.live_cell (k, c) VALUES (1, 1) USING TIMESTAMP 20 AND TTL 1"));
        Thread.sleep(2500);

        assertRowAliveByR("live_cell");
    }

    private static void assertNoRow(String table)
    {
        for (String restriction : RESTRICTIONS)
        {
            for (String selection : SELECTIONS)
                assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT " + selection + " FROM %s." + table + restriction), ConsistencyLevel.ALL));
        }
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT count(*) FROM %s." + table), ConsistencyLevel.ALL), row(0L));
    }

    private static void assertRowAliveByR(String table)
    {
        for (String restriction : RESTRICTIONS)
        {
            assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT k, c FROM %s." + table + restriction), ConsistencyLevel.ALL), row(1, 1));
            assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT k, c, x FROM %s." + table + restriction), ConsistencyLevel.ALL), row(1, 1, null));
            assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT * FROM %s." + table + restriction), ConsistencyLevel.ALL), row(1, 1, 7, null));
        }
        assertRows(CLUSTER.coordinator(1).execute(withKeyspace("SELECT count(*) FROM %s." + table), ConsistencyLevel.ALL), row(1L));
    }

    private static void createTable(String table)
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (k int, c int, r int, x int, PRIMARY KEY (k, c))" +
                                          " WITH read_repair = 'NONE' AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'}"));
    }
}
