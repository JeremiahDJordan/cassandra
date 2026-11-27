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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.Versions.Version;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.impl.AbstractCluster.CURRENT_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Reads paged in bytes, and aggregations, return every row through every coordinator at every stage of the upgrade
 * from DataStax 4.0 and of the rollback to it. The rows have expired cells, cells fetched but not selected that are
 * older than an expired row marker, or deletions on one of their two replicas.
 */
public class DataStax40BytePagingUpgradeTest extends DataStax40UpgradeTestBase
{
    private static final int NODES = 3;
    private static final int ROWS = 100;
    private static final int PAGE_SIZE_IN_BYTES = 1000;
    private static final int PAGE_SIZE_IN_ROWS = 1000;
    private static final int AGGREGATION_SUBPAGE_SIZE_IN_BYTES = 1024;
    private static final String TABLE = "CREATE TABLE %s.t (pk int, ck int, v int, w int, m map<int, text>, PRIMARY KEY (pk, ck))";

    private final String[] releases = new String[NODES + 1];
    private final int[] projected = new int[NODES + 1];
    private final List<Read> reads = new ArrayList<>();

    @Test
    public void testForward() throws Throwable
    {
        try (UpgradeableCluster cluster = clusterBuilder(NODES, dataStax40Version()).start())
        {
            setup(cluster);
            assertReads(cluster);
            for (String mode : new String[]{ "HCD_1", "UPGRADING", "NONE" })
            {
                for (int node = NODES; node >= 1; node--)
                {
                    moveNode(cluster, node, CURRENT_VERSION, mode);
                    assertReads(cluster);
                }
            }
        }
    }

    @Test
    public void testRollback() throws Throwable
    {
        Version v40 = dataStax40Version();
        try (UpgradeableCluster cluster = clusterBuilder(NODES, v40).start())
        {
            setup(cluster);
            // testForward reads at the stages on the way to HCD_1
            for (int node = NODES; node >= 1; node--)
                moveNode(cluster, node, CURRENT_VERSION, "HCD_1");
            for (int node = NODES; node >= 1; node--)
            {
                moveNode(cluster, node, v40, null);
                assertReads(cluster);
            }
        }
    }

    /**
     * Writes the rows and reads the expected results through node 1, unpaged or, for aggregations, in a single
     * subpage.
     */
    private void setup(UpgradeableCluster cluster)
    {
        Arrays.fill(releases, "4.0");
        cluster.schemaChange("CREATE KEYSPACE rf1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        cluster.schemaChange("CREATE KEYSPACE rf2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}");
        cluster.schemaChange(String.format(TABLE, "rf1"));
        cluster.schemaChange("CREATE TABLE rf1.p (pk int, ck int, v int, w int, x int, PRIMARY KEY (pk, ck))");
        cluster.schemaChange(String.format(TABLE, "rf2") + " WITH read_repair = 'NONE'");

        // RF 1: on every node a partition of expiring and other rows, one with expired map elements, and one whose
        // cells are older than a row marker that has expired
        String value = Strings.repeat("x", PAGE_SIZE_IN_BYTES / 2);
        int[] plain = new int[NODES + 1];
        int[] expired = new int[NODES + 1];
        int key = 0;
        for (int node = 1; node <= NODES; node++)
        {
            plain[node] = key = keyWithReplicas(cluster, "rf1", key, node);
            expired[node] = key = keyWithReplicas(cluster, "rf1", key + 1, node);
            projected[node] = key = keyWithReplicas(cluster, "rf1", key + 1, node);
            key++;
            for (int ck = 0; ck < ROWS; ck++)
            {
                cluster.coordinator(1).execute("INSERT INTO rf1.t (pk, ck, v, w) VALUES (?, ?, 0, 0) USING TTL ?", ONE, plain[node], ck, ck % 2 * 86400);
                cluster.coordinator(1).execute("UPDATE rf1.t SET m = m + {0: 'a'} WHERE pk = ? AND ck = ?", ONE, expired[node], ck);
                cluster.coordinator(1).execute("UPDATE rf1.t USING TTL 1 SET m = m + {1: '" + value + "', 2: '" + value + "'} WHERE pk = ? AND ck = ?",
                                               ONE, expired[node], ck);
                cluster.coordinator(1).execute("UPDATE rf1.p SET v = ?, w = 0 WHERE pk = ? AND ck = ?", ONE, ck, projected[node], ck);
            }
        }
        cluster.forEach(instance -> instance.flush("rf1"));
        // the map elements written before the row markers have expired too
        writeExpiredRowMarkers(cluster);
        for (int node = 1; node <= NODES; node++)
        {
            addRead(cluster, "SELECT ck FROM rf1.t WHERE pk = " + plain[node], "ONE", node, ROWS);
            addRead(cluster, "SELECT count(*) FROM rf1.t WHERE pk = " + plain[node], "ONE", node, 1);
            addRead(cluster, "SELECT ck, m FROM rf1.t WHERE pk = " + expired[node], "ONE", node, ROWS);
            addRead(cluster, "SELECT count(*) FROM rf1.t WHERE pk = " + expired[node], "ONE", node, 1);
            addRead(cluster, "SELECT v FROM rf1.p WHERE pk = " + projected[node], "ONE", node, ROWS);
        }

        // RF 2: two partitions on every pair of nodes, every third row deleted on the first or the second of the pair
        List<Integer> keys = new ArrayList<>();
        key = 0;
        for (int[] pair : new int[][]{ { 1, 2 }, { 2, 3 }, { 3, 1 } })
        {
            for (int deleteOn : pair)
            {
                int pk = key = keyWithReplicas(cluster, "rf2", key, pair);
                key++;
                keys.add(pk);
                for (int ck = 0; ck < ROWS; ck++)
                    cluster.coordinator(1).execute("INSERT INTO rf2.t (pk, ck, v, w) VALUES (?, ?, ?, 0)", ALL, pk, ck, ck);
                for (int ck = 0; ck < ROWS; ck += 3)
                    cluster.get(deleteOn).executeInternal("DELETE FROM rf2.t WHERE pk = ? AND ck = ?", pk, ck);
            }
        }
        int rows = keys.size() * (ROWS - (ROWS + 2) / 3);
        String in = keys.toString().replace('[', '(').replace(']', ')');
        addRead(cluster, "SELECT pk, ck, v FROM rf2.t WHERE pk IN " + in, "ALL", 0, rows);
        addRead(cluster, "SELECT pk, ck, v FROM rf2.t", "ALL", 0, rows);
        addRead(cluster, "SELECT pk, ck FROM rf2.t LIMIT 150", "ALL", 0, 150);
        addRead(cluster, "SELECT pk, ck, count(*) FROM rf2.t GROUP BY pk, ck", "ALL", 0, rows);
        addRead(cluster, "SELECT pk, count(*) FROM rf2.t GROUP BY pk", "ALL", 0, keys.size());
        addRead(cluster, "SELECT count(*) FROM rf2.t", "ALL", 0, 1);
    }

    /**
     * Writes a row marker that expires over the cells of the rows of rf1.p, and waits until it has expired.
     */
    private void writeExpiredRowMarkers(UpgradeableCluster cluster)
    {
        for (int node = 1; node <= NODES; node++)
            for (int ck = 0; ck < ROWS; ck++)
                cluster.coordinator(1).execute("INSERT INTO rf1.p (pk, ck, x) VALUES (?, ?, 0) USING TTL 1", ONE, projected[node], ck);
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
    }

    private void addRead(UpgradeableCluster cluster, String query, String consistency, int replica, int rows)
    {
        IInvokableInstance node = (IInvokableInstance) cluster.get(1);
        node.runOnInstance(BytePagingReads.aggregationSubPageBytes(Integer.MAX_VALUE));
        List<String> expected = rows(node.callOnInstance(BytePagingReads.read(query, consistency, Integer.MAX_VALUE, false)));
        assertEquals(query, rows, expected.size());
        reads.add(new Read(query, consistency, replica, expected));
    }

    /**
     * Reads through every node, in pages of {@code PAGE_SIZE_IN_BYTES}, and aggregations in pages of
     * {@code PAGE_SIZE_IN_ROWS} and subpages of {@code AGGREGATION_SUBPAGE_SIZE_IN_BYTES}, with a newer expired row
     * marker in the memtables of rf1.p.
     */
    private void assertReads(UpgradeableCluster cluster)
    {
        writeExpiredRowMarkers(cluster);
        for (int node = 1; node <= NODES; node++)
        {
            IInvokableInstance coordinator = (IInvokableInstance) cluster.get(node);
            coordinator.runOnInstance(BytePagingReads.aggregationSubPageBytes(AGGREGATION_SUBPAGE_SIZE_IN_BYTES));
            for (Read read : reads)
            {
                boolean aggregation = read.query.contains("count(");
                List<List<String>> pages = coordinator.callOnInstance(aggregation
                                                                      ? BytePagingReads.read(read.query, read.consistency, PAGE_SIZE_IN_ROWS, false)
                                                                      : BytePagingReads.read(read.query, read.consistency, PAGE_SIZE_IN_BYTES, true));
                String replica = read.replica == 0 ? "" : ", replica node " + read.replica;
                String context = stage() + ", coordinator node " + node + replica + ": " + read.query + " at " + read.consistency;
                assertEquals(context, read.expected, rows(pages));
                assertTrue(context + " in one page", aggregation || pages.stream().filter(page -> !page.isEmpty()).count() > 1);
            }
        }
    }

    private void moveNode(UpgradeableCluster cluster, int node, Version version, String mode) throws Exception
    {
        restart(cluster, node, version, mode);
        releases[node] = mode == null ? "4.0" : "5.0 " + mode;
    }

    private String stage()
    {
        StringBuilder stage = new StringBuilder();
        for (int node = 1; node <= NODES; node++)
            stage.append(node == 1 ? "" : ", ").append("node ").append(node).append(' ').append(releases[node]);
        return stage.toString();
    }

    private static int keyWithReplicas(UpgradeableCluster cluster, String keyspace, int from, int... nodes)
    {
        List<String> addresses = new ArrayList<>();
        for (int node : nodes)
            addresses.add(cluster.get(node).config().broadcastAddress().getAddress().getHostAddress() + ':');
        return ((IInvokableInstance) cluster.get(1)).callOnInstance(BytePagingReads.keyWithReplicas(keyspace, "t", from, addresses));
    }

    private static List<String> rows(List<List<String>> pages)
    {
        List<String> rows = new ArrayList<>();
        pages.forEach(rows::addAll);
        return rows;
    }

    private static class Read
    {
        final String query;
        final String consistency;
        final int replica;
        final List<String> expected;

        Read(String query, String consistency, int replica, List<String> expected)
        {
            this.query = query;
            this.consistency = consistency;
            this.replica = replica;
            this.expected = expected;
        }
    }
}
