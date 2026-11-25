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

import java.util.Map;
import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.shared.Versions.Version;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Bti "da" sstables with null components inside tuple and frozen collection values, in clustering columns and in the
 * partition keys of a secondary index, written by Apache Cassandra and read by a node of this build, and the other way.
 */
public class ApacheNullComponentTest extends ApacheMixedClusterTestBase
{
    private static final int ROWS = 10;
    private static final String[][] VALUES = { { "frozen<tuple<int, int, int>>", "(1, null, 1)", "(1, 0, 1)" },
                                               { "frozen<list<int>>", "[1, blobAsInt(0x), 1]", "[1, 0, 1]" } };
    private static final String[] ORDERS = { "ASC", "DESC" };

    @Test
    public void testUpgradeFromApache() throws Throwable
    {
        try (UpgradeableCluster cluster = startCluster(1, ApacheNullComponentTest::configure, apacheVersion()))
        {
            IUpgradeableInstance node = cluster.get(1);
            createSchema(cluster, 1);
            write(node);
            node.flush(KEYSPACE);
            node.shutdown().get();
            node.setVersion(CURRENT);
            node.startup();
            assertReads(node);
        }
    }

    @Test
    public void testEntireSSTableStreamingFromApache() throws Throwable
    {
        testEntireSSTableStreaming(apacheVersion(), CURRENT);
    }

    @Test
    public void testEntireSSTableStreamingToApache() throws Throwable
    {
        testEntireSSTableStreaming(CURRENT, apacheVersion());
    }

    private static void testEntireSSTableStreaming(Version from, Version to) throws Throwable
    {
        try (UpgradeableCluster cluster = startCluster(2, ApacheNullComponentTest::configure, from, to))
        {
            createSchema(cluster, 2);
            write(cluster.get(1));
            cluster.get(1).flush(KEYSPACE);
            cluster.get(2).nodetoolResult("rebuild", "dc0").asserts().success();
            for (int i = 0; i < VALUES.length; i++)
            {
                for (String order : ORDERS)
                    assertFalse(cluster.get(2).logs().grep("Started receiving sstable .* table = " + KEYSPACE + ".t" + i + order.toLowerCase()).getResult().isEmpty());
            }
            assertReads(cluster.get(2));
        }
    }

    private static void configure(IInstanceConfig config)
    {
        config.set("column_index_size", "0KiB")
              .set("sstable", Map.of("selected_format", "bti"))
              .set("storage_compatibility_mode", "NONE");
    }

    private static void createSchema(UpgradeableCluster cluster, int rf)
    {
        schemaChange(cluster, KEYSPACE, withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + rf + '}'));
        for (int i = 0; i < VALUES.length; i++)
        {
            for (String order : ORDERS)
                schemaChange(cluster, KEYSPACE, withKeyspace("CREATE TABLE %s.t" + i + order + " (pk int, ck " + VALUES[i][0] + ", c int, v int, " +
                                                             "PRIMARY KEY (pk, ck, c)) WITH CLUSTERING ORDER BY (ck " + order + ", c ASC)"));
        }
        schemaChange(cluster, KEYSPACE, withKeyspace("CREATE TABLE %s.idx (pk int, c int, v frozen<tuple<int, int, int>>, i int, PRIMARY KEY (pk, c))"));
        schemaChange(cluster, KEYSPACE, withKeyspace("CREATE INDEX idx_v ON %s.idx (v) USING 'legacy_local_table'"));
        schemaChange(cluster, KEYSPACE, withKeyspace("CREATE INDEX idx_i ON %s.idx (i) USING 'legacy_local_table'"));
    }

    private static void write(IUpgradeableInstance node)
    {
        for (int i = 0; i < VALUES.length; i++)
        {
            for (String order : ORDERS)
            {
                for (int j = 1; j < VALUES[i].length; j++)
                {
                    for (int c = 0; c < ROWS; c++)
                        node.executeInternal(withKeyspace("INSERT INTO %s.t" + i + order + " (pk, ck, c, v) VALUES (0, " + VALUES[i][j] + ", ?, ?)"), c, c);
                }
            }
        }
        for (int pk = 0; pk < ROWS; pk++)
        {
            node.executeInternal(withKeyspace("INSERT INTO %s.idx (pk, c, v, i) VALUES (?, 0, (1, null, 1), blobAsInt(0x))"), pk);
            node.executeInternal(withKeyspace("INSERT INTO %s.idx (pk, c, v, i) VALUES (?, 1, (1, 0, 1), 1)"), pk);
        }
    }

    private static void assertReads(IUpgradeableInstance node)
    {
        for (int i = 0; i < VALUES.length; i++)
        {
            for (String order : ORDERS)
            {
                String select = withKeyspace("SELECT c FROM %s.t" + i + order + " WHERE pk = 0 AND ck = " + VALUES[i][1]);
                String reversed = order.equals("ASC") ? " ORDER BY ck DESC, c DESC" : " ORDER BY ck ASC, c DESC";
                assertRows(node.executeInternal(select), rows(IntStream.range(0, ROWS)));
                assertRows(node.executeInternal(select + " AND c >= 5"), rows(IntStream.range(5, ROWS)));
                assertRows(node.executeInternal(select + " AND c < 5" + reversed), rows(IntStream.range(0, 5).map(c -> 4 - c)));
                assertRows(node.executeInternal(select + " AND c = 5"), row(5));
            }
        }
        assertEquals(ROWS, node.executeInternal(withKeyspace("SELECT pk FROM %s.idx WHERE v = (1, null, 1)")).length);
        assertEquals(ROWS, node.executeInternal(withKeyspace("SELECT pk FROM %s.idx WHERE i = blobAsInt(0x)")).length);
    }

    private static Object[][] rows(IntStream values)
    {
        return values.mapToObj(c -> row(c)).toArray(Object[][]::new);
    }
}
