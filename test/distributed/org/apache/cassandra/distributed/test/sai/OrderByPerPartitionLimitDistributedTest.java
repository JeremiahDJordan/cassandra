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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * ANN on regular and static vectors with {@code PER PARTITION LIMIT} and {@code LIMIT}, with RF=2 and RF=3 on 3 nodes,
 * from each coordinator, with consistent replicas. The best row of a partition by regular vector is usually not its
 * first row in clustering order, and the rows of a partition by static vector are tied and come in clustering order.
 */
public class OrderByPerPartitionLimitDistributedTest extends TestBaseImpl
{
    private static final int PARTITIONS = 12;
    private static final int ROWS_PER_PARTITION = 4;

    private static Cluster cluster;

    @BeforeClass
    public static void setup() throws IOException
    {
        // A single data directory, because a flush split across data directories writes several sstables, and a vector
        // memtable index can't be flushed to several sstables (see UnifiedCompactionStrategy#supportsL0Shards)
        cluster = Cluster.build(3)
                         .withDataDirCount(1)
                         .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                         .start();
        cluster.schemaChange("CREATE KEYSPACE rf2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}");
        cluster.schemaChange("CREATE KEYSPACE rf3 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
    }

    @AfterClass
    public static void teardown()
    {
        if (cluster != null)
            cluster.close();
    }

    /** A row with its distance to the query vector. */
    private static class RowDistance
    {
        final int pk, ck;
        final double distance;

        RowDistance(int pk, int ck, double distance)
        {
            this.pk = pk;
            this.ck = ck;
            this.distance = distance;
        }
    }

    private static String expected(List<RowDistance> rows, int perPartitionLimit, int limit)
    {
        List<RowDistance> sorted = new ArrayList<>(rows);
        sorted.sort(Comparator.comparingDouble(r -> r.distance));
        Map<Integer, Integer> taken = new HashMap<>();
        List<String> result = new ArrayList<>();
        for (RowDistance r : sorted)
        {
            if (result.size() >= limit)
                break;
            if (taken.merge(r.pk, 1, Integer::sum) > perPartitionLimit)
                continue;
            result.add("(" + r.pk + ',' + r.ck + ')');
        }
        return result.toString();
    }

    /**
     * Renders the selected pk and ck of the rows. Paged results can have more columns, used to order the pages.
     */
    private static String render(Iterator<Object[]> rows)
    {
        List<String> rendered = new ArrayList<>();
        while (rows.hasNext())
        {
            Object[] row = rows.next();
            rendered.add("(" + row[0] + ',' + row[1] + ')');
        }
        return rendered.toString();
    }

    private static void assertQuery(int node, String query, ConsistencyLevel cl, int pageSize, String expected)
    {
        String actual = pageSize > 0
                        ? render(cluster.coordinator(node).executeWithPaging(query, cl, pageSize))
                        : render(Arrays.asList(cluster.coordinator(node).execute(query, cl)).iterator());
        assertThat(actual).as("node%d %s page=%d: %s", node, cl, pageSize, query).isEqualTo(expected);
    }

    private static void assertOrdered(String table, String where, List<RowDistance> rows)
    {
        for (int node = 1; node <= 3; node++)
        {
            for (int limit : new int[]{ 1, 2, 3, 5, 7, 100 })
            {
                assertQuery(node, String.format("SELECT pk, ck FROM %s %s ORDER BY v ANN OF [1, 0] LIMIT %d", table, where, limit),
                            ConsistencyLevel.ALL, 0, expected(rows, Integer.MAX_VALUE, limit));
                for (int perPartitionLimit : new int[]{ 1, 2, 3 })
                {
                    String query = String.format("SELECT pk, ck FROM %s %s ORDER BY v ANN OF [1, 0] PER PARTITION LIMIT %d LIMIT %d",
                                                 table, where, perPartitionLimit, limit);
                    String expected = expected(rows, perPartitionLimit, limit);
                    assertQuery(node, query, ConsistencyLevel.ALL, 0, expected);
                    if (limit == 5)
                    {
                        assertQuery(node, query, ConsistencyLevel.ONE, 0, expected);
                        assertQuery(node, query, ConsistencyLevel.ALL, 2, expected);
                    }
                }
            }
        }
    }

    @Test
    public void testAnnPerPartitionLimit()
    {
        for (String ks : new String[]{ "rf2", "rf3" })
        {
            String regular = ks + ".regular", stat = ks + ".stat";
            cluster.schemaChange("CREATE TABLE " + regular + " (pk int, ck int, r int, v vector<float, 2>, PRIMARY KEY(pk, ck))");
            cluster.schemaChange("CREATE CUSTOM INDEX ON " + regular + "(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
            cluster.schemaChange("CREATE CUSTOM INDEX ON " + regular + "(r) USING 'StorageAttachedIndex'");
            cluster.schemaChange("CREATE TABLE " + stat + " (pk int, ck int, r int, v vector<float, 2> static, PRIMARY KEY(pk, ck))");
            cluster.schemaChange("CREATE CUSTOM INDEX ON " + stat + "(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
            cluster.schemaChange("CREATE CUSTOM INDEX ON " + stat + "(r) USING 'StorageAttachedIndex'");
            SAIUtil.waitForIndexQueryableOnAllNodes(cluster, ks);

            List<RowDistance> regularRows = new ArrayList<>();
            List<RowDistance> staticRows = new ArrayList<>();
            for (int pk = 0; pk < PARTITIONS; pk++)
            {
                // partitions by static vector: a permutation of the partitions
                double staticDistance = ((pk * 7) % PARTITIONS) / 100.0;
                cluster.coordinator(1).execute("INSERT INTO " + stat + " (pk, v) VALUES (?, [1, " + staticDistance + "])", ConsistencyLevel.ALL, pk);
                for (int ck = 0; ck < ROWS_PER_PARTITION; ck++)
                {
                    // rows by regular vector: a permutation of all the rows
                    double distance = (((pk * ROWS_PER_PARTITION + ck) * 29) % (PARTITIONS * ROWS_PER_PARTITION)) / 1000.0;
                    cluster.coordinator(1).execute("INSERT INTO " + regular + " (pk, ck, r, v) VALUES (?, ?, ?, [1, " + distance + "])", ConsistencyLevel.ALL, pk, ck, ck);
                    regularRows.add(new RowDistance(pk, ck, distance));
                    cluster.coordinator(1).execute("INSERT INTO " + stat + " (pk, ck, r) VALUES (?, ?, ?)", ConsistencyLevel.ALL, pk, ck, ck);
                    staticRows.add(new RowDistance(pk, ck, staticDistance + ck / 10000.0)); // tied rows by clustering
                }
            }
            List<RowDistance> regularRowsWithR2 = regularRows.stream()
                                                             .filter(r -> r.ck >= 2)
                                                             .collect(Collectors.toList());
            List<RowDistance> staticRowsWithR2 = staticRows.stream()
                                                           .filter(r -> r.ck >= 2)
                                                           .collect(Collectors.toList());

            for (int i = 0; i < 2; i++) // memtable, then sstables
            {
                assertOrdered(regular, "", regularRows);
                assertOrdered(regular, "WHERE r >= 2", regularRowsWithR2);
                assertOrdered(stat, "", staticRows);
                assertOrdered(stat, "WHERE r >= 2", staticRowsWithR2);
                cluster.forEach(n -> n.flush(ks));
            }
        }
    }
}
