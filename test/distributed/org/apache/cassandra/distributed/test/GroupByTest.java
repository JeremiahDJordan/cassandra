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

import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.Callable;

import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.apache.cassandra.serializers.TimeSerializer;
import org.apache.cassandra.serializers.TimestampSerializer;
import org.apache.cassandra.service.reads.ShortReadProtection;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class GroupByTest extends TestBaseImpl
{
    @Test
    public void groupByWithDeletesAndSrpOnPartitions() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2).withConfig((cfg) -> cfg.set("user_defined_functions_enabled", "true")).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck text, PRIMARY KEY (pk, ck))"));
            initFunctions(cluster);
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck) VALUES (1, '1') USING TIMESTAMP 0"));
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck) VALUES (2, '2') USING TIMESTAMP 0"));
            cluster.get(1).executeInternal(withKeyspace("DELETE FROM %s.tbl WHERE pk=0 AND ck='0'"));

            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck) VALUES (0, '0') USING TIMESTAMP 0"));
            cluster.get(2).executeInternal(withKeyspace("DELETE FROM %s.tbl WHERE pk=1 AND ck='1'"));
            cluster.get(2).executeInternal(withKeyspace("DELETE FROM %s.tbl WHERE pk=2 AND ck='2'"));

            for (String limitClause : new String[]{ "", "LIMIT 1", "LIMIT 10", "PER PARTITION LIMIT 1", "PER PARTITION LIMIT 10" })
            {
                String query = withKeyspace("SELECT concat(ck) FROM %s.tbl GROUP BY pk " + limitClause);
                for (int i = 1; i <= 4; i++)
                {
                    Iterator<Object[]> rows = cluster.coordinator(2).executeWithPaging(query, ConsistencyLevel.ALL, i);
                    assertRows(Iterators.toArray(rows, Object[].class));
                }
            }
        }
    }

    @Test
    public void groupByWithDeletesAndSrpOnRows() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2).withConfig((cfg) -> cfg.set("user_defined_functions_enabled", "true")).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck text, PRIMARY KEY (pk, ck))"));
            initFunctions(cluster);
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck) VALUES (0, '1') USING TIMESTAMP 0"));
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck) VALUES (0, '2') USING TIMESTAMP 0"));
            cluster.get(1).executeInternal(withKeyspace("DELETE FROM %s.tbl WHERE pk=0 AND ck='0'"));

            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck) VALUES (0, '0') USING TIMESTAMP 0"));
            cluster.get(2).executeInternal(withKeyspace("DELETE FROM %s.tbl WHERE pk=0 AND ck='1'"));
            cluster.get(2).executeInternal(withKeyspace("DELETE FROM %s.tbl WHERE pk=0 AND ck='2'"));

            for (String limitClause : new String[]{ "", "LIMIT 1", "LIMIT 10", "PER PARTITION LIMIT 1", "PER PARTITION LIMIT 10" })
            {
                String query = withKeyspace("SELECT concat(ck) FROM %s.tbl GROUP BY pk " + limitClause);
                for (int i = 1; i <= 4; i++)
                {
                    Iterator<Object[]> rows = cluster.coordinator(2).executeWithPaging(query, ConsistencyLevel.ALL, i);
                    assertRows(Iterators.toArray(rows, Object[].class));
                }
            }
        }
    }

    @Test
    public void groupByWithLimitAfterRangeTombstone() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck0 int, ck1 int, PRIMARY KEY (pk, ck0, ck1))"));
            ICoordinator coordinator = cluster.coordinator(1);

            // a range tombstone on both nodes
            coordinator.execute(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 2 WHERE pk = 1 AND ck0 > 2"), ConsistencyLevel.ALL);
            coordinator.execute(withKeyspace("INSERT INTO %s.tbl (pk, ck0, ck1) VALUES (1, 1, 1) USING TIMESTAMP 3"), ConsistencyLevel.ALL);
            coordinator.execute(withKeyspace("INSERT INTO %s.tbl (pk, ck0, ck1) VALUES (1, 3, 1) USING TIMESTAMP 3"), ConsistencyLevel.ALL);

            // a range tombstone on one node only
            coordinator.execute(withKeyspace("INSERT INTO %s.tbl (pk, ck0, ck1) VALUES (2, 1, 1) USING TIMESTAMP 1"), ConsistencyLevel.ALL);
            cluster.get(1).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 2 WHERE pk = 2 AND ck0 = 2"));
            coordinator.execute(withKeyspace("INSERT INTO %s.tbl (pk, ck0, ck1) VALUES (2, 2, 1) USING TIMESTAMP 3"), ConsistencyLevel.ALL);

            for (int pk = 1; pk <= 2; pk++)
            {
                int ck0 = pk == 1 ? 3 : 2;
                for (int pageSize = 1; pageSize <= 4; pageSize++)
                {
                    assertRows(coordinator.executeWithPaging(withKeyspace("SELECT pk, ck0, ck1 FROM %s.tbl WHERE pk = ? GROUP BY pk, ck0"),
                                                             ConsistencyLevel.ALL, pageSize, pk),
                               row(pk, 1, 1), row(pk, ck0, 1));
                    assertRows(coordinator.executeWithPaging(withKeyspace("SELECT pk, ck0, ck1 FROM %s.tbl WHERE pk = ? GROUP BY pk, ck0 LIMIT 1"),
                                                             ConsistencyLevel.ALL, pageSize, pk),
                               row(pk, 1, 1));
                }
                assertRows(coordinator.execute(withKeyspace("SELECT pk, ck0, ck1 FROM %s.tbl WHERE pk = ? GROUP BY pk, ck0 LIMIT 1"),
                                               ConsistencyLevel.ALL, pk),
                           row(pk, 1, 1));
            }
        }
    }

    @Test
    public void groupByWithSrpAfterRangeTombstone() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck0 int, ck1 int, v int, PRIMARY KEY (pk, ck0, ck1)) " +
                                              "WITH read_repair = 'NONE' AND speculative_retry = 'NONE'"));

            // on node 2, the row that ends group (1, 2) follows the opening bound of a range tombstone, and node 3 has
            // a newer deletion of group (1, 2)
            cluster.get(2).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 1 WHERE pk = 1 AND ck0 = 3"));
            for (int i = 2; i <= 3; i++)
                cluster.get(i).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck0, ck1, v) VALUES (1, 3, 0, 0) USING TIMESTAMP 2"));
            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck0, ck1, v) VALUES (1, 2, 2, 0) USING TIMESTAMP 3"));
            cluster.get(3).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 4 WHERE pk = 1 AND ck0 = 2 AND ck1 = 2"));

            for (String limit : new String[]{ "", "PER PARTITION LIMIT 1", "LIMIT 1" })
            {
                String query = withKeyspace("SELECT pk, ck0, count(*) FROM %s.tbl GROUP BY pk, ck0 " + limit);
                assertRows(cluster.coordinator(1).execute(query, ConsistencyLevel.ALL), row(1, 3, 1L));
                for (int pageSize = 1; pageSize <= 4; pageSize++)
                    assertRows(cluster.coordinator(1).executeWithPaging(query, ConsistencyLevel.ALL, pageSize), row(1, 3, 1L));
            }
        }
    }

    @Test
    public void groupByWithLimitAfterRangeTombstoneOnSomeReplicas() throws Throwable
    {
        // the range tombstone opens after row (1, 3, 0)
        testGroupByWithLimitAndRangeTombstoneOnSomeReplicas("(ck0, ck1) > (3, 0)");
    }

    @Test
    public void groupByWithLimitInRangeTombstoneOnSomeReplicas() throws Throwable
    {
        // the range tombstone opens before row (1, 3, 0)
        testGroupByWithLimitAndRangeTombstoneOnSomeReplicas("ck0 > 2");
    }

    private void testGroupByWithLimitAndRangeTombstoneOnSomeReplicas(String deletion) throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck0 int, ck1 int, v int, PRIMARY KEY (pk, ck0, ck1)) " +
                                              "WITH read_repair = 'NONE' AND speculative_retry = 'NONE'"));

            // nodes 1 and 3 delete row (1, 3, 1) of node 2 with a range tombstone that covers rows (1, 3, 1) and
            // (1, 4, 0), and row (1, 4, 0) is newer
            for (int i : new int[]{ 1, 3 })
                cluster.get(i).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 2 WHERE pk = 1 AND " + deletion));
            for (int i = 1; i <= 3; i++)
            {
                cluster.get(i).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck0, ck1, v) VALUES (1, 3, 0, 0) USING TIMESTAMP 3"));
                cluster.get(i).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck0, ck1, v) VALUES (1, 4, 0, 0) USING TIMESTAMP 3"));
            }
            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck0, ck1, v) VALUES (1, 3, 1, 0) USING TIMESTAMP 1"));

            String query = withKeyspace("SELECT pk, ck0, count(*) FROM %s.tbl WHERE pk = 1 GROUP BY pk, ck0");
            assertRows(cluster.coordinator(1).execute(query + " LIMIT 1", ConsistencyLevel.ALL), row(1, 3, 1L));
            for (int pageSize = 1; pageSize <= 3; pageSize++)
                assertRows(cluster.coordinator(1).executeWithPaging(query, ConsistencyLevel.ALL, pageSize), row(1, 3, 1L), row(1, 4, 1L));
        }
    }

    @Test
    public void groupByWithPagingInReverseOrderAfterRangeTombstoneOnSomeReplicas() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck0 int, ck1 int, v int, PRIMARY KEY (pk, ck0, ck1)) " +
                                              "WITH read_repair = 'NONE' AND speculative_retry = 'NONE'"));

            // nodes 1 and 3 delete row (1, 4, 0) of node 2 with a range tombstone, so the second page of group (1, 4)
            // has no row of theirs before row (1, 3, 0)
            for (int i = 1; i <= 3; i++)
                cluster.get(i).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck0, ck1, v) VALUES (1, 4, 0, 0) USING TIMESTAMP 1"));
            for (int i : new int[]{ 1, 3 })
                cluster.get(i).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 2 WHERE pk = 1 AND ck0 > 2"));
            for (int i : new int[]{ 1, 2 })
                cluster.get(i).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck0, ck1, v) VALUES (1, 4, 1, 0) USING TIMESTAMP 3"));
            for (int i = 1; i <= 3; i++)
                cluster.get(i).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck0, ck1, v) VALUES (1, 3, 0, 0) USING TIMESTAMP 4"));

            String query = withKeyspace("SELECT pk, ck0, count(*) FROM %s.tbl WHERE pk = 1 GROUP BY pk, ck0 ORDER BY ck0 DESC");
            for (int pageSize = 1; pageSize <= 3; pageSize++)
                assertRows(cluster.coordinator(1).executeWithPaging(query, ConsistencyLevel.ALL, pageSize), row(1, 4, 1L), row(1, 3, 1L));
        }
    }

    @Test
    public void groupByWithSrpAfterRangeTombstoneKeepsClusteringOrder() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).withInstanceInitializer(ClusteringOrderChecker::install).start()))
        {
            String[] deletions = { "ck0 > 1", "ck0 < 4", "ck0 > 1 AND ck0 < 4" };
            for (int t = 0; t < deletions.length * 2; t++)
            {
                String table = "tbl" + t;
                boolean reversed = t % 2 == 1;
                cluster.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (pk int, ck0 int, ck1 int, v int, PRIMARY KEY (pk, ck0, ck1)) " +
                                                  "WITH read_repair = 'NONE' AND speculative_retry = 'NONE'"));

                // a range tombstone on every node, with newer rows of two groups in it: node 2 has the row of the first
                // group, which nodes 1 and 3 have a newer deletion of, and every node has the row of the second group
                int first = reversed ? 3 : 2;
                int second = reversed ? 2 : 3;
                String insert = withKeyspace("INSERT INTO %s." + table + " (pk, ck0, ck1, v) VALUES (1, ?, 0, 0) USING TIMESTAMP ?");
                cluster.coordinator(1).execute(withKeyspace("DELETE FROM %s." + table + " USING TIMESTAMP 2 WHERE pk = 1 AND " + deletions[t / 2]), ConsistencyLevel.ALL);
                cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, second, 5L);
                cluster.get(2).executeInternal(insert, first, 3L);
                for (int i : new int[]{ 1, 3 })
                    cluster.get(i).executeInternal(withKeyspace("DELETE FROM %s." + table + " USING TIMESTAMP 4 WHERE pk = 1 AND ck0 = ? AND ck1 = 0"), first);

                for (String limit : new String[]{ "", "PER PARTITION LIMIT 1", "LIMIT 1" })
                {
                    String query = withKeyspace("SELECT pk, ck0, count(*) FROM %s." + table + " WHERE pk = 1 GROUP BY pk, ck0 " +
                                                (reversed ? "ORDER BY ck0 DESC " : "") + limit);
                    assertRows(cluster.coordinator(1).execute(query, ConsistencyLevel.ALL), row(1, second, 1L));
                    for (int pageSize = 1; pageSize <= 3; pageSize++)
                        assertRows(cluster.coordinator(1).executeWithPaging(query, ConsistencyLevel.ALL, pageSize), row(1, second, 1L));
                }
                long requests = cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).metric.shortReadProtectionRequests.getCount());
                Assert.assertTrue(requests > 0);
            }
        }
    }

    /**
     * Fails a read on node 1 if the response of a replica, with its short read protection responses, has an item that
     * sorts before the previous item.
     */
    public static class ClusteringOrderChecker extends Transformation<UnfilteredRowIterator>
    {
        static void install(ClassLoader cl, int node)
        {
            if (node == 1)
            {
                new ByteBuddy().rebase(ShortReadProtection.class)
                               .method(named("extend"))
                               .intercept(MethodDelegation.to(ClusteringOrderChecker.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static UnfilteredPartitionIterator extend(@SuperCall Callable<UnfilteredPartitionIterator> zuper) throws Exception
        {
            return Transformation.apply(zuper.call(), new ClusteringOrderChecker());
        }

        @Override
        protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            return Transformation.apply(partition, new Transformation<UnfilteredRowIterator>()
            {
                private ClusteringPrefix<?> last;

                @Override
                protected org.apache.cassandra.db.rows.Row applyToRow(org.apache.cassandra.db.rows.Row row)
                {
                    check(row.clustering());
                    return row;
                }

                @Override
                protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
                {
                    check(marker.clustering());
                    return marker;
                }

                private void check(ClusteringPrefix<?> clustering)
                {
                    if (null != last)
                    {
                        int cmp = partition.metadata().comparator.compare(clustering, last);
                        if (partition.isReverseOrder() ? cmp > 0 : cmp < 0)
                            throw new AssertionError(clustering.toString(partition.metadata()) + " after " + last.toString(partition.metadata()));
                    }
                    last = clustering;
                }
            });
        }
    }

    @Test
    public void groupByWithPerPartitionLimitAndSrpOnPartitionWithOnlyRangeTombstones() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'"));
            // partition 2 comes before partition 4 in token order
            for (int i = 1; i <= 2; i++)
            {
                cluster.get(i).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (2, 2, 1) USING TIMESTAMP 0"));
                cluster.get(i).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (2, 5, 1) USING TIMESTAMP 0"));
            }
            cluster.get(1).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 2 WHERE pk = 4 AND ck < 1"));
            cluster.get(2).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 1 WHERE pk = 4 AND ck < 2"));

            for (String filter : new String[]{ "", "WHERE v = 1 " })
            {
                String query = withKeyspace("SELECT pk, ck, count(*) FROM %s.tbl " + filter + "GROUP BY pk, ck PER PARTITION LIMIT 1 LIMIT 2" +
                                            (filter.isEmpty() ? "" : " ALLOW FILTERING"));
                assertRows(cluster.coordinator(1).execute(query, ConsistencyLevel.ALL), row(2, 2, 1L));
                for (int i = 1; i <= 4; i++)
                    assertRows(cluster.coordinator(1).executeWithPaging(query, ConsistencyLevel.ALL, i), row(2, 2, 1L));
            }
        }
    }

    @Test
    public void testGroupByWithAggregatesAndPaging() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2).withConfig((cfg) -> cfg.set("user_defined_functions_enabled", "true")).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v1 text, v2 text, v3 text, primary key (pk, ck))"));
            initFunctions(cluster);

            cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (pk, ck, v1, v2, v3) values (1,1,'1','1','1')"), ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (pk, ck, v1, v2, v3) values (1,2,'2','2','2')"), ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (pk, ck, v1, v2, v3) values (1,3,'3','3','3')"), ConsistencyLevel.ALL);

            for (int i = 1; i <= 4; i++)
            {
                assertRows(cluster.coordinator(1).executeWithPaging(withKeyspace("select concat(v1), concat(v2), concat(v3) from %s.tbl where pk = 1 group by pk"),
                                                                    ConsistencyLevel.ALL, i),
                           row("_ 1 2 3", "_ 1 2 3", "_ 1 2 3"));

                assertRows(cluster.coordinator(1).executeWithPaging(withKeyspace("select concat(v1), concat(v2), concat(v3) from %s.tbl where pk = 1 group by pk limit 1"),
                                                                    ConsistencyLevel.ALL, i),
                           row("_ 1 2 3", "_ 1 2 3", "_ 1 2 3"));

                assertRows(cluster.coordinator(1).executeWithPaging(withKeyspace("select * from %s.tbl where pk = 1 group by pk"),
                                                                    ConsistencyLevel.ALL, i),
                           row(1, 1, "1", "1", "1"));
            }
        }
    }

    @Test
    public void testGroupWithDeletesAndPaging() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2).withConfig(cfg -> cfg.with(Feature.GOSSIP, NETWORK, NATIVE_PROTOCOL)).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, PRIMARY KEY (pk, ck))"));
            ICoordinator coordinator = cluster.coordinator(1);
            coordinator.execute(withKeyspace("INSERT INTO %s.tbl (pk, ck) VALUES (0, 0)"), ConsistencyLevel.ALL);
            coordinator.execute(withKeyspace("INSERT INTO %s.tbl (pk, ck) VALUES (1, 1)"), ConsistencyLevel.ALL);

            cluster.get(1).executeInternal(withKeyspace("DELETE FROM %s.tbl WHERE pk=0 AND ck=0"));
            cluster.get(2).executeInternal(withKeyspace("DELETE FROM %s.tbl WHERE pk=1 AND ck=1"));
            String query = withKeyspace("SELECT * FROM %s.tbl GROUP BY pk");
            Iterator<Object[]> rows = coordinator.executeWithPaging(query, ConsistencyLevel.ALL, 1);
            assertRows(Iterators.toArray(rows, Object[].class));

            try (com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
                 Session session = c.connect())
            {
                SimpleStatement stmt = new SimpleStatement(withKeyspace("select * from %s.tbl where pk = 1 group by pk"));
                stmt.setFetchSize(1);
                Iterator<Row> rs = session.execute(stmt).iterator();
                Assert.assertFalse(rs.hasNext());
            }
        }
    }

    @Test
    public void testGroupByTimeRangesWithTimestampType() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.testWithTimestamp (pk int, time timestamp, v int, primary key (pk, time))"));
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTimestamp (pk, time, v) VALUES (1, '2016-09-27 16:10:00 UTC', 1)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTimestamp (pk, time, v) VALUES (1, '2016-09-27 16:12:00 UTC', 2)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTimestamp (pk, time, v) VALUES (1, '2016-09-27 16:14:00 UTC', 3)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTimestamp (pk, time, v) VALUES (1, '2016-09-27 16:15:00 UTC', 4)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTimestamp (pk, time, v) VALUES (1, '2016-09-27 16:21:00 UTC', 5)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTimestamp (pk, time, v) VALUES (1, '2016-09-27 16:22:00 UTC', 6)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTimestamp (pk, time, v) VALUES (1, '2016-09-27 16:26:00 UTC', 7)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTimestamp (pk, time, v) VALUES (1, '2016-09-27 16:26:20 UTC', 8)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTimestamp (pk, time, v) VALUES (2, '2016-09-27 16:26:20 UTC', 10)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTimestamp (pk, time, v) VALUES (2, '2016-09-27 16:30:00 UTC', 11)"), ConsistencyLevel.QUORUM);

            for (int pageSize : new int[] {2, 3, 4, 5, 7, 10})
            {
                for (String startingTime : new String[] {"", ", '2016-09-27 UTC'"} )
                {
                    String stmt = "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s.testWithTimestamp GROUP BY pk, floor(time, 5m" + startingTime + ")";
                    Iterator<Object[]> pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toTimestamp("2016-09-27 16:10:00 UTC"), 1, 3, 3L),
                               row(1, toTimestamp("2016-09-27 16:15:00 UTC"), 4, 4, 1L),
                               row(1, toTimestamp("2016-09-27 16:20:00 UTC"), 5, 6, 2L),
                               row(1, toTimestamp("2016-09-27 16:25:00 UTC"), 7, 8, 2L),
                               row(2, toTimestamp("2016-09-27 16:25:00 UTC"), 10, 10, 1L),
                               row(2, toTimestamp("2016-09-27 16:30:00 UTC"), 11, 11, 1L));

                    stmt = "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s.testWithTimestamp GROUP BY pk, floor(time, 5m" + startingTime + ") LIMIT 2";
                    pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toTimestamp("2016-09-27 16:10:00 UTC"), 1, 3, 3L),
                               row(1, toTimestamp("2016-09-27 16:15:00 UTC"), 4, 4, 1L));

                    stmt = "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s.testWithTimestamp GROUP BY pk, floor(time, 5m" + startingTime + ") PER PARTITION LIMIT 1";
                    pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toTimestamp("2016-09-27 16:10:00 UTC"), 1, 3, 3L),
                               row(2, toTimestamp("2016-09-27 16:25:00 UTC"), 10, 10, 1L));

                    stmt = "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s.testWithTimestamp WHERE pk = 1 GROUP BY pk, floor(time, 5m" + startingTime + ") ORDER BY time DESC";
                    pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toTimestamp("2016-09-27 16:25:00 UTC"), 7, 8, 2L),
                               row(1, toTimestamp("2016-09-27 16:20:00 UTC"), 5, 6, 2L),
                               row(1, toTimestamp("2016-09-27 16:15:00 UTC"), 4, 4, 1L),
                               row(1, toTimestamp("2016-09-27 16:10:00 UTC"), 1, 3, 3L));

                    stmt = "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s.testWithTimestamp WHERE pk = 1 GROUP BY pk, floor(time, 5m" + startingTime + ") ORDER BY time DESC LIMIT 2";
                    pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toTimestamp("2016-09-27 16:25:00 UTC"), 7, 8, 2L),
                               row(1, toTimestamp("2016-09-27 16:20:00 UTC"), 5, 6, 2L));
                }
            }
        }
    }

    @Test
    public void testGroupByTimeRangesWithDateType() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).start()))
        {

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.testWithDate (pk int, time date, v int, primary key (pk, time))"));

            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithDate (pk, time, v) VALUES (1, '2016-09-27', 1)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithDate (pk, time, v) VALUES (1, '2016-09-28', 2)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithDate (pk, time, v) VALUES (1, '2016-09-29', 3)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithDate (pk, time, v) VALUES (1, '2016-09-30', 4)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithDate (pk, time, v) VALUES (1, '2016-10-01', 5)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithDate (pk, time, v) VALUES (1, '2016-10-04', 6)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithDate (pk, time, v) VALUES (1, '2016-10-20', 7)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithDate (pk, time, v) VALUES (1, '2016-11-27', 8)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithDate (pk, time, v) VALUES (2, '2016-11-01', 10)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithDate (pk, time, v) VALUES (2, '2016-11-02', 11)"), ConsistencyLevel.QUORUM);

            for (int pageSize : new int[] {2, 3, 4, 5, 7, 10})
            {
                for (String startingTime : new String[] {"", ", '2016-06-01'"} )
                {

                    String stmt = "SELECT pk, floor(time, 1mo" + startingTime + "), min(v), max(v), count(v) FROM %s.testWithDate GROUP BY pk, floor(time, 1mo" + startingTime + ")";
                    Iterator<Object[]> pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toLocalDate("2016-09-01"), 1, 4, 4L),
                               row(1, toLocalDate("2016-10-01"), 5, 7, 3L),
                               row(1, toLocalDate("2016-11-01"), 8, 8, 1L),
                               row(2, toLocalDate("2016-11-01"), 10, 11, 2L));

                    stmt = "SELECT pk, floor(time, 1mo" + startingTime + "), min(v), max(v), count(v) FROM %s.testWithDate GROUP BY pk, floor(time, 1mo" + startingTime + ") LIMIT 2";
                    pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toLocalDate("2016-09-01"), 1, 4, 4L),
                               row(1, toLocalDate("2016-10-01"), 5, 7, 3L));

                    stmt = "SELECT pk, floor(time, 1mo" + startingTime + "), min(v), max(v), count(v) FROM %s.testWithDate GROUP BY pk, floor(time, 1mo" + startingTime + ") PER PARTITION LIMIT 1";
                    pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toLocalDate("2016-09-01"), 1, 4, 4L),
                               row(2, toLocalDate("2016-11-01"), 10, 11, 2L));

                    stmt = "SELECT pk, floor(time, 1mo" + startingTime + "), min(v), max(v), count(v) FROM %s.testWithDate WHERE pk = 1 GROUP BY pk, floor(time, 1mo" + startingTime + ") ORDER BY time DESC";
                    pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toLocalDate("2016-11-01"), 8, 8, 1L),
                               row(1, toLocalDate("2016-10-01"), 5, 7, 3L),
                               row(1, toLocalDate("2016-09-01"), 1, 4, 4L));

                    stmt = "SELECT pk, floor(time, 1mo" + startingTime + "), min(v), max(v), count(v) FROM %s.testWithDate WHERE pk = 1 GROUP BY pk, floor(time, 1mo" + startingTime + ") ORDER BY time DESC LIMIT 2";
                    pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toLocalDate("2016-11-01"), 8, 8, 1L),
                               row(1, toLocalDate("2016-10-01"), 5, 7, 3L));
                }
            }
        }
    }

        @Test
        public void testGroupByTimeRangesWithTimeType() throws Throwable
        {
            try (Cluster cluster = init(builder().withNodes(3).start()))
            {

                cluster.schemaChange(withKeyspace("CREATE TABLE %s.testWithTime (pk int, date date, time time, v int, primary key (pk, date, time))"));

                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTime (pk, date, time, v) VALUES (1, '2016-09-27', '16:10:00', 1)"), ConsistencyLevel.QUORUM);
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTime (pk, date, time, v) VALUES (1, '2016-09-27', '16:12:00', 2)"), ConsistencyLevel.QUORUM);
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTime (pk, date, time, v) VALUES (1, '2016-09-27', '16:14:00', 3)"), ConsistencyLevel.QUORUM);
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTime (pk, date, time, v) VALUES (1, '2016-09-27', '16:15:00', 4)"), ConsistencyLevel.QUORUM);
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTime (pk, date, time, v) VALUES (1, '2016-09-27', '16:21:00', 5)"), ConsistencyLevel.QUORUM);
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTime (pk, date, time, v) VALUES (1, '2016-09-27', '16:22:00', 6)"), ConsistencyLevel.QUORUM);
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTime (pk, date, time, v) VALUES (1, '2016-09-27', '16:26:00', 7)"), ConsistencyLevel.QUORUM);
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTime (pk, date, time, v) VALUES (1, '2016-09-27', '16:26:20', 8)"), ConsistencyLevel.QUORUM);
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTime (pk, date, time, v) VALUES (1, '2016-09-28', '16:26:20', 9)"), ConsistencyLevel.QUORUM);
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTime (pk, date, time, v) VALUES (1, '2016-09-28', '16:26:30', 10)"), ConsistencyLevel.QUORUM);

                for (int pageSize : new int[] {2, 3, 4, 5, 7, 10})
                {

                    String stmt = "SELECT pk, date, floor(time, 5m), min(v), max(v), count(v) FROM %s.testWithTime GROUP BY pk, date, floor(time, 5m)";
                    Iterator<Object[]> pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toLocalDate("2016-09-27"), toTime("16:10:00"), 1, 3, 3L),
                               row(1, toLocalDate("2016-09-27"), toTime("16:15:00"), 4, 4, 1L),
                               row(1, toLocalDate("2016-09-27"), toTime("16:20:00"), 5, 6, 2L),
                               row(1, toLocalDate("2016-09-27"), toTime("16:25:00"), 7, 8, 2L),
                               row(1, toLocalDate("2016-09-28"), toTime("16:25:00"), 9, 10, 2L));

                    stmt = "SELECT pk, date, floor(time, 5m), min(v), max(v), count(v) FROM %s.testWithTime GROUP BY pk, date, floor(time, 5m) LIMIT 2";
                    pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toLocalDate("2016-09-27"), toTime("16:10:00"), 1, 3, 3L),
                               row(1, toLocalDate("2016-09-27"), toTime("16:15:00"), 4, 4, 1L));

                    stmt = "SELECT pk, date, floor(time, 5m), min(v), max(v), count(v) FROM %s.testWithTime WHERE pk = 1 GROUP BY pk, date, floor(time, 5m) ORDER BY date DESC, time DESC";
                    pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toLocalDate("2016-09-28"), toTime("16:25:00"), 9, 10, 2L),
                               row(1, toLocalDate("2016-09-27"), toTime("16:25:00"), 7, 8, 2L),
                               row(1, toLocalDate("2016-09-27"), toTime("16:20:00"), 5, 6, 2L),
                               row(1, toLocalDate("2016-09-27"), toTime("16:15:00"), 4, 4, 1L),
                               row(1, toLocalDate("2016-09-27"), toTime("16:10:00"), 1, 3, 3L));

                    stmt = "SELECT pk, date, floor(time, 5m), min(v), max(v), count(v) FROM %s.testWithTime WHERE pk = 1 GROUP BY pk, date, floor(time, 5m) ORDER BY date DESC, time DESC LIMIT 2";
                    pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toLocalDate("2016-09-28"), toTime("16:25:00"), 9, 10, 2L),
                               row(1, toLocalDate("2016-09-27"), toTime("16:25:00"), 7, 8, 2L));
                }
            }
        }

    private static void initFunctions(Cluster cluster)
    {
        cluster.schemaChange(withKeyspace("CREATE FUNCTION %s.concat_strings_fn(a text, b text) " +
                                          "RETURNS NULL ON NULL INPUT " +
                                          "RETURNS text " +
                                          "LANGUAGE java " +
                                          "AS 'return a + \" \" + b;'"));

        cluster.schemaChange(withKeyspace("CREATE AGGREGATE %s.concat(text)" +
                                          " SFUNC concat_strings_fn" +
                                          " STYPE text" +
                                          " INITCOND '_'"));
    }

    private static Date toTimestamp(String timestampAsString)
    {
        return new Date(TimestampSerializer.dateStringToTimestamp(timestampAsString));
    }

    private static int toLocalDate(String dateAsString)
    {
        return SimpleDateSerializer.dateStringToDays(dateAsString) ;
    }

    private static long toTime(String timeAsString)
    {
        return TimeSerializer.timeStringToLong(timeAsString) ;
    }

}