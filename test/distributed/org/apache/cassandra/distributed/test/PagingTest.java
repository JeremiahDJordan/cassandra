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
import java.util.Iterator;

import com.google.common.collect.Iterators;
import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.metrics.TableMetrics;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertEquals;

public class PagingTest extends TestBaseImpl
{
    @Test
    public void testPaging() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).start());
             Cluster singleNode = init(builder().withNodes(1).withSubnet(1).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))"));
            singleNode.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))"));

            for (int i = 0; i < 10; i++)
            {
                for (int j = 0; j < 10; j++)
                {
                    cluster.coordinator(1)
                           .execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (1, ?, ?)"), QUORUM, i, j, i + i);
                    singleNode.coordinator(1)
                              .execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (1, ?, ?)"), QUORUM, i, j, i + i);
                }
            }

            int[] pageSizes = new int[]{ 1, 2, 3, 5, 10, 20, 50, Integer.MAX_VALUE };
            String[] statements = new String[]{ withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck >= 5"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5 AND ck <= 10"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5 LIMIT 3"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck >= 5 LIMIT 2"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5 AND ck <= 10 LIMIT 2"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5 ORDER BY ck DESC"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck >= 5 ORDER BY ck DESC"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5 AND ck <= 10 ORDER BY ck DESC"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5 ORDER BY ck DESC LIMIT 3"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck >= 5 ORDER BY ck DESC LIMIT 2"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5 AND ck <= 10 ORDER BY ck DESC LIMIT 2"),
                                                withKeyspace("SELECT DISTINCT pk FROM %s.tbl LIMIT 3"),
                                                withKeyspace("SELECT DISTINCT pk FROM %s.tbl WHERE pk IN (3,5,8,10)"),
                                                withKeyspace("SELECT DISTINCT pk FROM %s.tbl WHERE pk IN (3,5,8,10) LIMIT 2")
            };
            for (String statement : statements)
            {
                Object[][] noPagingRows = singleNode.coordinator(1).execute(statement, QUORUM);
                for (int pageSize : pageSizes)
                {
                    Iterator<Object[]> pagingRows = cluster.coordinator(1).executeWithPaging(statement, QUORUM, pageSize);
                    assertRows(Iterators.toArray(pagingRows, Object[].class), noPagingRows);
                }
            }
        }
    }

    @Test
    public void testPagingWithRangeTombstones() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, regular int, PRIMARY KEY (pk, ck))");
            cluster.coordinator(1).execute("DELETE FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck > 1 AND ck < 10", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("insert into " + KEYSPACE + ".tbl (pk, ck, regular) values (1,1,1)", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("insert into " + KEYSPACE + ".tbl (pk, ck, regular) values (1,2,2)", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("insert into " + KEYSPACE + ".tbl (pk, ck, regular) values (1,3,3)", ConsistencyLevel.ALL);
            cluster.forEach((node) -> node.flush(KEYSPACE));
            Iterator<Object[]> iter = cluster.coordinator(1).executeWithPaging("SELECT pk,ck,regular FROM " + KEYSPACE + ".tbl " +
                                                                               "WHERE pk=? AND ck>=? ORDER BY ck DESC;",
                                                                               ConsistencyLevel.QUORUM, 1,
                                                                               1, 1);

            assertRows(iter,
                       row(1, 3, 3),
                       row(1, 2, 2),
                       row(1, 1, 1));
        }
    }

    @Test
    public void testEachPageIsReadOnce() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))"));
            for (int i = 0; i < 3; i++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (0, ?, ?)"), ConsistencyLevel.ALL, i, i);

            // one page of a single partition read
            assertReads(cluster, "SELECT * FROM %s.tbl WHERE pk = 0", 10, 1, 0);
            // two pages of a single partition read, the second one not full
            assertReads(cluster, "SELECT * FROM %s.tbl WHERE pk = 0", 2, 2, 0);
            // one page of a range read
            assertReads(cluster, "SELECT * FROM %s.tbl", 10, 0, 1);
        }
    }

    private static void assertReads(Cluster cluster, String query, int pageSize, long expectedReads, long expectedScans)
    {
        long readsBefore = coordinatorReads(cluster);
        long scansBefore = coordinatorScans(cluster);

        assertRows(cluster.coordinator(1).executeWithPaging(withKeyspace(query), ConsistencyLevel.ALL, pageSize),
                   row(0, 0, 0),
                   row(0, 1, 1),
                   row(0, 2, 2));

        assertEquals("Single partition reads of " + query + " with page size " + pageSize,
                     expectedReads, coordinatorReads(cluster) - readsBefore);
        assertEquals("Range reads of " + query + " with page size " + pageSize,
                     expectedScans, coordinatorScans(cluster) - scansBefore);
    }

    private static long coordinatorReads(Cluster cluster)
    {
        return cluster.get(1).callOnInstance(() -> tableMetrics().coordinatorReadLatency.tableOrKeyspaceTimer().getCount());
    }

    private static long coordinatorScans(Cluster cluster)
    {
        return cluster.get(1).callOnInstance(() -> tableMetrics().coordinatorScanLatency.tableOrKeyspaceTimer().getCount());
    }

    private static TableMetrics tableMetrics()
    {
        return Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").metric;
    }
}
