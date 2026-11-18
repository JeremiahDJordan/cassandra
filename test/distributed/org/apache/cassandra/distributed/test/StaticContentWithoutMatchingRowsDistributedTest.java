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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.sai.SAIUtil;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * A partition whose rows don't match restrictions nested in a disjunction isn't returned as its static content, on
 * reads that need reconciliation, where the replicas return the deleted rows and the coordinator doesn't filter
 * restrictions on key columns again. With and without indexes.
 */
public class StaticContentWithoutMatchingRowsDistributedTest extends TestBaseImpl
{
    @Test
    public void testPartitionsWithoutMatchingRows() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2).withConfig(c -> c.with(GOSSIP, NETWORK)).start(), 2))
        {
            for (boolean indexed : new boolean[]{ false, true })
            {
                String table = KEYSPACE + (indexed ? ".indexed" : ".plain");
                cluster.schemaChange("CREATE TABLE " + table + " (pk int, ck int, v int, st int static, PRIMARY KEY (pk, ck))");
                if (indexed)
                {
                    cluster.schemaChange("CREATE CUSTOM INDEX ON " + table + "(ck) USING 'StorageAttachedIndex'");
                    cluster.schemaChange("CREATE CUSTOM INDEX ON " + table + "(st) USING 'StorageAttachedIndex'");
                    SAIUtil.waitForIndexQueryableOnAllNodes(cluster, KEYSPACE);
                }

                String[] writes = {
                    // 13, 14: a deleted row; 15: a range deletion; 30: a deleted row and a range deletion
                    "INSERT INTO %s (pk, ck, v) VALUES (13, 1, 1)", "DELETE FROM %s WHERE pk = 13 AND ck = 1",
                    "INSERT INTO %s (pk, st) VALUES (13, 1)",
                    "INSERT INTO %s (pk, ck, v) VALUES (14, 1, 1)", "DELETE FROM %s WHERE pk = 14 AND ck = 1",
                    "INSERT INTO %s (pk, st) VALUES (14, 0)",
                    "INSERT INTO %s (pk, ck, v) VALUES (15, 4, 9)", "DELETE FROM %s WHERE pk = 15 AND ck >= 0",
                    "INSERT INTO %s (pk, st) VALUES (15, 1)",
                    "INSERT INTO %s (pk, ck, v) VALUES (30, 1, 1)", "DELETE FROM %s WHERE pk = 30 AND ck = 1",
                    "DELETE FROM %s WHERE pk = 30 AND ck >= 5", "INSERT INTO %s (pk, st) VALUES (30, 1)",
                    "INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)",
                };
                for (String write : writes)
                    cluster.coordinator(1).execute(write.replace("%s", table), ConsistencyLevel.ALL);

                for (ConsistencyLevel cl : new ConsistencyLevel[]{ ConsistencyLevel.ONE, ConsistencyLevel.ALL })
                {
                    assertRows(cluster.coordinator(1).execute("SELECT pk, ck FROM " + table + " WHERE ck = 1 OR ck > 3 ALLOW FILTERING", cl),
                               row(1, 1));
                    assertRows(cluster.coordinator(1).execute("SELECT pk, ck FROM " + table + " WHERE pk = 13 AND (ck = 1 OR ck = 2) ALLOW FILTERING", cl));
                    assertRows(cluster.coordinator(1).execute("SELECT pk, ck FROM " + table + " WHERE pk = 14 AND (ck = 1 OR ck = 2) ALLOW FILTERING", cl));
                }
            }
        }
    }
}
