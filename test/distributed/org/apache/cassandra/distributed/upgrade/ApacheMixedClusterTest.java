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

import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.shared.Versions.Version;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.impl.AbstractCluster.CURRENT_VERSION;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ApacheMixedClusterTest extends ApacheMixedClusterTestBase
{
    private static final int ROWS = 20;

    @Test
    public void testReadAndWriteAcrossNodeKinds() throws Throwable
    {
        Version apache = apacheVersion();
        try (UpgradeableCluster cluster = startCluster(3, config -> {}, apache, CURRENT_VERSION))
        {
            assertTrue(cluster.get(1).getReleaseVersionString().startsWith(apache.version.getValue()));
            assertEquals(FBUtilities.getReleaseVersionString(), cluster.get(2).getReleaseVersionString());
            schemaChange(cluster, KEYSPACE, withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}"));
            schemaChange(cluster, KEYSPACE, withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));

            for (int pk = 0; pk < ROWS; pk++)
                cluster.coordinator(pk % 2 + 1).execute(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (?, ?)"), ALL, pk, pk);
            assertAllRows(cluster, 1, 2);

            bootstrapNode(cluster, CURRENT_VERSION);
            assertEquals(FBUtilities.getReleaseVersionString(), cluster.get(3).getReleaseVersionString());

            // after cleanup each row is only on its two replicas, so the counts add up only if node 3 received its ranges
            cluster.get(1).nodetoolResult("cleanup", KEYSPACE).asserts().success();
            cluster.get(2).nodetoolResult("cleanup", KEYSPACE).asserts().success();
            int localRows = 0;
            for (int node = 1; node <= 3; node++)
                localRows += cluster.get(node).executeInternal(withKeyspace("SELECT pk FROM %s.tbl")).length;
            assertTrue(cluster.get(3).executeInternal(withKeyspace("SELECT pk FROM %s.tbl")).length > 0);
            assertEquals(2 * ROWS, localRows);

            assertAllRows(cluster, 1, 2, 3);
        }
    }

    private static void assertAllRows(UpgradeableCluster cluster, int... coordinators)
    {
        for (int node : coordinators)
        {
            for (int pk = 0; pk < ROWS; pk++)
                assertRows(cluster.coordinator(node).execute(withKeyspace("SELECT pk, v FROM %s.tbl WHERE pk = ?"), ALL, pk), row(pk, pk));
        }
    }
}
