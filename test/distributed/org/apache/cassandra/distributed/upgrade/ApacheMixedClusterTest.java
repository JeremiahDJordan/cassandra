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

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertTrue;

public class ApacheMixedClusterTest extends ApacheMixedClusterTestBase
{
    private static final int ROWS = 20;

    @Test
    public void testReadAndWriteAcrossNodeKinds() throws Throwable
    {
        Version apache = apacheVersion();
        try (UpgradeableCluster cluster = startCluster(3, config -> {}, apache, CURRENT))
        {
            assertTrue(cluster.get(1).getReleaseVersionString().startsWith(apache.version.getValue()));
            schemaChange(cluster, KEYSPACE, withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}"));
            schemaChange(cluster, KEYSPACE, withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));

            for (int pk = 0; pk < ROWS; pk++)
                cluster.coordinator(pk % 2 + 1).execute(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (?, ?)"), ALL, pk, pk);
            assertAllRows(cluster, 1, 2);

            bootstrapNode(cluster, CURRENT);
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
