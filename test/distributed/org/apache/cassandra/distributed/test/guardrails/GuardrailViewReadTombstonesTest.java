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

package org.apache.cassandra.distributed.test.guardrails;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the {@link Guardrails#scannedTombstones} warnings of the read of the base rows that replicas do to build
 * materialized view updates reach the client through a coordinator that isn't a replica.
 * <p>
 * The client warnings of the guardrail are tested in {@link org.apache.cassandra.db.guardrails.GuardrailViewReadTombstonesTest}.
 */
public class GuardrailViewReadTombstonesTest extends GuardrailTester
{
    private static final int WARN_THRESHOLD = 100;
    private static final int RF = 3;

    private static Cluster cluster;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = Cluster.build(4)
                         .withConfig(c -> c.with(GOSSIP, NETWORK)
                                           .set("materialized_views_enabled", true)
                                           .set("tombstone_warn_threshold", WARN_THRESHOLD)
                                           .set("tombstone_failure_threshold", WARN_THRESHOLD * 10))
                         .start();
        cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + RF + "}");
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Override
    protected Cluster getCluster()
    {
        return cluster;
    }

    @After
    @Override
    public void afterTest()
    {
        schemaChange("DROP MATERIALIZED VIEW IF EXISTS %s_view");
        super.afterTest();
    }

    @Test
    public void testViewReadTombstones()
    {
        schemaChange("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");
        cluster.schemaChange(String.format("CREATE MATERIALIZED VIEW %s_view AS SELECT * FROM %s " +
                                           "WHERE k IS NOT NULL AND c IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k, c)",
                                           qualifiedTableName, qualifiedTableName));

        int key = keyNotOnNode4(0);
        assertThat(rangeDeletionWarnings(key, WARN_THRESHOLD - 10)).isEmpty();

        List<String> warnings = rangeDeletionWarnings(keyNotOnNode4(key + 1), WARN_THRESHOLD + 10);
        assertThat(warnings).isNotEmpty().hasSizeLessThanOrEqualTo(RF);
        assertThat(warnings).allMatch(w -> w.startsWith("Guardrail scanned_tombstones violated: Scanned over") &&
                                           w.endsWith("more than the warning threshold " + WARN_THRESHOLD));
    }

    // deletes rows of the partition, then the range of those rows through node 4, which isn't a replica
    private List<String> rangeDeletionWarnings(int key, int rows)
    {
        for (int c = 0; c < rows; c++)
            cluster.coordinator(1).execute(format("DELETE FROM %s WHERE k = ? AND c = ?"), ConsistencyLevel.ALL, key, c);

        // in a batch, as the in-JVM coordinator drops the warnings of a DELETE statement, which has no result
        return cluster.coordinator(4)
                      .executeWithResult(format("BEGIN UNLOGGED BATCH DELETE FROM %s WHERE k = ? AND c >= 0 AND c < ?; APPLY BATCH"),
                                         ConsistencyLevel.ALL, key, rows)
                      .warnings();
    }

    // the first key from the given one that node 4 doesn't replicate
    private int keyNotOnNode4(int from)
    {
        InetSocketAddress address = cluster.get(4).broadcastAddress();
        String table = tableName;
        return cluster.get(1).callOnInstance(() -> {
            InetAddressAndPort node4 = InetAddressAndPort.getByAddressOverrideDefaults(address.getAddress(), address.getPort());
            for (int key = from; ; key++)
            {
                if (!StorageService.instance.getNaturalReplicasForToken(KEYSPACE, table, String.valueOf(key)).endpoints().contains(node4))
                    return key;
            }
        });
    }
}
