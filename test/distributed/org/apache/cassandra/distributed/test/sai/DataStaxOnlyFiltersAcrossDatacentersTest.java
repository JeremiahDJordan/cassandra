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

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.MessagingService;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.distributed.shared.NetworkTopology.networkTopology;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * An upgrade from Apache Cassandra 4.0 done one datacenter at a time: dc1 is on this release, dc2 on the Apache
 * Cassandra 4.0 messaging version. The DataStax-only filter features are decided on the nodes the query can contact:
 * a dc1 coordinator accepts OR at the LOCAL_* consistency levels, LOCAL_SERIAL included, and not at the others, which can
 * reach dc2.
 */
public class DataStaxOnlyFiltersAcrossDatacentersTest extends TestBaseImpl
{
    private static final String OR = "SELECT k FROM %s.t WHERE n = 1 OR n = 2";
    private static final String RANGE = "SELECT k FROM %s.t WHERE n > 0 AND n < 2";
    private static final String SERIAL_OR = "SELECT k FROM %s.t WHERE k = 1 AND (n = 1 OR n = 2)";

    @Test
    public void testUpgradeByDatacenter() throws Throwable
    {
        try (Cluster cluster = Cluster.build(4)
                                      .withNodeIdTopology(networkTopology(4, node -> node <= 2 ? dcAndRack("dc1", "rack1") : dcAndRack("dc2", "rack1")))
                                      .withInstanceInitializer(SAIUtil.pinMessagingVersion(MessagingService.VERSION_40,
                                                                                           3, 4))
                                      .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                      .start())
        {
            createSchema(cluster);

            for (int node : new int[]{ 1, 2 })
            {
                for (ConsistencyLevel consistency : new ConsistencyLevel[]{ ConsistencyLevel.LOCAL_ONE, ConsistencyLevel.LOCAL_QUORUM })
                {
                    assertThat(cluster.coordinator(node).execute(withKeyspace(OR), consistency).length).isEqualTo(2);
                }
                for (ConsistencyLevel consistency : new ConsistencyLevel[]{ ConsistencyLevel.ONE, ConsistencyLevel.QUORUM })
                {
                    assertThatThrownBy(() -> cluster.coordinator(node).execute(withKeyspace(OR), consistency))
                        .hasMessageContaining(String.format(RowFilter.DATASTAX_ONLY_FEATURE_ERROR, "The OR operator"));
                }
                assertThat(cluster.coordinator(node).execute(withKeyspace(SERIAL_OR), ConsistencyLevel.LOCAL_SERIAL).length).isEqualTo(1);
                assertThatThrownBy(() -> cluster.coordinator(node).execute(withKeyspace(SERIAL_OR), ConsistencyLevel.SERIAL))
                    .hasMessageContaining(String.format(RowFilter.DATASTAX_ONLY_FEATURE_ERROR, "The OR operator"));
                assertThat(cluster.coordinator(node).execute(withKeyspace(RANGE), ConsistencyLevel.QUORUM).length).isEqualTo(1);
                assertThat(cluster.coordinator(node).execute(withKeyspace(RANGE), ConsistencyLevel.LOCAL_QUORUM).length).isEqualTo(1);
            }

            // a node on the Apache Cassandra messaging version rejects them at every consistency level
            for (ConsistencyLevel consistency : new ConsistencyLevel[]{ ConsistencyLevel.LOCAL_ONE, ConsistencyLevel.LOCAL_QUORUM })
            {
                assertThatThrownBy(() -> cluster.coordinator(3).execute(withKeyspace(OR), consistency))
                    .hasMessageContaining(String.format(RowFilter.DATASTAX_ONLY_FEATURE_ERROR, "The OR operator"));
            }
        }
    }

    /**
     * Creates the schema locally on each node, with fixed ids, because schema isn't propagated between nodes on
     * different major messaging versions, and inserts some data.
     */
    private static void createSchema(Cluster cluster)
    {
        String tableId = UUID.nameUUIDFromBytes(KEYSPACE.getBytes(StandardCharsets.UTF_8)).toString();
        for (int node = 1; node <= cluster.size(); node++)
        {
            cluster.get(node).schemaChangeInternal("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = " +
                                                   "{'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 2}");
            cluster.get(node).schemaChangeInternal(withKeyspace("CREATE TABLE IF NOT EXISTS %s.t (k int PRIMARY KEY, n int) WITH ID = " + tableId));
            cluster.get(node).schemaChangeInternal(withKeyspace("CREATE CUSTOM INDEX IF NOT EXISTS n_idx ON %s.t(n) USING 'StorageAttachedIndex'"));
        }
        for (int node = 1; node <= cluster.size(); node++)
            SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, "n_idx", node);

        for (int k = 0; k < 3; k++)
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (k, n) VALUES (?, ?)"), ConsistencyLevel.ALL, k, k);
    }
}
