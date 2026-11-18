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

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.DataStaxFeatureSupport;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.distributed.shared.NetworkTopology.networkTopology;
import static org.apache.cassandra.distributed.test.sai.LikeFilteringDistributedTest.REJECTION;
import static org.apache.cassandra.distributed.test.sai.LikeFilteringDistributedTest.keys;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * LIKE during an upgrade done one datacenter at a time: dc1 is on messaging version DS 21, dc2 on DS 20. A query
 * accepts LIKE while the nodes it can contact are on DS 21, so dc1 coordinators accept it at the LOCAL_* consistency
 * levels, and reject it at the others, which can reach dc2. A read that reaches dc2 anyway fails for those nodes.
 * Blob indexes need every node of the ring.
 */
public class LikeAcrossDatacentersTest extends TestBaseImpl
{
    /** The queries and the keys they select; as a plain "contains", the first one would also return 2. */
    private static final String[] QUERIES = { "SELECT k FROM %s.t WHERE %s LIKE 'abc'",
                                              "SELECT k FROM %s.t WHERE %s LIKE 'a%%c'" };
    private static final Integer[][] EXPECTED = { { 1 }, { 1, 3, 4 } };

    @Test
    public void testUpgradeByDatacenter() throws Throwable
    {
        try (Cluster cluster = Cluster.build(4)
                                      .withNodeIdTopology(networkTopology(4, node -> node <= 2 ? dcAndRack("dc1", "rack1") : dcAndRack("dc2", "rack1")))
                                      .withInstanceInitializer(BBHelper::install)
                                      .withConfig(config -> config.with(GOSSIP).with(NETWORK)
                                                                  .set("read_request_timeout", "60s")
                                                                  .set("range_request_timeout", "60s"))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 2}");
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v text, u text, b blob)"));
            cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.t(v) USING 'sai'"));
            SAIUtil.waitForIndexQueryableOnAllNodes(cluster, KEYSPACE);
            String[] values = { "abc", "xabcx", "a%c", "axxc" };
            for (int k = 1; k <= values.length; k++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (k, v, u) VALUES (?, ?, ?)"), ConsistencyLevel.ALL,
                                               k, values[k - 1], values[k - 1]);

            for (int node : new int[]{ 1, 2 })
            {
                ICoordinator coordinator = cluster.coordinator(node);
                for (ConsistencyLevel consistency : new ConsistencyLevel[]{ ConsistencyLevel.LOCAL_ONE, ConsistencyLevel.LOCAL_QUORUM })
                {
                    for (int q = 0; q < QUERIES.length; q++)
                    {
                        assertThat(keys(coordinator, QUERIES[q], "v", "", consistency)).containsExactly(EXPECTED[q]);
                        assertThat(keys(coordinator, QUERIES[q], "u", " ALLOW FILTERING", consistency))
                            .containsExactly(EXPECTED[q]);
                    }
                }
                assertRejected(cluster, node, ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL);
            }

            for (int node : new int[]{ 3, 4 })
                assertRejected(cluster, node, ConsistencyLevel.LOCAL_ONE, ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.ONE, ConsistencyLevel.QUORUM);

            // a read of node 1 that reaches dc2 although it shouldn't, as if node 1 didn't know their versions yet
            cluster.get(1).runOnInstance(() -> MessagingService.instance().outboundSink.clear());
            cluster.get(1).runOnInstance(() -> BBHelper.ignoreVersions.set(true));
            assertThatThrownBy(() -> keys(cluster.coordinator(1), QUERIES[0], "v", "", ConsistencyLevel.ALL))
                .hasMessageContaining("Operation failed");
            assertThatThrownBy(() -> keys(cluster.coordinator(1), "SELECT k FROM %s.t WHERE k = 1 AND %s LIKE 'abc'",
                                          "u", " ALLOW FILTERING", ConsistencyLevel.ALL))
                .hasMessageContaining("Operation failed");
            assertThat(cluster.get(1).logs().grep("Cannot serialize a row filter for messaging version " + MessagingService.VERSION_DS_20 +
                                                  ": the LIKE operator needs DS 21").getResult()).isNotEmpty();
            cluster.get(1).runOnInstance(() -> BBHelper.ignoreVersions.set(false));

            for (int node = 1; node <= cluster.size(); node++)
            {
                int coordinator = node;
                assertThatThrownBy(() -> cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.t(b) USING 'sai'"), false, cluster.get(coordinator)))
                    .hasMessageContaining("Cannot create a storage-attached index on the blob values of column b while there are nodes on a messaging version below DS 21");
            }
        }
    }

    private static void assertRejected(Cluster cluster, int node, ConsistencyLevel... consistencies)
    {
        for (ConsistencyLevel consistency : consistencies)
        {
            for (String query : QUERIES)
            {
                assertThatThrownBy(() -> keys(cluster.coordinator(node), query, "v", "", consistency))
                    .as(query + " at " + consistency + " on node " + node)
                    .hasMessageContaining(REJECTION);
                assertThatThrownBy(() -> keys(cluster.coordinator(node), query, "u", " ALLOW FILTERING", consistency))
                    .as(query + " at " + consistency + " on node " + node)
                    .hasMessageContaining(REJECTION);
            }
        }
    }

    /**
     * Pins dc2 to messaging version DS 20, and lets node 1 accept LIKE whatever nodes its reads can contact.
     */
    public static class BBHelper
    {
        static final AtomicBoolean ignoreVersions = new AtomicBoolean();

        public static void install(ClassLoader classLoader, Integer node)
        {
            SAIUtil.pinMessagingVersion(MessagingService.VERSION_DS_20, 3, 4).accept(classLoader, node);
            if (node == 1)
            {
                new ByteBuddy().rebase(DataStaxFeatureSupport.class)
                               .method(named("nodesNotSupportingRead"))
                               .intercept(MethodDelegation.to(BBHelper.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static Set<InetAddressAndPort> nodesNotSupportingRead(String keyspace,
                                                                     org.apache.cassandra.db.ConsistencyLevel consistency,
                                                                     @SuperCall Callable<Set<InetAddressAndPort>> zuperCall) throws Exception
        {
            return ignoreVersions.get() ? Collections.emptySet() : zuperCall.call();
        }
    }
}
