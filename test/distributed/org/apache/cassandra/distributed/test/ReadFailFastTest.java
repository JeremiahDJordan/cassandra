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

import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

/**
 * Tests that a read fails as soon as a replica fails it, unless the read may still get enough responses from a
 * speculative retry. Node 2 fails the reads that scan more than a few tombstones, and is the coordinator, so it's one of
 * the two replicas a QUORUM read of RF 3 contacts. The read timeouts are long, so a read that waits for them is
 * told apart from one that fails right away.
 */
public class ReadFailFastTest extends TestBaseImpl
{
    private static final int TIMEOUT_SECONDS = 30;

    private static Cluster cluster;

    @BeforeClass
    public static void setupCluster() throws Throwable
    {
        cluster = init(Cluster.build(3)
                              .withConfig(config -> {
                                  // the coordinator first contacts itself, whatever the latencies
                                  config.with(GOSSIP).with(NETWORK)
                                        .set("dynamic_snitch", false)
                                        .set("read_request_timeout_in_ms", TIMEOUT_SECONDS * 1000)
                                        .set("range_request_timeout_in_ms", TIMEOUT_SECONDS * 1000)
                                        .set("native_transport_timeout_in_ms", TIMEOUT_SECONDS * 1000);
                                  if (config.num() == 2)
                                      config.set("tombstone_warn_threshold", 2).set("tombstone_failure_threshold", 5);
                              })
                              .start(), 3);

        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s_nts WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"));
        // the speculative retry of t_slow waits longer than the read timeout, so its reads don't speculate
        for (String table : new String[]{ "%s.t", "%s_nts.t", "%s.t_slow" })
        {
            String retry = table.endsWith("t_slow") ? "60000ms" : "10ms";
            cluster.schemaChange(withKeyspace("CREATE TABLE " + table + " (k int, c int, v int, PRIMARY KEY (k, c)) WITH speculative_retry = '" + retry + "'"));
            for (int c = 0; c < 20; c++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO " + table + " (k, c, v) VALUES (0, ?, ?)"), ConsistencyLevel.ALL, c, c);
            for (int c = 0; c < 10; c++)
                cluster.coordinator(1).execute(withKeyspace("DELETE FROM " + table + " WHERE k = 0 AND c = ?"), ConsistencyLevel.ALL, c);
        }
    }

    @AfterClass
    public static void closeCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    /**
     * Range reads never send speculative retries.
     */
    @Test
    public void testRangeReadAtQuorum()
    {
        assertFailsFast("SELECT * FROM %s.t", ConsistencyLevel.QUORUM);
    }

    /**
     * Reads at EACH_QUORUM don't send speculative retries.
     */
    @Test
    public void testSinglePartitionReadAtEachQuorum()
    {
        assertFailsFast("SELECT * FROM %s_nts.t WHERE k = 0", ConsistencyLevel.EACH_QUORUM);
    }

    /**
     * A single partition read at QUORUM that decides not to speculate fails right away.
     */
    @Test
    public void testSinglePartitionReadAtQuorumWithoutRetry()
    {
        cluster.get(2).runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("t_slow").updateSpeculationThreshold());
        assertFailsFast("SELECT * FROM %s.t_slow WHERE k = 0", ConsistencyLevel.QUORUM);
    }

    /**
     * A single partition read at QUORUM gets the response of its speculative retry to the third replica.
     */
    @Test
    public void testSinglePartitionReadAtQuorumSpeculates()
    {
        // a new table waits half the read timeout before it speculates, until its threshold is computed
        cluster.get(2).runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("t").updateSpeculationThreshold());
        long retries = speculativeRetries();

        long start = System.nanoTime();
        Assertions.assertThat(cluster.coordinator(2).execute(withKeyspace("SELECT * FROM %s.t WHERE k = 0"), ConsistencyLevel.QUORUM).length)
                  .isEqualTo(10);
        Assertions.assertThat(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start)).isLessThan(TIMEOUT_SECONDS / 3);
        Assertions.assertThat(speculativeRetries()).isEqualTo(retries + 1);
    }

    private static long speculativeRetries()
    {
        return cluster.get(2).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("t").metric.speculativeRetries.getCount());
    }

    private static void assertFailsFast(String query, ConsistencyLevel consistency)
    {
        long start = System.nanoTime();
        Assertions.assertThatThrownBy(() -> cluster.coordinator(2).execute(withKeyspace(query), consistency))
                  .hasMessageContaining("Operation failed")
                  .hasMessageContaining("READ_TOO_MANY_TOMBSTONES");
        Assertions.assertThat(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start)).isLessThan(TIMEOUT_SECONDS / 3);
    }
}
