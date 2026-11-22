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

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_FAIL_MV_LOCKS_COUNT;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

public class ViewLockRetryFailureTest extends TestBaseImpl
{
    private static final int TOMBSTONE_FAIL_THRESHOLD = 10;

    /**
     * A replica whose first view lock attempt fails applies the write later on another mutation stage thread. A
     * failure of that retry is sent back to the coordinator like a failure of the first attempt.
     */
    @Test
    public void testFailureAfterViewLockRetryIsReported() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(TEST_FAIL_MV_LOCKS_COUNT, 1);
             Cluster cluster = init(builder().withNodes(2)
                                             .withConfig(c -> c.with(GOSSIP).set("materialized_views_enabled", true))
                                             .start(), 1))
        {
            cluster.setUncaughtExceptionsFilter(t -> TombstoneOverwhelmingException.class.getName().equals(t.getClass().getName()));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int, c int, v int, PRIMARY KEY (k, c))"));

            int key = keyOwnedByNode2(cluster);

            for (int c = 0; c <= TOMBSTONE_FAIL_THRESHOLD; c++)
                cluster.coordinator(1).execute(withKeyspace("DELETE FROM %s.t WHERE k = ? AND c = ?"), ONE, key, c);

            cluster.schemaChange(String.format("CREATE MATERIALIZED VIEW %s.mv AS SELECT * FROM %s.t " +
                                               "WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, k)", KEYSPACE, KEYSPACE));
            await().atMost(1, MINUTES).until(() -> cluster.get(2).callOnInstance(() -> SystemKeyspace.isViewBuilt(KEYSPACE, "mv")));
            cluster.get(2).runOnInstance(() -> DatabaseDescriptor.setTombstoneFailureThreshold(TOMBSTONE_FAIL_THRESHOLD));

            // the view read for the partition deletion fails, first on the retry after the lock miss, then on a first attempt
            String delete = withKeyspace("DELETE FROM %s.t WHERE k = ?");
            for (int i = 0; i < 2; i++)
                assertThatThrownBy(() -> cluster.coordinator(1).execute(delete, ONE, key))
                .hasMessageContaining("Operation failed")
                .hasMessageContaining(RequestFailureReason.READ_TOO_MANY_TOMBSTONES.name());
        }
    }

    private static int keyOwnedByNode2(Cluster cluster)
    {
        return cluster.get(2).callOnInstance(() -> {
            int key = 0;
            while (!StorageService.instance.getNaturalReplicasForToken(KEYSPACE, "t", Integer.toString(key))
                                           .endpoints()
                                           .contains(FBUtilities.getBroadcastAddressAndPort()))
                key++;
            return key;
        });
    }
}
