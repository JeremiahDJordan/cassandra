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

import java.util.Optional;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.sensors.ActiveSensorsFactory;
import org.apache.cassandra.sensors.Sensor;
import org.apache.cassandra.sensors.SensorEncoder;
import org.apache.cassandra.sensors.Type;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.config.CassandraRelevantProperties.SENSORS_FACTORY;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
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
        try (WithProperties properties = new WithProperties("cassandra.test.fail_mv_locks_count", "1");
             Cluster cluster = init(builder().withNodes(2)
                                             .withConfig(c -> c.with(GOSSIP).set("enable_materialized_views", true))
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
            cluster.get(2).runOnInstance(() -> {
                DatabaseDescriptor.getGuardrailsConfig().setTombstoneWarnThreshold(TOMBSTONE_FAIL_THRESHOLD);
                DatabaseDescriptor.getGuardrailsConfig().setTombstoneFailureThreshold(TOMBSTONE_FAIL_THRESHOLD);
            });

            // the view read for the partition deletion fails, first on the retry after the lock miss, then on a first attempt
            String delete = withKeyspace("DELETE FROM %s.t WHERE k = ?");
            for (int i = 0; i < 2; i++)
                assertThatThrownBy(() -> cluster.coordinator(1).execute(delete, ONE, key))
                .hasMessageContaining("Operation failed")
                .hasMessageContaining(RequestFailureReason.READ_TOO_MANY_TOMBSTONES.name());
        }
    }

    /**
     * A write that is applied but whose acknowledgement can't be built gets no failure response.
     */
    @Test
    public void testAppliedWriteIsNotReportedAsFailed() throws Throwable
    {
        try (WithProperties properties = new WithProperties(SENSORS_FACTORY.getKey(), FailingEncoderSensorsFactory.class.getName());
             Cluster cluster = init(builder().withNodes(2).start(), 1))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v int)"));
            int key = keyOwnedByNode2(cluster);

            assertThatThrownBy(() -> cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (k, v) VALUES (?, ?)"), ONE, key, 42))
            .hasMessageContaining("Operation timed out");
            assertRows(cluster.get(2).executeInternal(withKeyspace("SELECT v FROM %s.t WHERE k = ?"), key), row(42));
        }
    }

    public static class FailingEncoderSensorsFactory extends ActiveSensorsFactory
    {
        @Override
        public SensorEncoder createSensorEncoder()
        {
            return new SensorEncoder()
            {
                @Override
                public Optional<String> encodeRequestSensorName(Sensor sensor)
                {
                    if (sensor.getType() == Type.WRITE_EXECUTION_TIME)
                        throw new IllegalStateException("Testing sensor encoder failure");
                    return Optional.empty();
                }

                @Override
                public Optional<String> encodeGlobalSensorName(Sensor sensor)
                {
                    return Optional.empty();
                }
            };
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
