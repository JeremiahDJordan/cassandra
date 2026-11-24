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

import java.lang.reflect.Method;

import org.junit.Test;

import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_CASSANDRA_5_MODE_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Nodes in storage_compatibility_mode CASSANDRA_5 and UPGRADING in one JVM each use the messaging and storage
 * versions of their mode, and a node that ran in UPGRADING warns in CASSANDRA_5 of the data Apache Cassandra 5.0
 * can't read.
 */
public class Cassandra5ModeTest extends TestBaseImpl
{
    // the hints version of messaging version 13, the one Apache Cassandra 5.0 writes
    private static final int HINTS_VERSION_50 = 3;

    @Test
    public void testVersionsPerNode() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(TEST_CASSANDRA_5_MODE_ENABLED, true);
             Cluster cluster = Cluster.build(2)
                                      .withConfig(config -> config.set("storage_compatibility_mode", config.num() == 1 ? "CASSANDRA_5" : "UPGRADING"))
                                      .start())
        {
            assertThat(versions(cluster.get(1))).containsExactly(MessagingService.VERSION_50, MessagingService.VERSION_50, CommitLogDescriptor.VERSION_50, HINTS_VERSION_50);
            int[] upgrading = versions(cluster.get(2));
            assertThat(upgrading[0]).isEqualTo(MessagingService.VERSION_DS_21);
            assertThat(upgrading[1]).isEqualTo(MessagingService.VERSION_DS_20);
            assertThat(upgrading[2]).isNotEqualTo(CommitLogDescriptor.VERSION_50);

            cluster.get(2).schemaChangeInternal(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"));
            cluster.get(2).schemaChangeInternal(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));
            cluster.get(2).schemaChangeInternal(withKeyspace("CREATE INDEX ON %s.tbl (v) USING 'sai'"));
            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (1, 1)"));
            cluster.get(2).flush(KEYSPACE);
            cluster.get(2).shutdown().get();
            cluster.get(2).config().set("storage_compatibility_mode", "CASSANDRA_5");
            cluster.get(2).startup();
            assertThat(cluster.get(2).logs().grep("This node holds data Apache Cassandra 5.0 can't read").getResult())
            .anyMatch(line -> line.contains("SAI indexes without complete aa groups on " + KEYSPACE + ".tbl; rewrite them with nodetool upgradesstables -a"));
            assertThat(cluster.get(1).logs().grep("This node holds data Apache Cassandra 5.0 can't read").getResult()).isEmpty();
        }
    }

    private static int[] versions(IInvokableInstance instance)
    {
        return instance.callOnInstance(() -> {
            try
            {
                Method hintsVersion = Class.forName("org.apache.cassandra.hints.HintsDescriptor").getDeclaredMethod("currentStorageVersion");
                hintsVersion.setAccessible(true);
                return new int[]{ MessagingService.current_version,
                                  StorageCompatibilityMode.current().storageMessagingVersion(),
                                  CommitLogDescriptor.currentStorageVersion(),
                                  (int) hintsVersion.invoke(null) };
            }
            catch (ReflectiveOperationException e)
            {
                throw new AssertionError(e);
            }
        });
    }
}
