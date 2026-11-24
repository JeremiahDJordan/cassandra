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

package org.apache.cassandra.distributed.test.repair;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import org.awaitility.Awaitility;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;

import static org.junit.Assert.assertEquals;

/**
 * Tests that the auto_repair options of a table survive a cluster metadata snapshot.
 */
public class AutoRepairMetadataSnapshotTest extends TestBaseImpl
{
    private static final Map<String, String> EXPECTED = ImmutableMap.of("full_enabled", "false",
                                                                        "incremental_enabled", "true",
                                                                        "preview_repaired_enabled", "true",
                                                                        "priority", "3");

    @Test
    public void testRestartAfterMetadataSnapshot() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int PRIMARY KEY, v int)"));
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.tbl WITH auto_repair = {'full_enabled': 'false', 'priority': '3'}"));

            IInvokableInstance node = cluster.get(1);
            long epoch = node.callOnInstance(() -> ClusterMetadataService.instance().triggerSnapshot().epoch.getEpoch());
            // the snapshot is stored by a log listener after the epoch is enacted
            Awaitility.waitAtMost(30, TimeUnit.SECONDS)
                      .until(() -> node.callOnInstance(() -> ClusterMetadataService.instance().snapshotManager().getSnapshot(Epoch.create(epoch)) != null));

            String keyspace = KEYSPACE;
            assertEquals(EXPECTED, node.callOnInstance(() -> {
                ClusterMetadata snapshot = ClusterMetadataService.instance().snapshotManager().getSnapshot(Epoch.create(epoch));
                return new HashMap<>(snapshot.schema.getKeyspaceMetadata(keyspace).tables.getNullable("tbl").params.autoRepair.asMap());
            }));

            node.shutdown().get();
            node.startup();

            assertEquals(EXPECTED, node.callOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                return new HashMap<>(metadata.schema.getKeyspaceMetadata(keyspace).tables.getNullable("tbl").params.autoRepair.asMap());
            }));
        }
    }
}
