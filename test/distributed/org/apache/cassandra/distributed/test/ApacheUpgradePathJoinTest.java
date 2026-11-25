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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.service.ApacheUpgradePath;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A node in storage_compatibility_mode CASSANDRA_5 doesn't join a cluster with nodes of this build that aren't on the
 * Apache upgrade path, nor one whose path has ended, and keeps its record of the last compatibility mode then.
 */
public class ApacheUpgradePathJoinTest extends TestBaseImpl
{
    private static final String[] MODES = { "CASSANDRA_5", "HCD_1", "NONE", "CASSANDRA_5" };

    @Test
    public void testModeNodeRefusesNodeOutsidePath() throws Throwable
    {
        try (Cluster cluster = Cluster.build(4)
                                      .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                                  .set("storage_compatibility_mode", MODES[config.num() - 1]))
                                      .createWithoutStarting())
        {
            cluster.get(1).startup();
            cluster.get(1).shutdown().get();
            cluster.get(1).config().set("storage_compatibility_mode", "UPGRADING");
            cluster.get(1).startup();
            cluster.get(2).startup();
            cluster.get(3).startup();
            cluster.get(3).nodetoolResult("decommission", "--force").asserts().success();
            cluster.get(3).shutdown().get();

            Path record = Paths.get(cluster.get(4).config().getString("metadata_directory")).resolve(ApacheUpgradePath.FILE);
            Files.createDirectories(record.getParent());
            Files.write(record, Collections.singletonList("HCD_1"));
            assertThatThrownBy(() -> cluster.get(4).startup())
            .hasMessageContaining("storage_compatibility_mode CASSANDRA_5 can't join this cluster: ")
            .hasMessageMatching("(?s).*/127\\.0\\.0\\.2:7012 runs messaging version \\d+ in storage_compatibility_mode HCD_1, last compatibility mode HCD_1.*")
            .hasMessageContaining("the Apache upgrade path has ended, as no node runs at messaging version 13; start this node in UPGRADING")
            .hasMessageNotContaining("127.0.0.1")
            .hasMessageNotContaining("127.0.0.3");
            assertThat(Files.readAllLines(record)).containsExactly("HCD_1");
        }
    }
}
