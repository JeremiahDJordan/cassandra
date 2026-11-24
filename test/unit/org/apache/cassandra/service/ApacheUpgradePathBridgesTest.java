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

package org.apache.cassandra.service;

import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Which peers a node on the Apache upgrade path exchanges schema and repairs with across messaging versions 13 and 110,
 * and the schema changes checked beside a node at 13.
 */
public class ApacheUpgradePathBridgesTest extends CQLTester
{
    private static final VersionedValue.VersionedValueFactory values = StorageService.instance.valueFactory;
    private static InetAddressAndPort modeMember;

    @BeforeClass
    public static void setUpApacheUpgradePath() throws Exception
    {
        // in UPGRADING after CASSANDRA_5, beside a ring member in CASSANDRA_5
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.UPGRADING);
        FileUtils.write(new File(DatabaseDescriptor.getMetadataDirectory(), ApacheUpgradePath.FILE),
                        Collections.singletonList(StorageCompatibilityMode.CASSANDRA_5.name()),
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        ApacheUpgradePath.instance.initialize();
        ApacheUpgradePath.instance.beforeGossipStart(new HashMap<>());
    }

    @Before
    public void setUpModeMember() throws Exception
    {
        // gossip evicts a member without tokens after a while
        modeMember = peer("127.0.0.2", MessagingService.VERSION_50, "5.0.10", "mode=CASSANDRA_5;compat=CASSANDRA_5");
        StorageService.instance.getTokenMetadata().updateHostId(UUID.randomUUID(), modeMember);
        ApacheUpgradePath.instance.onJoin(modeMember, null);
    }

    @After
    public void restoreMode()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.UPGRADING);
    }

    // this node runs at messaging version DS 21, so only peers at 13 are on the other side of the bridge
    @Test
    public void testBridges() throws Exception
    {
        assertThat(ApacheUpgradePath.bridges(modeMember, MessagingService.VERSION_50)).isTrue();
        assertThat(ApacheUpgradePath.bridges(peer("127.0.0.3", MessagingService.VERSION_50, "5.0.9", null), MessagingService.VERSION_50)).isTrue();

        assertThat(ApacheUpgradePath.bridges(InetAddressAndPort.getByName("127.0.0.5"), MessagingService.VERSION_50)).isFalse();
        assertThat(ApacheUpgradePath.bridges(peer("127.0.0.6", MessagingService.VERSION_50, "4.1.0", null), MessagingService.VERSION_50)).isFalse();
        assertThat(ApacheUpgradePath.bridges(peer("127.0.0.7", MessagingService.VERSION_50, null, null), MessagingService.VERSION_50)).isFalse();
        assertThat(ApacheUpgradePath.bridges(peer("127.0.0.8", MessagingService.VERSION_50, "5.0.10", "mode=UPGRADING;compat=CASSANDRA_5"), MessagingService.VERSION_50)).isFalse();

        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
        assertThat(ApacheUpgradePath.bridges(modeMember, MessagingService.VERSION_50)).isFalse();
    }

    @Test
    public void testRepairRefusal() throws Exception
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        InetAddressAndPort unidentified = peer("127.0.0.10", MessagingService.VERSION_50, "4.1.0", null);
        assertThat(ApacheUpgradePath.repairRefusal(unidentified)).contains("has an unknown messaging version");
        MessagingService.instance().versions.set(unidentified, MessagingService.VERSION_50);
        MessagingService.instance().versions.set(modeMember, MessagingService.VERSION_50);
        assertThat(ApacheUpgradePath.repairRefusal(modeMember)).isNull();
        assertThat(ApacheUpgradePath.repairRefusal(unidentified)).contains("runs messaging version 13 and isn't identified as a node on the Apache upgrade path");

        assertThatThrownBy(() -> ActiveRepairService.instance().prepareForRepair(nextTimeUUID(), FBUtilities.getBroadcastAddressAndPort(), Set.of(unidentified),
                                                                                 RepairOption.parse(Collections.emptyMap(), IPartitioner.global()),
                                                                                 false, Collections.singletonList(getCurrentColumnFamilyStore())))
        .hasMessageContaining("Rejecting repair: /127.0.0.10:7012 runs messaging version 13");
    }

    @Test
    public void testSchemaChangesCheckedBesideNodeAtVersion13()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
        assertThatThrownBy(() -> schemaChange("CREATE FUNCTION " + KEYSPACE + ".f (a int) CALLED ON NULL INPUT RETURNS int DETERMINISTIC LANGUAGE java AS 'return a;'"))
        .hasMessageContaining("DETERMINISTIC");
    }

    /**
     * A gossip member with the given messaging version, and release and modes unless they are null.
     */
    private static InetAddressAndPort peer(String address, int version, String release, String modes) throws Exception
    {
        InetAddressAndPort endpoint = InetAddressAndPort.getByName(address);
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        states.put(ApplicationState.NET_VERSION, values.networkVersion(version));
        if (release != null)
            states.put(ApplicationState.RELEASE_VERSION, values.releaseVersion(release));
        if (modes != null)
            states.put(ApacheUpgradePath.GOSSIP_STATE, values.compatibilityModes(modes));
        EndpointState state = new EndpointState(new HeartBeatState(1, 0));
        state.addApplicationStates(states);
        Gossiper.instance.endpointStateMap.put(endpoint, state);
        return endpoint;
    }
}
