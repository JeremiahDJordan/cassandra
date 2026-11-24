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
import java.util.HashMap;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ApacheUpgradePathGateTest extends CQLTester
{
    @BeforeClass
    public static void setUpApacheUpgradePath() throws Exception
    {
        // in UPGRADING after CASSANDRA_5
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.UPGRADING);
        FileUtils.write(new File(DatabaseDescriptor.getMetadataDirectory(), ApacheUpgradePath.FILE),
                        Collections.singletonList(StorageCompatibilityMode.CASSANDRA_5.name()),
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        ApacheUpgradePath.instance.initialize();
        ApacheUpgradePath.instance.beforeGossipStart(new HashMap<>());
    }

    @Test
    public void testGateEndsWhenNoMemberRunsAtVersion13() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        String like = "SELECT * FROM %s WHERE v LIKE 'a%%'";
        String deterministic = "CREATE FUNCTION " + KEYSPACE + ".f (a int) CALLED ON NULL INPUT RETURNS int DETERMINISTIC LANGUAGE java AS 'return a;'";
        VersionedValue.VersionedValueFactory values = StorageService.instance.valueFactory;

        // a member in CASSANDRA_5
        InetAddressAndPort modeMember = InetAddressAndPort.getByName("127.0.0.2");
        UUID hostId = UUID.randomUUID();
        Gossiper.instance.initializeNodeUnsafe(modeMember, hostId, MessagingService.VERSION_50, 1);
        Gossiper.instance.injectApplicationState(modeMember, ApacheUpgradePath.GOSSIP_STATE, values.compatibilityModes("mode=CASSANDRA_5;compat=CASSANDRA_5"));
        StorageService.instance.getTokenMetadata().updateHostId(hostId, modeMember);
        ApacheUpgradePath.instance.onJoin(modeMember, null);
        assertThatThrownBy(() -> execute(like)).hasMessageContaining("LIKE restriction on column v isn't supported");
        assertThatThrownBy(() -> schemaChange(deterministic)).hasMessageContaining("DETERMINISTIC");

        // a member of unknown version, which gossip removes before the ring does
        InetAddressAndPort unknownMember = InetAddressAndPort.getByName("127.0.0.3");
        StorageService.instance.getTokenMetadata().updateHostId(UUID.randomUUID(), unknownMember);
        ApacheUpgradePath.instance.onJoin(unknownMember, null);
        assertThatThrownBy(() -> schemaChange(deterministic)).hasMessageContaining("as the members [/127.0.0.3:7012] don't advertise their storage_compatibility_mode");
        ApacheUpgradePath.instance.onRemove(unknownMember);
        StorageService.instance.getTokenMetadata().removeEndpoint(unknownMember);
        assertThatThrownBy(() -> execute(like)).hasMessageContaining("LIKE restriction on column v isn't supported");

        // the member in CASSANDRA_5 moves to UPGRADING
        VersionedValue version = values.networkVersion(MessagingService.VERSION_DS_21);
        Gossiper.instance.injectApplicationState(modeMember, ApplicationState.NET_VERSION, version);
        Gossiper.instance.injectApplicationState(modeMember, ApacheUpgradePath.GOSSIP_STATE, values.compatibilityModes("mode=UPGRADING;compat=CASSANDRA_5"));
        MessagingService.instance().versions.set(modeMember, MessagingService.VERSION_DS_21);
        ApacheUpgradePath.instance.onChange(modeMember, ApplicationState.NET_VERSION, version);
        execute(like);

        // another member of unknown version joins, and gossip removes it before the ring does
        StorageService.instance.getTokenMetadata().updateHostId(UUID.randomUUID(), unknownMember);
        ApacheUpgradePath.instance.onJoin(unknownMember, null);
        assertThatThrownBy(() -> execute(like)).hasMessageContaining("LIKE restriction on column v isn't supported");
        ApacheUpgradePath.instance.onRemove(unknownMember);
        execute(like);
        StorageService.instance.getTokenMetadata().removeEndpoint(unknownMember);
        schemaChange(deterministic);
    }
}
