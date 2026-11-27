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
package org.apache.cassandra.db;

import java.util.Collections;

import com.google.common.collect.ImmutableMultimap;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.ByteOrderedPartitioner.BytesToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.locator.TokenMetadataProvider;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_TMD_PROVIDER_PROPERTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

/**
 * Cleanup and writes with a {@link TokenMetadataProvider} that keeps a keyspace's token metadata apart from the node's.
 */
public class KeyspaceTokenMetadataTest
{
    private static final String KEYSPACE = "KeyspaceTokenMetadataTest";
    private static final String TABLE = "Standard1";

    @BeforeClass
    public static void defineSchema()
    {
        CUSTOM_TMD_PROVIDER_PROPERTY.setString(KeyspaceTokenMetadataProvider.class.getName());
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), SchemaLoader.standardCFMD(KEYSPACE, TABLE));
    }

    @Before
    public void clearKeyspaceTokenMetadata()
    {
        StorageService.instance.getTokenMetadataForKeyspace(KEYSPACE).clearUnsafe();
    }

    @Test
    public void testCleanupIsAbortedWhenKeyspaceTokenMetadataHasPendingRanges() throws Exception
    {
        TokenMetadata keyspaceTokenMetadata = StorageService.instance.getTokenMetadataForKeyspace(KEYSPACE);
        assertNotSame(StorageService.instance.getTokenMetadata(), keyspaceTokenMetadata);

        Range<Token> range = new Range<>(new BytesToken(new byte[]{ 0 }), new BytesToken(new byte[]{ 1 }));
        keyspaceTokenMetadata.setPendingRangesUnsafe(KEYSPACE, ImmutableMultimap.of(range, Replica.fullReplica(FBUtilities.getBroadcastAddressAndPort(), range)));

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        assertEquals(CompactionManager.AllSSTableOpStatus.ABORTED, CompactionManager.instance.performCleanup(cfs, 1));
    }

    @Test
    public void testCleanupKeepsRangesOwnedInKeyspaceTokenMetadata() throws Exception
    {
        StorageService.instance.getTokenMetadata().clearUnsafe();
        StorageService.instance.getTokenMetadataForKeyspace(KEYSPACE).updateNormalToken(new BytesToken(new byte[]{ 50 }), FBUtilities.getBroadcastAddressAndPort());

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        new RowUpdateBuilder(cfs.metadata(), 0, "key").clustering("name").add("val", "value").build().applyUnsafe();
        Util.flush(cfs);

        assertEquals(CompactionManager.AllSSTableOpStatus.SUCCESSFUL, CompactionManager.instance.performCleanup(cfs, 1));
        assertEquals(1, Util.getAll(Util.cmd(cfs).build()).size());
    }

    @Test
    public void testWritesIncludePendingReplicasOfKeyspaceTokenMetadata() throws Exception
    {
        TokenMetadata keyspaceTokenMetadata = StorageService.instance.getTokenMetadataForKeyspace(KEYSPACE);
        Range<Token> range = new Range<>(new BytesToken(new byte[]{ 0 }), new BytesToken(new byte[]{ 2 }));
        Replica pending = Replica.fullReplica(InetAddressAndPort.getByName("127.0.0.2"), range);
        keyspaceTokenMetadata.setPendingRangesUnsafe(KEYSPACE, ImmutableMultimap.of(range, pending));

        Token token = new BytesToken(new byte[]{ 1 });
        assertEquals(Collections.singletonList(pending.endpoint()), ReplicaLayout.forTokenWriteLiveAndDown(Keyspace.open(KEYSPACE), token).pending().endpointList());
    }

    public static class KeyspaceTokenMetadataProvider implements TokenMetadataProvider
    {
        private final TokenMetadata keyspaceTokenMetadata = new TokenMetadata();
        private volatile TokenMetadata tokenMetadata = new TokenMetadata();

        @Override
        public TokenMetadata getTokenMetadata()
        {
            return tokenMetadata;
        }

        @Override
        public TokenMetadata getTokenMetadataForKeyspace(String keyspace)
        {
            return KEYSPACE.equals(keyspace) ? keyspaceTokenMetadata : tokenMetadata;
        }

        @Override
        public void replaceTokenMetadata(TokenMetadata newTokenMetadata)
        {
            tokenMetadata = newTokenMetadata;
        }
    }
}
