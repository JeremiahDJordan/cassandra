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

import com.google.common.collect.ImmutableMultimap;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.ByteOrderedPartitioner.BytesToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.locator.TokenMetadataProvider;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_TMD_PROVIDER_PROPERTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

/**
 * Cleanup with a {@link TokenMetadataProvider} that keeps a keyspace's pending ranges in its own token metadata.
 */
public class CleanupKeyspaceTokenMetadataTest
{
    private static final String KEYSPACE = "CleanupKeyspaceTokenMetadataTest";
    private static final String TABLE = "Standard1";

    @BeforeClass
    public static void defineSchema()
    {
        CUSTOM_TMD_PROVIDER_PROPERTY.setString(KeyspaceTokenMetadataProvider.class.getName());
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), SchemaLoader.standardCFMD(KEYSPACE, TABLE));
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
