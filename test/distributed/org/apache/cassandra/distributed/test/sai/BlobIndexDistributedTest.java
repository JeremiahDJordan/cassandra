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

package org.apache.cassandra.distributed.test.sai;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.VersionGenerator;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.net.MessagingService;
import org.assertj.core.api.Assertions;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Tests that SAI indexes on blob values (CASSANDRA-20012) are only created while no node is on a messaging version
 * below DS 21, such as Apache Cassandra 5.0 nodes and DataStax nodes older than 5.0.10.
 */
public class BlobIndexDistributedTest extends TestBaseImpl
{
    private static final String REJECTION = "Cannot create a storage-attached index on the blob values of column b while there are nodes on a messaging version below DS 21";

    @Test
    public void testBlobIndexWithAllDataStaxNodes() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 2))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, b blob)"));
            cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.t(b) USING 'sai'"));
            SAIUtil.waitForIndexQueryableOnAllNodes(cluster, KEYSPACE);

            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (k, b) VALUES (0, 0x00)"), ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (k, b) VALUES (1, 0x01)"), ConsistencyLevel.ALL);

            for (int i = 1; i <= cluster.size(); i++)
                assertRows(cluster.coordinator(i).execute(withKeyspace("SELECT k FROM %s.t WHERE b = 0x01"), ConsistencyLevel.ALL),
                           row(1));
        }
    }

    @Test
    public void testBlobIndexWithApacheNode() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withInstanceInitializer(
                                               SAIUtil.pinMessagingVersion(MessagingService.VERSION_50, 1))
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 2))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, b blob, f frozen<list<blob>>)"));

            for (int i = 1; i <= cluster.size(); i++)
            {
                int node = i;
                Assertions.assertThatThrownBy(() -> cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.t(b) USING 'sai'"), false, cluster.get(node)))
                          .hasMessageContaining(REJECTION);
            }

            // frozen collections of blobs were already supported
            cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.t(FULL(f)) USING 'sai'"));
        }
    }

    /**
     * A DataStax 5.0 node before 5.0.10, or a node of this release pinned to its messaging version, DS 20.
     */
    @Test
    public void testBlobIndexWithOlderDataStaxNode() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withInstanceInitializer(
                                               SAIUtil.pinMessagingVersion(MessagingService.VERSION_DS_20, 1))
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 2))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, b blob)"));
            for (int i = 1; i <= cluster.size(); i++)
            {
                int node = i;
                Assertions.assertThatThrownBy(() -> cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.t(b) USING 'sai'"), false, cluster.get(node)))
                          .hasMessageContaining(REJECTION);
            }
        }
    }

    /**
     * Only the messaging version tells what a node supports: a node on DS 21 that gossips the release version of an
     * earlier DataStax 5.0 build doesn't block blob indexes.
     */
    @Test
    public void testBlobIndexWithOlderReleaseVersion() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withInstanceInitializer(OlderReleaseVersion::install)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 2))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, b blob)"));
            cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.t(b) USING 'sai'"), false, cluster.get(2));
        }
    }

    /**
     * Makes the first node gossip the release version of a DataStax 5.0 build older than blob indexes.
     */
    public static class OlderReleaseVersion
    {
        public static void install(ClassLoader classLoader, int node)
        {
            if (node == 1)
            {
                new ByteBuddy().rebase(VersionedValue.VersionedValueFactory.class)
                               .method(named("releaseVersion").and(takesArguments(0)))
                               .intercept(MethodDelegation.to(OlderReleaseVersion.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static VersionedValue releaseVersion()
        {
            return VersionedValue.unsafeMakeVersionedValue("5.0.9.0", VersionGenerator.getNextVersion());
        }
    }
}
