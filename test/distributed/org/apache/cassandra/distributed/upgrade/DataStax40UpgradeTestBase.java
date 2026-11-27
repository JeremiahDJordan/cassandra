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

package org.apache.cassandra.distributed.upgrade;

import org.apache.commons.lang3.ArrayUtils;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.impl.InstanceConfig;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.Versions.Version;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_DATASTAX_40_DTEST_JAR_DIR;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

/**
 * Base for tests of DataStax 4.0 nodes moved along the upgrade procedure. The 4.0 nodes run from the single dtest jar
 * in the directory named by {@code cassandra.test.datastax_40_dtest_jar_dir}; without that property the tests are
 * skipped.
 */
public abstract class DataStax40UpgradeTestBase extends UpgradeTestBase
{
    /**
     * The DataStax 4.0 version of the dtest jar in the configured directory. Its nodes also load the classes that the
     * jar doesn't have, such as {@link BytePagingReads}, from this build.
     */
    protected static Version dataStax40Version()
    {
        Version version = ApacheMixedClusterTestBase.dtestJarVersion(TEST_DATASTAX_40_DTEST_JAR_DIR);
        return new Version(version.version, ArrayUtils.addAll(version.classpath, AbstractCluster.CURRENT_VERSION.classpath));
    }

    /**
     * A cluster of {@code nodes} nodes of {@code version}, connected by real internode and native connections.
     */
    protected static UpgradeableCluster.Builder clusterBuilder(int nodes, Version version)
    {
        return UpgradeableCluster.build(nodes)
                                 .withVersion(version)
                                 .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                                 // DataStax 4.0's NativeLibrary classes have other method signatures than this build's
                                 .withSharedClasses(name -> !name.startsWith("org.apache.cassandra.utils.NativeLibrary") && AbstractCluster.SHARED_PREDICATE.test(name));
    }

    /**
     * Restarts the node on {@code version} in storage_compatibility_mode {@code mode}, or without the setting, as 4.0
     * needs, for a null mode, and waits until every node sees every node up.
     */
    protected static void restart(UpgradeableCluster cluster, int node, Version version, String mode) throws Exception
    {
        IUpgradeableInstance instance = cluster.get(node);
        instance.shutdown().get();
        if (mode == null)
            ((InstanceConfig) instance.config()).remove("storage_compatibility_mode");
        else
            instance.config().set("storage_compatibility_mode", mode);
        instance.setVersion(version);
        instance.startup();
        cluster.forEach(ClusterUtils::awaitRingHealthy);
    }
}
