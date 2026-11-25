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

import java.util.Arrays;
import java.util.Collection;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.distributed.UpgradeableCluster;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.impl.AbstractCluster.CURRENT_VERSION;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The DataStax-only features that are off on the Apache upgrade path, on two nodes of this build in
 * storage_compatibility_mode CASSANDRA_5 beside an Apache node: each is refused by both of them.
 */
@RunWith(Parameterized.class)
public class Cassandra5ModeFeaturesTest extends ApacheMixedClusterTestBase
{
    private static UpgradeableCluster cluster;

    @Parameterized.Parameter
    public String feature;

    @Parameterized.Parameter(1)
    public String statement;

    @Parameterized.Parameter(2)
    public String refusal;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> features()
    {
        return Arrays.asList(new Object[][]{
            { "OR", "SELECT k FROM %s.t WHERE a = 1 OR b = 2", "while the query can contact nodes on a messaging version below DS 10" },
            { "LIKE", "SELECT k FROM %s.t WHERE s LIKE 'x%%'", "while the query can contact nodes on a messaging version" },
            { "ANN options", "SELECT k FROM %s.t ORDER BY v ANN OF [1, 1] LIMIT 10 WITH ann_options = {'rerank_k': 10}", "ANN options are not supported in clusters below DS 11" },
            { "index hints", "SELECT k FROM %s.t WHERE a = 1 WITH included_indexes = {t_a}", "Index hints are not supported in clusters below DS 12" },
            { "optimizer options", "SELECT k FROM %s.t WHERE a = 1 WITH optimizer_options = {'query_optimization_level': '0'}", "SAI optimizer options are not supported in clusters below DS 20" },
            { "analyzed index", "CREATE CUSTOM INDEX ON %s.t(u) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}", "Apache Cassandra 5.0" },
            { "blob index", "CREATE CUSTOM INDEX ON %s.t(bl) USING 'StorageAttachedIndex'", "while there are nodes on a messaging version below DS 21" },
        });
    }

    @BeforeClass
    public static void setUpCluster() throws Throwable
    {
        cluster = startCluster(3,
                               config -> {
                                   // one data directory, which vector indexes need on the path
                                   config.set("data_file_directories", new String[]{ ((String[]) config.get("data_file_directories"))[0] });
                                   if (config.num() > 1)
                                       config.set("storage_compatibility_mode", "CASSANDRA_5");
                               },
                               apacheVersion(), CURRENT_VERSION, CURRENT_VERSION);
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}"), false, cluster.get(2));
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, a int, b int, s text, u text, bl blob, v vector<float, 2>)"), false, cluster.get(2));
        for (String column : new String[]{ "a", "b", "s", "v" })
            cluster.schemaChange(withKeyspace("CREATE INDEX t_" + column + " ON %s.t (" + column + ") USING 'sai'"), false, cluster.get(2));
    }

    @AfterClass
    public static void tearDownCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testRefused()
    {
        for (int node = 2; node <= 3; node++)
        {
            int coordinator = node;
            assertThatThrownBy(() -> cluster.coordinator(coordinator).execute(withKeyspace(statement), ONE))
            .as(feature + " on node " + coordinator)
            .hasMessageContaining(refusal);
        }
    }
}
