/*
 * Copyright DataStax, Inc.
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

package org.apache.cassandra.distributed.test.sai.features;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.test.sai.SAIUtil;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.index.FeatureNeedsIndexRebuildException;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Tests the availabilty of features in different versions of the SAI on-disk format in a multinode, multiversion cluster.
 * </p>
 * This is done with a 2-node cluster where the first node has the newer version of the SAI on-disk format and the
 * second node has the older version being tested configured as the default current version.
 * </p>
 * Additionally, there will be a keyspace per version configured to use that version on the second node instead of the
 * default one.
 */
public abstract class FeaturesVersionSupportTester extends TestBaseImpl
{
    private static final Map<String, Version> VERSIONS_PER_KEYSPACE = Version.ALL.stream().collect(Collectors.toMap(v -> "ks_" + v.toString(), v -> v));

    private static Version version;
    private static Cluster cluster;

    // To be called by inheritors in a method annotated with @BeforeClass
    protected static void initCluster(Version version) throws IOException
    {
        FeaturesVersionSupportTester.version = version;

        cluster = init(Cluster.build(2)
                              .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                              .start(), 1);

        for (String keyspace : VERSIONS_PER_KEYSPACE.keySet())
            cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    }

    @Before
    public void before()
    {
        // The first node, which will be the coordinator in all tests, uses the latest version for all keyspaces
        cluster.get(1).runOnInstance(() -> org.apache.cassandra.index.sai.SAIUtil.setCurrentVersion(Version.LATEST));

        // The second node will use the tested version as the default, and specific versions for known keyspaces
        String versionString = version.toString();
        cluster.get(2).runOnInstance(() -> org.apache.cassandra.index.sai.SAIUtil.setCurrentVersion(Version.parse(versionString), VERSIONS_PER_KEYSPACE));
    }

    @AfterClass
    public static void cleanup()
    {
        cluster.close();
    }

    /**
     * Test that vector indexes are supported with on-disk format versions from {@link Version#CA}.
     * Nodes using older versions should fail their index build, although the index will still exist.
     * This includes nodes set to use a version that supports ANN, but with a pre-existing index in a version that
     * doesn't support ANN.
     */
    @Test
    public void testANN()
    {
        test(this::testANN);
    }

    private void testANN(String keyspace, Version version)
    {
        cluster.schemaChange("CREATE TABLE " + keyspace + ".ann (k int PRIMARY KEY, x int, v vector<float, 2>)");

        ICoordinator coordinator = cluster.coordinator(1);
        coordinator.execute("INSERT INTO " + keyspace + ".ann (k, x, v) VALUES (0, 0, [1.0, 2.0])", ALL);
        coordinator.execute("INSERT INTO " + keyspace + ".ann (k, x, v) VALUES (1, 1, [2.0, 3.0])", ALL);
        coordinator.execute("INSERT INTO " + keyspace + ".ann (k, x, v) VALUES (2, 2, [3.0, 4.0])", ALL);
        coordinator.execute("INSERT INTO " + keyspace + ".ann (k, x, v) VALUES (3, 3, [4.0, 5.0])", ALL);
        cluster.forEach(node -> node.flush(keyspace));

        // The second node is on the tested version, so it would accept or reject the index creation depending on
        // whether that version supports it.
        testANN(coordinator, keyspace, "does not support vector indexes");

        // Create a non-vector index with the tested version, so there are some per-sstable components around
        cluster.schemaChange("CREATE CUSTOM INDEX ON " + keyspace + ".ann(x) USING 'StorageAttachedIndex'");
        SAIUtil.waitForIndexQueryableOnAllNodes(cluster, keyspace);

        // Set the version of the second node to the latest version, so all nodes are in the latest version,
        // but there is still a non-vector index with components on the tested version.
        cluster.get(2).runOnInstance(() -> org.apache.cassandra.index.sai.SAIUtil.setCurrentVersion(Version.LATEST));
        testANN(coordinator, keyspace, "The current sstables have other indexes using the on-disk format version");
    }

    private void testANN(ICoordinator coordinator, String keyspace, String expectedErrorMessage)
    {
        String createIndexQuery = "CREATE CUSTOM INDEX ann_idx ON " + keyspace + ".ann(v) USING 'StorageAttachedIndex' " +
                                  "WITH OPTIONS = {'similarity_function' : 'euclidean'}";
        String annSelectQuery = "SELECT k FROM " + keyspace + ".ann ORDER BY v ANN OF [2.5, 3.5] LIMIT 3";
        String geoSelectQuery = "SELECT k FROM " + keyspace + ".ann WHERE GEO_DISTANCE(v, [2.5, 3.5]) < 157000";

        if (version.onOrAfter(Version.JVECTOR_EARLIEST))
        {
            cluster.schemaChange(createIndexQuery);
            SAIUtil.waitForIndexQueryableOnAllNodes(cluster, keyspace);
            Assertions.assertThat(coordinator.execute(annSelectQuery, ALL)).hasNumberOfRows(3);
            Assertions.assertThat(coordinator.execute(geoSelectQuery, ALL)).hasNumberOfRows(2);
        }
        else
        {
            // The on-disk format version in node 2 is too old to support indexes on vector columns.
            // That can be due to either the default version for new indexes in node 2 for the keyspace, or the
            // existence of other indexes using an old version that forces the new index to use that same old version.
            cluster.setUncaughtExceptionsFilter((i, t) -> i == 2 &&
                                                          t.getClass().getName().contains(FeatureNeedsIndexRebuildException.class.getName()) &&
                                                          t.getMessage().contains(expectedErrorMessage));
            cluster.schemaChange(createIndexQuery);
            SAIUtil.assertIndexBuildFailed(cluster.get(1), cluster.get(2), keyspace, "ann_idx");
        }
        cluster.schemaChange("DROP INDEX IF EXISTS " + keyspace + ".ann_idx");
    }

    /**
     * Test that BM25 queries are supported with on-disk format versions from {@link Version#EC}.
     * Older versions should reject BM25 queries with {@link RequestFailureReason#FEATURE_NEEDS_INDEX_REBUILD} errors.
     */
    @Test
    public void testBM25()
    {
        test(this::testBM25);
    }

    private void testBM25(String keyspace, Version version)
    {
        cluster.schemaChange("CREATE TABLE " + keyspace + ".bm25 (k int PRIMARY KEY, v text)");

        ICoordinator coordinator = cluster.coordinator(1);
        coordinator.execute("INSERT INTO " + keyspace + ".bm25 (k, v) VALUES (1, 'apple')", ALL);
        coordinator.execute("INSERT INTO " + keyspace + ".bm25 (k, v) VALUES (2, 'orange')", ALL);
        coordinator.execute("INSERT INTO " + keyspace + ".bm25 (k, v) VALUES (3, 'banana')", ALL);
        coordinator.execute("INSERT INTO " + keyspace + ".bm25 (k, v) VALUES (4, 'kiwi')", ALL);
        cluster.forEach(node -> node.flush(keyspace));

        String index = "bm25_idx";
        cluster.schemaChange("CREATE CUSTOM INDEX " + index + " ON " + keyspace + ".bm25(v) " +
                             "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                             "WITH OPTIONS = {" +
                             "'index_analyzer': '{" +
                             "\"tokenizer\" : {\"name\" : \"standard\"}, " +
                             "\"filters\" : [{\"name\" : \"porterstem\"}]}'}");
        SAIUtil.waitForIndexQueryableOnAllNodes(cluster, keyspace);

        String query = "SELECT k FROM " + keyspace + ".bm25 ORDER BY v BM25 OF 'apple' LIMIT 3";

        if (version.onOrAfter(Version.BM25_EARLIEST))
        {
            Assertions.assertThat(coordinator.execute(query, ALL)).hasNumberOfRows(1);
        }
        else
        {
            Assertions.assertThatThrownBy(() -> coordinator.execute(query, ALL))
                      .hasMessageContaining(RequestFailureReason.FEATURE_NEEDS_INDEX_REBUILD.name());
        }

        // Test with the coordinator also using the tested version.
        // In this case the query will fail with a meaningful error message, instead of generic per-node error codes.
        String versionString = version.toString();
        cluster.get(1).runOnInstance(() -> org.apache.cassandra.index.sai.SAIUtil.setCurrentVersion(Version.parse(versionString), VERSIONS_PER_KEYSPACE));
        if (version.onOrAfter(Version.BM25_EARLIEST))
        {
            Assertions.assertThat(coordinator.execute(query, ALL)).hasNumberOfRows(1);
        }
        else
        {
            Assertions.assertThatThrownBy(() -> coordinator.execute(query, ALL))
                      .hasMessageContaining(String.format(QueryController.INDEX_VERSION_DOES_NOT_SUPPORT_BM25, index));
        }
        cluster.get(1).runOnInstance(() -> org.apache.cassandra.index.sai.SAIUtil.setCurrentVersion(Version.LATEST));
    }

    /**
     * Test that ordering by an index on a table with clustering columns is supported with on-disk format versions from
     * {@link Version#ROW_AWARE_EARLIEST}. Older versions index partitions rather than rows, so they should reject these
     * queries with {@link RequestFailureReason#FEATURE_NEEDS_INDEX_REBUILD} errors, whether the other indexes of the
     * query match any rows or not. Tables without clustering columns are supported with all versions.
     */
    @Test
    public void testOrdering()
    {
        // only the default keyspace, whose second node uses the tested version: tables in every keyspace make the debug
        // reference leak detector too slow to stop when the cluster is closed
        testOrdering(KEYSPACE, version);
    }

    private void testOrdering(String keyspace, Version version)
    {
        cluster.schemaChange("CREATE TABLE " + keyspace + ".wide (pk int, c int, val int, a int, PRIMARY KEY(pk, c))");
        cluster.schemaChange("CREATE CUSTOM INDEX wide_val_idx ON " + keyspace + ".wide(val) USING 'StorageAttachedIndex'");
        cluster.schemaChange("CREATE CUSTOM INDEX wide_a_idx ON " + keyspace + ".wide(a) USING 'StorageAttachedIndex'");
        cluster.schemaChange("CREATE TABLE " + keyspace + ".skinny (pk int PRIMARY KEY, val int, a int)");
        cluster.schemaChange("CREATE CUSTOM INDEX skinny_val_idx ON " + keyspace + ".skinny(val) USING 'StorageAttachedIndex'");
        cluster.schemaChange("CREATE CUSTOM INDEX skinny_a_idx ON " + keyspace + ".skinny(a) USING 'StorageAttachedIndex'");
        SAIUtil.waitForIndexQueryableOnAllNodes(cluster, keyspace);

        // Enough partitions to have data in both nodes. The values of a partition decrease with the clustering. The
        // queries select the ordered column, which the in-JVM coordinator returns anyway.
        ICoordinator coordinator = cluster.coordinator(1);
        for (int pk = 0; pk < 10; pk++)
        {
            coordinator.execute("INSERT INTO " + keyspace + ".skinny (pk, val, a) VALUES (?, ?, ?)", ALL, pk, 9 - pk, pk % 2);
            for (int c = 0; c < 4; c++)
                coordinator.execute("INSERT INTO " + keyspace + ".wide (pk, c, val, a) VALUES (?, ?, ?, ?)", ALL, pk, c, pk * 10 + 3 - c, c % 2);
        }
        cluster.forEach(node -> node.flush(keyspace));

        String orderQuery = "SELECT pk, c, val FROM " + keyspace + ".wide ORDER BY val ASC LIMIT 3";
        String hybridQuery = "SELECT pk, c, val FROM " + keyspace + ".wide WHERE a = 0 ORDER BY val ASC LIMIT 3";
        String noMatchQuery = "SELECT pk, c, val FROM " + keyspace + ".wide WHERE a = 7 ORDER BY val ASC LIMIT 3";

        if (version.onOrAfter(Version.ROW_AWARE_EARLIEST))
        {
            assertRows(coordinator.execute(orderQuery, ALL), row(0, 3, 0), row(0, 2, 1), row(0, 1, 2));
            assertRows(coordinator.execute(hybridQuery, ALL), row(0, 2, 1), row(0, 0, 3), row(1, 2, 11));
            assertRows(coordinator.execute(noMatchQuery, ALL));
        }
        else
        {
            for (String query : new String[]{ orderQuery, hybridQuery, noMatchQuery })
                Assertions.assertThatThrownBy(() -> coordinator.execute(query, ALL))
                          .hasMessageContaining(RequestFailureReason.FEATURE_NEEDS_INDEX_REBUILD.name());
        }

        // Tables without clustering columns are supported in all versions
        assertRows(coordinator.execute("SELECT pk, val FROM " + keyspace + ".skinny ORDER BY val ASC LIMIT 3", ALL), row(9, 0), row(8, 1), row(7, 2));
        assertRows(coordinator.execute("SELECT pk, val FROM " + keyspace + ".skinny WHERE a = 0 ORDER BY val ASC LIMIT 2", ALL), row(8, 1), row(6, 3));

        // Test with the coordinator also using the tested version.
        // In this case the query will fail with a meaningful error message, instead of generic per-node error codes.
        String versionString = version.toString();
        cluster.get(1).runOnInstance(() -> org.apache.cassandra.index.sai.SAIUtil.setCurrentVersion(Version.parse(versionString), VERSIONS_PER_KEYSPACE));
        if (version.onOrAfter(Version.ROW_AWARE_EARLIEST))
        {
            assertRows(coordinator.execute(orderQuery, ALL), row(0, 3, 0), row(0, 2, 1), row(0, 1, 2));
        }
        else
        {
            for (String query : new String[]{ orderQuery, hybridQuery, noMatchQuery })
                Assertions.assertThatThrownBy(() -> coordinator.execute(query, ALL))
                          .hasMessageContaining("Ordering with index wide_val_idx isn't supported on table " + keyspace + ".wide")
                          .hasMessageContaining("Those versions index partitions rather than rows");
        }
        cluster.get(1).runOnInstance(() -> org.apache.cassandra.index.sai.SAIUtil.setCurrentVersion(Version.LATEST));

        cluster.schemaChange("DROP TABLE " + keyspace + ".wide");
        cluster.schemaChange("DROP TABLE " + keyspace + ".skinny");
    }

    /**
     * Test that index-time analyzers are supported with all on-disk format versions.
     */
    @Test
    public void testIndexAnalyzer()
    {
        test(this::testIndexAnalyzer);
    }

    private void testIndexAnalyzer(String keyspace, Version version)
    {
        cluster.schemaChange("CREATE TABLE " + keyspace + ".analyzer (k int PRIMARY KEY, v text)");

        ICoordinator coordinator = cluster.coordinator(1);
        coordinator.execute("INSERT INTO " + keyspace + ".analyzer (k, v) VALUES (0, 'Quick fox')", ALL);
        coordinator.execute("INSERT INTO " + keyspace + ".analyzer (k, v) VALUES (1, 'Lazy dogs')", ALL);
        cluster.forEach(node -> node.flush(keyspace));

        cluster.schemaChange("CREATE CUSTOM INDEX analyzer_idx ON " + keyspace + ".analyzer(v)" +
                             " USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'" +
                             "WITH OPTIONS = { 'index_analyzer': 'standard' }");
        SAIUtil.waitForIndexQueryableOnAllNodes(cluster, keyspace);

        Assertions.assertThat(coordinator.execute("SELECT * FROM " + keyspace + ".analyzer WHERE v = 'dogs'", ALL))
                  .hasNumberOfRows(1);
    }

    /**
     * Test that query-time analyzers are supported with all on-disk format versions.
     */
    @Test
    public void testQueryAnalyzer()
    {
        test(this::testQueryAnalyzer);
    }

    private void testQueryAnalyzer(String keyspace, Version version)
    {
        cluster.schemaChange("CREATE TABLE " + keyspace + ".q_analyzer (k int PRIMARY KEY, v text)");

        ICoordinator coordinator = cluster.coordinator(1);
        coordinator.execute("INSERT INTO " + keyspace + ".q_analyzer (k, v) VALUES (1, 'astra quick fox')", ALL);
        coordinator.execute("INSERT INTO " + keyspace + ".q_analyzer (k, v) VALUES (2, 'astra2 quick fox')", ALL);
        coordinator.execute("INSERT INTO " + keyspace + ".q_analyzer (k, v) VALUES (3, 'astra3 quick foxes')", ALL);
        cluster.forEach(node -> node.flush(keyspace));

        cluster.schemaChange("CREATE CUSTOM INDEX q_analyzer_idx ON " + keyspace + ".q_analyzer(v) " +
                             "USING 'StorageAttachedIndex' WITH OPTIONS = {" +
                             "'index_analyzer': '{" +
                             "  \"tokenizer\" : { \"name\" : \"whitespace\", \"args\" : {} }," +
                             "  \"filters\" : [ { \"name\" : \"lowercase\", \"args\": {} }, " +
                             "                  { \"name\" : \"edgengram\", \"args\": { \"minGramSize\":\"1\", \"maxGramSize\":\"30\" } }]," +
                             "  \"charFilters\" : []}', " +
                             "'query_analyzer': '{" +
                             "  \"tokenizer\" : { \"name\" : \"whitespace\", \"args\" : {} }," +
                             "  \"filters\" : [ {\"name\" : \"lowercase\",\"args\": {}} ]}'}");
        SAIUtil.waitForIndexQueryableOnAllNodes(cluster, keyspace);

        String query = "SELECT k FROM " + keyspace + ".q_analyzer WHERE v : ";
        Assertions.assertThat(coordinator.execute(query + "'ast'", ALL)).hasNumberOfRows(3);
        Assertions.assertThat(coordinator.execute(query + "'astra'", ALL)).hasNumberOfRows(3);
        Assertions.assertThat(coordinator.execute(query + "'astra2'", ALL)).hasNumberOfRows(1);
        Assertions.assertThat(coordinator.execute(query + "'fox'", ALL)).hasNumberOfRows(3);
        Assertions.assertThat(coordinator.execute(query + "'foxes'", ALL)).hasNumberOfRows(1);
    }

    private static void test(BiConsumer<String, Version> testMethod)
    {
        testMethod.accept(KEYSPACE, version);
        VERSIONS_PER_KEYSPACE.forEach(testMethod);
    }
}
