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

package org.apache.cassandra.index.sai.cql;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.FileUtils;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v9.V9SSTableComponentsWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Queries on tables whose distinct partition keys share a token (three groups of six), compared with the same queries
 * filtering a table without indexes. The rows are in a memtable then one SSTable, or spread over six SSTables (one key
 * of each group in each) then compacted; the partition key store has blocks of two keys, so a token spans blocks.
 */
public class TokenCollisionQueryTest extends SAITester.Versioned
{
    private static final String SKINNY = "CREATE TABLE %s (pk blob PRIMARY KEY, a int, b int)";
    private static final String WIDE = "CREATE TABLE %s (pk blob, ck int, s int static, a int, b int, " +
                                       "PRIMARY KEY (pk, ck))";
    private static final int KEYS_PER_TOKEN = 6;
    private static final int PAGE_SIZE = 2;
    // SAITester's default, and no optimization
    private static final int[] OPTIMIZATION_LEVELS = { 1, 0 };

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        // the default version and the version with the V9 primary key maps
        return Stream.of(Version.parse(CassandraRelevantProperties.SAI_CURRENT_VERSION.getString()), Version.GA)
                     .distinct()
                     .map(v -> new Object[]{ v })
                     .collect(Collectors.toList());
    }

    private List<DecoratedKey> keys;
    private List<DecoratedKey> collidingKeys;
    private String indexed;
    private String unindexed;

    @Before
    public void setup()
    {
        requireNetwork();
        V9SSTableComponentsWriter.setPartitionBlockShift(1);
    }

    @After
    public void reset()
    {
        V9SSTableComponentsWriter.setPartitionBlockShift(V9SSTableComponentsWriter.DEFAULT_PARTITION_BLOCK_SHIFT);
    }

    @Test
    public void testSkinny()
    {
        for (boolean spread : new boolean[]{ false, true })
        {
            createTables(SKINNY, "a", "b");
            load(spread, (i, key) -> insert("INSERT INTO %s (pk, a, b) VALUES (?, ?, ?)", key.getKey(), i % 3, i % 2));

            List<String> queries = commonQueries("pk, a, b");
            assertInLayouts(spread, () -> assertQueries(queries));
        }
    }

    @Test
    public void testWideWithStaticRows()
    {
        for (boolean spread : new boolean[]{ false, true })
        {
            createTables(WIDE, "s", "a", "b");
            load(spread, (i, key) -> {
                insert("INSERT INTO %s (pk, s) VALUES (?, ?)", key.getKey(), i % 2);
                for (int ck = 0; ck < 3; ck++)
                    insert("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", key.getKey(), ck, (i + ck) % 3, (i * ck) % 2);
            });

            String columns = "pk, ck, s, a, b";
            List<String> queries = new ArrayList<>(commonQueries(columns));
            queries.add("SELECT " + columns + " FROM %s WHERE s = 1");
            queries.add("SELECT " + columns + " FROM %s WHERE s = 0 AND a = 1");
            queries.add("SELECT " + columns + " FROM %s WHERE s = 1 AND a = 2 AND b = 0");
            queries.add("SELECT " + columns + " FROM %s WHERE s = 1 OR a = 2");
            queries.add(String.format("SELECT %s FROM %%s WHERE token(pk) >= token(%s) AND s = 0", columns, hex(collidingKeys.get(7))));
            for (DecoratedKey key : collidingKeys)
                queries.add(String.format("SELECT %s FROM %%s WHERE pk = %s AND a = 1", columns, hex(key)));

            assertInLayouts(spread, () -> assertQueries(queries));
        }
    }

    @Test
    public void testVector()
    {
        for (boolean spread : new boolean[]{ false, true })
        {
            createTable("CREATE TABLE %s (pk blob, ck int, a int, v vector<float, 2>, PRIMARY KEY (pk, ck))");
            indexed = currentTable();
            createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
            createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' " +
                        "WITH OPTIONS = {'similarity_function' : 'euclidean'}");
            disableCompaction();
            keys = keys();

            // the closer a row is to [0, 0], the earlier it is in this list, which is in SSTable order
            List<String> rows = new ArrayList<>();
            for (int i = 0; i < keys.size(); i++)
                for (int ck = 0; ck < 2; ck++)
                    rows.add(hex(keys.get(i)) + ':' + ck + ':' + (i % 3));
            load(spread, (i, key) -> {
                for (int ck = 0; ck < 2; ck++)
                    execute("INSERT INTO %s (pk, ck, a, v) VALUES (?, ?, ?, ?)", key.getKey(), ck, i % 3,
                            vector(new float[]{ 2 * i + ck + 1, 1 }));
            });

            assertInLayouts(spread, () -> {
                for (int a = 0; a < 3; a++)
                {
                    String value = ":" + a;
                    List<String> matching = rows.stream().filter(r -> r.endsWith(value)).collect(Collectors.toList());
                    assertVectorQuery("SELECT pk, ck, a FROM %s WHERE a = " + a + " ORDER BY v ANN OF [0, 0] LIMIT 3",
                                      matching.subList(0, 3));
                    assertVectorQuery("SELECT pk, ck, a FROM %s WHERE a = " + a + " ORDER BY v ANN OF [0, 0] LIMIT 100", matching);
                }
                assertVectorQueryLimits("SELECT pk, ck, a FROM %s ORDER BY v ANN OF [0, 0]", rows);

                for (DecoratedKey key : collidingKeys)
                {
                    List<String> inTokenRange = new ArrayList<>();
                    for (int r = 0; r < rows.size(); r++)
                        if (keys.get(r / 2).getToken().compareTo(key.getToken()) <= 0)
                            inTokenRange.add(rows.get(r));
                    assertVectorQueryLimits(String.format("SELECT pk, ck, a FROM %%s WHERE token(pk) <= token(%s) " +
                                                          "ORDER BY v ANN OF [0, 0]", hex(key)),
                                            inTokenRange);
                    List<String> inPartition = rows.stream().filter(r -> r.startsWith(hex(key) + ':')).collect(Collectors.toList());
                    assertVectorQueryLimits(String.format("SELECT pk, ck, a FROM %%s WHERE pk = %s " +
                                                          "ORDER BY v ANN OF [0, 0]", hex(key)),
                                            inPartition);
                }
            });
        }
    }

    /**
     * SSTables of the version of the test and SSTables of another version, alternately: the keys of a token are in
     * SSTables of both versions.
     */
    @Test
    public void testMixedVersions()
    {
        Version other = version == Version.GA ? Version.EC : Version.GA;
        createTables("CREATE TABLE %s (pk blob, ck int, a int, b int, PRIMARY KEY (pk, ck))", "a", "b");
        for (int sstable = 0; sstable < KEYS_PER_TOKEN; sstable++)
        {
            SAIUtil.setCurrentVersion(sstable % 2 == 0 ? version : other);
            for (int i = 0; i < keys.size(); i++)
            {
                DecoratedKey key = keys.get(i);
                if (sstableOf(key) != sstable)
                    continue;
                for (int ck = 0; ck < 2; ck++)
                    insert("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", key.getKey(), ck, (i + ck) % 3, i % 2);
            }
            flush();
        }

        List<String> queries = commonQueries("pk, ck, a, b");
        assertQueries(queries);

        compact();
        assertQueries(queries);
    }

    /**
     * The SSTables and SAI files that Apache Cassandra 5.0 wrote with the rows of {@link #testSkinny()}
     * (test/data/legacy-sai/apache-aa/collisions-skinny*, see its README), read with the primary key maps of version
     * "ga", then compacted.
     */
    @Test
    public void testApacheSkinny() throws Exception
    {
        testApache(SKINNY, "collisions-skinny", "collisions_skinny_", "a", "b");
    }

    @Test
    public void testApacheSkinnySpread() throws Exception
    {
        testApache(SKINNY, "collisions-skinny-spread", "collisions_skinny_", "a", "b");
    }

    /**
     * As {@link #testApacheSkinny()}, with the rows of {@link #testWideWithStaticRows()}.
     */
    @Test
    public void testApacheWide() throws Exception
    {
        testApache(WIDE, "collisions-wide", "collisions_wide_", "s", "a", "b");
    }

    @Test
    public void testApacheWideSpread() throws Exception
    {
        testApache(WIDE, "collisions-wide-spread", "collisions_wide_", "s", "a", "b");
    }

    private void testApache(String create, String data, String indexPrefix, String... indexedColumns) throws Exception
    {
        unindexed = createTable(create);
        indexed = createTable(create);
        for (String column : indexedColumns)
            createIndex("CREATE INDEX " + indexPrefix + column + " ON %s(" + column + ") USING 'sai'");
        disableCompaction();
        keys = keys();

        boolean wide = indexedColumns.length == 3;
        String table = KEYSPACE + '.' + unindexed;
        for (int i = 0; i < keys.size(); i++)
        {
            ByteBuffer key = keys.get(i).getKey();
            if (!wide)
            {
                execute("INSERT INTO " + table + " (pk, a, b) VALUES (?, ?, ?)", key, i % 3, i % 2);
                continue;
            }
            execute("INSERT INTO " + table + " (pk, s) VALUES (?, ?)", key, i % 2);
            for (int ck = 0; ck < 3; ck++)
                execute("INSERT INTO " + table + " (pk, ck, a, b) VALUES (?, ?, ?, ?)", key, ck, (i + ck) % 3, (i * ck) % 2);
        }

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        FileUtils.copySSTablesAndIndexes(cfs.getDirectories().getDirectoryForNewSSTables().toJavaIOFile(), "apache-aa/" + data);
        cfs.loadNewSSTables();
        assertThat(cfs.getLiveSSTables()).hasSize(data.endsWith("-spread") ? KEYS_PER_TOKEN : 1);
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertThat(group.descriptorFor(sstable).perSSTableComponents().version()).isEqualTo(Version.APACHE_AA);

        String columns = wide ? "pk, ck, s, a, b" : "pk, a, b";
        List<String> queries = new ArrayList<>(commonQueries(columns));
        if (wide)
        {
            queries.add("SELECT " + columns + " FROM %s WHERE s = 0 AND a = 1");
            queries.add("SELECT " + columns + " FROM %s WHERE s = 1 OR a = 2");
        }
        assertQueries(queries);

        compact();
        assertQueries(queries);
    }

    private void createTables(String create, String... indexedColumns)
    {
        unindexed = createTable(create);
        indexed = createTable(create);
        for (String column : indexedColumns)
            createIndex("CREATE CUSTOM INDEX ON %s(" + column + ") USING 'StorageAttachedIndex'");
        disableCompaction();
        keys = keys();
    }

    // three groups of KEYS_PER_TOKEN keys that share a token, and keys of other tokens between them; sets collidingKeys
    private List<DecoratedKey> keys()
    {
        IPartitioner partitioner = getCurrentColumnFamilyStore().getPartitioner();
        List<DecoratedKey> keys = new ArrayList<>();
        collidingKeys = new ArrayList<>();
        List<Token> tokens = new ArrayList<>();
        for (String prefix : new String[]{ "group0", "group1", "group2" })
        {
            List<DecoratedKey> group = Util.murmurCollisions(partitioner, prefix, KEYS_PER_TOKEN);
            collidingKeys.addAll(group);
            tokens.add(group.get(0).getToken());
        }
        collidingKeys.sort(Comparator.naturalOrder());
        tokens.sort(Comparator.naturalOrder());
        keys.addAll(collidingKeys);
        keys.addAll(Util.keysBetweenTokens(partitioner, tokens.get(0), tokens.get(1), KEYS_PER_TOKEN));
        keys.addAll(Util.keysBetweenTokens(partitioner, tokens.get(1), tokens.get(2), KEYS_PER_TOKEN));
        keys.sort(Comparator.naturalOrder());
        return keys;
    }

    // spread, each SSTable has one key of each group and one of each run of keys of other tokens
    private int sstableOf(DecoratedKey key)
    {
        int colliding = collidingKeys.indexOf(key);
        if (colliding >= 0)
            return colliding % KEYS_PER_TOKEN;
        int i = keys.indexOf(key);
        int previousColliding = i;
        while (previousColliding >= 0 && !collidingKeys.contains(keys.get(previousColliding)))
            previousColliding--;
        return (i - previousColliding - 1) % KEYS_PER_TOKEN;
    }

    private void load(boolean spread, BiConsumer<Integer, DecoratedKey> insertPartition)
    {
        for (int sstable = 0; sstable < (spread ? KEYS_PER_TOKEN : 1); sstable++)
        {
            for (int i = 0; i < keys.size(); i++)
                if (!spread || sstableOf(keys.get(i)) == sstable)
                    insertPartition.accept(i, keys.get(i));
            if (spread)
                flush();
        }
        if (spread)
            assertThat(getCurrentColumnFamilyStore().getLiveSSTables()).hasSize(KEYS_PER_TOKEN);
    }

    private List<String> commonQueries(String columns)
    {
        String select = "SELECT " + columns + " FROM %s WHERE ";
        List<String> queries = new ArrayList<>();
        queries.add(select + "a = 0");
        queries.add(select + "b = 1");
        queries.add(select + "a = 0 AND b = 1");
        queries.add(select + "a = 1 AND b = 0");
        queries.add(select + "a = 2 OR b = 1");
        queries.add(select + "(a = 0 OR a = 1) AND b = 1");
        // the first, middle and last group
        for (int k : new int[]{ 0, KEYS_PER_TOKEN, 2 * KEYS_PER_TOKEN })
        {
            String key = hex(collidingKeys.get(k));
            queries.add(select + "token(pk) >= token(" + key + ") AND a = 1");
            queries.add(select + "token(pk) = token(" + key + ") AND b = 0");
            queries.add(select + "token(pk) <= token(" + key + ") AND a = 0 AND b = 0");
            queries.add(select + "token(pk) > token(" + key + ") AND (a = 0 OR b = 0)");
        }
        for (DecoratedKey key : collidingKeys)
            queries.add(select + "pk = " + hex(key) + " AND (a = 0 OR b = 0)");
        return queries;
    }

    private void insert(String query, Object... values)
    {
        execute(query.replace("%s", KEYSPACE + '.' + unindexed), values);
        execute(query.replace("%s", KEYSPACE + '.' + indexed), values);
    }

    private void assertInLayouts(boolean spread, Runnable assertions)
    {
        assertions.run();
        if (spread)
            compact();
        else
            flush();
        assertions.run();
    }

    private void assertQueries(List<String> queries)
    {
        // with the query optimizer, and without it, so that the plans intersect the iterators of all the indexes
        for (int optimizationLevel : OPTIMIZATION_LEVELS)
        {
            CassandraRelevantProperties.SAI_QUERY_OPT_LEVEL.setInt(optimizationLevel);
            for (String query : queries)
                assertQuery(query, optimizationLevel);
        }
    }

    private void assertQuery(String query, int optimizationLevel)
    {
        String onIndexed = query.replace("%s", KEYSPACE + '.' + indexed);
        String description = String.format("%s (version %s, %d SSTables, optimization level %d)", onIndexed, version,
                                           getCurrentColumnFamilyStore().getLiveSSTables().size(), optimizationLevel);
        try
        {
            List<String> expected = rows(executeInternal(query.replace("%s", KEYSPACE + '.' + unindexed) + " ALLOW FILTERING"));

            assertThat(rows(execute(onIndexed))).as("execute: %s", description).isEqualTo(expected);
            assertThat(rows(executeInternal(onIndexed))).as("executeInternal: %s", description).isEqualTo(expected);
            assertThat(rows(executeNetWithPaging(onIndexed, PAGE_SIZE).all())).as("paging: %s", description).isEqualTo(expected);
            for (int limit : new int[]{ 1, 3 })
            {
                List<String> limited = expected.subList(0, Math.min(limit, expected.size()));
                assertThat(rows(execute(onIndexed + " LIMIT " + limit))).as("execute, LIMIT %d: %s", limit, description).isEqualTo(limited);
                assertThat(rows(executeInternal(onIndexed + " LIMIT " + limit)))
                          .as("executeInternal, LIMIT %d: %s", limit, description).isEqualTo(limited);
            }
        }
        catch (RuntimeException e)
        {
            throw new AssertionError(description, e);
        }
    }

    private void assertVectorQueryLimits(String query, List<String> expected)
    {
        assertVectorQuery(query + " LIMIT 1", expected.subList(0, 1));
        assertVectorQuery(query + " LIMIT 100", expected);
    }

    private void assertVectorQuery(String query, List<String> expected)
    {
        for (int optimizationLevel : OPTIMIZATION_LEVELS)
        {
            CassandraRelevantProperties.SAI_QUERY_OPT_LEVEL.setInt(optimizationLevel);
            String description = String.format("%s (version %s, %d SSTables, optimization level %d)", query, version,
                                               getCurrentColumnFamilyStore().getLiveSSTables().size(), optimizationLevel);
            try
            {
                assertThat(vectorRows(execute(query))).as("execute: %s", description).isEqualTo(expected);
                assertThat(vectorRows(executeInternal(query))).as("executeInternal: %s", description).isEqualTo(expected);
            }
            catch (RuntimeException e)
            {
                throw new AssertionError(description, e);
            }
        }
    }

    private static List<String> vectorRows(UntypedResultSet result)
    {
        List<String> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : result)
            rows.add("0x" + ByteBufferUtil.bytesToHex(row.getBytes("pk")) + ':' + row.getInt("ck") + ':' + row.getInt("a"));
        return rows;
    }

    private static List<String> rows(UntypedResultSet result)
    {
        List<String> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : result)
            rows.add(result.metadata().stream().map(c -> hex(row.getBytes(c.name.toString()))).collect(Collectors.joining(":")));
        return rows;
    }

    private static List<String> rows(List<com.datastax.driver.core.Row> result)
    {
        List<String> rows = new ArrayList<>();
        for (com.datastax.driver.core.Row row : result)
        {
            List<String> values = new ArrayList<>();
            for (int i = 0; i < row.getColumnDefinitions().size(); i++)
                values.add(hex(row.getBytesUnsafe(i)));
            rows.add(String.join(":", values));
        }
        return rows;
    }

    private static String hex(DecoratedKey key)
    {
        return "0x" + ByteBufferUtil.bytesToHex(key.getKey());
    }

    private static String hex(ByteBuffer value)
    {
        return value == null ? "null" : ByteBufferUtil.bytesToHex(value);
    }
}
