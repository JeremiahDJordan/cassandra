/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.index.sai.cql;

import java.util.Arrays;
import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;

import static com.google.common.collect.ObjectArrays.concat;
import static org.junit.Assert.assertEquals;

// This test is primarily handling edge conditions, error conditions
// and basic functionality. Comprehensive type testing of collections
// is in the cql/types/collections package
//TODO Sort out statement restrictions assertion
public class CollectionIndexingTest extends SAITester.Versioned
{
    @Before
    public void setup() throws Throwable
    {
        requireNetwork();
    }

    @Test
    public void indexMap()
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");
        assertEquals(2, execute("SELECT * FROM %s WHERE value CONTAINS 'v1'").size());

        assertEmpty(execute("SELECT pk FROM %s WHERE value NOT CONTAINS 'v1'"));

        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE value NOT CONTAINS 'v2'"),
                                row(2));
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE value NOT CONTAINS 'v3'"),
                                row(1));

        flush();

        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE value NOT CONTAINS 'v2'"),
                                row(2));
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE value NOT CONTAINS 'v3'"),
                                row(1));
    }

    @Test
    public void indexEmptyMaps()
    {
        createTable("CREATE TABLE %s (pk int primary key, value map<int, text>)");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");

        // Test memtable index:
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 1, new HashMap<Integer, String>() {{
            put(1, "v1");
            put(2, "v2");
        }});
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 2, new HashMap<Integer, String>());

        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE value CONTAINS 'v1'"),
                                row(1));
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE value NOT CONTAINS 'v1'"),
                                row(2));

        // Test sstable index:
        flush();

        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE value CONTAINS 'v1'"),
                                row(1));
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE value NOT CONTAINS 'v1'"),
                                row(2));

        // Add one more row with an empty map and flush.
        // This will create an sstable with no index.
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 3, new HashMap<Integer, String>());
        flush();

        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE value CONTAINS 'v1'"),
                                row(1));
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE value NOT CONTAINS 'v1'"),
                                row(2), row(3));
    }

    @Test
    public void indexQueryEmpty()
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");
        assertEquals(0, execute("SELECT * FROM %s WHERE value CONTAINS ''").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE value CONTAINS '' AND value CONTAINS 'v1'").size());
    }

    @Test
    public void indexMapKeys()
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(KEYS(value)) USING 'StorageAttachedIndex'");
        assertEquals(2, execute("SELECT * FROM %s WHERE value CONTAINS KEY 1").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE value NOT CONTAINS KEY 1").size());
        assertEquals(2, execute("SELECT * FROM %s WHERE value NOT CONTAINS KEY 5").size());

        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 3, new HashMap<Integer, String>() {{
            put(1, "v1");
            put(3, "v4");
        }});
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE value NOT CONTAINS KEY 2"), row(3));

        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 4, new HashMap<Integer, String>());
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE value NOT CONTAINS KEY 2"), row(3), row(4));
    }

    @Test
    public void indexMapValues()
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(VALUES(value)) USING 'StorageAttachedIndex'");
        assertEquals(2, execute("SELECT * FROM %s WHERE value CONTAINS 'v1'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE value NOT CONTAINS 'v1'").size());
        assertEquals(2, execute("SELECT * FROM %s WHERE value NOT CONTAINS 'v5'").size());
    }

    @Test
    public void indexMapEntries()
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(value)) USING 'StorageAttachedIndex'");
        assertEquals(2, execute("SELECT * FROM %s WHERE value[1] = 'v1'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE value[1] = 'v1' AND value[2] = 'v2'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE value[1] != 'v1'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE value[1] != 'v2' AND value[2] != 'v2'").size());
        assertEquals(2, execute("SELECT * FROM %s WHERE value[1] != 'v3'").size());
    }

    @Test
    public void indexFrozenList()
    {
        createPopulatedFrozenList();
        createIndex("CREATE CUSTOM INDEX ON %s(FULL(value)) USING 'StorageAttachedIndex'");
        assertEquals(2, execute("SELECT * FROM %s WHERE value = ?", Arrays.asList(1, 2, 3)).size());
    }

    @Test
    public void indexFrozenMap()
    {
        createPopulatedFrozenMap();
        createIndex("CREATE CUSTOM INDEX ON %s(FULL(value)) USING 'StorageAttachedIndex'");
        assertEquals(1, execute("SELECT * FROM %s WHERE value = ?", new HashMap<Integer, String>() {{
            put(1, "v1");
            put(2, "v2");
        }}).size());

    }

    @Test
    public void indexFrozenMapQueryKeys()
    {
        createPopulatedFrozenMap();
        createIndex("CREATE CUSTOM INDEX ON %s(FULL(value)) USING 'StorageAttachedIndex'");
        assertUnsupportedIndexOperator("SELECT * FROM %s WHERE value contains key 1");
        assertUnsupportedIndexOperator("SELECT * FROM %s WHERE value not contains key 1");
        assertEquals(2, execute("SELECT * FROM %s WHERE value contains key 1 ALLOW FILTERING").size());
    }

    @Test
    public void indexFrozenMapQueryValues()
    {
        createPopulatedFrozenMap();
        createIndex("CREATE CUSTOM INDEX ON %s(FULL(value)) USING 'StorageAttachedIndex'");
        assertUnsupportedIndexOperator("SELECT * FROM %s WHERE value contains 'v1'");
        assertUnsupportedIndexOperator("SELECT * FROM %s WHERE value not contains 'v1'");
        assertEquals(2, execute("SELECT * FROM %s WHERE value contains 'v1' ALLOW FILTERING").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE value not contains 'v1' ALLOW FILTERING").size());
    }

    @Test
    public void indexFrozenMapQueryEntries()
    {
        createPopulatedFrozenMap();
        createIndex("CREATE CUSTOM INDEX ON %s(FULL(value)) USING 'StorageAttachedIndex'");
        assertInvalidMessage("Map-entry equality predicates on frozen map column value are not supported",
                "SELECT * FROM %s WHERE value[1] = 'v1'");
    }

    @Test
    public void indexMapEntriesQueryEq()
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(value)) USING 'StorageAttachedIndex'");
        assertInvalidMessage("Collection column 'value' (map<int, text>) cannot be restricted by a '=' relation",
                "SELECT * FROM %s WHERE value = ?", Arrays.asList(1, 2));
    }

    @Test
    public void indexMapEntriesQueryKeys()
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(value)) USING 'StorageAttachedIndex'");
        assertUnsupportedIndexOperator("SELECT * FROM %s WHERE value contains key 1");
        assertUnsupportedIndexOperator("SELECT * FROM %s WHERE value not contains key 1");
        assertEquals(2, execute("SELECT * FROM %s WHERE value contains key 1 ALLOW FILTERING").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE value not contains key 1 ALLOW FILTERING").size());
    }

    @Test
    public void indexMapEntriesQueryValues()
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(value)) USING 'StorageAttachedIndex'");
        assertUnsupportedIndexOperator("SELECT * FROM %s WHERE value contains 'v1'");
        assertUnsupportedIndexOperator("SELECT * FROM %s WHERE value not contains 'v1'");
        assertEquals(2, execute("SELECT * FROM %s WHERE value contains 'v1' ALLOW FILTERING").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE value not contains 'v1' ALLOW FILTERING").size());
    }

    @Test
    public void indexMapKeysQueryEq()
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(KEYS(value)) USING 'StorageAttachedIndex'");
        assertInvalidMessage("Collection column 'value' (map<int, text>) cannot be restricted by a '=' relation",
                "SELECT * FROM %s WHERE value = ?", Arrays.asList(1, 2));
    }

    @Test
    public void indexMapKeysQueryValues()
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(KEYS(value)) USING 'StorageAttachedIndex'");
        assertUnsupportedIndexOperator("SELECT * FROM %s WHERE value contains 'v1'");
        assertEquals(2, execute("SELECT * FROM %s WHERE value contains 'v1' ALLOW FILTERING").size());
    }

    @Test
    public void indexMapKeysQueryEntries()
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(KEYS(value)) USING 'StorageAttachedIndex'");
        assertUnsupportedIndexOperator("SELECT * FROM %s WHERE value[1] = 'v1'");
        assertEquals(2, execute("SELECT * FROM %s WHERE value[1] = 'v1' ALLOW FILTERING").size());
    }

    @Test
    public void indexMapValuesQueryEq()
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(VALUES(value)) USING 'StorageAttachedIndex'");
        assertInvalidMessage("Collection column 'value' (map<int, text>) cannot be restricted by a '=' relation",
                "SELECT * FROM %s WHERE value = ?", Arrays.asList(1, 2));
    }

    @Test
    public void indexMapValuesQueryKeys()
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(VALUES(value)) USING 'StorageAttachedIndex'");
        assertUnsupportedIndexOperator("SELECT * FROM %s WHERE value contains key 1");
        assertUnsupportedIndexOperator("SELECT * FROM %s WHERE value not contains key 1");
        assertEquals(2, execute("SELECT * FROM %s WHERE value contains key 1 ALLOW FILTERING").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE value not contains key 1 ALLOW FILTERING").size());
    }

    @Test
    public void indexMapValuesQueryEntries()
    {
        createPopulatedMap();
        createIndex("CREATE CUSTOM INDEX ON %s(VALUES(value)) USING 'StorageAttachedIndex'");
        assertUnsupportedIndexOperator("SELECT * FROM %s WHERE value[1] = 'v1'");
        assertUnsupportedIndexOperator("SELECT * FROM %s WHERE value[1] != 'v1'");
        assertEquals(2, execute("SELECT * FROM %s WHERE value[1] = 'v1' ALLOW FILTERING").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE value[1] != 'v1' ALLOW FILTERING").size());
    }

    @Test
    public void notContainsShouldReturnUpdatedRows() throws Throwable
    {
        createTable("CREATE TABLE %s(id int PRIMARY KEY, text_map map<text, text>)");
        createIndex("CREATE CUSTOM INDEX ON %s(values(text_map)) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s(id, text_map) values (1, {'k1':'v1'})");
        flush();
        // This update overwrites 'v1', so now the map does not contain 'v1' and the row should be returned
        // by the NOT CONTAINS 'v1' query. We purposefuly make this update after flush, so it ends up in a separate
        // index than the original row.
        execute("INSERT INTO %s(id, text_map) values (1, {'k2':'v2'})");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT id FROM %s WHERE text_map NOT CONTAINS 'v1'"), row(1));
        });
    }

    @Test
    public void testUpdateMapToNullValue() throws Throwable
    {
        createTable("CREATE TABLE %s(id int PRIMARY KEY, text_map map<text, text>)");
        createIndex("CREATE CUSTOM INDEX ON %s(values(text_map)) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        execute("INSERT INTO %s(id, text_map) values (1, {'k1':'v1'})");
        execute("INSERT INTO %s(id, text_map) values (2, {'k1':'v2'})");
        assertRows(execute("SELECT id FROM %s WHERE text_map CONTAINS 'v1'"), row(1));
        assertRows(execute("SELECT id FROM %s WHERE text_map NOT CONTAINS 'v1'"), row(2));
        // Overwrite with null
        execute("INSERT INTO %s(id, text_map) values (1, null)");
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT id FROM %s WHERE text_map CONTAINS 'v1'"));
            assertRows(execute("SELECT id FROM %s WHERE text_map NOT CONTAINS 'v1'"), row(1), row(2));
        });
    }

    /**
     * CONTAINS, CONTAINS KEY and their negations on unindexed frozen collections are evaluated on the elements of the
     * collections when another expression of the query is served by an index.
     */
    @Test
    public void unindexedFrozenCollectionRegularColumns() throws Throwable
    {
        testUnindexedFrozenCollections("PRIMARY KEY (k, x))", false, false);
    }

    @Test
    public void unindexedFrozenCollectionClusteringColumns() throws Throwable
    {
        testUnindexedFrozenCollections("PRIMARY KEY (k, x, m, s, l))", false, true);
    }

    @Test
    public void unindexedFrozenCollectionDescendingClusteringColumns() throws Throwable
    {
        testUnindexedFrozenCollections("PRIMARY KEY (k, x, m, s, l)) WITH CLUSTERING ORDER BY (x ASC, m DESC, s DESC, l DESC)", false, true);
    }

    @Test
    public void unindexedFrozenCollectionStaticColumns() throws Throwable
    {
        testUnindexedFrozenCollections("PRIMARY KEY (k, x))", true, false);
    }

    private void testUnindexedFrozenCollections(String primaryKey, boolean statics, boolean clustering) throws Throwable
    {
        String st = statics ? " static" : "";
        createTable("CREATE TABLE %s (k int, x int, m frozen<map<int, text>>" + st + ", s frozen<set<int>>" + st +
                    ", l frozen<list<int>>" + st + ", v int" + st + ", " + primaryKey);
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k, x, m, s, l, v) VALUES (0, 0, {1: 'a'}, {1}, [1], 1)");
        execute("INSERT INTO %s (k, x, m, s, l, v) VALUES (1, 0, {2: 'b'}, {2}, [2], 1)");
        execute("INSERT INTO %s (k, x, m, s, l, v) VALUES (2, 0, {2: 'b', 3: 'c'}, {2, 3}, [3, 2], 1)");
        execute("INSERT INTO %s (k, x, m, s, l, v) VALUES (3, 0, {2: 'b'}, {2}, [2], 2)");

        // collections without elements: empty ones, and missing ones where they aren't primary key columns
        execute("INSERT INTO %s (k, x, m, s, l, v) VALUES (4, 0, ?, ?, ?, 1)", map(), set(), list());
        if (!clustering)
            execute("INSERT INTO %s (k, x, v) VALUES (5, 0, 1)");
        Object[][] withoutElements = clustering ? new Object[][]{ row(4) } : new Object[][]{ row(4), row(5) };

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE v = 1 AND m CONTAINS KEY 2 ALLOW FILTERING"), row(1), row(2));
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE v = 1 AND m CONTAINS 'b' ALLOW FILTERING"), row(1), row(2));
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE v = 1 AND m CONTAINS 'c' ALLOW FILTERING"), row(2));
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE v = 1 AND s CONTAINS 2 ALLOW FILTERING"), row(1), row(2));
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE v = 1 AND l CONTAINS 2 ALLOW FILTERING"), row(1), row(2));
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE v = 1 AND m CONTAINS KEY 2 AND m CONTAINS 'c' ALLOW FILTERING"), row(2));
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE v = 1 AND m NOT CONTAINS KEY 2 ALLOW FILTERING"), concat(new Object[][]{ row(0) }, withoutElements, Object[].class));
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE v = 1 AND m NOT CONTAINS 'b' ALLOW FILTERING"), concat(new Object[][]{ row(0) }, withoutElements, Object[].class));
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE v = 1 AND s NOT CONTAINS 2 ALLOW FILTERING"), concat(new Object[][]{ row(0) }, withoutElements, Object[].class));
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE v = 1 AND l NOT CONTAINS 3 ALLOW FILTERING"), concat(new Object[][]{ row(0), row(1) }, withoutElements, Object[].class));
            assertRows(execute("SELECT k FROM %s WHERE k = 1 AND v = 1 AND s CONTAINS 2 ALLOW FILTERING"), row(1));
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE v = 1 AND m = {2: 'b'} ALLOW FILTERING"), row(1));
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE v = 1 AND l > [2] ALLOW FILTERING"), row(2));
        });
    }

    @Test
    public void unindexedFrozenCollectionPartitionKeyColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 frozen<list<int>>, c int, v int, PRIMARY KEY ((k1, k2), c))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k1, k2, c, v) VALUES (1, [1, 2], 0, 1)");
        execute("INSERT INTO %s (k1, k2, c, v) VALUES (2, [3], 0, 1)");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k1 FROM %s WHERE v = 1 AND k2 = [1, 2] ALLOW FILTERING"), row(1));
            assertRows(execute("SELECT k1 FROM %s WHERE v = 1 AND k2 CONTAINS 3 ALLOW FILTERING"), row(2));
            assertRows(execute("SELECT k1 FROM %s WHERE v = 1 AND k2 NOT CONTAINS 3 ALLOW FILTERING"), row(1));
        });
    }

    private void createPopulatedMap()
    {
        createTable("CREATE TABLE %s (pk int primary key, value map<int, text>)");
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 1, new HashMap<Integer, String>() {{
            put(1, "v1");
            put(2, "v2");
        }});
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 2, new HashMap<Integer, String>() {{
            put(1, "v1");
            put(2, "v3");
        }});
    }

    private void createPopulatedFrozenMap()
    {
        createTable("CREATE TABLE %s (pk int primary key, value frozen<map<int, text>>)");
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 1, new HashMap<Integer, String>() {{
            put(1, "v1");
            put(2, "v2");
        }});
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 2, new HashMap<Integer, String>() {{
            put(1, "v1");
            put(2, "v3");
        }});
    }

    private void createPopulatedFrozenList()
    {
        createTable("CREATE TABLE %s (pk int primary key, value frozen<list<int>>)");
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 1, Arrays.asList(1, 2, 3));
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 2, Arrays.asList(1, 2, 3));
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 3, Arrays.asList(4, 5, 6));
        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 4, Arrays.asList(1, 2, 7));
    }

    private void assertUnsupportedIndexOperator(String query, Object... values)
    {
//        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, "value"),
//                query, values);
    }
}
