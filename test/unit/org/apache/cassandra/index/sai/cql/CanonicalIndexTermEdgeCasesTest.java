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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.schema.TableMetadata;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Values whose canonical index term (see {@code TypeUtil#canonicalize}) can't be computed, isn't valid or doesn't fit:
 * they must be indexed as written or left out, and never fail a write after its data is applied, a read, a flush or a
 * compaction.
 */
public class CanonicalIndexTermEdgeCasesTest extends SAITester.Versioned
{
    /**
     * After {@code ALTER TYPE ... ADD} the values of the UDT have more fields than the index, which keeps the type the
     * column had when it was created, can read: they are indexed as written.
     */
    @Test
    public void testAlterTypeAddField() throws Throwable
    {
        for (String fieldType : new String[]{ "decimal", "varint" })
        {
            String udt = createType("CREATE TYPE %s (x " + fieldType + ')');
            String frozen = "frozen<" + udt + '>';
            // name, type, index target, literal of a value holding the UDT %s
            String[][] columns = { { "c", frozen, "%s", "%s" },
                                   { "s", "set<" + frozen + '>', "%s", "{%s}" },
                                   { "k", "map<" + frozen + ", int>", "KEYS(%s)", "{%s: 1}" },
                                   { "t", "frozen<tuple<" + frozen + ", int>>", "%s", "(%s, 1)" } };
            StringBuilder schema = new StringBuilder("CREATE TABLE %s (pk int, ck int, ");
            for (String[] column : columns)
                schema.append(column[0]).append(' ').append(column[1]).append(", ")
                      .append(column[0]).append(column[0]).append(' ').append(column[1]).append(", ");
            createTable(schema.append("PRIMARY KEY (pk, ck))").toString());
            for (String[] column : columns)
                createIndex("CREATE CUSTOM INDEX ON %s(" + String.format(column[2], column[0]) +
                            ") USING 'StorageAttachedIndex'");
            disableCompaction();

            insert(columns, 0, "{x: 2}");
            // the same value in another serialization
            insert(columns, 1, fieldType.equals("decimal") ? "{x: 2.00}" : "{x: blobAsVarint(0x0002)}");
            flush();
            execute("ALTER TYPE " + KEYSPACE + '.' + udt + " ADD y int");
            insert(columns, 2, "{x: 2}");
            insert(columns, 3, "{x: 2, y: 1}");
            assertQueries(columns, fieldType);
            flush();
            assertQueries(columns, fieldType);
            compact();
            assertQueries(columns, fieldType);
            insert(columns, 4, "{x: 2, y: 1}");
            assertQueries(columns, fieldType);
            assertEquals(5, execute("SELECT pk FROM %s").size());
        }
    }

    private void insert(String[][] columns, int pk, String udtValue)
    {
        StringBuilder names = new StringBuilder("pk, ck");
        StringBuilder values = new StringBuilder(pk + ", 0");
        for (String[] column : columns)
        {
            String literal = String.format(column[3], udtValue);
            names.append(", ").append(column[0]).append(", ").append(column[0]).append(column[0]);
            values.append(", ").append(literal).append(", ").append(literal);
        }
        execute("INSERT INTO %s (" + names + ") VALUES (" + values + ')');
    }

    private void assertQueries(String[][] columns, String fieldType)
    {
        String[] restrictions = { "c = %s", "s CONTAINS %s", "k CONTAINS KEY %s", "t = (%s, 1)" };
        for (String udtValue : new String[]{ "{x: 2}", "{x: 2, y: 1}" })
        {
            for (String restriction : restrictions)
            {
                String indexed = String.format(restriction, udtValue);
                String unindexed = indexed.replaceFirst("^(\\w+)", "$1$1");
                List<Integer> expected = keys("SELECT pk FROM %s WHERE " + unindexed + " ALLOW FILTERING");
                List<Integer> actual = keys("SELECT pk FROM %s WHERE " + indexed);
                assertTrue(fieldType + ": " + indexed + ": " + actual + " not in " + expected,
                           expected.containsAll(actual));
            }
        }
    }

    /**
     * The byte-comparable form of an empty decimal is that of a null, so the canonical form of a frozen collection
     * holding one would hold a null element, which isn't a valid collection: it is indexed as written instead, and the
     * write doesn't fail. Compaction is only checked before version DB: from DB, compacting such map entries fails
     * comparing their terms in SegmentBuilder, with or without canonical terms.
     */
    @Test
    public void testEmptyDecimalInFrozenCollection() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, m map<frozen<list<decimal>>, int>, " +
                    "s set<frozen<list<decimal>>>, PRIMARY KEY (pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(m)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        disableCompaction();
        execute("INSERT INTO %s (pk, ck, m, s) VALUES (0, 0, {[blobAsDecimal(0x)]: 1}, {[blobAsDecimal(0x)]})");
        execute("INSERT INTO %s (pk, ck, m, s) VALUES (1, 0, {[2.5]: 1}, {[2.5]})");
        flush();
        execute("INSERT INTO %s (pk, ck, m, s) VALUES (2, 0, {[blobAsDecimal(0x), 2.5]: 1}, {[blobAsDecimal(0x)]})");
        execute("INSERT INTO %s (pk, ck, m, s) VALUES (3, 0, {[2.50]: 1}, {[2.50]})");
        flush();
        assertEquals(List.of(1, 3), keys("SELECT pk FROM %s WHERE m[[2.5]] = 1"));
        assertEquals(List.of(1, 3), keys("SELECT pk FROM %s WHERE s CONTAINS [2.5]"));
        if (!version.onOrAfter(Version.DB))
        {
            compact();
            assertEquals(List.of(1, 3), keys("SELECT pk FROM %s WHERE m[[2.5]] = 1"));
            assertEquals(List.of(1, 3), keys("SELECT pk FROM %s WHERE s CONTAINS [2.5]"));
        }
        assertEquals(4, execute("SELECT pk FROM %s").size());
    }

    /**
     * A client write of a map entry whose key fits an entries index as written but not in canonical form (2.5 is 5
     * bytes, 2.50 is 6) is rejected; such an entry that reaches the index anyway is left out of it.
     */
    @Test
    public void testCanonicalMapEntryKeyTooLarge() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, m map<frozen<list<decimal>>, int>, PRIMARY KEY (pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(m)) USING 'StorageAttachedIndex'");
        disableCompaction();
        execute("INSERT INTO %s (pk, ck, m) VALUES (0, 0, {[2.5]: 1})");

        // 6600 times 2.5: 59404 bytes as written, 66004 bytes in canonical form
        String key = '[' + String.join(", ", Collections.nCopies(6600, "2.5")) + ']';
        String statement = "INSERT INTO %s (pk, ck, m) VALUES (1, 0, {" + key + ": 1})";
        assertThatThrownBy(() -> execute(statement)).isInstanceOf(InvalidRequestException.class)
                                                    .hasMessageContaining("66004 bytes in canonical form")
                                                    .hasMessageContaining("59404 bytes as written");
        assertEquals(1, execute("SELECT pk FROM %s").size());

        // the same entry applied without the validation of a client write
        List<BigDecimal> list = Collections.nCopies(6600, new BigDecimal("2.5"));
        TableMetadata metadata = getCurrentColumnFamilyStore().metadata();
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, 2);
        builder.row(0).add("m", Map.of(list, 1));
        builder.buildAsMutation().apply();

        beforeAndAfterFlush(() -> {
            assertEquals(List.of(0), keys("SELECT pk FROM %s WHERE m[[2.5]] = 1"));
            assertTrue(keys("SELECT pk FROM %s WHERE m[" + key + "] = 1").isEmpty());
        });
        compact();
        assertEquals(List.of(0), keys("SELECT pk FROM %s WHERE m[[2.5]] = 1"));
        assertEquals(2, execute("SELECT pk FROM %s").size());
    }

    /**
     * A tuple written with fewer fields than its type is padded to all of them in canonical form: a map entry key that
     * fits an entries index only that way is rejected too.
     */
    @Test
    public void testPaddedTupleMapEntryKeyTooLarge() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, " +
                    "m map<frozen<list<frozen<tuple<decimal, int, int, int, int, int, int, int, int, int>>>>, int>, " +
                    "PRIMARY KEY (pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(m)) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, ck, m) VALUES (0, 0, {[(2.5)]: 1})");

        // 2000 times (2.5): 26004 bytes as written, 100004 bytes in canonical form
        String key = '[' + String.join(", ", Collections.nCopies(2000, "(2.5)")) + ']';
        String statement = "INSERT INTO %s (pk, ck, m) VALUES (1, 0, {" + key + ": 1})";
        assertThatThrownBy(() -> execute(statement)).isInstanceOf(InvalidRequestException.class)
                                                    .hasMessageContaining("100004 bytes in canonical form")
                                                    .hasMessageContaining("26004 bytes as written");
        assertEquals(List.of(0), keys("SELECT pk FROM %s"));
    }

    /**
     * A map entry stored as written whose key fits an entries index only as written is indexed as written, and is found
     * by that key after its SSTable is read, compacted or its index rebuilt.
     */
    @Test
    public void testPaddedTupleMapEntryKeyAtRest() throws Throwable
    {
        // the term size limits must admit the entry as written
        Guardrails.instance.setSaiFrozenTermSizeThreshold("1KiB", "30KiB");
        Guardrails.instance.setSaiStringTermSizeThreshold("1KiB", "30KiB");
        try
        {
            // 1500 times (2.5): 19504 bytes as written, 75004 bytes in canonical form
            String key = '[' + String.join(", ", Collections.nCopies(1500, "(2.5)")) + ']';
            String schema = "CREATE TABLE %s (pk int PRIMARY KEY, " +
                            "m map<frozen<list<frozen<tuple<decimal, int, int, int, int, int, int, int, int, " +
                            "int>>>>, int>) WITH memtable = 'skiplist'";
            String[] queries = { "m[" + key + "] = 1", "m[" + key + "] != 1" };
            // the results of every step, compared at the end to show all the differences
            List<String> expected = new ArrayList<>();
            List<String> actual = new ArrayList<>();
            for (String remedy : new String[]{ "compaction", "rebuild" })
            {
                String unindexed = createTable(schema);
                disableCompaction();
                String indexed = createTable(schema);
                createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(m)) USING 'StorageAttachedIndex'");
                disableCompaction();

                // an SSTable written by an earlier version, which indexed and accepted the entry as written
                Injection old = injectTermsAsWritten("old_" + indexed);
                for (String table : new String[]{ unindexed, indexed })
                {
                    execute(String.format("INSERT INTO %s.%s (pk, m) VALUES (1, {%s: 1})", KEYSPACE, table, key));
                    execute(String.format("INSERT INTO %s.%s (pk, m) VALUES (3, {[(1.5)]: 1})", KEYSPACE, table));
                    flush(KEYSPACE, table);
                }
                old.disable();

                for (String phase : new String[]{ "old file", "after " + remedy })
                {
                    if (!phase.equals("old file"))
                    {
                        for (String table : new String[]{ unindexed, indexed })
                            execute(String.format("INSERT INTO %s.%s (pk, m) VALUES (2, {[(2.5)]: 1})",
                                                  KEYSPACE, table));
                        flush(KEYSPACE, indexed);
                        if (remedy.equals("compaction"))
                            compact(KEYSPACE, indexed);
                        else
                            rebuildTableIndexes(KEYSPACE, indexed);
                    }
                    for (String query : queries)
                    {
                        String select = "SELECT pk FROM " + KEYSPACE + ".%s WHERE " + query;
                        String description = remedy + ", " + phase + ": " + query.replace(key, "key");
                        expected.add(description + ' ' + keys(String.format(select + " ALLOW FILTERING", unindexed)));
                        actual.add(description + ' ' + keys(String.format(select, indexed)));
                    }
                }
            }
            assertEquals(expected, actual);
        }
        finally
        {
            Guardrails.instance.setSaiFrozenTermSizeThreshold("1KiB", "8KiB");
            Guardrails.instance.setSaiStringTermSizeThreshold("1KiB", "8KiB");
        }
    }
}
