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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A range query on an entry of an indexed map ({@code m[k] > v}) with a key of 32768 bytes or more returns the rows
 * that ALLOW FILTERING on an unindexed copy returns, from the memtable and after flush. The max term size is raised
 * when the class is loaded, as SAI reads it once (each test class runs in its own JVM). Compaction isn't checked: the
 * SSTable index writer of a compaction can't hold terms over 32766 bytes. Keys too large to index return a subset of
 * the rows.
 */
public class MapEntriesLargeKeyRangeTest extends SAITester.Versioned
{
    static
    {
        CassandraRelevantProperties.SAI_MAX_STRING_TERM_SIZE.setInt(128);
    }

    private static final int[] KEY_SIZES = { 1, 32767, 32768, 40000, 65534 };

    @Test
    public void testRangeQueriesWithLargeKeys() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, m map<text, text>, u map<text, text>, PRIMARY KEY (pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(m)) USING 'StorageAttachedIndex'");

        String[] keys = new String[KEY_SIZES.length];
        for (int i = 0; i < keys.length; i++)
        {
            keys[i] = String.valueOf((char) ('a' + i)).repeat(KEY_SIZES[i]);
            String entries = "{'k': 'b', '" + keys[i] + "': 'b'}";
            execute("INSERT INTO %s (pk, ck, m, u) VALUES (?, 0, " + entries + ", " + entries + ')', i);
            execute("INSERT INTO %s (pk, ck, m, u) VALUES (?, 1, {'k': 'c'}, {'k': 'c'})", i);
        }

        // whether the entries of each key size are indexed at all
        boolean[] indexed = new boolean[keys.length];
        for (int i = 0; i < keys.length; i++)
            indexed[i] = !rows("SELECT pk, ck FROM %s WHERE m['" + keys[i] + "'] = 'b'").isEmpty();
        assertTrue("the entries with a 1-byte key must be indexed", indexed[0]);

        beforeAndAfterFlush(() -> {
            for (int i = 0; i < keys.length; i++)
            {
                for (String op : new String[]{ ">", ">=", "<", "<=", "=" })
                {
                    for (String value : new String[]{ "'a'", "'b'", "'c'" })
                    {
                        String restriction = "['" + keys[i] + "'] " + op + ' ' + value;
                        List<String> unindexed = rows("SELECT pk, ck FROM %s WHERE u" + restriction +
                                                      " ALLOW FILTERING");
                        List<String> actual = rows("SELECT pk, ck FROM %s WHERE m" + restriction);
                        String description = "key of " + KEY_SIZES[i] + " bytes, " + op + ' ' + value;
                        if (indexed[i])
                            assertEquals(description, unindexed, actual);
                        else
                            assertTrue(description + ": " + actual + " not in " + unindexed,
                                       unindexed.containsAll(actual));
                    }
                }
            }
        });
    }

    private List<String> rows(String query)
    {
        List<String> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : execute(query))
            rows.add(row.getInt("pk") + ":" + row.getInt("ck"));
        rows.sort(String::compareTo);
        return rows;
    }
}
