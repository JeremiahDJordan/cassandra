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

import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;

/**
 * {@code m[k] != v} on a map entries index returns the rows whose map doesn't have the entry, like ALLOW FILTERING, at
 * every index version, also for keys that sort differently by their bytes than by their type. Every row here has a
 * non-empty map.
 */
public class MapEntriesNotEqualTest extends SAITester.Versioned
{
    @Test
    public void testNotEqual() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, " +
                    "d map<decimal, int>, dd map<decimal, int>, " +
                    "n map<int, int>, nn map<int, int>, " +
                    "t map<text, int>, tt map<text, int>, " +
                    "PRIMARY KEY (pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(d)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(n)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(t)) USING 'StorageAttachedIndex'");

        String[] decimals = { "2.5", "3.5", "7.5", "-1.5", "0.5", "2.50", "-10", "100.25" };
        String[] ints = { "-2", "-1", "0", "1", "2" };
        String[] texts = { "'a'", "'b'", "'ab'", "''", "'z'" };
        for (int i = 0; i < decimals.length; i++)
        {
            int value = i % 2;
            String d = '{' + decimals[i] + ": " + value + '}';
            String n = '{' + ints[i % ints.length] + ": " + value + '}';
            String t = '{' + texts[i % texts.length] + ": " + value + '}';
            execute(String.format("INSERT INTO %%s (pk, ck, d, dd, n, nn, t, tt) " +
                                  "VALUES (%d, 0, %s, %2$s, %s, %3$s, %s, %4$s)", i, d, n, t));
            if (i == decimals.length / 2)
                flush();
        }

        List<String> queries = new ArrayList<>();
        for (String value : new String[]{ "0", "1" })
        {
            for (String key : decimals)
                queries.add("%d[" + key + "] != " + value);
            for (String key : ints)
                queries.add("%n[" + key + "] != " + value);
            for (String key : texts)
                queries.add("%t[" + key + "] != " + value);
        }

        runThenFlushThenCompact(() -> {
            for (String query : queries)
            {
                String indexed = query.replace("%d", "d").replace("%n", "n").replace("%t", "t");
                String unindexed = query.replace("%d", "dd").replace("%n", "nn").replace("%t", "tt");
                assertEquals(indexed,
                             keys("SELECT pk FROM %s WHERE " + unindexed + " ALLOW FILTERING"),
                             keys("SELECT pk FROM %s WHERE " + indexed));
            }
        });
    }
}
