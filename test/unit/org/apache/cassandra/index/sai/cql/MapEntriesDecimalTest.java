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
 * An SAI index on the entries of a map with decimal keys or values finds the entries whose decimal equals the queried
 * one whatever their scale (2.5, 2.50 and 2.500 are equal decimals with different serializations), like ALLOW
 * FILTERING does, also when the trie memtable or a compaction changed their scale.
 */
public class MapEntriesDecimalTest extends SAITester.Versioned
{
    @Test
    public void testCompactedEntryWithDecimalKey() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, m map<decimal, int>, PRIMARY KEY (pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(m)) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, ck, m) VALUES (1, 0, {-1.5: 1})");
        execute("INSERT INTO %s (pk, ck, m) VALUES (3, 0, {2.5: 1})");
        flush();
        execute("INSERT INTO %s (pk, ck, m) VALUES (5, 0, {7.5: 1})");
        flush();

        runThenFlushThenCompact(() -> {
            assertEquals(List.of(1), keys("SELECT pk FROM %s WHERE m[-1.5] = 1"));
            assertEquals(List.of(3), keys("SELECT pk FROM %s WHERE m[2.5] = 1"));
            assertEquals(List.of(1), keys("SELECT pk FROM %s WHERE m[-1.5] > 0"));
            assertEquals(List.of(5), keys("SELECT pk FROM %s WHERE m[7.5] >= 1"));
        });
    }

    /**
     * Decimal keys and values written and queried with different scales, compared with ALLOW FILTERING on unindexed
     * copies of the columns.
     */
    @Test
    public void testScalesOfKeysAndValues() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, k map<decimal, int>, kk map<decimal, int>, " +
                    "w map<int, decimal>, ww map<int, decimal>, " +
                    "kw map<decimal, decimal>, kkww map<decimal, decimal>, " +
                    "PRIMARY KEY (pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(k)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(w)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(kw)) USING 'StorageAttachedIndex'");

        String[] values = { "2.5", "2.50", "1.25", "7", "-1.5", "-1.500", "0", "0.00",
                            "123456789012345678901234567890.5" };
        for (int i = 0; i < values.length; i++)
        {
            String v = values[i];
            execute(String.format("INSERT INTO %%s (pk, ck, k, kk, w, ww, kw, kkww) " +
                                  "VALUES (%d, 0, {%s: 1}, {%2$s: 1}, " +
                                  "{1: %2$s}, {1: %2$s}, {%2$s: %2$s}, {%2$s: %2$s})", i, v));
            if (i == values.length / 2)
                flush();
        }

        List<String> queries = new ArrayList<>();
        String[] literals = { "2.5", "2.50", "2.500", "7", "7.0", "-1.5", "-1.50", "0.0", "1.250",
                              "123456789012345678901234567890.50" };
        for (String v : literals)
        {
            queries.add("%a[" + v + "] = 1");
            queries.add("%a[" + v + "] > 0");
            queries.add("%a[" + v + "] <= 1");
            queries.add("%b[1] = " + v);
            queries.add("%b[1] > " + v);
            queries.add("%b[1] <= " + v);
            queries.add("%c[" + v + "] = " + v);
            queries.add("%c[" + v + "] >= 2.5");
        }

        runThenFlushThenCompact(() -> {
            for (String query : queries)
            {
                String indexed = query.replace("%a", "k").replace("%b", "w").replace("%c", "kw");
                String unindexed = query.replace("%a", "kk").replace("%b", "ww").replace("%c", "kkww");
                assertEquals(indexed,
                             keys("SELECT pk FROM %s WHERE " + unindexed + " ALLOW FILTERING"),
                             keys("SELECT pk FROM %s WHERE " + indexed));
            }
        });
    }
}
