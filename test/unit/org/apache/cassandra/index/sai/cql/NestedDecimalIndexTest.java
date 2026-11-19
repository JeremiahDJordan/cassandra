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
 * Decimals and varints nested in a frozen tuple, UDT or collection (as a column, a set element, a map key or value) or
 * in a map entry have several serializations for the same value (2.5 and 2.50, 0x0001 and 0x01): the index finds the
 * rows whose value equals the queried one whatever its serialization, like ALLOW FILTERING does.
 */
public class NestedDecimalIndexTest extends SAITester.Versioned
{
    @Test
    public void testNestedDecimalsAndVarints() throws Throwable
    {
        String point = createType("CREATE TYPE %s (x decimal, y int)");
        String[][] columns = {
            // name, type, index target, literal of a value holding %s (a decimal), restriction on %c with %s
            { "s", "set<frozen<tuple<decimal, int>>>", "%s", "{(%s, 1)}", "%c CONTAINS (%s, 1)" },
            { "mk", "map<frozen<tuple<decimal, int>>, int>", "KEYS(%s)", "{(%s, 1): 1}", "%c CONTAINS KEY (%s, 1)" },
            { "me", "map<frozen<tuple<decimal, int>>, int>", "ENTRIES(%s)", "{(%s, 1): 1}", "%c[(%s, 1)] = 1" },
            { "ml", "map<frozen<list<decimal>>, int>", "ENTRIES(%s)", "{[%s]: 1}", "%c[[%s]] = 1" },
            { "mv", "map<int, frozen<tuple<decimal, int>>>", "ENTRIES(%s)", "{1: (%s, 1)}", "%c[1] = (%s, 1)" },
            { "t", "frozen<tuple<decimal, int>>", "%s", "(%s, 1)", "%c = (%s, 1)" },
            { "fl", "frozen<list<decimal>>", "FULL(%s)", "[%s]", "%c = [%s]" },
            { "u", "frozen<" + point + '>', "%s", "{x: %s, y: 1}", "%c = {x: %s, y: 1}" },
            { "mi", "map<varint, int>", "ENTRIES(%s)", "{%s: 1}", "%c[%s] = 1" },
            { "vi", "varint", "%s", "%s", "%c = %s|%c > %s|%c <= %s" },
            { "sv", "set<varint>", "%s", "{%s}", "%c CONTAINS %s" },
            { "lv", "list<varint>", "%s", "[%s]", "%c CONTAINS %s" } };

        StringBuilder schema = new StringBuilder("CREATE TABLE %s (pk int, ck int, ");
        for (String[] column : columns)
            schema.append(column[0]).append(' ').append(column[1]).append(", ")
                  .append(column[0]).append(column[0]).append(' ').append(column[1]).append(", ");
        createTable(schema.append("PRIMARY KEY (pk, ck))").toString());
        for (String[] column : columns)
        {
            String target = String.format(column[2], column[0]);
            createIndex("CREATE CUSTOM INDEX ON %s(" + target + ") USING 'StorageAttachedIndex'");
        }

        String[] decimals = { "2.5", "2.50", "7", "-1.5" };
        String[] varints = { "1", "blobAsVarint(0x0001)", "7", "blobAsVarint(0xFFFF)" };
        for (int pk = 0; pk < decimals.length; pk++)
        {
            StringBuilder names = new StringBuilder("pk, ck");
            StringBuilder values = new StringBuilder(pk + ", 0");
            for (String[] column : columns)
            {
                String value = isVarint(column) ? varints[pk] : decimals[pk];
                String literal = String.format(column[3], value);
                names.append(", ").append(column[0]).append(", ").append(column[0]).append(column[0]);
                values.append(", ").append(literal).append(", ").append(literal);
            }
            execute("INSERT INTO %s (" + names + ") VALUES (" + values + ')');
            if (pk == 1)
                flush();
        }

        // column name, restriction with %c for the column
        List<String[]> queries = new ArrayList<>();
        for (String[] column : columns)
        {
            String[] literals = isVarint(column)
                                ? new String[]{ "1", "blobAsVarint(0x0001)", "blobAsVarint(0x00000007)", "-1",
                                                "blobAsVarint(0xFFFF)" }
                                : new String[]{ "2.5", "2.50", "2.500", "7.0", "-1.50" };
            for (String restriction : column[4].split("\\|"))
            {
                for (String literal : literals)
                    queries.add(new String[]{ column[0], restriction.replace("%s", literal) });
            }
        }

        runThenFlushThenCompact(() -> {
            for (String[] query : queries)
            {
                String indexed = query[1].replace("%c", query[0]);
                String unindexed = query[1].replace("%c", query[0] + query[0]);
                assertEquals(indexed,
                             keys("SELECT pk FROM %s WHERE " + unindexed + " ALLOW FILTERING"),
                             keys("SELECT pk FROM %s WHERE " + indexed));
            }
        });
    }

    /**
     * Rewrites of a value in another serialization of the same value, in the same memtable: the old and the new values
     * have the same index term, so the memtable index must not remove the term the update adds when it removes the
     * old value's.
     */
    @Test
    public void testRewritesInAnotherSerialization() throws Throwable
    {
        String point = createType("CREATE TYPE %s (x decimal, y int)");
        createTable("CREATE TABLE %s (pk int, ck int, " +
                    "t frozen<tuple<decimal, int>>, tt frozen<tuple<decimal, int>>, " +
                    "u frozen<" + point + ">, uu frozen<" + point + ">, " +
                    "n frozen<tuple<frozen<tuple<decimal, int>>, int>>, " +
                    "nn frozen<tuple<frozen<tuple<decimal, int>>, int>>, " +
                    "fm frozen<map<int, decimal>>, fmfm frozen<map<int, decimal>>, " +
                    "l list<frozen<tuple<decimal, int>>>, ll list<frozen<tuple<decimal, int>>>, " +
                    "PRIMARY KEY (pk, ck))");
        for (String target : new String[]{ "t", "u", "n", "FULL(fm)", "l" })
            createIndex("CREATE CUSTOM INDEX ON %s(" + target + ") USING 'StorageAttachedIndex'");

        String[][] writes = {
            // a value rewritten in another serialization
            { "t = (-1.500, 1)", "t = (-1.5, 1)" },
            { "u = {x: -1.500, y: 1}", "u = {x: -1.5, y: 1}" },
            { "n = ((-1.500, 1), 1)", "n = ((-1.5, 1), 1)" },
            { "fm = {1: -1.500}", "fm = {1: -1.5}" },
            { "l = [(-1.500, 1)]", "l = [(-1.5, 1)]" } };
        for (int pk = 0; pk < writes.length; pk++)
        {
            for (String write : writes[pk])
            {
                for (boolean twin : new boolean[]{ false, true })
                {
                    String statement = "UPDATE %s SET " + write + " WHERE pk = " + pk + " AND ck = 0";
                    execute(twin ? twinColumns(statement) : statement);
                }
            }
        }

        String[] restrictions = { "%c = (-1.5, 1)", "%c = (-1.500, 1)", "%c = {x: -1.5, y: 1}", "%c = {x: -1.50, y: 1}",
                                  "%c = ((-1.5, 1), 1)", "%c = {1: -1.5}", "%c = {1: -1.50}",
                                  "%c CONTAINS (-1.5, 1)", "%c CONTAINS (-1.500, 1)" };
        String[][] columns = { { "t", "tt" }, { "u", "uu" }, { "n", "nn" }, { "fm", "fmfm" }, { "l", "ll" } };
        beforeAndAfterFlush(() -> {
            for (String restriction : restrictions)
            {
                for (String[] column : columns)
                {
                    if (!fits(restriction, column[0]))
                        continue;
                    String indexed = restriction.replace("%c", column[0]);
                    String unindexed = restriction.replace("%c", column[1]);
                    assertEquals(indexed,
                                 keys("SELECT pk FROM %s WHERE " + unindexed + " ALLOW FILTERING"),
                                 keys("SELECT pk FROM %s WHERE " + indexed));
                }
            }
        });
    }

    /** Whether the restriction applies to the column, by the shape of its literal. */
    private static boolean fits(String restriction, String column)
    {
        switch (column)
        {
            case "t": return restriction.startsWith("%c = (") && !restriction.startsWith("%c = ((");
            case "u": return restriction.startsWith("%c = {x");
            case "n": return restriction.startsWith("%c = ((");
            case "fm": return restriction.startsWith("%c = {1");
            default: return restriction.contains("CONTAINS");
        }
    }

    private static String twinColumns(String statement)
    {
        return statement.replaceAll("\\b(t|u|n|fm|l)\\b(?= =)", "$1$1");
    }

    private static boolean isVarint(String[] column)
    {
        // by name: the generated name of the UDT contains the name of the test method
        return List.of("mi", "vi", "sv", "lv").contains(column[0]);
    }
}
