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

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

/**
 * {@code !=} on an index of a frozen value (tuple, UDT) returns the rows whose value differs, like ALLOW FILTERING, at
 * every index version, also for values that sort differently by their bytes than by their type.
 */
public class FrozenNotEqualTest extends SAITester.Versioned
{
    private static final String[] INTS = { "2", "-1", "7", "0", "300" };

    @Test
    public void testRegularColumns() throws Throwable
    {
        String udt = createType("CREATE TYPE %s (x int, y int)");
        // name, type, literal of a value holding %s (an int)
        String[][] columns = { { "ti", "frozen<tuple<int, int>>", "(%s, 1)" },
                               { "tx", "frozen<tuple<text, int>>", "('%s', 1)" },
                               { "td", "frozen<tuple<decimal, int>>", "(%s, 1)" },
                               { "u", "frozen<" + udt + '>', "{x: %s, y: 1}" } };
        StringBuilder schema = new StringBuilder("CREATE TABLE %s (pk int, ck int, ");
        for (String[] column : columns)
            schema.append(column[0]).append(' ').append(column[1]).append(", ")
                  .append(column[0]).append(column[0]).append(' ').append(column[1]).append(", ");
        createTable(schema.append("PRIMARY KEY (pk, ck))").toString());
        for (String[] column : columns)
            createIndex("CREATE CUSTOM INDEX ON %s(" + column[0] + ") USING 'StorageAttachedIndex'");

        for (int pk = 0; pk < INTS.length; pk++)
        {
            StringBuilder names = new StringBuilder("pk, ck");
            StringBuilder values = new StringBuilder(pk + ", 0");
            for (String[] column : columns)
            {
                String literal = String.format(column[2], INTS[pk]);
                names.append(", ").append(column[0]).append(", ").append(column[0]).append(column[0]);
                values.append(", ").append(literal).append(", ").append(literal);
            }
            execute("INSERT INTO %s (" + names + ") VALUES (" + values + ')');
            if (pk == 2)
                flush();
        }

        runThenFlushThenCompact(() -> {
            for (String[] column : columns)
            {
                for (String value : INTS)
                {
                    String literal = String.format(column[2], value);
                    String indexed = column[0] + " != " + literal;
                    String unindexed = column[0] + column[0] + " != " + literal;
                    assertEquals(indexed,
                                 keys("SELECT pk FROM %s WHERE " + unindexed + " ALLOW FILTERING"),
                                 keys("SELECT pk FROM %s WHERE " + indexed));
                }
            }
        });
    }

    @Test
    public void testClusteringColumn() throws Throwable
    {
        String schema = "CREATE TABLE %s (pk int, c frozen<tuple<int, int>>, v int, PRIMARY KEY (pk, c))";
        String unindexed = createTable(schema);
        String indexed = createTable(schema);
        createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex'");
        for (int pk = 0; pk < INTS.length; pk++)
        {
            for (String table : new String[]{ unindexed, indexed })
                execute(String.format("INSERT INTO %s.%s (pk, c, v) VALUES (%d, (%s, 1), 0)",
                                      KEYSPACE, table, pk % 2, INTS[pk]));
            if (pk == 2)
                flush(KEYSPACE, indexed);
        }

        for (String phase : new String[]{ "memtable", "flushed", "compacted" })
        {
            if (phase.equals("flushed"))
                flush(KEYSPACE, indexed);
            else if (phase.equals("compacted"))
                compact(KEYSPACE, indexed);
            for (String value : INTS)
            {
                // ALLOW FILTERING on both: a restriction on a clustering column alone needs it, and the index of the
                // indexed table still serves it
                String query = "SELECT pk, c FROM %s.%s WHERE c != (" + value + ", 1) ALLOW FILTERING";
                assertEquals(phase + ": " + query,
                             rows(String.format(query, KEYSPACE, unindexed)),
                             rows(String.format(query, KEYSPACE, indexed)));
            }
        }
    }

    private List<String> rows(String query)
    {
        List<String> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : execute(query))
            rows.add(row.getInt("pk") + ":" + ByteBufferUtil.bytesToHex(row.getBytes("c")));
        rows.sort(String::compareTo);
        return rows;
    }
}
