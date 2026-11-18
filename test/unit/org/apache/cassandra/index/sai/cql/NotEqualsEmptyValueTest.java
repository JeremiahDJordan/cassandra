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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;

import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.junit.Assert.assertEquals;

/**
 * {@code !=} and {@code NOT IN} on indexed regular and static columns of frozen tuples and UDTs, whose empty value SAI
 * indexes, return what {@code ALLOW FILTERING} returns without the index. The same queries on clustering columns are
 * in {@link ReversedClusteringIndexTest}.
 */
public class NotEqualsEmptyValueTest extends SAITester
{
    @Test
    public void testFrozenTuple()
    {
        checkNotEquals("frozen<tuple<int, text>>", EMPTY_BYTE_BUFFER, tuple(1, "a"), tuple(1, "b"));
    }

    @Test
    public void testFrozenUDT()
    {
        String udt = createType("CREATE TYPE %s (i int, t text)");
        checkNotEquals("frozen<" + udt + '>', EMPTY_BYTE_BUFFER,
                       userType("i", 1, "t", "a"), userType("i", 1, "t", "b"));
    }

    private void checkNotEquals(String type, Object... values)
    {
        String schema = String.format("CREATE TABLE %%s (pk int, ck int, s %s static, v %s, PRIMARY KEY (pk, ck))", type, type);
        String indexed = createTable(schema);
        createIndex(String.format("CREATE INDEX ON %s.%s(v) USING 'sai'", KEYSPACE, indexed));
        createIndex(String.format("CREATE INDEX ON %s.%s(s) USING 'sai'", KEYSPACE, indexed));
        String filtered = createTable(schema);

        // Each partition has a different static value, and one row per regular value
        for (String table : new String[]{ indexed, filtered })
        {
            for (int pk = 0; pk < values.length; pk++)
            {
                for (int ck = 0; ck < values.length; ck++)
                    executeFormattedQuery(String.format("INSERT INTO %s.%s (pk, ck, v) VALUES (?, ?, ?)", KEYSPACE, table),
                                          pk, ck, values[ck]);
                executeFormattedQuery(String.format("INSERT INTO %s.%s (pk, s) VALUES (?, ?)", KEYSPACE, table),
                                      pk, values[pk]);
            }
        }

        checkQueries(indexed, filtered, values);
        flush(KEYSPACE, indexed, filtered);
        checkQueries(indexed, filtered, values);
    }

    private void checkQueries(String indexed, String filtered, Object[] values)
    {
        for (String column : new String[]{ "v", "s" })
        {
            for (int i = 0; i < values.length; i++)
            {
                Object next = values[(i + 1) % values.length];
                checkQuery(indexed, filtered, column + " != ?", values[i]);
                checkQuery(indexed, filtered, column + " NOT IN (?, ?)", values[i], next);
            }
        }
    }

    private void checkQuery(String indexed, String filtered, String where, Object... values)
    {
        assertEquals(where + " with " + describe(values), select(filtered, where, values), select(indexed, where, values));
    }

    private List<String> select(String table, String where, Object... values)
    {
        String query = String.format("SELECT pk, ck FROM %s.%s WHERE %s ALLOW FILTERING", KEYSPACE, table, where);
        UntypedResultSet rows = executeFormattedQuery(query, values);
        List<String> keys = new ArrayList<>();
        for (UntypedResultSet.Row row : rows)
            keys.add(row.getInt("pk") + ":" + row.getInt("ck"));
        keys.sort(String::compareTo);
        return keys;
    }

    private static String describe(Object[] values)
    {
        List<String> descriptions = new ArrayList<>();
        for (Object value : values)
            descriptions.add(value == EMPTY_BYTE_BUFFER ? "<empty>" : String.valueOf(value));
        return descriptions.toString();
    }
}
