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
import java.util.Set;
import java.util.TreeSet;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;

/**
 * Restrictions with an empty bound or element on a non-literal index, whose empty values aren't indexed (see
 * CASSANDRA-20313 and {@link EmptyValuesTest}). The index returns what filtering without the index returns, except the
 * rows holding an empty value, which it can't find for any restriction; the tests restrict the indexed column alone,
 * as such a row can still be found through another restriction.
 */
public class EmptyBoundsTest extends SAITester.Versioned
{
    @Test
    public void testEmptyBounds()
    {
        // the second value of each type is its smallest non-empty value where it has one
        checkEmptyBounds("int", "blobAsInt", "-2147483648", "2", false);
        checkEmptyBounds("bigint", "blobAsBigint", "-9223372036854775808", "2", false);
        checkEmptyBounds("varint", "blobAsVarint", "-1", "2", false);
        checkEmptyBounds("decimal", "blobAsDecimal", "1.5", "2.5", false);
        checkEmptyBounds("double", "blobAsDouble", "-1.5", "2.5", false);
        checkEmptyBounds("float", "blobAsFloat", "-1.5", "2.5", false);
        checkEmptyBounds("timestamp", "blobAsTimestamp", "0", "2000", false);
        checkEmptyBounds("timeuuid", "blobAsTimeuuid", "00000000-0000-1000-8000-000000000001", "00000001-0000-1000-8000-000000000001", false);
        // the index serves only = and IN on a uuid column; the other restrictions are filtered without it
        checkEmptyBounds("uuid", "blobAsUuid", "00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002", true);
        checkEmptyBounds("inet", "blobAsInet", "'0.0.0.0'", "'10.0.0.2'", false);
    }

    private void checkEmptyBounds(String type, String blobAs, String x, String y, boolean equalityOnly)
    {
        String empty = blobAs + "(0x)";
        List<String> schemas = new ArrayList<>();
        schemas.add("CREATE TABLE %s (pk int, ck int, v " + type + ", u " + type + ", PRIMARY KEY (pk, ck))");
        schemas.add("CREATE TABLE %s (pk int, ck int, v " + type + " static, u " + type + " static, PRIMARY KEY (pk, ck))");
        schemas.add("CREATE TABLE %s (pk int, v " + type + ", ck int, u " + type + ", PRIMARY KEY (pk, v, ck))");
        schemas.add("CREATE TABLE %s (pk int, v " + type + ", ck int, u " + type + ", PRIMARY KEY (pk, v, ck)) " +
                    "WITH CLUSTERING ORDER BY (v DESC, ck ASC)");

        List<String> queries = new ArrayList<>();
        for (String bound : new String[]{ empty, x, y })
        {
            queries.add("%c = " + bound);
            queries.add("%c IN (" + bound + ", " + y + ')');
            if (equalityOnly)
                continue;
            for (String op : new String[]{ "!=", "<", "<=", ">", ">=" })
                queries.add("%c " + op + ' ' + bound);
            queries.add("%c NOT IN (" + bound + ", " + y + ')');
        }
        if (!equalityOnly)
        {
            queries.add("%c > " + empty + " AND %c <= " + y);
            queries.add("%c >= " + empty + " AND %c < " + y);
            queries.add("%c > " + x + " AND %c <= " + empty);
        }

        for (String schema : schemas)
        {
            // the unindexed column u holds the same values as the indexed column v
            createTable(schema);
            createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
            String insert = "INSERT INTO %%s (pk, ck, v, u) VALUES (?, 0, %1$s, %1$s)";
            execute(String.format(insert, empty), 0);
            execute(String.format(insert, x), 1);
            execute(String.format(insert, y), 2);
            Set<Integer> unindexedRows = Set.of(0, 3);

            assertMatchesFiltering(schema, "memtable", queries, unindexedRows, empty);
            flush();
            execute(String.format(insert, empty), 3);
            execute(String.format(insert, x), 4);
            flush();
            assertMatchesFiltering(schema, "sstables", queries, unindexedRows, empty);
            compact();
            assertMatchesFiltering(schema, "compacted", queries, unindexedRows, empty);
        }
    }

    /**
     * Collections of non-literal elements holding empty elements. An index search for an empty element finds nothing, and
     * NOT CONTAINS of an empty element returns what filtering without the index returns.
     */
    @Test
    public void testEmptyElements()
    {
        checkEmptyElements("int", "blobAsInt", "1");
        checkEmptyElements("timestamp", "blobAsTimestamp", "1000");
        checkEmptyElements("inet", "blobAsInet", "'10.0.0.1'");
        checkEmptyElements("varint", "blobAsVarint", "1");
    }

    private void checkEmptyElements(String type, String blobAs, String x)
    {
        String empty = blobAs + "(0x)";
        // collection type, literal of a collection holding %s, index target, operator
        String[][] layouts = { { "list<" + type + '>', "[%s]", "v", "CONTAINS" },
                               { "set<" + type + '>', "{%s}", "v", "CONTAINS" },
                               { "map<" + type + ", int>", "{%s: 1}", "KEYS(v)", "CONTAINS KEY" },
                               { "map<int, " + type + '>', "{1: %s}", "VALUES(v)", "CONTAINS" } };
        for (String[] layout : layouts)
        {
            createTable("CREATE TABLE %s (pk int, ck int, v " + layout[0] + ", u " + layout[0] + ", PRIMARY KEY (pk, ck))");
            createIndex("CREATE CUSTOM INDEX ON %s(" + layout[2] + ") USING 'StorageAttachedIndex'");
            String insert = "INSERT INTO %%s (pk, ck, v, u) VALUES (?, 0, %1$s, %1$s)";
            execute(String.format(insert, String.format(layout[1], empty)), 0);
            execute(String.format(insert, String.format(layout[1], x)), 1);
            if (!layout[0].startsWith("map"))
                execute(String.format(insert, String.format(layout[1], empty + ", " + x)), 2);
            execute("INSERT INTO %s (pk, ck) VALUES (3, 0)");
            // the rows whose collection holds no indexed element
            Set<Integer> unindexedRows = Set.of(0, 4);

            List<String> queries = new ArrayList<>();
            for (String element : new String[]{ empty, x })
            {
                queries.add("%c " + layout[3] + ' ' + element);
                queries.add("%c NOT " + layout[3] + ' ' + element);
            }

            assertMatchesFiltering(layout[0], "memtable", queries, unindexedRows, empty);
            flush();
            execute(String.format(insert, String.format(layout[1], empty)), 4);
            flush();
            assertMatchesFiltering(layout[0], "sstables", queries, unindexedRows, empty);
            compact();
            assertMatchesFiltering(layout[0], "compacted", queries, unindexedRows, empty);
        }
    }

    /**
     * Checks that each query (with %c standing for the column) on the indexed column v returns the rows that it returns
     * on the unindexed column u, except the rows without any indexed value, which the index can't find. A CONTAINS of
     * an empty element finds nothing, as empty elements aren't indexed, and a NOT CONTAINS returns all the rows that
     * filtering returns.
     */
    private void assertMatchesFiltering(String schema, String phase, List<String> queries, Set<Integer> unindexedRows, String empty)
    {
        for (String query : queries)
        {
            Set<Integer> expected = partitions("SELECT * FROM %s WHERE " + query.replace("%c", "u") + " ALLOW FILTERING");
            if (query.contains(" CONTAINS") && !query.contains("NOT CONTAINS") && query.endsWith(empty))
                expected.clear();
            else if (!query.contains("NOT CONTAINS"))
                expected.removeAll(unindexedRows);
            Set<Integer> actual = partitions("SELECT * FROM %s WHERE " + query.replace("%c", "v") + " ALLOW FILTERING");
            Assertions.assertThat(actual)
                      .describedAs("%s, %s, %s: %s", version, schema, phase, query.replace("%c", "v"))
                      .isEqualTo(expected);
        }
    }

    private Set<Integer> partitions(String query)
    {
        Set<Integer> keys = new TreeSet<>();
        for (UntypedResultSet.Row row : execute(query))
            keys.add(row.getInt("pk"));
        return keys;
    }
}
