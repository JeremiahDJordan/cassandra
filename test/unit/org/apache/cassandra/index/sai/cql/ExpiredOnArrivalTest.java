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
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertTrue;

/**
 * Cells that are already expired when the memtable index is given them (a hint, a replayed or delayed write, or a TTL
 * that ends between the statement and the index) are held by the index like the values that expire later, so that
 * writing the same value again, or another one, doesn't lose the row.
 */
public class ExpiredOnArrivalTest extends SAITester
{
    private static final String[] MEMTABLES = { "", "skiplist", "trie_stage1", "trie_stage2", "trie_stage3" };

    @Test
    public void testExpiredOnArrival() throws Throwable
    {
        List<String> failures = new ArrayList<>();
        for (String memtable : MEMTABLES)
        {
            String schema = "CREATE TABLE %s (pk int PRIMARY KEY, v text, n int, mv map<text, text>, x int)" +
                            (memtable.isEmpty() ? "" : " WITH memtable = '" + memtable + '\'');
            String indexed = createTable(schema);
            for (String target : new String[]{ "v", "n", "VALUES(mv)" })
                createIndex(String.format("CREATE INDEX ON %s.%s(%s) USING 'sai'", KEYSPACE, indexed, target));
            String filtered = createTable(schema);

            for (String table : new String[]{ indexed, filtered })
                writeScenarios(table);

            String source = memtable.isEmpty() ? "trie" : memtable;
            checkQueries(source + " memtable", indexed, filtered, failures);
            flush(KEYSPACE, indexed, filtered);
            checkQueries(source + " sstable", indexed, filtered, failures);
        }
        assertTrue(String.format("%d failures:%n%s", failures.size(), String.join("\n", failures)), failures.isEmpty());
    }

    private void writeScenarios(String table)
    {
        // 1: a value expired on arrival, then written again
        writeExpired(table, 1, "v", "a");
        writeExpired(table, 1, "n", 5);
        write(table, "UPDATE %s SET v = 'a', n = 5 WHERE pk = 1");
        // 2: a value expired on arrival, then another value
        writeExpired(table, 2, "v", "a");
        writeExpired(table, 2, "n", 5);
        write(table, "UPDATE %s SET v = 'b', n = 6 WHERE pk = 2");
        // 3: a map value expired on arrival, then the same key written again with the same value
        writeExpired(table, 3, "mv", Collections.singletonMap("k1", "a"));
        write(table, "UPDATE %s SET mv['k1'] = 'a' WHERE pk = 3");
        // 5: a value expired on arrival, and nothing else
        writeExpired(table, 5, "v", "a");
        writeExpired(table, 5, "n", 5);
        // 7: the empty value, expired on arrival, then written again
        writeExpired(table, 7, "v", "");
        write(table, "UPDATE %s SET v = '' WHERE pk = 7");
        // 8: the same with a map value
        writeExpired(table, 8, "mv", Collections.singletonMap("k1", ""));
        write(table, "UPDATE %s SET mv['k1'] = '' WHERE pk = 8");
        // 6: control, not expired
        write(table, "INSERT INTO %s (pk, v, n, mv) VALUES (6, 'a', 5, {'k1': 'a'})");
        write(table, "UPDATE %s SET v = 'a', n = 5, mv['k1'] = 'a' WHERE pk = 6");
    }

    /**
     * Writes a cell whose TTL expired 90 seconds ago.
     */
    private static void writeExpired(String table, int pk, String column, Object value)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
        long localDeletionTime = FBUtilities.nowInSeconds() - 100; // with a TTL of 10, the expiration time
        new RowUpdateBuilder(cfs.metadata(), localDeletionTime, FBUtilities.timestampMicros() - 1000, 10, pk)
        .add(column, value)
        .build()
        .apply();
    }

    private void write(String table, String statement)
    {
        executeFormattedQuery(String.format(statement, KEYSPACE + '.' + table));
    }

    private void checkQueries(String source, String indexed, String filtered, List<String> failures)
    {
        for (String where : new String[]{ "v = 'a'", "v = 'b'", "v = ''", "n = 5", "n = 6", "n != 5", "n != 6", "n > 0",
                                          "mv CONTAINS 'a'", "mv CONTAINS 'x'", "mv CONTAINS ''",
                                          "mv NOT CONTAINS 'a'", "mv NOT CONTAINS 'x'", "mv NOT CONTAINS ''" })
        {
            String expected = select(filtered, where);
            String actual = select(indexed, where);
            if (!expected.equals(actual))
                failures.add(String.format("[%s] WHERE %s: expected %s but got %s", source, where, expected, actual));
        }
    }

    private String select(String table, String where)
    {
        String query = String.format("SELECT pk FROM %s.%s WHERE %s ALLOW FILTERING", KEYSPACE, table, where);
        UntypedResultSet rows = executeFormattedQuery(query);
        List<Integer> keys = new ArrayList<>();
        for (UntypedResultSet.Row row : rows)
            keys.add(row.getInt("pk"));
        keys.sort(Integer::compareTo);
        return keys.toString();
    }
}
