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
import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.memory.MemoryIndex;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.memory.TrieMemoryIndex;
import org.apache.cassandra.index.sai.memory.TrieMemtableIndex;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Checks the document lengths, the numbers of terms that BM25 scoring uses, of the rows of an analyzed index that are
 * updated in the memtable: they must be those of the new values.
 */
public class BM25UpdateTest extends SAITester
{
    private static final String ANALYZED_INDEX = "CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' " +
                                                 "WITH OPTIONS = {'index_analyzer': 'standard'}";

    @Test
    public void testDocLengths()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text, w int)");
        createIndex(ANALYZED_INDEX);

        execute("INSERT INTO %s (pk, v) VALUES (1, 'apple pie crust')");
        // from the empty value to a value
        execute("INSERT INTO %s (pk, v) VALUES (2, '')");
        execute("UPDATE %s SET v = 'apple pie crust' WHERE pk = 2");
        // from a longer value to a shorter one
        execute("INSERT INTO %s (pk, v) VALUES (3, 'x y z w q')");
        execute("UPDATE %s SET v = 'apple pie crust' WHERE pk = 3");
        // from a value to the empty value
        execute("INSERT INTO %s (pk, v) VALUES (4, 'apple pie crust')");
        execute("UPDATE %s SET v = '' WHERE pk = 4");
        // from no value to a value
        execute("INSERT INTO %s (pk, w) VALUES (5, 1)");
        execute("UPDATE %s SET v = 'apple pie crust' WHERE pk = 5");
        // a value, deleted: no document
        execute("INSERT INTO %s (pk, v) VALUES (6, 'apple pie crust')");
        execute("DELETE v FROM %s WHERE pk = 6");
        // from no value to the empty value
        execute("INSERT INTO %s (pk, w) VALUES (7, 1)");
        execute("UPDATE %s SET v = '' WHERE pk = 7");
        execute("INSERT INTO %s (pk, v) VALUES (8, '')");
        // from a shorter value to a longer one
        execute("INSERT INTO %s (pk, v) VALUES (9, 'apple')");
        execute("UPDATE %s SET v = 'apple pie crust and cream' WHERE pk = 9");

        Map<Integer, Integer> expected = new TreeMap<>();
        for (int pk : new int[]{ 1, 2, 3, 5 })
            expected.put(pk, 3);
        for (int pk : new int[]{ 4, 7, 8 })
            expected.put(pk, 0);
        expected.put(9, 5);
        assertEquals(expected, memtableDocLengths(getIndexContexts(KEYSPACE, currentTable()).iterator().next()));
    }

    @Test
    public void testRankingAfterUpdates()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex(ANALYZED_INDEX);

        // 'apple' once in each row: the shorter the document, the higher the score
        // 1 term, after an update from 8 terms
        execute("INSERT INTO %s (pk, v) VALUES (1, 'apple banana cherry date elder fig grape honeydew')");
        execute("UPDATE %s SET v = 'apple' WHERE pk = 1");
        // 2 terms
        execute("INSERT INTO %s (pk, v) VALUES (2, 'apple banana')");
        // 4 terms, after an update from the empty value
        execute("INSERT INTO %s (pk, v) VALUES (3, '')");
        execute("UPDATE %s SET v = 'apple banana cherry date' WHERE pk = 3");
        // 3 terms
        execute("INSERT INTO %s (pk, v) VALUES (4, 'apple banana cherry')");

        assertRanking("memtable");
        flush();
        assertRanking("sstable");
    }

    private void assertRanking(String source)
    {
        UntypedResultSet rows = execute("SELECT pk FROM %s ORDER BY v BM25 OF 'apple' LIMIT 10");
        List<Integer> order = new ArrayList<>();
        for (UntypedResultSet.Row row : rows)
            order.add(row.getInt("pk"));
        assertThat(order).as(source).containsExactly(1, 2, 4, 3);
    }

    private static Map<Integer, Integer> memtableDocLengths(IndexContext context)
    {
        Map<Integer, Integer> lengths = new TreeMap<>();
        for (MemtableIndex memtableIndex : context.getLiveMemtables().values())
            for (MemoryIndex memoryIndex : ((TrieMemtableIndex) memtableIndex).getRangeIndexes())
                ((TrieMemoryIndex) memoryIndex).getDocLengths()
                                               .forEach((pk, length) -> lengths.put(Int32Type.instance.compose(pk.partitionKey().getKey()), length));
        return lengths;
    }
}
