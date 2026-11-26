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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.memory.MemoryIndex;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.memory.TrieMemoryIndex;
import org.apache.cassandra.index.sai.memory.TrieMemtableIndex;

import static org.junit.Assert.assertEquals;

/**
 * A row deleted by a range or partition deletion and written again in the same memtable with other values: the
 * indexes must not keep its old values, which BM25 and NOT CONTAINS would trust without checking the row.
 */
@RunWith(Parameterized.class)
public class RowWrittenAgainAfterDeletionTest extends SAITester
{
    @Parameterized.Parameter
    public String memtable;

    @Parameterized.Parameter(1)
    public String deletion;

    @Parameterized.Parameters(name = "{0} {1}")
    public static Collection<Object[]> data()
    {
        return Arrays.stream(new String[]{ "trie", "trie_stage3", "skiplist" })
                     .flatMap(memtable -> Arrays.stream(new String[]{ "range", "partition" }).map(deletion -> new Object[]{ memtable, deletion }))
                     .collect(Collectors.toList());
    }

    @Test
    public void testQueriesAfterRewrite() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, st text static, v int, a text, tags set<text>, vec vector<float, 2>, " +
                    "PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(tags) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");
        createIndex("CREATE CUSTOM INDEX ON %s(st) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");
        disableCompaction();

        execute("INSERT INTO %s (pk, ck, v, a, tags, vec) VALUES (0, 1, 10, 'banana', {'banana'}, [5.0, 5.0])");
        execute("INSERT INTO %s (pk, ck, v, a, tags, vec) VALUES (1, 6, 40, 'apple pie', {'apple'}, [1.0, 1.0])");
        execute("INSERT INTO %s (pk, st) VALUES (1, 'apple static')");
        if (deletion.equals("range"))
            execute("DELETE FROM %s WHERE pk = 1 AND ck >= 0 AND ck < 9");
        else
            execute("DELETE FROM %s WHERE pk = 1");
        execute("INSERT INTO %s (pk, ck, v, a, tags, vec) VALUES (1, 6, 20, 'pear tart', {'pear'}, [9.0, 9.0])");
        if (deletion.equals("partition"))
            execute("INSERT INTO %s (pk, st) VALUES (1, 'pear static')");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk, ck FROM %s ORDER BY a BM25 OF 'apple' LIMIT 10"));
            assertRows(execute("SELECT pk, ck FROM %s WHERE v > 0 ORDER BY a BM25 OF 'apple' LIMIT 10"));
            assertRows(execute("SELECT pk, ck FROM %s ORDER BY a BM25 OF 'pear' LIMIT 10"), row(1, 6));
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE tags NOT CONTAINS 'apple'"), row(0, 1), row(1, 6));
            assertRows(execute("SELECT pk, ck FROM %s WHERE tags NOT CONTAINS 'pear'"), row(0, 1));
            assertRows(execute("SELECT pk, ck FROM %s WHERE tags CONTAINS 'apple'"));
            assertRows(execute("SELECT pk, ck FROM %s WHERE a : 'apple'"));
            assertRows(execute("SELECT pk, ck FROM %s WHERE v = 40"));
            assertRows(execute("SELECT pk, ck FROM %s WHERE v = 20"), row(1, 6));
            assertRows(execute("SELECT pk, ck FROM %s ORDER BY v ASC LIMIT 10"), row(0, 1), row(1, 6));
            assertRows(execute("SELECT pk, ck FROM %s ORDER BY vec ANN OF [1.0, 1.0] LIMIT 1"), row(0, 1));
            assertRows(execute("SELECT pk, ck FROM %s WHERE pk = 1 ORDER BY vec ANN OF [1.0, 1.0] LIMIT 10"), row(1, 6));
            if (deletion.equals("partition"))
            {
                assertRows(execute("SELECT pk, ck FROM %s WHERE st : 'apple'"));
                assertRows(execute("SELECT pk, ck FROM %s WHERE st : 'pear'"), row(1, 6));
            }
        });
    }

    /**
     * A row written again with the same text: the memtable index must count each of its terms once, not once for the
     * deleted row and once for the new one, and BM25 must rank the rows as it does on a table that never had the
     * deletion.
     */
    @Test
    public void testBM25AfterRewriteWithSameText() throws Throwable
    {
        String schema = "CREATE TABLE %s (pk int, ck int, a text, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'";
        String analyzed = "CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }";
        String oracle = createTable(schema);
        createIndex(analyzed);
        disableCompaction();
        String table = createTable(schema);
        String index = createIndex(analyzed);
        disableCompaction();

        String[] rows = { "INSERT INTO %s (pk, ck, a) VALUES (1, 1, 'apple pie')",
                          "INSERT INTO %s (pk, ck, a) VALUES (2, 1, 'apple apple pie tart')",
                          "INSERT INTO %s (pk, ck, a) VALUES (3, 1, 'cherry tart')",
                          "INSERT INTO %s (pk, ck, a) VALUES (6, 1, 'apple pie crust')" };
        for (String row : rows)
        {
            execute(String.format(row, KEYSPACE + '.' + oracle));
            execute(String.format(row, KEYSPACE + '.' + table));
        }
        execute(String.format(deletion.equals("range") ? "DELETE FROM %s WHERE pk = 6 AND ck >= 0 AND ck < 9" : "DELETE FROM %s WHERE pk = 6",
                              KEYSPACE + '.' + table));
        execute(String.format(rows[3], KEYSPACE + '.' + table));

        // each term of row 6 once in the memtable index
        IndexContext context = getIndexContext(KEYSPACE, table, index);
        List<Integer> frequencies = new ArrayList<>();
        for (MemtableIndex memtableIndex : context.getLiveMemtables().values())
            for (MemoryIndex memoryIndex : ((TrieMemtableIndex) memtableIndex).getRangeIndexes())
                ((TrieMemoryIndex) memoryIndex).iterator().forEachRemaining(term -> {
                    for (MemoryIndex.PkWithFrequency key : term.right)
                        if (Int32Type.instance.compose(key.pk.partitionKey().getKey()) == 6)
                            frequencies.add(key.frequency);
                });
        assertEquals(List.of(1, 1, 1), frequencies);

        flush(KEYSPACE, oracle);
        flush(KEYSPACE, table);
        for (String term : new String[]{ "apple", "pie", "crust", "tart" })
        {
            String query = "SELECT pk FROM %s ORDER BY a BM25 OF '" + term + "' LIMIT 10";
            assertEquals(term, pks(String.format(query, KEYSPACE + '.' + oracle)), pks(String.format(query, KEYSPACE + '.' + table)));
        }
    }

    /**
     * A write can remove data of a row and write nothing in its place while the row survives: a collection deletion
     * on a row without a row marker, or a range or partition deletion older than some of the row's cells. The row
     * must stay in the indexes on its clustering column and partition key component.
     */
    @Test
    public void testKeyColumnIndexesKeepRowsThatLoseData() throws Throwable
    {
        String[] collectionDeletions = { "UPDATE %s SET m = null WHERE pk1 = 0 AND pk2 = 0 AND ck = 1",
                                         "DELETE m FROM %s WHERE pk1 = 0 AND pk2 = 0 AND ck = 1" };
        for (String collectionDeletion : collectionDeletions)
        {
            createKeyColumnIndexedTable();
            execute("UPDATE %s SET v = 1, m = {1: 1} WHERE pk1 = 0 AND pk2 = 0 AND ck = 1");
            execute(collectionDeletion);
            beforeAndAfterFlush(() -> {
                assertRows(execute("SELECT pk1, pk2, ck, v FROM %s WHERE ck = 1"), row(0, 0, 1, 1));
                assertRows(execute("SELECT pk1, pk2, ck, v FROM %s WHERE pk2 = 0"), row(0, 0, 1, 1));
                assertRows(execute("SELECT pk1, pk2, ck, v FROM %s WHERE m CONTAINS 1"));
            });
        }

        // the deletion is older than n, so the row keeps n
        createKeyColumnIndexedTable();
        execute("INSERT INTO %s (pk1, pk2, ck, v) VALUES (0, 0, 1, 1) USING TIMESTAMP 10");
        execute("UPDATE %s USING TIMESTAMP 30 SET n = 3 WHERE pk1 = 0 AND pk2 = 0 AND ck = 1");
        if (deletion.equals("range"))
            execute("DELETE FROM %s USING TIMESTAMP 20 WHERE pk1 = 0 AND pk2 = 0 AND ck >= 0 AND ck <= 3");
        else
            execute("DELETE FROM %s USING TIMESTAMP 20 WHERE pk1 = 0 AND pk2 = 0");
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk1, pk2, ck, n FROM %s WHERE ck = 1"), row(0, 0, 1, 3));
            assertRows(execute("SELECT pk1, pk2, ck, n FROM %s WHERE pk2 = 0"), row(0, 0, 1, 3));
            assertRows(execute("SELECT pk1, pk2, ck, n FROM %s WHERE v = 1"));
        });
    }

    private void createKeyColumnIndexedTable()
    {
        createTable("CREATE TABLE %s (pk1 int, pk2 int, ck int, v int, n int, m map<int, int>, PRIMARY KEY ((pk1, pk2), ck)) " +
                    "WITH memtable = '" + memtable + "'");
        createIndex("CREATE CUSTOM INDEX ON %s(ck) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(pk2) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(values(m)) USING 'StorageAttachedIndex'");
        disableCompaction();
    }

    /**
     * A range or partition deletion older than some cells of a row removes its older cells, and the row survives with
     * the newer ones. The removed text must leave the index, so that BM25 doesn't find the row by it after the text is
     * written again.
     */
    @Test
    public void testBM25AfterDeletionThatTheRowSurvives() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, t text, n int, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
        createIndex("CREATE CUSTOM INDEX ON %s(t) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");
        disableCompaction();
        execute("INSERT INTO %s (pk, ck, t) VALUES (0, 1, 'cherry pie')");
        execute("INSERT INTO %s (pk, ck, t) VALUES (1, 1, 'apple pie') USING TIMESTAMP 10");
        execute("UPDATE %s USING TIMESTAMP 30 SET n = 3 WHERE pk = 1 AND ck = 1");
        if (deletion.equals("range"))
            execute("DELETE FROM %s USING TIMESTAMP 20 WHERE pk = 1 AND ck >= 0 AND ck <= 3");
        else
            execute("DELETE FROM %s USING TIMESTAMP 20 WHERE pk = 1");
        execute("UPDATE %s USING TIMESTAMP 40 SET t = 'pear tart' WHERE pk = 1 AND ck = 1");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk, ck FROM %s ORDER BY t BM25 OF 'apple' LIMIT 10"));
            assertRows(execute("SELECT pk, ck FROM %s ORDER BY t BM25 OF 'pear' LIMIT 10"), row(1, 1));
            assertRows(execute("SELECT pk, ck FROM %s WHERE t : 'pear'"), row(1, 1));
        });
    }

    private List<Integer> pks(String query)
    {
        List<Integer> pks = new ArrayList<>();
        for (UntypedResultSet.Row row : execute(query))
            pks.add(row.getInt("pk"));
        return pks;
    }
}
