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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.vector.VectorMemtableIndex;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

import static org.junit.Assert.assertEquals;

/**
 * A deletion older than a row's indexed values doesn't remove them from the SAI memtable index; a newer one does, and
 * the row isn't returned.
 */
@RunWith(Parameterized.class)
public class OlderDeletionKeepsIndexedValuesTest extends SAITester
{
    /** The deletions to check, each with %d for its timestamp. */
    private static final String[] DELETIONS = {
        "DELETE FROM %%s USING TIMESTAMP %d WHERE pk = 0 AND ck = 1",
        "DELETE FROM %%s USING TIMESTAMP %d WHERE pk = 0 AND ck >= 0 AND ck <= 2",
        "DELETE FROM %%s USING TIMESTAMP %d WHERE pk = 0",
        "DELETE vec, v, t FROM %%s USING TIMESTAMP %d WHERE pk = 0 AND ck = 1",
    };

    @Parameterized.Parameter
    public String memtable;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object> parameters()
    {
        return Arrays.asList("skiplist", "trie", "trie_stage1", "trie_stage2", "trie_stage3");
    }

    private String vectorIndex;

    private void createTableWithIndexes()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, t text, vec vector<float, 2>, svec vector<float, 2> static, " +
                    "PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
        disableCompaction();
        vectorIndex = createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(svec) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(t) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
    }

    @Test
    public void testOlderDeletion()
    {
        List<String> wrong = new ArrayList<>();
        for (String rowWrite : new String[]{ "UPDATE %s USING TIMESTAMP 1540 SET v = 1, t = 'apple pie', vec = [1.0, 2.0] WHERE pk = 0 AND ck = 1",
                                             "INSERT INTO %s (pk, ck, v, t, vec) VALUES (0, 1, 1, 'apple pie', [1.0, 2.0]) USING TIMESTAMP 1540" })
        {
            for (String deletion : DELETIONS)
            {
                createTableWithIndexes();
                execute(rowWrite);
                execute("UPDATE %s USING TIMESTAMP 1540 SET svec = [3.0, 4.0] WHERE pk = 0");
                execute(String.format(deletion, 1535));

                String context = memtable + ": " + rowWrite + "; " + describe(deletion);
                if (!keyUnderVector(1f, 2f))
                    wrong.add(context + ": the memtable index lost the key from its vector");
                assertRows(execute("SELECT ck, v, t, vec, svec FROM %s WHERE pk = 0"), row(1, 1, "apple pie", vector(1f, 2f), vector(3f, 4f)));
                for (String source : new String[]{ "memtable", "sstable" })
                {
                    if (source.equals("sstable"))
                        flush();
                    String c = context + ", " + source;
                    check(wrong, c, List.of(1), "SELECT ck FROM %s ORDER BY vec ANN OF [1.0, 1.0] LIMIT 10");
                    check(wrong, c, List.of(1), "SELECT ck FROM %s WHERE pk = 0 ORDER BY vec ANN OF [1.0, 1.0] LIMIT 10");
                    check(wrong, c, List.of(1), "SELECT ck FROM %s ORDER BY svec ANN OF [3.0, 3.0] LIMIT 10");
                    check(wrong, c, List.of(1), "SELECT ck FROM %s WHERE v = 1");
                    check(wrong, c, List.of(1), "SELECT ck FROM %s WHERE t : 'apple'");
                    check(wrong, c, List.of(1), "SELECT ck FROM %s ORDER BY t BM25 OF 'apple' LIMIT 10");
                }
            }
        }
        assertEquals(List.of(), wrong);
    }

    @Test
    public void testNewerDeletion()
    {
        List<String> wrong = new ArrayList<>();
        for (String rowWrite : new String[]{ "UPDATE %s USING TIMESTAMP 1540 SET v = 1, t = 'apple pie', vec = [1.0, 2.0] WHERE pk = 0 AND ck = 1",
                                             "INSERT INTO %s (pk, ck, v, t, vec) VALUES (0, 1, 1, 'apple pie', [1.0, 2.0]) USING TIMESTAMP 1540" })
        {
            for (String deletion : DELETIONS)
            {
                createTableWithIndexes();
                execute(rowWrite);
                execute(String.format(deletion, 1545));
                // another row, so that ANN has something to return
                execute("INSERT INTO %s (pk, ck, v, t, vec) VALUES (1, 2, 2, 'cherry', [5.0, 5.0]) USING TIMESTAMP 1550");

                String context = memtable + ": " + rowWrite + "; " + describe(deletion);
                // TrieMemtable and TrieMemtableStage3 remove what a deletion shadows, and so must their vector index;
                // the other memtables keep stale entries, which the queries and the flush filter out
                if ((memtable.equals("trie") || memtable.equals("trie_stage3")) && keyUnderVector(1f, 2f))
                    wrong.add(context + ": the memtable index still has the key under the deleted vector");
                for (String source : new String[]{ "memtable", "sstable" })
                {
                    if (source.equals("sstable"))
                        flush();
                    String c = context + ", " + source;
                    check(wrong, c, List.of(2), "SELECT ck FROM %s ORDER BY vec ANN OF [1.0, 1.0] LIMIT 10");
                    check(wrong, c, List.of(), "SELECT ck FROM %s WHERE v = 1");
                    check(wrong, c, List.of(), "SELECT ck FROM %s WHERE t : 'apple'");
                    check(wrong, c, List.of(), "SELECT ck FROM %s ORDER BY t BM25 OF 'apple' LIMIT 10");
                }
            }
        }
        assertEquals(List.of(), wrong);
    }

    /**
     * Another table shape: a composite partition key, two clustering columns (one descending), a static vector. The
     * lookup the vector index makes builds a trie key from the clustering and the column.
     */
    @Test
    public void testOlderDeletionOtherShape()
    {
        List<String> wrong = new ArrayList<>();
        String[] deletions = {
            "DELETE FROM %%s USING TIMESTAMP %d WHERE pk1 = 0 AND pk2 = 'a' AND c1 = 1 AND c2 = 'x'",
            "DELETE FROM %%s USING TIMESTAMP %d WHERE pk1 = 0 AND pk2 = 'a' AND c1 = 1 AND c2 >= 'a' AND c2 <= 'z'",
            "DELETE FROM %%s USING TIMESTAMP %d WHERE pk1 = 0 AND pk2 = 'a' AND c1 >= 0 AND c1 <= 2",
            "DELETE FROM %%s USING TIMESTAMP %d WHERE pk1 = 0 AND pk2 = 'a'",
        };
        for (String deletion : deletions)
        {
            createTable("CREATE TABLE %s (pk1 int, pk2 text, c1 int, c2 text, v int, vec vector<float, 2>, svec vector<float, 2> static, " +
                        "PRIMARY KEY ((pk1, pk2), c1, c2)) WITH CLUSTERING ORDER BY (c1 DESC, c2 ASC) AND memtable = '" + memtable + "'");
            disableCompaction();
            createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
            createIndex("CREATE CUSTOM INDEX ON %s(svec) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
            createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
            execute("UPDATE %s USING TIMESTAMP 1540 SET v = 1, vec = [1.0, 2.0] WHERE pk1 = 0 AND pk2 = 'a' AND c1 = 1 AND c2 = 'x'");
            execute("UPDATE %s USING TIMESTAMP 1540 SET svec = [3.0, 4.0] WHERE pk1 = 0 AND pk2 = 'a'");
            execute(String.format(deletion, 1535));

            String context = memtable + ": " + describe(deletion);
            assertRows(execute("SELECT c1, c2, v, vec, svec FROM %s WHERE pk1 = 0 AND pk2 = 'a'"), row(1, "x", 1, vector(1f, 2f), vector(3f, 4f)));
            for (String source : new String[]{ "memtable", "sstable" })
            {
                if (source.equals("sstable"))
                    flush();
                String c = context + ", " + source;
                checkC2(wrong, c, "SELECT c2 FROM %s ORDER BY vec ANN OF [1.0, 1.0] LIMIT 10");
                checkC2(wrong, c, "SELECT c2 FROM %s WHERE pk1 = 0 AND pk2 = 'a' ORDER BY vec ANN OF [1.0, 1.0] LIMIT 10");
                checkC2(wrong, c, "SELECT c2 FROM %s ORDER BY svec ANN OF [3.0, 3.0] LIMIT 10");
                checkC2(wrong, c, "SELECT c2 FROM %s WHERE v = 1");
            }
        }
        assertEquals(List.of(), wrong);
    }

    private void checkC2(List<String> wrong, String context, String query)
    {
        List<String> actual = new ArrayList<>();
        for (UntypedResultSet.Row row : execute(query))
            actual.add(row.getString("c2"));
        if (!actual.equals(List.of("x")))
            wrong.add(context + ": " + query + " -> " + actual + ", expected [x]");
    }

    private boolean keyUnderVector(float x, float y)
    {
        IndexContext context = getIndexContext(vectorIndex);
        Memtable current = getCurrentColumnFamilyStore().getCurrentMemtable();
        VectorMemtableIndex memtableIndex = (VectorMemtableIndex) context.getLiveMemtables().get(current);
        PrimaryKey key = context.keyFactory().create(Util.dk(Int32Type.instance.decompose(0)), Clustering.make(Int32Type.instance.decompose(1)));
        ByteBuffer vector = VectorType.getInstance(FloatType.instance, 2).decompose(List.of(x, y));
        return memtableIndex.getGraph().keysOf(vector).contains(key);
    }

    private void check(List<String> wrong, String context, List<Integer> expected, String query)
    {
        List<Integer> actual = cks(query);
        if (!expected.equals(actual))
            wrong.add(context + ": " + query + " -> " + actual + ", expected " + expected);
    }

    private List<Integer> cks(String query)
    {
        List<Integer> cks = new ArrayList<>();
        for (UntypedResultSet.Row row : execute(query))
            cks.add(row.getInt("ck"));
        return cks;
    }

    private static String describe(String deletion)
    {
        return deletion.replace("%%s", "t").replace("%d", "<timestamp>");
    }
}
