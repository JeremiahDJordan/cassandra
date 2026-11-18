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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.vector.VectorMemtableIndex;
import org.apache.cassandra.index.sai.disk.vector.VectorPostings;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;

import static org.junit.Assert.assertEquals;

/**
 * An index on a vector in a partition key column, in an sstable whose postings have the key of a partition's static
 * row alone, or the keys of its rows alone (as older versions wrote them): those keys give the partition, its live
 * rows, or its static row once no row is live.
 */
public class PrimaryKeyVectorOldPostingsTest extends SAITester.Versioned
{
    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Arrays.stream(new Version[]{ Version.EC, Version.ED, Version.GA }).map(v -> new Object[]{ v }).collect(Collectors.toList());
    }

    // the postings of older versions: a row key and the key of the static row of its partition are the same key
    private final Injection oldPostings = Injections.newCustom("old_vector_postings")
                                                    .add(InvokePointBuilder.newInvokePoint().onClass(VectorPostings.class).onMethod("isSameKey").atEntry())
                                                    .add(ActionBuilder.newActionBuilder().actions().doReturn("$1.equals($2)"))
                                                    .build();

    @Before
    public void setUp() throws Throwable
    {
        Injections.inject(oldPostings);
        oldPostings.disable();
    }

    @After
    public void tearDown()
    {
        Injections.deleteAll();
    }

    @Test
    public void testIndexFlushedWithOldPostings() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 vector<float, 2>, c int, s int static, r int, PRIMARY KEY ((k1, k2), c))");
        String index = createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        disableCompaction();

        oldPostings.enable();
        execute("INSERT INTO %s (k1, k2, s) VALUES (1, [1, 0], 1)");
        execute("INSERT INTO %s (k1, k2, c, r) VALUES (1, [1, 0], 1, 1)");
        execute("INSERT INTO %s (k1, k2, c, r) VALUES (1, [1, 0], 2, 2)");
        execute("INSERT INTO %s (k1, k2, c, r) VALUES (2, [1, 0.1], 1, 1)");
        // the postings of partition 1's vector have the key of its static row alone
        IndexContext context = getIndexContext(index);
        Memtable memtable = getCurrentColumnFamilyStore().getCurrentMemtable();
        ByteBuffer vector = VectorType.getInstance(FloatType.instance, 2).decompose(List.of(1f, 0f));
        assertEquals(1, ((VectorMemtableIndex) context.getLiveMemtables().get(memtable)).getGraph().keysOf(vector).size());
        flush();
        oldPostings.disable();

        String select = "SELECT k1, c FROM %s ";
        check(execute(select + "ORDER BY k2 ANN OF [1, 0] LIMIT 10"), row(1, 1), row(1, 2), row(2, 1));
        assertEquals(1, execute(select + "ORDER BY k2 ANN OF [1, 0] LIMIT 1").one().getInt("k1"));
        check(execute(select + "ORDER BY k2 ANN OF [1, 0] LIMIT 2"), row(1, 1), row(1, 2));
        check(execute(select + "WHERE k1 = 1 AND k2 = [1, 0] ORDER BY k2 ANN OF [1, 0] LIMIT 10"), row(1, 1), row(1, 2));
        assertEquals(2, execute(select + "ORDER BY k2 ANN OF [1, 0] PER PARTITION LIMIT 1 LIMIT 10").size());

        // a row deleted since, and rows written since with the fixed postings
        execute("DELETE FROM %s WHERE k1 = 1 AND k2 = [1, 0] AND c = 1");
        execute("INSERT INTO %s (k1, k2, c, r) VALUES (1, [1, 0], 3, 3)");
        beforeAndAfterFlush(() -> {
            check(execute(select + "ORDER BY k2 ANN OF [1, 0] LIMIT 10"), row(1, 2), row(1, 3), row(2, 1));
            check(execute(select + "WHERE k1 = 1 AND k2 = [1, 0] ORDER BY k2 ANN OF [1, 0] LIMIT 10"), row(1, 2), row(1, 3));
        });

        compact();
        check(execute(select + "ORDER BY k2 ANN OF [1, 0] LIMIT 10"), row(1, 2), row(1, 3), row(2, 1));
    }

    /**
     * The rows written before the static row: the postings have the row keys alone, which give the static row once all
     * the rows are dead.
     */
    @Test
    public void testIndexFlushedWithOldPostingsRowsFirst() throws Throwable
    {
        for (String variant : new String[]{ "rows live", "rows deleted in the memtable", "rows deleted and flushed",
                                            "rows expired", "rows deleted and compacted" })
        {
            createTable("CREATE TABLE %s (k1 int, k2 vector<float, 2>, c int, s int static, r int, PRIMARY KEY ((k1, k2), c))");
            String index = createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
            disableCompaction();

            oldPostings.enable();
            String ttl = variant.equals("rows expired") ? " USING TTL 1" : "";
            execute("INSERT INTO %s (k1, k2, c, r) VALUES (1, [1, 0], 1, 1)" + ttl);
            execute("INSERT INTO %s (k1, k2, c, r) VALUES (1, [1, 0], 2, 2)" + ttl);
            execute("INSERT INTO %s (k1, k2, s) VALUES (1, [1, 0], 1)");
            execute("INSERT INTO %s (k1, k2, c, r) VALUES (2, [1, 0.1], 1, 1)");
            // the postings of partition 1's vector have the keys of its rows alone
            IndexContext context = getIndexContext(index);
            Memtable memtable = getCurrentColumnFamilyStore().getCurrentMemtable();
            ByteBuffer vector = VectorType.getInstance(FloatType.instance, 2).decompose(List.of(1f, 0f));
            assertEquals(2, ((VectorMemtableIndex) context.getLiveMemtables().get(memtable)).getGraph().keysOf(vector).size());
            flush();
            oldPostings.disable();

            String select = "SELECT k1, c FROM %s ";
            if (variant.equals("rows live"))
            {
                check(variant, execute(select + "ORDER BY k2 ANN OF [1, 0] LIMIT 10"), row(1, 1), row(1, 2), row(2, 1));
                check(variant, execute(select + "ORDER BY k2 ANN OF [1, 0] LIMIT 2"), row(1, 1), row(1, 2));
                check(variant, execute(select + "WHERE k1 = 1 AND k2 = [1, 0] ORDER BY k2 ANN OF [1, 0] LIMIT 10"), row(1, 1), row(1, 2));
                continue;
            }

            switch (variant)
            {
                case "rows expired":
                    Thread.sleep(2100);
                    break;
                default:
                    execute("DELETE FROM %s WHERE k1 = 1 AND k2 = [1, 0] AND c = 1");
                    execute("DELETE FROM %s WHERE k1 = 1 AND k2 = [1, 0] AND c = 2");
                    if (!variant.endsWith("memtable"))
                        flush();
                    if (variant.endsWith("compacted"))
                        compact();
            }
            // partition 1 is its static row alone
            check(variant, execute(select + "ORDER BY k2 ANN OF [1, 0] LIMIT 10"), row(1, null), row(2, 1));
            check(variant, execute(select + "ORDER BY k2 ANN OF [1, 0] LIMIT 1"), row(1, null));
            check(variant, execute(select + "WHERE k1 = 1 AND k2 = [1, 0] ORDER BY k2 ANN OF [1, 0] LIMIT 10"), row(1, null));
        }
    }

    /**
     * The key of a static row gives the rows of its partition, so one key can produce several rows: they must not make
     * the rows of a worse partition push the rows of a better one out of the limit.
     */
    @Test
    public void testStaticKeyGivingSeveralRows() throws Throwable
    {
        oldPostings.disable();
        // the result depends on the token order of the partitions, so try both orders of a few pairs
        for (int[] ids : new int[][]{ { 1, 2 }, { 2, 1 }, { 3, 4 }, { 4, 3 }, { 5, 6 }, { 6, 5 } })
        {
            createTable("CREATE TABLE %s (k1 int, k2 vector<float, 2>, c int, s int static, r int, PRIMARY KEY ((k1, k2), c))");
            createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
            disableCompaction();
            int b = ids[0], c = ids[1];
            // the nearest partition has a static row and two rows, the next one a static row alone, then one row
            execute("INSERT INTO %s (k1, k2, s) VALUES (9, [1, 0], 1)");
            execute("INSERT INTO %s (k1, k2, c, r) VALUES (9, [1, 0], 1, 1)");
            execute("INSERT INTO %s (k1, k2, c, r) VALUES (9, [1, 0], 2, 2)");
            execute("INSERT INTO %s (k1, k2, s) VALUES (?, [1, 0.1], 1)", b);
            execute("INSERT INTO %s (k1, k2, c, r) VALUES (?, [1, 0.2], 1, 1)", c);
            beforeAndAfterFlush(() -> check(execute("SELECT k1, c FROM %s ORDER BY k2 ANN OF [1, 0] LIMIT 3"),
                                            row(9, 1), row(9, 2), row(b, null)));
        }
    }

    private void check(UntypedResultSet result, Object[]... rows)
    {
        check("", result, rows);
    }

    private void check(String variant, UntypedResultSet result, Object[]... rows)
    {
        assertRowsIgnoringOrder(variant + " on " + version, result, rows);
    }
}
