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

package org.apache.cassandra.db.memtable;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;

/**
 * Decimal clustering keys, set elements, map keys and legacy index values with a scale of Integer.MAX_VALUE, read
 * from the memtable and after a flush.
 */
@RunWith(Parameterized.class)
public class DecimalScaleLimitMemtableTest extends CQLTester
{
    private static final BigDecimal[] VALUES = { new BigDecimal("-5E-2147483647"), new BigDecimal("1E-2147483647"), new BigDecimal("1.25") };

    @Parameterized.Parameter
    public String memtable;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object> parameters()
    {
        // "default" means no memtable option on the table, i.e. TrieMemtable
        return Arrays.asList("default", "TrieMemtable", "TrieMemtableStage1", "SkipListMemtable");
    }

    private String withMemtable(String createTable)
    {
        if ("default".equals(memtable))
            return createTable;
        return createTable + " WITH memtable = {'class': '" + memtable + "'}";
    }

    @Test
    public void testClusteringAndCellPaths() throws Throwable
    {
        createTable(withMemtable("CREATE TABLE %s (pk int, ck decimal, v int, s set<decimal>, m map<decimal, int>, PRIMARY KEY (pk, ck))"));
        disableCompaction();
        for (int i = 0; i < VALUES.length; i++)
            execute("INSERT INTO %s (pk, ck, v, s, m) VALUES (0, ?, ?, ?, ?)", VALUES[i], i, set(VALUES[i]), map(VALUES[i], i));

        assertClusteringAndCellPaths();
        flush();
        assertClusteringAndCellPaths();
    }

    private void assertClusteringAndCellPaths() throws Throwable
    {
        assertRows(execute("SELECT * FROM %s WHERE pk = 0"),
                   row(0, VALUES[0], map(VALUES[0], 0), set(VALUES[0]), 0),
                   row(0, VALUES[1], map(VALUES[1], 1), set(VALUES[1]), 1),
                   row(0, VALUES[2], map(VALUES[2], 2), set(VALUES[2]), 2));
        for (int i = 0; i < VALUES.length; i++)
            assertRows(execute("SELECT v FROM %s WHERE pk = 0 AND ck = ?", VALUES[i]), row(i));
    }

    @Test
    public void testRangeDeletion() throws Throwable
    {
        createTable(withMemtable("CREATE TABLE %s (pk int, ck decimal, v int, PRIMARY KEY (pk, ck))"));
        disableCompaction();
        for (int i = 0; i < VALUES.length; i++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (0, ?, ?)", VALUES[i], i);
        execute("DELETE FROM %s WHERE pk = 0 AND ck >= ? AND ck <= ?", VALUES[1], VALUES[1]);

        assertRows(execute("SELECT * FROM %s WHERE pk = 0"), row(0, VALUES[0], 0), row(0, VALUES[2], 2));
        flush();
        assertRows(execute("SELECT * FROM %s WHERE pk = 0"), row(0, VALUES[0], 0), row(0, VALUES[2], 2));
    }

    @Test
    public void testLegacyIndex() throws Throwable
    {
        createTable(withMemtable("CREATE TABLE %s (k int PRIMARY KEY, v decimal)"));
        createIndex("CREATE INDEX ON %s (v)");
        disableCompaction();
        for (int i = 0; i < VALUES.length; i++)
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, VALUES[i]);

        assertLegacyIndexQueries();
        flush();
        assertLegacyIndexQueries();
    }

    private void assertLegacyIndexQueries() throws Throwable
    {
        for (int i = 0; i < VALUES.length; i++)
            assertRows(execute("SELECT k FROM %s WHERE v = ?", VALUES[i]), row(i));
    }
}
