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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.exceptions.QueryReferencesTooManyIndexesAbortException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;

/**
 * Tests the SSTable indexes the {@code sai_sstable_indexes_per_query} guardrail counts: the highest number of SSTable
 * indexes that a single expression searches (CASSANDRA-20243). Every SSTable index whose key range overlaps the query
 * is searched.
 */
public class SSTableIndexesPerQueryGuardrailTest extends SAITester
{
    private static final int SSTABLES = 3;

    private int warnThreshold;
    private int failThreshold;

    @Before
    public void setup() throws Throwable
    {
        warnThreshold = DatabaseDescriptor.getSaiSSTableIndexesPerQueryWarnThreshold();
        failThreshold = DatabaseDescriptor.getSaiSSTableIndexesPerQueryFailThreshold();

        createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
        disableCompaction();
    }

    @After
    public void restoreThresholds()
    {
        DatabaseDescriptor.getGuardrailsConfig().setSaiSSTableIndexesPerQueryThreshold(warnThreshold, failThreshold);
    }

    /**
     * Each of the two indexed columns has an index on each of the SSTables, so the query references twice as many
     * SSTable indexes as there are SSTables, but each column search only touches one per SSTable.
     */
    @Test
    public void testCountsIndexesPerExpression() throws Throwable
    {
        for (int i = 0; i < SSTABLES; i++)
        {
            execute("INSERT INTO %s (k, c, a, b) VALUES (?, 0, 0, 0)", i);
            flush();
        }

        setFailThreshold(SSTABLES);
        assertRowCount(execute("SELECT * FROM %s WHERE a = 0 AND b = 0"), SSTABLES);
        assertRowCount(execute("SELECT * FROM %s WHERE a = 0 OR b = 0"), SSTABLES);

        setFailThreshold(SSTABLES - 1);
        assertFails("SELECT * FROM %s WHERE a = 0 AND b = 0", SSTABLES);
        assertFails("SELECT * FROM %s WHERE a = 0", SSTABLES);
    }

    /**
     * The query searches every SSTable index in the key range, including those whose terms can't match the expression,
     * so they all count.
     */
    @Test
    public void testCountsIndexesWithoutMatchingTerms() throws Throwable
    {
        for (int i = 0; i < SSTABLES; i++)
        {
            execute("INSERT INTO %s (k, c, a, b) VALUES (?, 0, 0, 0)", i);
            flush();
        }
        for (int i = 0; i < SSTABLES; i++)
        {
            execute("INSERT INTO %s (k, c, a, b) VALUES (?, 0, 1, 1)", SSTABLES + i);
            flush();
        }

        setFailThreshold(2 * SSTABLES);
        assertRowCount(execute("SELECT * FROM %s WHERE a = 0"), SSTABLES);

        setFailThreshold(2 * SSTABLES - 1);
        assertFails("SELECT * FROM %s WHERE a = 0", 2 * SSTABLES);
        assertFails("SELECT * FROM %s WHERE a = 5", 2 * SSTABLES);
    }

    /**
     * The expressions that order the results (ANN, BM25 and ORDER BY on an index) search every SSTable index in the
     * key range, so they count like the other expressions.
     */
    @Test
    public void testCountsOrderingIndexes() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, t text, vec vector<float, 2>)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(t) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex'");
        disableCompaction();

        for (int i = 0; i < SSTABLES; i++)
        {
            execute("INSERT INTO %s (k, v, t, vec) VALUES (?, ?, 'apple', ?)", i, i, vector((float) i + 1, 1f));
            flush();
        }

        String ann = "SELECT k FROM %s ORDER BY vec ANN OF [1, 1] LIMIT 2";
        String hybridAnn = "SELECT k FROM %s WHERE v = 0 ORDER BY vec ANN OF [1, 1] LIMIT 2";
        String bm25 = "SELECT k FROM %s ORDER BY t BM25 OF 'apple' LIMIT 2";
        String orderBy = "SELECT k FROM %s ORDER BY v LIMIT 2";

        setFailThreshold(SSTABLES);
        assertRowCount(execute(ann), 2);
        assertRowCount(execute(hybridAnn), 1);
        assertRowCount(execute(bm25), 2);
        assertRowCount(execute(orderBy), 2);

        setFailThreshold(SSTABLES - 1);
        assertFails(ann, SSTABLES);
        assertFails(hybridAnn, SSTABLES);
        assertFails(bm25, SSTABLES);
        assertFails(orderBy, SSTABLES);
    }

    /**
     * An SSTable index whose keys are outside the queried key range isn't searched, so it isn't counted.
     */
    @Test
    public void testIgnoresIndexesOutsideKeyRange() throws Throwable
    {
        for (int i = 0; i < SSTABLES; i++)
        {
            execute("INSERT INTO %s (k, c, a, b) VALUES (?, 0, 0, 0)", i);
            flush();
        }

        setFailThreshold(1);
        assertRowCount(execute("SELECT * FROM %s WHERE k = 0 AND a = 0"), 1);
        assertFails("SELECT * FROM %s WHERE a = 0", SSTABLES);
    }

    /**
     * A single partition query doesn't search the SSTable indexes whose SSTable bloom filter doesn't have the key, even
     * if the key is in the key range of the SSTable, so they aren't counted, also with several expressions.
     */
    @Test
    public void testIgnoresIndexesWithoutPartition() throws Throwable
    {
        for (int i = 0; i < SSTABLES; i++)
        {
            for (int k = 1; k <= 20; k++)
                execute("INSERT INTO %s (k, c, a, b) VALUES (?, 0, 0, 0)", k);
            if (i > 0)
                execute("INSERT INTO %s (k, c, a, b) VALUES (0, 0, 0, 0)");
            flush();
        }
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        DecoratedKey key = cfs.decorateKey(Int32Type.instance.decompose(0));
        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertTrue(sstable.first.compareTo(key) < 0 && sstable.last.compareTo(key) > 0);

        setFailThreshold(SSTABLES - 1);
        assertRows(execute("SELECT k, c FROM %s WHERE k = 0 AND a = 0"), row(0, 0));
        assertRows(execute("SELECT k, c FROM %s WHERE k = 0 AND a = 0 AND b = 0"), row(0, 0));
        assertRows(execute("SELECT k, c FROM %s WHERE k = 0 AND (a = 0 OR b = 0)"), row(0, 0));
        assertFails("SELECT * FROM %s WHERE a = 0", SSTABLES);

        setFailThreshold(SSTABLES - 2);
        assertFails("SELECT * FROM %s WHERE k = 0 AND a = 0", SSTABLES - 1);
        assertFails("SELECT * FROM %s WHERE k = 0 AND a = 0 AND b = 0", SSTABLES - 1);
    }

    private static void setFailThreshold(int fail)
    {
        DatabaseDescriptor.getGuardrailsConfig().setSaiSSTableIndexesPerQueryThreshold(-1, fail);
    }

    private void assertFails(String query, int referencedIndexes)
    {
        assertThatThrownBy(() -> execute(query)).isInstanceOf(QueryReferencesTooManyIndexesAbortException.class)
                                                 .hasMessageContaining("referenced " + referencedIndexes + " SSTable indexes");
    }
}
