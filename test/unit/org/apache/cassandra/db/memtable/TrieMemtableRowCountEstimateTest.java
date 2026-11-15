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

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link Memtable#estimateRowCount} divides {@link Memtable#getLiveDataSize()} by
 * {@link Memtable#getEstimatedAverageRowSize()}, so both must measure a row the same way. {@link TrieMemtable} does not
 * count keys (clustering, cell paths) in its live data size, but reads rows back as {@code BTreeRow}s whose
 * {@code dataSize()} does. The SAI query planner and the EstimatedRowCount table metric use this estimate.
 */
public class TrieMemtableRowCountEstimateTest extends CQLTester
{
    private static final int PARTITIONS = 10;
    private static final int ROWS_PER_PARTITION = 10;
    private static final int ROWS = PARTITIONS * ROWS_PER_PARTITION;

    @Test
    public void testTextClusteringAndCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck text, v int, m map<text, int>, s set<int>, PRIMARY KEY (pk, ck)) " +
                    "WITH memtable = 'trie'");
        disableCompaction();

        for (int pk = 0; pk < PARTITIONS; pk++)
        {
            for (int r = 0; r < ROWS_PER_PARTITION; r++)
            {
                // A 64-byte clustering key and 32-byte map keys: neither is counted in the trie's live data size.
                // Setting whole collections also writes a complex deletion for each of them.
                String ck = String.format("%064d", r);
                execute("INSERT INTO %s (pk, ck, v, m, s) VALUES (?, ?, ?, ?, ?)",
                        pk, ck, r,
                        map(String.format("%032d", 1), 1, String.format("%032d", 2), 2, String.format("%032d", 3), 3),
                        set(1, 2, 3));
            }
        }

        assertEstimatedRowCount();
    }

    @Test
    public void testCollectionAppends() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, m map<text, int>, PRIMARY KEY (pk, ck)) WITH memtable = 'trie'");
        disableCompaction();

        // UPDATEs: no row marker and no complex deletion, only map cells with 32-byte keys
        for (int pk = 0; pk < PARTITIONS; pk++)
            for (int ck = 0; ck < ROWS_PER_PARTITION; ck++)
                execute("UPDATE %s SET m = m + ? WHERE pk = ? AND ck = ?",
                        map(String.format("%032d", ck), ck, String.format("%032d", ck + 1), ck + 1), pk, ck);

        assertEstimatedRowCount();
    }

    private void assertEstimatedRowCount()
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Memtable memtable = cfs.getCurrentMemtable();
        assertThat(memtable).isInstanceOf(TrieMemtable.class);
        // allow for the average row size being truncated to whole bytes
        assertThat(Memtable.estimateRowCount(memtable)).isBetween((long) (ROWS * 0.95), (long) (ROWS * 1.05));
    }
}
