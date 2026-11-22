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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.utils.memory.NativePool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TrieMemtableStage2AccountingTest extends CQLTester
{
    // Overrides CQLTester.setUpClass to run before it
    @BeforeClass
    public static void setUpClass()
    {
        MemtableSizeTestBase.setup(Config.MemtableAllocationType.offheap_objects, Murmur3Partitioner.instance);
        assertTrue(AbstractAllocatorMemtable.MEMORY_POOL instanceof NativePool);
    }

    /**
     * A row marker that a write adds to an existing row is owned on-heap as when the row is written with it at once.
     */
    @Test
    public void testRowMarkerAddedToExistingRow()
    {
        assertSameOnHeap("UPDATE %s USING TIMESTAMP 1000 SET v = 1 WHERE pk = 'test' AND ck = ?",
                         "INSERT INTO %s (pk, ck, v) VALUES ('test', ?, 1) USING TIMESTAMP 1000");
    }

    /**
     * A row deletion that a write adds to an existing row is owned on-heap as when the row is written with it at once.
     */
    @Test
    public void testRowDeletionAddedToExistingRow()
    {
        assertSameOnHeap("UPDATE %s USING TIMESTAMP 1000 SET v = 1 WHERE pk = 'test' AND ck = ?",
                         "DELETE FROM %s USING TIMESTAMP 500 WHERE pk = 'test' AND ck = ?");
    }

    /**
     * Applies the two writes to each row of one table one after the other, and to each row of another table in one
     * batch, and checks that both tables own the same on-heap memory.
     */
    private void assertSameOnHeap(String first, String second)
    {
        String merged = createTable("CREATE TABLE %s (pk text, ck int, v int, PRIMARY KEY (pk, ck)) WITH memtable = 'trie_stage2'");
        String batched = createTable("CREATE TABLE %s (pk text, ck int, v int, PRIMARY KEY (pk, ck)) WITH memtable = 'trie_stage2'");
        ColumnFamilyStore cfsMerged = getColumnFamilyStore(KEYSPACE, merged);
        ColumnFamilyStore cfsBatched = getColumnFamilyStore(KEYSPACE, batched);
        Util.flush(cfsMerged);
        Util.flush(cfsBatched);
        Memtable memtableMerged = cfsMerged.getCurrentMemtable();
        Memtable memtableBatched = cfsBatched.getCurrentMemtable();

        String mergedTable = KEYSPACE + '.' + merged;
        String batchedTable = KEYSPACE + '.' + batched;
        for (int ck = 0; ck < 50; ck++)
        {
            execute(String.format(first, mergedTable), ck);
            execute(String.format(second, mergedTable), ck);
            execute("BEGIN UNLOGGED BATCH " + String.format(first, batchedTable) + "; " + String.format(second, batchedTable) + "; APPLY BATCH", ck, ck);
        }

        assertSame(memtableMerged, cfsMerged.getCurrentMemtable());
        assertSame(memtableBatched, cfsBatched.getCurrentMemtable());
        assertEquals(Memtable.getMemoryUsage(memtableBatched).ownsOnHeap, Memtable.getMemoryUsage(memtableMerged).ownsOnHeap);
    }
}
