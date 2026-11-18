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

import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.FileUtils;
import org.apache.cassandra.index.sai.disk.format.Version;

import static org.junit.Assert.assertEquals;

/**
 * The index of map entries with decimal keys or values, in an SSTable written at version AA before the decimals were
 * canonicalized, holds the entries as written: queries find the entries written the same way until a compaction or
 * rebuild writes the canonical form.
 * <p>
 * Test data in test/data/legacy-sai/aa-decimal-map-entries was written by a flush at version AA by the code before the
 * change: a table {@code (pk int, ck int, k map<decimal, int>, w map<int, decimal>, PRIMARY KEY (pk, ck))},
 * indexes {@code k_idx} on {@code ENTRIES(k)} and {@code w_idx} on {@code ENTRIES(w)}, and the rows
 * (0, 0, {2.5: 1}, {1: 2.5}), (1, 0, {2.50: 1}, {1: 2.50}), (2, 0, {-1.5: 1}, {1: -1.5}), (3, 0, {7: 1}, {1: 7}) and
 * (4, 0, {1.25: 1}, {1: 1.25}). Its data holds the map keys with the scale the trie memtable gave them (2.5 as 2.50),
 * its index holds them as written.
 */
public class MapEntriesDecimalLegacyTest extends SAITester
{
    @Test
    public void testLegacyIndexThenCompaction() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, k map<decimal, int>, w map<int, decimal>, PRIMARY KEY (pk, ck))");
        createIndex("CREATE CUSTOM INDEX k_idx ON %s(ENTRIES(k)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX w_idx ON %s(ENTRIES(w)) USING 'StorageAttachedIndex'");
        disableCompaction();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        FileUtils.copySSTablesAndIndexes(cfs.getDirectories().getDirectoryForNewSSTables().toJavaIOFile(),
                                         "aa-decimal-map-entries");
        cfs.loadNewSSTables();
        waitForTableIndexesQueryable();
        assertEquals(Set.of(Version.AA), getSSTableIndexVersions(KEYSPACE, currentTable()));

        // the legacy index finds both the entry written as 2.5 and the one written as 2.50
        assertKeys("SELECT pk FROM %s WHERE k[2.5] = 1", 0, 1);
        assertKeys("SELECT pk FROM %s WHERE k[-1.5] = 1", 2);
        assertKeys("SELECT pk FROM %s WHERE k[7] = 1", 3);
        assertKeys("SELECT pk FROM %s WHERE k[1.25] = 1", 4);
        assertKeys("SELECT pk FROM %s WHERE k[2.5] > 0", 0, 1);
        assertKeys("SELECT pk FROM %s WHERE k[-1.5] <= 1", 2);
        assertKeys("SELECT pk FROM %s WHERE w[1] = 2.5", 0, 1);
        assertKeys("SELECT pk FROM %s WHERE w[1] = -1.5", 2);
        assertKeys("SELECT pk FROM %s WHERE w[1] = 1.25", 4);
        assertKeys("SELECT pk FROM %s WHERE w[1] > 2.5", 3);
        // the inequality is a full index scan before version DB: it doesn't depend on how the entries were written
        assertKeys("SELECT pk FROM %s WHERE k[2.5] != 1", 2, 3, 4);
        assertKeys("SELECT pk FROM %s WHERE k[7] != 1", 0, 1, 2, 4);
        assertKeys("SELECT pk FROM %s WHERE k[-1.5] != 1", 0, 1, 3, 4);
        assertKeys("SELECT pk FROM %s WHERE w[1] != 2.5", 2, 3, 4);

        // A compaction writes the index with the current version, in canonical form: every scale finds every entry.
        enableCompaction();
        compact();
        assertKeys("SELECT pk FROM %s WHERE k[2.5] = 1", 0, 1);
        assertKeys("SELECT pk FROM %s WHERE k[2.50] = 1", 0, 1);
        assertKeys("SELECT pk FROM %s WHERE k[2.500] = 1", 0, 1);
        assertKeys("SELECT pk FROM %s WHERE k[-1.50] = 1", 2);
        assertKeys("SELECT pk FROM %s WHERE k[7.0] = 1", 3);
        assertKeys("SELECT pk FROM %s WHERE k[2.50] > 0", 0, 1);
        assertKeys("SELECT pk FROM %s WHERE w[1] = 2.50", 0, 1);
        assertKeys("SELECT pk FROM %s WHERE w[1] = 7.00", 3);
        assertKeys("SELECT pk FROM %s WHERE w[1] >= 2.500", 0, 1, 3);
        assertKeys("SELECT pk FROM %s WHERE k[2.50] != 1", 2, 3, 4);
        assertKeys("SELECT pk FROM %s WHERE w[1] != 2.50", 2, 3, 4);
    }
}
