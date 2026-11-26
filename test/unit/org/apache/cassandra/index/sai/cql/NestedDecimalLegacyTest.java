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
 * The index of a frozen value holding a decimal, written before such values were canonicalized, holds the values as
 * written: queries find the values written the same way until a compaction or rebuild writes the canonical form.
 * <p>
 * Test data in test/data/legacy-sai/ec-nested-decimal was written by a flush at version EC by the code before the
 * change: a table
 * {@code (pk int, ck int, s set<frozen<tuple<decimal, int>>>, t frozen<tuple<decimal, int>>, PRIMARY KEY (pk, ck))},
 * indexes {@code s_idx} on {@code s} and {@code t_idx} on {@code t}, and the rows (0, 0, {(2.5, 1)}, (2.5, 1)),
 * (1, 0, {(2.50, 1)}, (2.50, 1)) and (2, 0, {(7, 1)}, (7, 1)).
 */
public class NestedDecimalLegacyTest extends SAITester
{
    @Test
    public void testLegacyIndexThenCompaction() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, s set<frozen<tuple<decimal, int>>>, " +
                    "t frozen<tuple<decimal, int>>, PRIMARY KEY (pk, ck))");
        createIndex("CREATE CUSTOM INDEX s_idx ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX t_idx ON %s(t) USING 'StorageAttachedIndex'");
        disableCompaction();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        FileUtils.copySSTablesAndIndexes(cfs.getDirectories().getDirectoryForNewSSTables().toJavaIOFile(),
                                         "ec-nested-decimal");
        cfs.loadNewSSTables();
        waitForTableIndexesQueryable();
        assertEquals(Set.of(Version.EC), getSSTableIndexVersions(KEYSPACE, currentTable()));

        // The legacy index finds the values queried as they were written: (2.5, 1) finds the row written as
        // (2.5, 1) (the query's bytes) and the one written as (2.50, 1) (the canonical form).
        assertKeys("SELECT pk FROM %s WHERE s CONTAINS (2.5, 1)", 0, 1);
        assertKeys("SELECT pk FROM %s WHERE t = (2.5, 1)", 0, 1);
        assertKeys("SELECT pk FROM %s WHERE s CONTAINS (7, 1)", 2);
        assertKeys("SELECT pk FROM %s WHERE t = (7, 1)", 2);

        // A compaction writes the index in canonical form: every scale finds every row.
        compact();
        for (String value : new String[]{ "2.5", "2.50", "2.500" })
        {
            assertKeys("SELECT pk FROM %s WHERE s CONTAINS (" + value + ", 1)", 0, 1);
            assertKeys("SELECT pk FROM %s WHERE t = (" + value + ", 1)", 0, 1);
        }
        assertKeys("SELECT pk FROM %s WHERE t = (7.0, 1)", 2);
    }
}
