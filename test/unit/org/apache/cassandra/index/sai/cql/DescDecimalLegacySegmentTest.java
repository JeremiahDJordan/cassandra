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
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.disk.FileUtils;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

/**
 * SAI indexes on a DESC decimal clustering column flushed before CASSANDRA-20100 hold reversed terms, which can't be
 * searched since decimals are indexed unreversed: they are rebuilt rather than read.
 * <p>
 * Test data in test/data/legacy-sai/desc-decimal was written by a flush at index version EC by the code before
 * CASSANDRA-20100 was applied: a table {@code (p int, c decimal, v int, PRIMARY KEY (p, c)) WITH CLUSTERING ORDER BY
 * (c DESC)}, an index {@code c_idx} on {@code c}, and rows (0, -1.1, 1), (0, 2.1, 2), (1, 2.9, 3) and
 * (1, 400000000000000000000.5, 4).
 */
public class DescDecimalLegacySegmentTest extends SAITester
{
    private static final String TABLE = "CREATE TABLE %s (p int, c decimal, v int, PRIMARY KEY (p, c)) WITH CLUSTERING ORDER BY (c DESC)";
    private static final String TABLE_WITH_STATIC = "CREATE TABLE %s (p int, c decimal, s int static, v int, PRIMARY KEY (p, c)) " +
                                                    "WITH CLUSTERING ORDER BY (c DESC)";
    private static final String INDEX = "CREATE CUSTOM INDEX c_idx ON %s(c) USING 'StorageAttachedIndex'";

    /**
     * Importing the SSTable rebuilds its obsolete index before the SSTable is added.
     */
    @Test
    public void testImportRebuilds() throws Throwable
    {
        createTable(TABLE);
        createIndex(INDEX);
        disableCompaction();

        copyFixture();
        getCurrentColumnFamilyStore().loadNewSSTables();
        waitForTableIndexesQueryable();

        assertQueries();
        assertRebuilt();
    }

    /**
     * Creating the index on an SSTable that still has the obsolete components of an index of the same name rebuilds them.
     */
    @Test
    public void testInitialBuildRebuilds() throws Throwable
    {
        createTable(TABLE);
        disableCompaction();
        copyFixture();
        getCurrentColumnFamilyStore().loadNewSSTables();
        assertThat(getCurrentColumnFamilyStore().getLiveSSTables()).hasSize(1);

        createIndex(INDEX);
        waitForTableIndexesQueryable();

        assertQueries();
        assertRebuilt();
    }

    /**
     * An SSTable added without building its indexes (as on entire-SSTable streaming) has its obsolete index refused
     * rather than read: the index stops being queryable, and the next initial build (a restart) rebuilds it.
     */
    @Test
    public void testObsoleteIndexIsRefusedThenRebuiltOnRestart() throws Throwable
    {
        createTable(TABLE);
        createIndex(INDEX);
        disableCompaction();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        copyFixture();
        File dataFile = cfs.getDirectories().getDirectoryForNewSSTables().tryList(f -> f.name().endsWith("-Data.db"))[0];
        SSTableReader reader = SSTableReader.open(cfs, Descriptor.fromFile(dataFile));
        cfs.addSSTable(reader);

        assertThat(getIndexContext("c_idx").getView().getIndexes()).isEmpty();
        assertThatThrownBy(() -> execute("SELECT v FROM %s WHERE c = 2.1")).hasMessageContaining("INDEX_NOT_AVAILABLE");

        simulateNodeRestart();

        assertQueries();
        assertRebuilt();
    }

    /**
     * The index of an sstable without any row with a value for the column (only a static row) is empty: it isn't
     * mistaken for an obsolete one, in the flushed sstable or after a restart.
     */
    @Test
    public void testEmptyIndexWithStaticRowOnly() throws Throwable
    {
        createTable(TABLE_WITH_STATIC);
        createIndex(INDEX);
        disableCompaction();

        execute("INSERT INTO %s (p, c, v) VALUES (0, 2.1, 2)");
        flush();
        execute("INSERT INTO %s (p, s) VALUES (1, 1)");
        flush();
        assertValues("SELECT v FROM %s WHERE c = 2.1", 2);

        simulateNodeRestart();
        assertValues("SELECT v FROM %s WHERE c = 2.1", 2);
        assertThat(getIndexContext("c_idx").getView().getIndexes()).hasSize(2);
    }

    /**
     * Same as {@link #testEmptyIndexWithStaticRowOnly()}, with an sstable holding only a partition deletion.
     */
    @Test
    public void testEmptyIndexWithPartitionDeletionOnly() throws Throwable
    {
        createTable(TABLE_WITH_STATIC);
        createIndex(INDEX);
        disableCompaction();

        execute("INSERT INTO %s (p, c, v) VALUES (0, 2.1, 2)");
        flush();
        execute("DELETE FROM %s WHERE p = 7");
        flush();
        assertValues("SELECT v FROM %s WHERE c = 2.1", 2);

        simulateNodeRestart();
        assertValues("SELECT v FROM %s WHERE c = 2.1", 2);
        assertThat(getIndexContext("c_idx").getView().getIndexes()).hasSize(2);
    }

    private void copyFixture() throws Exception
    {
        FileUtils.copySSTablesAndIndexes(getCurrentColumnFamilyStore().getDirectories().getDirectoryForNewSSTables().toJavaIOFile(), "desc-decimal");
    }

    private void assertRebuilt()
    {
        List<SSTableIndex> indexes = new ArrayList<>(getIndexContext("c_idx").getView().getIndexes());
        assertEquals(1, indexes.size());
        assertEquals(TypeUtil.DECIMAL_APPROXIMATION_BYTES, indexes.get(0).minTerm().remaining());
        assertEquals(TypeUtil.DECIMAL_APPROXIMATION_BYTES, indexes.get(0).maxTerm().remaining());
    }

    private void assertQueries()
    {
        assertValues("SELECT v FROM %s WHERE c = 2.1", 2);
        assertValues("SELECT v FROM %s WHERE c >= 2.1 AND c <= 2.9", 2, 3);
        assertValues("SELECT v FROM %s WHERE c > 2.5", 3, 4);
        assertValues("SELECT v FROM %s WHERE c < 2.5", 1, 2);
        assertValues("SELECT v FROM %s WHERE c >= -1.1 AND c <= 400000000000000000000.5", 1, 2, 3, 4);
    }

    private void assertValues(String query, Integer... expected)
    {
        List<Integer> actual = new ArrayList<>();
        for (UntypedResultSet.Row row : execute(query))
            actual.add(row.getInt("v"));
        Collections.sort(actual);
        assertEquals(query, List.of(expected), actual);
    }
}
