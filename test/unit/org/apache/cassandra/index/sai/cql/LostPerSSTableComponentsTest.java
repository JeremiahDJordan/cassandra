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
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndexBuilder;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The per-SSTable components of an SSTable are lost while its indexes keep their per-index components: the build that
 * writes per-SSTable components again at the next start numbers the rows anew, and the per-index components of the
 * other indexes, which may point to the wrong rows, are rebuilt rather than read.
 */
public class LostPerSSTableComponentsTest extends SAITester.Versioned.WithImmutableComponents
{
    private static final int PARTITIONS = 24;

    @After
    public void tearDown()
    {
        StorageAttachedIndexBuilder.NUMBER_EMPTY_STATIC_ROWS = false;
    }

    @Test
    public void testLostComponentsOfAnEarlierRelease() throws Throwable
    {
        checkLostComponents(true, false);
    }

    @Test
    public void testLostSparseComponentsOfAnEarlierRelease() throws Throwable
    {
        checkLostComponents(true, true);
    }

    @Test
    public void testLostComponentsWithRowsOfADroppedColumn() throws Throwable
    {
        checkLostComponents(false, false);
    }

    @Test
    public void testLostSparseComponentsWithRowsOfADroppedColumn() throws Throwable
    {
        checkLostComponents(false, true);
    }

    /**
     * @param earlierRelease whether the lost components numbered the empty static rows (of most partitions, which
     *                       have no static row), as earlier releases did; otherwise, rows whose only cells belong to a
     *                       column dropped since
     * @param sparse whether v and w are only set on the rows of the first quarter of the partitions (in token order)
     */
    private void checkLostComponents(boolean earlierRelease, boolean sparse) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, st int static, f int, v int, w int, d int, PRIMARY KEY (pk, ck))");
        disableCompaction();
        if (!earlierRelease)
        {
            createIndex("CREATE CUSTOM INDEX v_idx ON %s(v) USING 'StorageAttachedIndex'");
            createIndex("CREATE CUSTOM INDEX w_idx ON %s(w) USING 'StorageAttachedIndex'");
        }
        for (int pk = 0; pk < PARTITIONS; pk++)
        {
            if (earlierRelease)
            {
                for (int ck = 0; ck < 3; ck++)
                    execute("INSERT INTO %s (pk, ck, f) VALUES (?, ?, 0)", pk, ck);
                // most partitions have no static row
                if (pk % 4 == 0)
                    execute("INSERT INTO %s (pk, st) VALUES (?, ?)", pk, pk);
            }
            else
            {
                execute("INSERT INTO %s (pk, ck, f) VALUES (?, 0, 0)", pk);
                execute("UPDATE %s SET d = 1 WHERE pk = ? AND ck = 1", pk);
                execute("INSERT INTO %s (pk, ck, f) VALUES (?, 2, 0)", pk);
            }
        }
        List<Integer> partitions = new ArrayList<>();
        for (UntypedResultSet.Row row : execute("SELECT DISTINCT pk FROM %s"))
            partitions.add(row.getInt("pk"));
        int indexed = sparse ? PARTITIONS / 4 : PARTITIONS;
        int[] clusterings = earlierRelease ? new int[]{ 0, 1, 2 } : new int[]{ 0, 2 };
        for (int i = 0; i < indexed; i++)
        {
            int pk = partitions.get(i);
            for (int ck : clusterings)
                execute("UPDATE %s SET v = ?, w = ? WHERE pk = ? AND ck = ?", pk * 10 + ck, pk, pk, ck);
        }
        flush();
        if (earlierRelease)
        {
            StorageAttachedIndexBuilder.NUMBER_EMPTY_STATIC_ROWS = true;
            try
            {
                createIndex("CREATE CUSTOM INDEX v_idx ON %s(v) USING 'StorageAttachedIndex'");
                createIndex("CREATE CUSTOM INDEX w_idx ON %s(w) USING 'StorageAttachedIndex'");
            }
            finally
            {
                StorageAttachedIndexBuilder.NUMBER_EMPTY_STATIC_ROWS = false;
            }
        }
        else
        {
            schemaChange(String.format("ALTER TABLE %s.%s DROP d", KEYSPACE, currentTable()));
        }
        assertQueries(partitions, indexed, clusterings);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        for (SSTableReader sstable : cfs.getLiveSSTables())
            group.descriptorFor(sstable).perSSTableComponents().invalidate(sstable, cfs.getTracker());

        restart();
        assertQueries(partitions, indexed, clusterings);
        // and once loaded from what that start wrote
        restart();
        assertQueries(partitions, indexed, clusterings);
    }

    /**
     * Simulates a restart, and waits for the initialization tasks of the indexes, whatever their outcome: they aren't
     * marked as building, so waitForIndexBuilds wouldn't wait for them.
     */
    private void restart() throws Exception
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.indexManager.listIndexes().forEach(index -> ((StorageAttachedIndexGroup) cfs.indexManager.getIndexGroup(index)).reset());
        List<Future<?>> tasks = new ArrayList<>();
        cfs.indexManager.listIndexes().forEach(index -> tasks.add(cfs.indexManager.buildIndex(index)));
        cfs.indexManager.executePreJoinTasksBlocking(true);
        for (Future<?> task : tasks)
        {
            try
            {
                task.get(180, TimeUnit.SECONDS);
            }
            catch (ExecutionException e)
            {
                // the index is then non-queryable, which assertQueries reports
            }
        }
    }

    private void assertQueries(List<Integer> partitions, int indexed, int[] clusterings)
    {
        assertTrue("v_idx must be queryable", isIndexQueryable(KEYSPACE, "v_idx"));
        assertTrue("w_idx must be queryable", isIndexQueryable(KEYSPACE, "w_idx"));
        for (int i = 0; i < partitions.size(); i++)
        {
            int pk = partitions.get(i);
            Set<String> all = new TreeSet<>();
            for (int ck : clusterings)
            {
                Set<String> expected = i < indexed ? Set.of(pk + ":" + ck) : Set.of();
                assertEquals("v = " + (pk * 10 + ck), expected, rows(execute("SELECT pk, ck FROM %s WHERE v = ?", pk * 10 + ck)));
                all.addAll(expected);
            }
            assertEquals("w = " + pk, all, rows(execute("SELECT pk, ck FROM %s WHERE w = ?", pk)));
        }
    }

    private static Set<String> rows(UntypedResultSet result)
    {
        Set<String> rows = new TreeSet<>();
        for (UntypedResultSet.Row row : result)
            rows.add(row.getInt("pk") + ":" + row.getInt("ck"));
        return rows;
    }
}
