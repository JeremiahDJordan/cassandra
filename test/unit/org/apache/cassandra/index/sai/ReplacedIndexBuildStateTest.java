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

package org.apache.cassandra.index.sai;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The build state of an index that was dropped and created again with the same name, while builds of the dropped
 * index run.
 */
public class ReplacedIndexBuildStateTest extends SAITester
{
    private static final int ROWS = 10;

    @After
    public void cleanUp()
    {
        Injections.deleteAll();
    }

    /**
     * An index dropped and recreated with the same name while the initial build of the dropped one runs: the end of
     * that build doesn't make the new index look idle while its own initial build runs.
     */
    @Test
    public void testIndexRecreatedWhileDroppedIndexBuilds() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        writeRows(0);

        // Hold the initial build of the first index once it has started building its SSTable, and drop the index...
        Injections.Barrier holdDroppedBuild = barrier("hold_dropped_index_build",
                                                      InvokePointBuilder.newInvokePoint()
                                                                        .onClass(StorageAttachedIndexBuilder.class)
                                                                        .onMethod("indexSSTable"));
        Injections.inject(holdDroppedBuild);
        String index = createIndexAsync("CREATE CUSTOM INDEX recreated_idx ON %s(v) USING 'StorageAttachedIndex'");
        waitForAssert(() -> assertThat(holdDroppedBuild.getCount()).isEqualTo(1));
        dropIndex("DROP INDEX %s." + index);

        // ...recreate it (its initialization task queues behind the one of the dropped index)...
        Injections.Barrier holdNewBuild = barrier("hold_recreated_index_build",
                                                  InvokePointBuilder.newInvokePoint()
                                                                    .onClass(StorageAttachedIndexBuilder.class)
                                                                    .onMethod("build"));
        Injections.inject(holdNewBuild);
        createIndexAsync("CREATE CUSTOM INDEX recreated_idx ON %s(v) USING 'StorageAttachedIndex'");
        assertThat(cfs.indexManager.isIndexBuilding(index)).isTrue();

        // ...let the build of the dropped index end, and hold the initial build of the new index once it has selected
        // its SSTables: the new index is still being built.
        holdDroppedBuild.countDown();
        holdDroppedBuild.disable();
        waitForAssert(() -> assertThat(holdNewBuild.getCount()).isEqualTo(1), 60, TimeUnit.SECONDS);
        assertThat(cfs.indexManager.isIndexBuilding(index)).describedAs("new index building after the dropped index's build ended").isTrue();

        holdNewBuild.countDown();
        holdNewBuild.disable();
        waitForAssert(() -> assertThat(cfs.indexManager.isIndexBuilding(index)).isFalse(), 60, TimeUnit.SECONDS);

        assertThat(cfs.indexManager.isIndexQueryable(index)).isTrue();
        assertThat(rowsThroughV()).isEqualTo(ROWS);
    }

    /**
     * A build of an index that has been dropped and replaced by another one with the same name must not leave the
     * replacement looking as building (or a rebuild of it would be rejected as concurrent with that build).
     */
    @Test
    public void testBuildOfReplacedIndexDoesNotLeaveReplacementBuilding() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        writeRows(0);
        String index = createIndex("CREATE CUSTOM INDEX replaced_idx ON %s(v) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        Index replaced = cfs.indexManager.getIndexByName(index);
        dropIndex("DROP INDEX %s." + index);
        createIndex("CREATE CUSTOM INDEX replaced_idx ON %s(v) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        assertThat(cfs.indexManager.getIndexByName(index)).isNotSameAs(replaced);

        for (boolean isFullRebuild : new boolean[]{ false, true })
        {
            try
            {
                cfs.indexManager.buildIndexesBlocking(cfs.getLiveSSTables(), Collections.singleton(replaced), isFullRebuild);
            }
            catch (Throwable t)
            {
                // the build of a dropped index may fail: only the state of its replacement matters here
            }
            assertThat(cfs.indexManager.isIndexBuilding(index)).isFalse();
        }

        cfs.indexManager.rebuildIndexesBlocking(Collections.singleton(index));
        assertThat(cfs.indexManager.isIndexQueryable(index)).isTrue();
        assertThat(rowsThroughV()).isEqualTo(ROWS);
    }

    private void writeRows(int firstKey)
    {
        for (int k = firstKey; k < firstKey + ROWS; k++)
            execute("INSERT INTO %s (k, v, w) VALUES (?, ?, ?)", k, k % 2, k % 2);
        flush();
    }

    private int rowsThroughV()
    {
        return execute("SELECT k FROM %s WHERE v = 0").size() + execute("SELECT k FROM %s WHERE v = 1").size();
    }

    private static Injections.Barrier barrier(String name, InvokePointBuilder at)
    {
        return Injections.newBarrier(name, 2, false).add(at).build();
    }
}
