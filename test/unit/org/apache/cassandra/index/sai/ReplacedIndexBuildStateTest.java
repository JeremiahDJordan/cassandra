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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
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
        Injections.Hold.releaseAll();
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
        Injections.Hold droppedIndexBuildHold = Injections.newHold("dropped_index_build")
                                                          .add(InvokePointBuilder.newInvokePoint()
                                                                                 .onClass(StorageAttachedIndexBuilder.class)
                                                                                 .onMethod("indexSSTable"))
                                                          .build();
        Injections.inject(droppedIndexBuildHold);
        try
        {
            String index = createIndexAsync("CREATE CUSTOM INDEX recreated_idx ON %s(v) USING 'StorageAttachedIndex'");
            droppedIndexBuildHold.awaitHeld();
            dropIndex("DROP INDEX %s." + index);

            // ...recreate it (its initialization task queues behind the one of the dropped index)...
            Injections.Hold recreatedIndexBuildHold = Injections.newHold("recreated_index_build")
                                                                .add(InvokePointBuilder.newInvokePoint()
                                                                                       .onClass(StorageAttachedIndexBuilder.class)
                                                                                       .onMethod("build"))
                                                                .build();
            Injections.inject(recreatedIndexBuildHold);
            createIndexAsync("CREATE CUSTOM INDEX recreated_idx ON %s(v) USING 'StorageAttachedIndex'");
            assertThat(cfs.indexManager.isIndexBuilding(index)).isTrue();

            // ...let the build of the dropped index end, and hold the initial build of the new index once it has
            // selected its SSTables: the new index is still being built.
            droppedIndexBuildHold.release();
            recreatedIndexBuildHold.awaitHeld();
            assertThat(cfs.indexManager.isIndexBuilding(index)).describedAs("new index building after the dropped index's build ended").isTrue();

            recreatedIndexBuildHold.release();
            waitForAssert(() -> assertThat(cfs.indexManager.isIndexBuilding(index)).isFalse(), 60, TimeUnit.SECONDS);

            assertThat(cfs.indexManager.isIndexQueryable(index)).isTrue();
            assertThat(rowsThroughV()).isEqualTo(ROWS);
        }
        finally
        {
            Injections.Hold.releaseAll();
        }
    }

    /**
     * A build of an index that has been dropped and replaced by another one with the same name does nothing, and doesn't
     * leave the replacement looking as building (or a rebuild of it would be rejected as concurrent with that build).
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

        Injections.Counter builds = builderCounter("replaced_index_builds");
        Injections.inject(builds);
        for (boolean isFullRebuild : new boolean[]{ false, true })
        {
            cfs.indexManager.buildIndexesBlocking(cfs.getLiveSSTables(), Collections.singleton(replaced), isFullRebuild);
            assertThat(builds.get()).isZero();
            assertThat(cfs.indexManager.isIndexBuilding(index)).isFalse();
            assertThat(cfs.indexManager.needsFullRebuild(index)).isFalse();
        }

        cfs.indexManager.rebuildIndexesBlocking(Collections.singleton(index));
        assertThat(cfs.indexManager.isIndexQueryable(index)).isTrue();
        assertThat(rowsThroughV()).isEqualTo(ROWS);
    }

    /**
     * A build of the only SAI index of the table, which was dropped before the build marks it as building: the build
     * does nothing, and the index created again with the same name is built.
     */
    @Test
    public void testBuildOfDroppedLastIndexDoesNotLeaveNameBuilding() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        writeRows(0);
        String index = createIndex("CREATE CUSTOM INDEX dropped_idx ON %s(v) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        Index dropped = cfs.indexManager.getIndexByName(index);
        dropIndex("DROP INDEX %s." + index);

        Injections.Counter builds = builderCounter("dropped_index_builds");
        Injections.inject(builds);
        cfs.indexManager.buildIndexesBlocking(cfs.getLiveSSTables(), Collections.singleton(dropped), true);
        assertThat(builds.get()).isZero();
        assertThat(cfs.indexManager.isIndexBuilding(index)).isFalse();
        assertThat(cfs.indexManager.needsFullRebuild(index)).isFalse();

        createIndex("CREATE CUSTOM INDEX dropped_idx ON %s(v) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        assertThat(cfs.indexManager.isIndexBuilding(index)).isFalse();
        cfs.indexManager.rebuildIndexesBlocking(Collections.singleton(index));
        assertThat(cfs.indexManager.isIndexQueryable(index)).isTrue();
        assertThat(rowsThroughV()).isEqualTo(ROWS);
    }

    /**
     * A full rebuild of two indexes takes them before the first one is dropped and created again with the same name,
     * and marks them as building while the new index builds: the rebuild builds the other index, and the new index is
     * built.
     */
    @Test
    public void testRebuildOfDroppedAndOtherIndexBuildsOtherIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        writeRows(0);
        createIndex("CREATE CUSTOM INDEX recreated_idx ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX other_idx ON %s(w) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        // Hold a full rebuild of both indexes before it marks them as building...
        Injections.Counter builds = builderCounter("rebuild_with_replacement_builds");
        Injections.inject(builds);
        Injections.Hold rebuildHold = Injections.newHold("rebuild")
                                                .add(InvokePointBuilder.newInvokePoint()
                                                                       .onClass(SecondaryIndexManager.class)
                                                                       .onMethod("buildIndexesBlocking")
                                                                       .atEntry())
                                                .build();
        Injections.inject(rebuildHold);
        try
        {
            CompletableFuture<Void> rebuild = CompletableFuture.runAsync(() -> rebuildIndexes("recreated_idx", "other_idx"));
            rebuildHold.awaitHeld();

            // ...drop the first index and create it again, holding its initial build...
            dropIndex("DROP INDEX %s.recreated_idx");
            Injections.Hold replacementInitialBuildHold = Injections.newHold("replacement_initial_build")
                                                                    .add(InvokePointBuilder.newInvokePoint()
                                                                                           .onClass(StorageAttachedIndex.class)
                                                                                           .onMethod("findNonIndexedSSTables"))
                                                                    .build();
            Injections.inject(replacementInitialBuildHold);
            createIndexAsync("CREATE CUSTOM INDEX recreated_idx ON %s(v) USING 'StorageAttachedIndex'");
            replacementInitialBuildHold.awaitHeld();

            // ...and let the rebuild go on, then the initial build.
            assertThat(rebuild.isDone()).isFalse();
            rebuildHold.release();
            rebuild.get(60, TimeUnit.SECONDS);
            assertThat(builds.get()).isPositive();
            assertThat(cfs.indexManager.isIndexBuilding("recreated_idx")).isTrue();
            replacementInitialBuildHold.release();
            waitForAssert(() -> assertThat(cfs.indexManager.isIndexBuilding("recreated_idx")).isFalse(), 60, TimeUnit.SECONDS);

            for (String index : new String[]{ "recreated_idx", "other_idx" })
            {
                assertThat(cfs.indexManager.isIndexBuilding(index)).isFalse();
                assertThat(cfs.indexManager.needsFullRebuild(index)).isFalse();
                assertThat(cfs.indexManager.isIndexQueryable(index)).isTrue();
            }
            assertThat(rowsThroughV()).isEqualTo(ROWS);
            assertThat(execute("SELECT k FROM %s WHERE w = 0").size() + execute("SELECT k FROM %s WHERE w = 1").size()).isEqualTo(ROWS);
        }
        finally
        {
            Injections.Hold.releaseAll();
        }
    }

    /**
     * A full rebuild of two indexes takes them before the first one is dropped: the rebuild builds the other index.
     */
    @Test
    public void testRebuildOfDroppedAndOtherIndexWithoutReplacementBuildsOtherIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        writeRows(0);
        createIndex("CREATE CUSTOM INDEX dropped_idx ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX other_idx ON %s(w) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        Injections.Counter builds = builderCounter("rebuild_without_replacement_builds");
        Injections.inject(builds);
        Injections.Hold rebuildHold = Injections.newHold("rebuild")
                                                .add(InvokePointBuilder.newInvokePoint()
                                                                       .onClass(SecondaryIndexManager.class)
                                                                       .onMethod("buildIndexesBlocking")
                                                                       .atEntry())
                                                .build();
        Injections.inject(rebuildHold);
        try
        {
            CompletableFuture<Void> rebuild = CompletableFuture.runAsync(() -> rebuildIndexes("dropped_idx", "other_idx"));
            rebuildHold.awaitHeld();
            dropIndex("DROP INDEX %s.dropped_idx");
            assertThat(rebuild.isDone()).isFalse();
            rebuildHold.release();
            rebuild.get(60, TimeUnit.SECONDS);
            assertThat(builds.get()).isPositive();

            for (String index : new String[]{ "dropped_idx", "other_idx" })
            {
                assertThat(cfs.indexManager.isIndexBuilding(index)).isFalse();
                assertThat(cfs.indexManager.needsFullRebuild(index)).isFalse();
            }
            assertThat(cfs.indexManager.isIndexQueryable("other_idx")).isTrue();
            assertThat(execute("SELECT k FROM %s WHERE w = 0").size() + execute("SELECT k FROM %s WHERE w = 1").size()).isEqualTo(ROWS);
        }
        finally
        {
            Injections.Hold.releaseAll();
        }
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

    private static Injections.Counter builderCounter(String name)
    {
        return Injections.newCounter(name)
                         .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.class).onMethod("build"))
                         .build();
    }
}
