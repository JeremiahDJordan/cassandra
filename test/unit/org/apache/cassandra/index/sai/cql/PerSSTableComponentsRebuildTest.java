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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.StorageAttachedIndexBuilder;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.TOCComponent;

import static org.apache.cassandra.config.CassandraRelevantProperties.IMMUTABLE_SAI_COMPONENTS;
import static org.apache.cassandra.config.CassandraRelevantProperties.IMMUTABLE_SAI_COMPONENTS_MIN_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Full rebuilds (nodetool rebuild_index) replace the per-SSTable components that all the SAI indexes of an SSTable
 * share: they must still repair damaged ones, must not leave the indexes they don't rebuild on components that are
 * gone, and a builder waiting for another build of the SSTable must still be stoppable.
 */
public class PerSSTableComponentsRebuildTest extends SAITester
{
    private static final AtomicInteger writerFailures = new AtomicInteger();

    // called by byteman
    @SuppressWarnings("unused")
    public static boolean failOnce()
    {
        return writerFailures.getAndDecrement() > 0;
    }

    private boolean defaultImmutableSetting;

    @Before
    public void saveImmutableSetting()
    {
        defaultImmutableSetting = IMMUTABLE_SAI_COMPONENTS.getBoolean();
    }

    @After
    public void cleanUp() throws Throwable
    {
        Injections.deleteAll();
        IMMUTABLE_SAI_COMPONENTS.setBoolean(defaultImmutableSetting);
    }

    /**
     * A full rebuild deletes and rewrites the per-SSTable components, which is how damaged ones (still marked complete,
     * as nothing has reloaded them) are repaired. It must not fail because it can't read them first.
     */
    @Test
    public void testRebuildRepairsDamagedPerSSTableComponents() throws Throwable
    {
        List<String> failures = new ArrayList<>();
        for (boolean staticColumn : new boolean[]{ false, true })
        {
            for (CorruptionType corruption : CorruptionType.values())
            {
                for (IndexComponentType component : Version.current(KEYSPACE).onDiskFormat().perSSTableComponentTypes(true))
                {
                    createTable(staticColumn ? "CREATE TABLE %s (pk int, ck int, st int static, v int, w int, PRIMARY KEY (pk, ck))"
                                             : "CREATE TABLE %s (pk int, ck int, v int, w int, PRIMARY KEY (pk, ck))");
                    disableCompaction();
                    String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
                    String w = createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
                    for (int pk = 0; pk < 10; pk++)
                        for (int ck = 0; ck < 3; ck++)
                            execute("INSERT INTO %s (pk, ck, v, w) VALUES (?, ?, ?, ?)", pk, ck, pk * 10 + ck, pk);
                    flush();
                    try
                    {
                        corruptIndexComponent(component, corruption);
                    }
                    catch (Throwable t)
                    {
                        // not a component of this sstable
                        continue;
                    }

                    String label = (staticColumn ? "static column, " : "") + corruption + ' ' + component;
                    try
                    {
                        ColumnFamilyStore.rebuildSecondaryIndex(KEYSPACE, currentTable(), v, w);
                        waitForIndexBuilds(KEYSPACE, v);
                        waitForIndexBuilds(KEYSPACE, w);
                        if (!isIndexQueryable(KEYSPACE, v) || !isIndexQueryable(KEYSPACE, w))
                        {
                            failures.add(label + ": not queryable");
                            continue;
                        }
                        for (int pk = 0; pk < 10; pk++)
                        {
                            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE v = ?", pk * 10 + 1), row(pk, 1));
                            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE w = ?", pk), row(pk, 0), row(pk, 1), row(pk, 2));
                        }
                    }
                    catch (Throwable t)
                    {
                        failures.add(label + ": " + t);
                    }
                }
            }
        }
        if (!failures.isEmpty())
            fail(failures.size() + " failures:\n" + String.join("\n", failures));
    }

    /**
     * When a full rebuild of some indexes fails because it numbered the rows differently than the per-SSTable
     * components it replaced (here forced by silencing the dropped-column trigger, which would otherwise make it
     * rebuild every index), the components it replaced are gone: the other indexes must not stay queryable on
     * per-index components whose numbering is gone, and must be right once rebuilt.
     */
    @Test
    public void testIndexesLeftByTheFullRebuildSafetyNet() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        for (Version version : new Version[]{ Version.DC, Version.LATEST })
        {
            SAIUtil.setCurrentVersion(version);
            createTable("CREATE TABLE %s (pk int, ck int, st int static, v int, w int, d int, PRIMARY KEY (pk, ck))");
            disableCompaction();
            String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
            String w = createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
            for (int pk = 0; pk < 12; pk++)
            {
                execute("INSERT INTO %s (pk, ck, v, w) VALUES (?, 0, ?, ?)", pk, pk * 10, pk);
                execute("UPDATE %s SET d = 1 WHERE pk = ? AND ck = 1", pk);
                execute("INSERT INTO %s (pk, ck, v, w) VALUES (?, 2, ?, ?)", pk, pk * 10 + 2, pk);
            }
            flush();
            schemaChange(String.format("ALTER TABLE %s.%s DROP d", KEYSPACE, currentTable()));

            ActionBuilder returnFalse = ActionBuilder.newActionBuilder();
            returnFalse.actions().doReturn("false");
            Injections.inject(Injections.newCustom("silence_dropped_column_trigger_" + UUID.randomUUID())
                                        .add(InvokePointBuilder.newInvokePoint()
                                                               .onClass(StorageAttachedIndexBuilder.class)
                                                               .onMethod("mayHaveDroppedColumnCells")
                                                               .atEntry())
                                        .add(returnFalse)
                                        .build());
            assertThatThrownBy(() -> rebuildIndexes(v)).hasStackTraceContaining("but the per-SSTable index components this build replaces had");
            waitForIndexBuilds(KEYSPACE, v);
            assertFalse("w is on per-index components whose per-SSTable components are gone", isIndexQueryable(KEYSPACE, w));

            // the obvious retry, then a restart, which builds w again
            rebuildIndexes(v);
            Injections.deleteAll();
            simulateNodeRestart();
            for (int pk = 0; pk < 12; pk++)
            {
                assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE w = ?", pk), row(pk, 0), row(pk, 2));
                assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE v = ?", pk * 10 + 2), row(pk, 2));
            }
        }
    }

    /**
     * A full rebuild of v that also rebuilds w, as a column was dropped since the SSTable was written, fails once it has
     * deleted the per-SSTable components and before its writer exists: w, which used them, becomes non-queryable too.
     */
    @Test
    public void testFailedRebuildOfDeletedComponentsMakesTheOtherIndexesNonQueryable() throws Throwable
    {
        testFailedRebuildOfReplacedComponents(false, false);
    }

    /**
     * The same with immutable components: the rebuild deletes none of the components w uses, which stays queryable.
     */
    @Test
    public void testFailedRebuildOfImmutableComponentsKeepsTheOtherIndexesQueryable() throws Throwable
    {
        testFailedRebuildOfReplacedComponents(true, false);
    }

    /**
     * The same with immutable components the rebuild can't read: w, which used them, becomes non-queryable.
     */
    @Test
    public void testFailedRebuildOfUnreadableImmutableComponentsMakesTheOtherIndexesNonQueryable() throws Throwable
    {
        testFailedRebuildOfReplacedComponents(true, true);
    }

    /**
     * The per-SSTable components of version ba, which has no immutable components, are deleted by a full rebuild of v
     * that fails as it creates its writer, while w keeps serving the SSTable. Once the SSTable is loaded again with
     * version dc and immutable components, which keep the components a rebuild replaces, a full rebuild of v finds the
     * per-SSTable components missing, rebuilds w with it, and fails once it has prepared them, deleting nothing; by the
     * end of the sequence w, which has no per-SSTable components to use, is non-queryable.
     */
    @Test
    public void testMissingSharedComponentsAfterReopeningSSTable() throws Throwable
    {
        Version previous = SAIUtil.currentVersion();
        String minImmutableVersion = IMMUTABLE_SAI_COMPONENTS_MIN_VERSION.getString();
        IMMUTABLE_SAI_COMPONENTS.setBoolean(true);
        IMMUTABLE_SAI_COMPONENTS_MIN_VERSION.setString("ca");
        SAIUtil.setCurrentVersion(Version.BA);
        try
        {
            createTable("CREATE TABLE %s (pk int, ck int, v int, w int, PRIMARY KEY (pk, ck))");
            disableCompaction();
            String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
            String w = createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
            for (int pk = 0; pk < 10; pk++)
                for (int ck = 0; ck < 3; ck++)
                    execute("INSERT INTO %s (pk, ck, v, w) VALUES (?, ?, ?, ?)", pk, ck, pk * 10 + ck, pk);
            flush();
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
            StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);

            injectWriterFailure();
            assertThatThrownBy(() -> rebuildIndexes(v)).hasStackTraceContaining("Injected writer failure");
            Injections.deleteAll();
            waitForIndexBuilds(KEYSPACE, v);
            assertTrue(isIndexQueryable(KEYSPACE, w));

            SAIUtil.setCurrentVersion(Version.DC);
            group.onSSTableChanged(Collections.singleton(sstable), Collections.emptyList(), group.getIndexes(), false);
            group.onSSTableChanged(Collections.emptyList(), Collections.singleton(sstable), group.getIndexes(), false);

            injectFailureOnce(InvokePointBuilder.newInvokePoint()
                                                .onClass(StorageAttachedIndexBuilder.class)
                                                .onMethod("prepareForRebuild")
                                                .atExit());
            assertThatThrownBy(() -> rebuildIndexes(v)).hasStackTraceContaining("Injected writer failure");
            Injections.deleteAll();
            waitForIndexBuilds(KEYSPACE, v);
            assertFalse(isIndexQueryable(KEYSPACE, w));
        }
        finally
        {
            SAIUtil.setCurrentVersion(previous);
            IMMUTABLE_SAI_COMPONENTS_MIN_VERSION.setString(minImmutableVersion);
        }
    }

    /**
     * A partial build of v over its complete components, which it deletes, fails as it creates its writer: v becomes
     * non-queryable, and w, whose files the build didn't touch, stays queryable.
     */
    @Test
    public void testFailedBuildOfDeletedPerIndexComponentsMakesOnlyThatIndexNonQueryable() throws Throwable
    {
        testFailedPartialBuildOfPerIndexComponents(false,
                                                   InvokePointBuilder.newInvokePoint()
                                                                     .onClass(StorageAttachedIndexWriter.class)
                                                                     .onMethod("<init>")
                                                                     .atEntry());
    }

    /**
     * The same, when the build fails as it rewrites the TOC to unlist the components of v.
     */
    @Test
    public void testFailedBuildAsItUnlistsPerIndexComponentsMakesOnlyThatIndexNonQueryable() throws Throwable
    {
        testFailedPartialBuildOfPerIndexComponents(false,
                                                   InvokePointBuilder.newInvokePoint()
                                                                     .onClass(TOCComponent.class)
                                                                     .onMethod("rewriteTOC")
                                                                     .atEntry());
    }

    /**
     * The same, when the TOC doesn't list the components of v, and the build fails once it has deleted them.
     */
    @Test
    public void testFailedBuildAsItDeletesUnlistedPerIndexComponentsMakesOnlyThatIndexNonQueryable() throws Throwable
    {
        testFailedPartialBuildOfPerIndexComponents(true,
                                                   InvokePointBuilder.newInvokePoint()
                                                                     .onClass(StorageAttachedIndexBuilder.class)
                                                                     .onMethod("prepareForRebuild")
                                                                     .atExit());
    }

    private void testFailedPartialBuildOfPerIndexComponents(boolean unlisted, InvokePointBuilder failurePoint) throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTable("CREATE TABLE %s (pk int, ck int, v int, w int, PRIMARY KEY (pk, ck))");
        disableCompaction();
        String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        String w = createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        for (int pk = 0; pk < 10; pk++)
            for (int ck = 0; ck < 3; ck++)
                execute("INSERT INTO %s (pk, ck, v, w) VALUES (?, ?, ?, ?)", pk, ck, pk * 10 + ck, pk);
        flush();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        if (unlisted)
            sstable.unregisterComponents(group.descriptorFor(sstable).perIndexComponents(getIndexContext(v)).allAsCustomComponents(), null);

        injectFailureOnce(failurePoint);
        assertThat(partialBuild(group, sstable, v)).hasStackTraceContaining("Injected writer failure");
        assertTrue(writerFailures.get() <= 0);
        Injections.deleteAll();
        assertFalse(isIndexQueryable(KEYSPACE, v));
        assertTrue(isIndexQueryable(KEYSPACE, w));
        for (int pk = 0; pk < 10; pk++)
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE w = ?", pk), row(pk, 0), row(pk, 1), row(pk, 2));
    }

    private static void injectWriterFailure() throws Throwable
    {
        injectFailureOnce(InvokePointBuilder.newInvokePoint()
                                            .onClass(StorageAttachedIndexWriter.class)
                                            .onMethod("<init>")
                                            .atEntry());
    }

    private static void injectFailureOnce(InvokePointBuilder failurePoint) throws Throwable
    {
        writerFailures.set(1);
        ActionBuilder throwOnce = ActionBuilder.newActionBuilder();
        throwOnce.conditions()
                 .when(PerSSTableComponentsRebuildTest.class.getName() + ".failOnce()")
                 .actions()
                 .doThrow(RuntimeException.class, Expression.quote("Injected writer failure"));
        Injections.inject(Injections.newCustom("fail_" + UUID.randomUUID())
                                    .add(failurePoint)
                                    .add(throwOnce)
                                    .build());
    }

    /**
     * @param unreadable whether the rebuild can't read the per-SSTable components it replaces
     */
    private void testFailedRebuildOfReplacedComponents(boolean immutable, boolean unreadable) throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(immutable);
        createTable("CREATE TABLE %s (pk int, ck int, v int, w int, d int, PRIMARY KEY (pk, ck))");
        disableCompaction();
        String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        String w = createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        for (int pk = 0; pk < 10; pk++)
            for (int ck = 0; ck < 3; ck++)
                execute("INSERT INTO %s (pk, ck, v, w, d) VALUES (?, ?, ?, ?, 1)", pk, ck, pk * 10 + ck, pk);
        flush();
        schemaChange(String.format("ALTER TABLE %s.%s DROP d", KEYSPACE, currentTable()));
        assertTrue(isIndexQueryable(KEYSPACE, w));

        if (unreadable)
        {
            ActionBuilder failRead = ActionBuilder.newActionBuilder();
            failRead.actions().doThrow(RuntimeException.class, Expression.quote("Injected read failure"));
            Injections.inject(Injections.newCustom("fail_read_" + UUID.randomUUID())
                                        .add(InvokePointBuilder.newInvokePoint()
                                                               .onClass(StorageAttachedIndexBuilder.class)
                                                               .onMethod("rowCount")
                                                               .atEntry())
                                        .add(failRead)
                                        .build());
        }
        injectWriterFailure();
        assertThatThrownBy(() -> rebuildIndexes(v)).hasStackTraceContaining("Injected writer failure");
        assertTrue(writerFailures.get() <= 0);
        Injections.deleteAll();
        waitForIndexBuilds(KEYSPACE, v);
        assertFalse(isIndexQueryable(KEYSPACE, v));
        boolean keepsW = immutable && !unreadable;
        assertEquals(keepsW, isIndexQueryable(KEYSPACE, w));
        if (keepsW)
        {
            for (int pk = 0; pk < 10; pk++)
                assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE w = ?", pk), row(pk, 0), row(pk, 1), row(pk, 2));
        }

        rebuildIndexes(v, w);
        for (int pk = 0; pk < 10; pk++)
        {
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE v = ?", pk * 10 + 1), row(pk, 1));
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE w = ?", pk), row(pk, 0), row(pk, 1), row(pk, 2));
        }
    }

    /**
     * A build waiting for the lock of an SSTable (here a CREATE INDEX, while a full rebuild of the SSTable holds it) must
     * stop when the index builds are stopped (nodetool stop INDEX_BUILD), not when the build holding the lock is done.
     */
    @Test
    public void testStopWhileWaitingForTheLock() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, st int static, v int, w int, PRIMARY KEY (pk, ck))");
        disableCompaction();
        String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        for (int pk = 0; pk < 20; pk++)
        {
            for (int ck = 0; ck < 3; ck++)
                execute("INSERT INTO %s (pk, ck, v, w) VALUES (?, ?, ?, ?)", pk, ck, pk * 10 + ck, pk);
            if (pk % 4 == 0)
                execute("INSERT INTO %s (pk, st) VALUES (?, ?)", pk, pk);
        }
        flush();

        // the full rebuild of v holds the lock for 30 seconds, without looking at stop requests
        long pauseMillis = TimeUnit.SECONDS.toMillis(30);
        Injections.inject(Injections.newPause("hold_the_lock_" + UUID.randomUUID(), pauseMillis)
                                    .add(InvokePointBuilder.newInvokePoint()
                                                           .onClass(StorageAttachedIndexBuilder.class)
                                                           .onMethod("numbersEmptyStaticRows")
                                                           .atEntry())
                                    .build());
        String keyspace = KEYSPACE;
        String table = currentTable();
        CompletableFuture<Void> rebuild = CompletableFuture.runAsync(() -> ColumnFamilyStore.rebuildSecondaryIndex(keyspace, table, v));
        Thread.sleep(2000);

        // CREATE INDEX w waits for the lock
        String w = createIndexAsync("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        Thread.sleep(2000);

        long stop = System.nanoTime();
        CompactionManager.instance.stopCompaction("INDEX_BUILD");
        waitForIndexBuilds(KEYSPACE, w);
        long waitedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stop);
        assertTrue("The build of w stopped after " + waitedMillis + " ms, with the lock still held", waitedMillis < pauseMillis / 2);
        assertFalse(isIndexQueryable(KEYSPACE, w));

        try
        {
            rebuild.get(2, TimeUnit.MINUTES);
        }
        catch (Throwable stopped)
        {
            // the rebuild of v was stopped too, once its pause was over
        }
        Injections.deleteAll();

        // both indexes can be built again
        rebuildIndexes(v, w);
        for (int pk = 0; pk < 20; pk++)
        {
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE v = ?", pk * 10 + 1), row(pk, 1));
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE w = ?", pk), row(pk, 0), row(pk, 1), row(pk, 2));
        }
    }
}
