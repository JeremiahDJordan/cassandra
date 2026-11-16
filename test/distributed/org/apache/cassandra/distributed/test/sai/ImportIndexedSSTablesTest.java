/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.test.sai;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.FieldValue;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexBuilder;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;
import org.awaitility.core.ConditionTimeoutException;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class ImportIndexedSSTablesTest extends TestBaseImpl
{
    private static final String INJECTED_FAILURE = "Injected index build failure";

    private static Cluster cluster;

    @BeforeClass
    public static void startup() throws IOException
    {
        cluster = init(Cluster.build(2).withConfig(c -> c.with(NETWORK, GOSSIP))
                                       .withInstanceInitializer(ByteBuddyHelper::installErrors)
                                       .start());
        cluster.setUncaughtExceptionsFilter(t -> Throwables.anyCauseMatches(t, c -> INJECTED_FAILURE.equals(c.getMessage())));

        cluster.disableAutoCompaction(KEYSPACE);
    }

    @AfterClass
    public static void shutdown()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testIndexBuildingFailureDuringImport()
    {
        String table = "fail_during_import_test";
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int PRIMARY KEY, v text)", KEYSPACE, table));

        IInvokableInstance first = cluster.get(1);
        first.runOnInstance(()-> ByteBuddyHelper.failValidation = false);
        first.runOnInstance(()-> ByteBuddyHelper.interruptBuild = true);

        first.executeInternal(String.format("INSERT INTO %s.%s(pk, v) VALUES (?, ?)", KEYSPACE, table), 1, "v1");
        first.flush(KEYSPACE);

        Object[][] rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(1);

        first.runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).clearUnsafe());

        String indexName = table + "_v_index";
        cluster.schemaChange(String.format("CREATE INDEX %s ON %s.%s(v) USING 'sai'", indexName, KEYSPACE, table));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, indexName);

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(0);
        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(0);

        first.runOnInstance(() ->
                assertThatThrownBy(() ->
                        ColumnFamilyStore.loadNewSSTables(KEYSPACE, table)).hasRootCauseExactlyInstanceOf(CompactionInterruptedException.class));

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(0);
        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(0);
    }

    @Test
    public void testImportBuildsSSTableIndexes()
    {
        String table = "import_build_test";
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int PRIMARY KEY, v text)", KEYSPACE, table));

        IInvokableInstance first = cluster.get(1);
        first.runOnInstance(()-> ByteBuddyHelper.failValidation = false);
        first.runOnInstance(()-> ByteBuddyHelper.interruptBuild = false);

        first.executeInternal(String.format("INSERT INTO %s.%s(pk, v) VALUES (?, ?)", KEYSPACE, table), 1, "v1");
        first.flush(KEYSPACE);

        Object[][] rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(1);

        first.runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).clearUnsafe());

        String indexName = table + "_v_index";
        cluster.schemaChange(String.format("CREATE INDEX %s ON %s.%s(v) USING 'sai'", indexName, KEYSPACE, table));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, indexName);

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(0);
        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(0);

        first.runOnInstance(() -> ColumnFamilyStore.loadNewSSTables(KEYSPACE, table));

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(1);
        assertThat(rs[0][0]).isEqualTo(1);

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(1);
        assertThat(rs[0][0]).isEqualTo(1);
    }

    @Test
    public void testValidationFailureDuringImport()
    {
        String table = "validation_failure_test";
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int PRIMARY KEY, v text)", KEYSPACE, table));

        IInvokableInstance first = cluster.get(1);
        first.runOnInstance(()-> ByteBuddyHelper.failValidation = true);
        first.runOnInstance(()-> ByteBuddyHelper.interruptBuild = false);

        first.executeInternal(String.format("INSERT INTO %s.%s(pk, v) VALUES (?, ?)", KEYSPACE, table), 1, "v1");
        first.flush(KEYSPACE);

        Object[][] rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(1);

        String indexName = table + "_v_index";
        cluster.schemaChange(String.format("CREATE INDEX %s ON %s.%s(v) USING 'sai'", indexName, KEYSPACE, table));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, indexName);

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(1);
        assertThat(rs[0][0]).isEqualTo(1);

        first.runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).clearUnsafe());

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(0);
        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(0);

        first.runOnInstance(() ->
                assertThatThrownBy(() ->
                        ColumnFamilyStore.loadNewSSTables(KEYSPACE, table)).hasRootCauseExactlyInstanceOf(CorruptIndexException.class));

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(0);
        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(0);
    }

    @Test
    public void testImportIncludesExistingSSTableIndexes()
    {
        String table = "existing_indexes_test";
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int PRIMARY KEY, v text)", KEYSPACE, table));

        IInvokableInstance first = cluster.get(1);
        first.runOnInstance(()-> ByteBuddyHelper.failValidation = false);
        first.runOnInstance(()-> ByteBuddyHelper.interruptBuild = false);

        first.executeInternal(String.format("INSERT INTO %s.%s(pk, v) VALUES (?, ?)", KEYSPACE, table), 1, "v1");
        first.flush(KEYSPACE);

        Object[][] rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(1);

        String indexName = table + "_v_index";
        cluster.schemaChange(String.format("CREATE INDEX %s ON %s.%s(v) USING 'sai'", indexName, KEYSPACE, table));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, indexName);

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(1);
        assertThat(rs[0][0]).isEqualTo(1);

        first.runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).clearUnsafe());

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(0);
        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(0);

        first.runOnInstance(() -> ColumnFamilyStore.loadNewSSTables(KEYSPACE, table));

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(1);
        assertThat(rs[0][0]).isEqualTo(1);

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(1);
        assertThat(rs[0][0]).isEqualTo(1);
    }

    @Test
    public void testIndexCreatedBeforeAddingImportedSSTables()
    {
        String table = "index_created_before_adding_test";
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int PRIMARY KEY, v text)", KEYSPACE, table));

        IInvokableInstance first = cluster.get(1);
        first.runOnInstance(()-> ByteBuddyHelper.failValidation = false);
        first.runOnInstance(()-> ByteBuddyHelper.interruptBuild = false);

        first.executeInternal(String.format("INSERT INTO %s.%s(pk, v) VALUES (?, ?)", KEYSPACE, table), 1, "v1");
        first.flush(KEYSPACE);

        first.runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).clearUnsafe());

        // Hold the import once it has checked the indexes of the imported SSTables, before it adds them, and create an
        // index meanwhile
        first.runOnInstance(() -> ByteBuddyHelper.holdBeforeAdding(table));
        CompletableFuture<Void> load = CompletableFuture.runAsync(() -> first.runOnInstance(() -> ColumnFamilyStore.loadNewSSTables(KEYSPACE, table)));
        String indexName = table + "_v_index";
        try
        {
            await().atMost(1, TimeUnit.MINUTES).until(() -> load.isDone() || first.callOnInstance(ByteBuddyHelper::isHeldBeforeAdding));
            assertThat(first.callOnInstance(ByteBuddyHelper::isHeldBeforeAdding)).describedAs("The import should be held").isTrue();

            cluster.schemaChange(String.format("CREATE INDEX %s ON %s.%s(v) USING 'sai'", indexName, KEYSPACE, table));

            // The initial build of the new index must not complete without the imported SSTables
            try
            {
                await().atMost(10, TimeUnit.SECONDS).until(() -> first.callOnInstance(() -> isQueryable(table, indexName)));
            }
            catch (ConditionTimeoutException e)
            {
                // the index is still not queryable
            }
        }
        finally
        {
            first.runOnInstance(ByteBuddyHelper::releaseBeforeAdding);
        }

        load.join();
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, indexName);

        Object[][] rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(1);
        assertThat(rs[0][0]).isEqualTo(1);
    }

    @Test
    public void testIndexCreatedWhileStartingBeforeAddingImportedSSTables() throws Exception
    {
        String table = "index_created_while_starting_test";
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int PRIMARY KEY, v text, w int)", KEYSPACE, table));
        // The import builds this legacy index on the SSTables once it adds them, under the monitor of the index manager
        cluster.schemaChange(String.format("CREATE INDEX %s_w_index ON %s.%s(w) USING 'legacy_local_table'", table, KEYSPACE, table));

        IInvokableInstance first = cluster.get(1);
        first.runOnInstance(()-> ByteBuddyHelper.failValidation = false);
        first.runOnInstance(()-> ByteBuddyHelper.interruptBuild = false);

        first.executeInternal(String.format("INSERT INTO %s.%s(pk, v, w) VALUES (?, ?, ?)", KEYSPACE, table), 1, "v1", 10);
        first.flush(KEYSPACE);

        first.runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).clearUnsafe());

        // Hold the import once it has checked the indexes of the imported SSTables, before it adds them, and create an
        // index meanwhile, while the node is starting
        first.runOnInstance(() -> ByteBuddyHelper.holdBeforeAdding(table));
        CompletableFuture<Void> load = CompletableFuture.runAsync(() -> first.runOnInstance(() -> ColumnFamilyStore.loadNewSSTables(KEYSPACE, table)));
        String indexName = table + "_v_index";
        try
        {
            await().atMost(1, TimeUnit.MINUTES).until(() -> load.isDone() || first.callOnInstance(ByteBuddyHelper::isHeldBeforeAdding));
            assertThat(first.callOnInstance(ByteBuddyHelper::isHeldBeforeAdding)).describedAs("The import should be held").isTrue();

            first.runOnInstance(() -> StorageService.instance.setStartingModeUnsafe());
            // The index creation initializes the index under the monitor of the index manager, so it must not wait for
            // the import, which needs that monitor once it adds the SSTables
            CompletableFuture<Void> create = CompletableFuture.runAsync(() -> cluster.schemaChange(String.format("CREATE INDEX %s ON %s.%s(v) USING 'sai'", indexName, KEYSPACE, table)));
            try
            {
                create.get(30, TimeUnit.SECONDS);
            }
            catch (TimeoutException e)
            {
                // Fail the import rather than add the SSTables, so that the index creation completes
                first.runOnInstance(ByteBuddyHelper::failBeforeAdding);
                create.get(1, TimeUnit.MINUTES);
                throw new AssertionError("The index creation waited for the import", e);
            }
            finally
            {
                first.runOnInstance(() -> StorageService.instance.setNormalModeUnsafe());
            }

            // The new index must not be made queryable without the imported SSTables
            try
            {
                await().atMost(10, TimeUnit.SECONDS).until(() -> first.callOnInstance(() -> isQueryable(table, indexName)));
            }
            catch (ConditionTimeoutException e)
            {
                // the index is still not queryable
            }
        }
        finally
        {
            first.runOnInstance(ByteBuddyHelper::releaseBeforeAdding);
        }

        load.join();
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, indexName);

        Object[][] rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).describedAs("Rows found through the index").isEqualTo(1);
        assertThat(rs[0][0]).isEqualTo(1);
        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE w = ?", KEYSPACE, table), 10);
        assertThat(rs.length).describedAs("Rows found through the legacy index").isEqualTo(1);
    }

    @Test
    public void testPreJoinTaskBeforeAddingImportedSSTables()
    {
        String table = "pre_join_before_adding_test";
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int PRIMARY KEY, v text)", KEYSPACE, table));

        IInvokableInstance first = cluster.get(1);
        first.runOnInstance(()-> ByteBuddyHelper.failValidation = false);
        first.runOnInstance(()-> ByteBuddyHelper.interruptBuild = false);

        first.executeInternal(String.format("INSERT INTO %s.%s(pk, v) VALUES (?, ?)", KEYSPACE, table), 1, "v1");
        first.flush(KEYSPACE);

        first.runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).clearUnsafe());

        // Hold the import once it has checked the indexes of the imported SSTables, before it adds them, and create an
        // index and run the pre-join tasks meanwhile
        first.runOnInstance(() -> ByteBuddyHelper.holdBeforeAdding(table));
        CompletableFuture<Void> load = CompletableFuture.runAsync(() -> first.runOnInstance(() -> ColumnFamilyStore.loadNewSSTables(KEYSPACE, table)));
        String indexName = table + "_v_index";
        try
        {
            await().atMost(1, TimeUnit.MINUTES).until(() -> load.isDone() || first.callOnInstance(ByteBuddyHelper::isHeldBeforeAdding));
            assertThat(first.callOnInstance(ByteBuddyHelper::isHeldBeforeAdding)).describedAs("The import should be held").isTrue();

            cluster.schemaChange(String.format("CREATE INDEX %s ON %s.%s(v) USING 'sai'", indexName, KEYSPACE, table));
            first.runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).indexManager.executePreJoinTasksBlocking(false));

            // Hold the initial build of the new index once the import has added the SSTables
            first.runOnInstance(() -> ByteBuddyHelper.holdBuild(table));
        }
        finally
        {
            first.runOnInstance(ByteBuddyHelper::releaseBeforeAdding);
        }

        load.join();
        try
        {
            await().atMost(1, TimeUnit.MINUTES).until(() -> first.callOnInstance(ByteBuddyHelper::isBuildHeld));

            // The index must not be queryable before its initial build has built the imported SSTables
            Object[][] rs = first.executeInternal(String.format("SELECT pk FROM %s.%s", KEYSPACE, table));
            assertThat(rs.length).describedAs("Imported rows").isEqualTo(1);
            boolean queryable = first.callOnInstance(() -> isQueryable(table, indexName));
            rs = queryable ? first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1") : null;
            assertThat(queryable).describedAs("The index is queryable before its initial build has built the imported SSTables, and finds %s rows",
                                              rs == null ? null : rs.length).isFalse();
        }
        finally
        {
            first.runOnInstance(ByteBuddyHelper::releaseBuild);
        }

        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, indexName);

        Object[][] rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).describedAs("Rows found through the index").isEqualTo(1);
        assertThat(rs[0][0]).isEqualTo(1);
    }

    @Test
    public void testIndexBuildingFailureAfterBuildingAnSSTable()
    {
        String table = "fail_after_sstable_test";
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int PRIMARY KEY, v text)", KEYSPACE, table));

        IInvokableInstance first = cluster.get(1);
        first.runOnInstance(()-> ByteBuddyHelper.failValidation = false);
        first.runOnInstance(()-> ByteBuddyHelper.interruptBuild = false);

        first.executeInternal(String.format("INSERT INTO %s.%s(pk, v) VALUES (?, ?)", KEYSPACE, table), 1, "v1");
        first.flush(KEYSPACE);
        first.executeInternal(String.format("INSERT INTO %s.%s(pk, v) VALUES (?, ?)", KEYSPACE, table), 2, "v2");
        first.flush(KEYSPACE);

        first.runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).clearUnsafe());

        String indexName = table + "_v_index";
        cluster.schemaChange(String.format("CREATE INDEX %s ON %s.%s(v) USING 'sai'", indexName, KEYSPACE, table));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, indexName);

        // The index build fails on the second imported SSTable, so does the import, and the SSTable built before the
        // failure doesn't stay in the index...
        first.runOnInstance(() -> ByteBuddyHelper.failBuildAfterSSTables(table, 1));
        try
        {
            first.runOnInstance(() ->
                    assertThatThrownBy(() ->
                            ColumnFamilyStore.loadNewSSTables(KEYSPACE, table)).hasRootCauseMessage(INJECTED_FAILURE));
        }
        finally
        {
            first.runOnInstance(() -> ByteBuddyHelper.failBuildAfterSSTables(null, 0));
        }

        Object[][] rs = first.executeInternal(String.format("SELECT pk FROM %s.%s", KEYSPACE, table));
        assertThat(rs.length).isEqualTo(0);
        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(0);
        assertThat(first.callOnInstance(() -> indexedSSTables(table, indexName))).isZero();

        // ...and a new import succeeds.
        first.runOnInstance(() -> ColumnFamilyStore.loadNewSSTables(KEYSPACE, table));
        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(1);
        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v2");
        assertThat(rs.length).isEqualTo(1);
        assertThat(first.callOnInstance(() -> indexedSSTables(table, indexName))).isEqualTo(2);
    }

    private static boolean isQueryable(String table, String index)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
        return cfs.indexManager.isIndexQueryable(cfs.indexManager.getIndexByName(index));
    }

    private static int indexedSSTables(String table, String index)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
        return ((StorageAttachedIndex) cfs.indexManager.getIndexByName(index)).view().getIndexes().size();
    }

    public static class ByteBuddyHelper
    {
        static volatile boolean interruptBuild = false;
        static volatile boolean failValidation = false;

        private static volatile String heldBeforeAddingTable;
        private static volatile CountDownLatch heldBeforeAdding = CountDownLatch.newCountDownLatch(1);
        private static volatile CountDownLatch releaseBeforeAdding = CountDownLatch.newCountDownLatch(1);
        private static volatile boolean failBeforeAdding;

        private static volatile String failBuildAfterSSTablesTable;
        private static volatile int sstablesBeforeFailure;

        private static volatile String heldBuildTable;
        private static volatile CountDownLatch buildHeld = CountDownLatch.newCountDownLatch(1);
        private static volatile CountDownLatch releaseBuild = CountDownLatch.newCountDownLatch(1);

        static void installErrors(ClassLoader loader, int node)
        {
            new ByteBuddy().rebase(StorageAttachedIndexBuilder.class)
                           .method(named("isStopRequested"))
                           .intercept(MethodDelegation.to(ByteBuddyHelper.class))
                           .make()
                           .load(loader, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(SAICodecUtils.class)
                           .method(named("validateChecksum"))
                           .intercept(MethodDelegation.to(ByteBuddyHelper.class))
                           .make()
                           .load(loader, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(Tracker.class)
                           .method(named("addSSTables").and(takesArguments(1)))
                           .intercept(MethodDelegation.to(AddInterceptor.class))
                           .make()
                           .load(loader, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(StorageAttachedIndexWriter.class)
                           .method(named("createBuilderWriter"))
                           .intercept(MethodDelegation.to(BuilderWriterInterceptor.class))
                           .make()
                           .load(loader, ClassLoadingStrategy.Default.INJECTION);
        }

        static void holdBeforeAdding(String table)
        {
            heldBeforeAdding = CountDownLatch.newCountDownLatch(1);
            releaseBeforeAdding = CountDownLatch.newCountDownLatch(1);
            failBeforeAdding = false;
            heldBeforeAddingTable = table;
        }

        static boolean isHeldBeforeAdding()
        {
            return heldBeforeAdding.count() == 0;
        }

        static void releaseBeforeAdding()
        {
            heldBeforeAddingTable = null;
            releaseBeforeAdding.decrement();
        }

        static void failBeforeAdding()
        {
            failBeforeAdding = true;
            releaseBeforeAdding();
        }

        static void holdBuild(String table)
        {
            buildHeld = CountDownLatch.newCountDownLatch(1);
            releaseBuild = CountDownLatch.newCountDownLatch(1);
            heldBuildTable = table;
        }

        static boolean isBuildHeld()
        {
            return buildHeld.count() == 0;
        }

        static void releaseBuild()
        {
            heldBuildTable = null;
            releaseBuild.decrement();
        }

        static void failBuildAfterSSTables(String table, int sstables)
        {
            sstablesBeforeFailure = sstables;
            failBuildAfterSSTablesTable = table;
        }

        @SuppressWarnings("unused")
        public static boolean isStopRequested(@SuperCall Callable<Boolean> zuper)
        {
            if (interruptBuild)
                return true;

            try
            {
                return zuper.call();
            }
            catch (Exception e)
            {
                throw Throwables.unchecked(e);
            }
        }

        @SuppressWarnings("unused")
        public static void validateChecksum(IndexInput input, @SuperCall Callable<Void> zuper) throws IOException
        {
            if (failValidation)
                throw new CorruptIndexException("Injected failure!", "Test resource");

            try
            {
                zuper.call();
            }
            catch (Exception e)
            {
                throw Throwables.unchecked(e);
            }
        }
    
        public static class AddInterceptor
        {
            @SuppressWarnings("unused")
            public static void addSSTables(@FieldValue("cfstore") ColumnFamilyStore cfs, @SuperCall Callable<Void> zuper) throws Exception
            {
                if (cfs.name.equals(heldBeforeAddingTable))
                {
                    heldBeforeAdding.decrement();
                    releaseBeforeAdding.awaitUninterruptibly(1, TimeUnit.MINUTES);
                    if (failBeforeAdding)
                        throw new RuntimeException(INJECTED_FAILURE);
                }
                zuper.call();
            }
        }

        public static class BuilderWriterInterceptor
        {
            @SuppressWarnings("unused")
            public static StorageAttachedIndexWriter createBuilderWriter(IndexDescriptor indexDescriptor,
                                                                         @SuperCall Callable<StorageAttachedIndexWriter> zuper) throws Exception
            {
                if (indexDescriptor.sstableDescriptor.cfname.equals(failBuildAfterSSTablesTable) && sstablesBeforeFailure-- <= 0)
                    throw new RuntimeException(INJECTED_FAILURE);

                if (indexDescriptor.sstableDescriptor.cfname.equals(heldBuildTable))
                {
                    buildHeld.decrement();
                    releaseBuild.awaitUninterruptibly(1, TimeUnit.MINUTES);
                }

                return zuper.call();
            }
        }
    }
}
