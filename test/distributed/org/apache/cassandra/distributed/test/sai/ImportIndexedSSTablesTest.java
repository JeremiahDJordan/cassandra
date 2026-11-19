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
import org.apache.cassandra.distributed.test.sai.IndexBuildInterceptor.Hold;
import org.apache.cassandra.index.sai.StorageAttachedIndexBuilder;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Throwables;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.fail;

public class ImportIndexedSSTablesTest extends TestBaseImpl
{
    private static Cluster cluster;

    @BeforeClass
    public static void startup() throws IOException
    {
        cluster = init(Cluster.build(2).withConfig(c -> c.with(NETWORK, GOSSIP))
                                       .withInstanceInitializer(ByteBuddyHelper::installErrors)
                                       .start());
        IndexBuildInterceptor.ignoreInjectedFailures(cluster);

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
        SAIUtil.waitForIndexQueryableOnFirstNode(cluster, KEYSPACE, indexName);

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(0);
        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(0);

        first.runOnInstance(() ->
                assertThatThrownBy(() ->
                        ColumnFamilyStore.loadNewSSTables(KEYSPACE, table)).hasRootCauseExactlyInstanceOf(CompactionInterruptedException.class));

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(0);

        try
        {
            // The injected ByteBuddy error will cause the index build to fail, so querying the index should fail.
            first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
            fail("Expected IndexNotAvailableException exception");
        }
        catch (Exception e)
        {
            assertThat(e.getClass().getName()).contains("IndexNotAvailableException");
            assertThat(e.getMessage()).contains("The secondary index 'fail_during_import_test_v_index' is not yet available");
        }
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
        SAIUtil.waitForIndexQueryableOnFirstNode(cluster, KEYSPACE, indexName);

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
        SAIUtil.waitForIndexQueryableOnFirstNode(cluster, KEYSPACE, indexName);

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
        SAIUtil.waitForIndexQueryableOnFirstNode(cluster, KEYSPACE, indexName);

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
        CompletableFuture<Void> load = startHeldImport(first, table);
        String indexName = table + "_v_index";
        try
        {
            cluster.schemaChange(String.format("CREATE INDEX %s ON %s.%s(v) USING 'sai'", indexName, KEYSPACE, table));

            // The initial build of the new index must not complete without the imported SSTables
            SAIUtil.awaitIndexQueryableQuietly(first, KEYSPACE, table, indexName);
        }
        finally
        {
            first.runOnInstance(() -> ByteBuddyHelper.BEFORE_ADDING.release());
        }

        load.join();
        SAIUtil.waitForIndexQueryableOnFirstNode(cluster, KEYSPACE, indexName);

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
        CompletableFuture<Void> load = startHeldImport(first, table);
        String indexName = table + "_v_index";
        try
        {
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
            SAIUtil.awaitIndexQueryableQuietly(first, KEYSPACE, table, indexName);
        }
        finally
        {
            first.runOnInstance(() -> ByteBuddyHelper.BEFORE_ADDING.release());
        }

        load.join();
        SAIUtil.waitForIndexQueryableOnFirstNode(cluster, KEYSPACE, indexName);

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
        CompletableFuture<Void> load = startHeldImport(first, table);
        String indexName = table + "_v_index";
        try
        {
            cluster.schemaChange(String.format("CREATE INDEX %s ON %s.%s(v) USING 'sai'", indexName, KEYSPACE, table));
            first.runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).indexManager.executePreJoinTasksBlocking(false));

            // Hold the initial build of the new index once the import has added the SSTables
            first.runOnInstance(() -> IndexBuildInterceptor.BUILD.hold(table));
        }
        finally
        {
            first.runOnInstance(() -> ByteBuddyHelper.BEFORE_ADDING.release());
        }

        load.join();
        try
        {
            await().atMost(1, TimeUnit.MINUTES).until(() -> first.callOnInstance(() -> IndexBuildInterceptor.BUILD.isHeld()));

            // The index must not be queryable before its initial build has built the imported SSTables
            Object[][] rs = first.executeInternal(String.format("SELECT pk FROM %s.%s", KEYSPACE, table));
            assertThat(rs.length).describedAs("Imported rows").isEqualTo(1);
            boolean queryable = SAIUtil.isIndexQueryable(first, KEYSPACE, table, indexName);
            rs = queryable ? first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1") : null;
            assertThat(queryable).describedAs("The index is queryable before its initial build has built the imported SSTables, and finds %s rows",
                                              rs == null ? null : rs.length).isFalse();
        }
        finally
        {
            first.runOnInstance(() -> IndexBuildInterceptor.BUILD.release());
        }

        SAIUtil.waitForIndexQueryableOnFirstNode(cluster, KEYSPACE, indexName);

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
        SAIUtil.waitForIndexQueryableOnFirstNode(cluster, KEYSPACE, indexName);

        // The index build fails on the second imported SSTable, so does the import, and the SSTable built before the
        // failure doesn't stay in the index
        first.runOnInstance(() -> IndexBuildInterceptor.failBuildAfterSSTables(table, 1));
        try
        {
            first.runOnInstance(() ->
                    assertThatThrownBy(() ->
                            ColumnFamilyStore.loadNewSSTables(KEYSPACE, table)).hasRootCauseMessage(IndexBuildInterceptor.INJECTED_FAILURE));
        }
        finally
        {
            first.runOnInstance(() -> IndexBuildInterceptor.failBuildAfterSSTables(null, 0));
        }

        Object[][] rs = first.executeInternal(String.format("SELECT pk FROM %s.%s", KEYSPACE, table));
        assertThat(rs.length).isEqualTo(0);
        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(0);
        assertThat(SAIUtil.indexedSSTables(first, KEYSPACE, table, indexName)).isZero();

        // A new import succeeds
        first.runOnInstance(() -> ColumnFamilyStore.loadNewSSTables(KEYSPACE, table));
        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(1);
        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v2");
        assertThat(rs.length).isEqualTo(1);
        assertThat(SAIUtil.indexedSSTables(first, KEYSPACE, table, indexName)).isEqualTo(2);
    }

    /** Starts importing the SSTables of the table, and waits until the import is held before it adds them. */
    private static CompletableFuture<Void> startHeldImport(IInvokableInstance node, String table)
    {
        node.runOnInstance(() -> ByteBuddyHelper.holdBeforeAdding(table));
        CompletableFuture<Void> load = CompletableFuture.runAsync(() -> node.runOnInstance(() -> ColumnFamilyStore.loadNewSSTables(KEYSPACE, table)));
        try
        {
            await().atMost(1, TimeUnit.MINUTES).until(() -> load.isDone() || node.callOnInstance(() -> ByteBuddyHelper.BEFORE_ADDING.isHeld()));
            assertThat(node.callOnInstance(() -> ByteBuddyHelper.BEFORE_ADDING.isHeld())).describedAs("The import should be held").isTrue();
        }
        catch (Throwable t)
        {
            node.runOnInstance(() -> ByteBuddyHelper.BEFORE_ADDING.release());
            throw t;
        }
        return load;
    }

    public static class ByteBuddyHelper
    {
        static volatile boolean interruptBuild = false;
        static volatile boolean failValidation = false;

        static final Hold BEFORE_ADDING = new Hold();
        private static volatile boolean failBeforeAdding;

        static void installErrors(ClassLoader loader, int node)
        {
            new ByteBuddy().rebase(StorageAttachedIndexBuilder.class)
                           .method(named("isStopRequested"))
                           .intercept(MethodDelegation.to(ByteBuddyHelper.class))
                           .method(named("indexSSTable"))
                           .intercept(MethodDelegation.to(IndexBuildInterceptor.IndexSSTableInterceptor.class))
                           .make()
                           .load(loader, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(SAICodecUtils.class)
                           .method(named("validateChecksum"))
                           .intercept(MethodDelegation.to(ByteBuddyHelper.class))
                           .make()
                           .load(loader, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(Tracker.class)
                           .method(named("addSSTables").and(takesArguments(2)))
                           .intercept(MethodDelegation.to(AddInterceptor.class))
                           .make()
                           .load(loader, ClassLoadingStrategy.Default.INJECTION);
        }

        static void holdBeforeAdding(String table)
        {
            failBeforeAdding = false;
            BEFORE_ADDING.hold(table);
        }

        static void failBeforeAdding()
        {
            failBeforeAdding = true;
            BEFORE_ADDING.release();
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
                if (BEFORE_ADDING.reach(cfs.name) && failBeforeAdding)
                    throw new RuntimeException(IndexBuildInterceptor.INJECTED_FAILURE);
                zuper.call();
            }
        }
    }
}
