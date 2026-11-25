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

package org.apache.cassandra.index.sai.disk.apache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.V1SearchableIndex;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The lifecycle of SAI indexes in storage_compatibility_mode CASSANDRA_5, whose components are in Apache Cassandra
 * 5.0's format: builds on sstables that have components, rebuilds, and indexes dropped and created again.
 */
public class Cassandra5ModeIndexLifecycleTest extends ApacheIndexTester
{
    private StorageCompatibilityMode mode;

    @Before
    public void enterCassandra5Mode()
    {
        mode = DatabaseDescriptor.getStorageCompatibilityMode();
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.CASSANDRA_5);
    }

    @After
    public void leaveCassandra5Mode()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(mode);
    }

    @Test
    public void testIndexOnRowsExpiredSinceFlush() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, s text, t text, PRIMARY KEY (pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        disableCompaction();
        writeRowsExpiringAfterFlush();

        createIndex("CREATE CUSTOM INDEX ON %s(t) USING 'StorageAttachedIndex'");
        assertLiveRows("s", "t");
    }

    @Test
    public void testStaticIndexOnRowsExpiredSinceFlush() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, st text static, s text, t text, PRIMARY KEY (pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        disableCompaction();
        for (int pk = 0; pk < 9; pk++)
        {
            // static rows that stay live, but in partition 5, which expires, partition 6, which is deleted, and
            // partition 8, which has none; partition 7 has no live regular row left
            if (pk == 5)
                execute("INSERT INTO %s (pk, st) VALUES (?, 'gone') USING TTL 1", pk);
            else if (pk != 8)
                execute("INSERT INTO %s (pk, st) VALUES (?, ?)", pk, "st" + (pk % 2));
            if (pk == 6)
                execute("DELETE st FROM %s WHERE pk = ?", pk);
            for (int ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, s, t) VALUES (?, ?, 's', 't') USING TTL " + (pk == 7 || ck % 3 == 0 ? 1 : 0), pk, ck);
        }
        flush();
        Thread.sleep(2000);

        createIndex("CREATE CUSTOM INDEX ON %s(st) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(t) USING 'StorageAttachedIndex'");
        assertRowCount(execute("SELECT pk, ck FROM %s WHERE st = 'st0'"), 18);
        assertRowCount(execute("SELECT pk, ck FROM %s WHERE st = 'st1'"), 13);
        assertRowCount(execute("SELECT pk, ck FROM %s WHERE st = 'gone'"), 0);
        assertRowCount(execute("SELECT pk, ck FROM %s WHERE t = 't'"), 48);
        assertRowCount(execute("SELECT pk, ck FROM %s WHERE s = 's'"), 48);

        // the static key of each partition looks up its static row
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());
        SSTableContext context = SSTableContext.create(sstable, StorageAttachedIndexGroup.getIndexGroup(cfs).descriptorFor(sstable).perSSTableComponents());
        try (PrimaryKeyMap map = context.primaryKeyMapFactory().newPerSSTablePrimaryKeyMap())
        {
            List<DecoratedKey> partitions = new ArrayList<>();
            for (int pk = 0; pk < 8; pk++)
                partitions.add(cfs.decorateKey(Int32Type.instance.decompose(pk)));
            partitions.sort(Comparator.naturalOrder());
            for (DecoratedKey partition : partitions)
            {
                long rowId = map.exactRowIdOrInvertedCeiling(context.primaryKeyFactory().create(partition, Clustering.STATIC_CLUSTERING));
                assertThat(rowId).isNotNegative();
                assertThat(map.primaryKeyFromRowId(rowId).loadDeferred().clustering()).isEqualTo(Clustering.STATIC_CLUSTERING);
            }
        }
        finally
        {
            context.close();
        }
    }

    @Test
    public void testIndexRecreatedWhileStreamed() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, s text)");
        String index = createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        disableCompaction();
        for (int pk = 0; pk < 10; pk++)
            execute("INSERT INTO %s (pk, s) VALUES (?, ?)", pk, "s" + (pk % 2));
        flush();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());

        assertThat(sstable.streamRebuildState().tryBeginStreaming()).isTrue();
        boolean streaming = true;
        CountDownLatch holdOptionalTasks = new CountDownLatch(1);
        try
        {
            // the stream defers the deletion of the dropped index's files, and holds back the build of the new one
            dropIndex("DROP INDEX %s." + index);
            createIndexAsync("CREATE CUSTOM INDEX " + index + " ON %s(s) USING 'StorageAttachedIndex'");
            waitForAssert(() -> assertThat(cfs.indexManager.isIndexBuilding(index)).isFalse());

            // the deferred deletion runs after the new index is built
            ScheduledExecutors.optionalTasks.execute(() -> Uninterruptibles.awaitUninterruptibly(holdOptionalTasks));
            sstable.streamRebuildState().endStreaming();
            streaming = false;
            cfs.indexManager.rebuildIndexesBlocking(Collections.singleton(index));
        }
        finally
        {
            holdOptionalTasks.countDown();
            if (streaming)
                sstable.streamRebuildState().endStreaming();
        }
        ScheduledExecutors.optionalTasks.submit(() -> {}).get();

        assertThat(filesOf(sstable, index)).contains(APACHE_PREFIX + index + "+ColumnComplete.db");
        assertRowCount(execute("SELECT pk FROM %s WHERE s = 's1'"), 5);
    }

    @Test
    public void testRebuildWhileQueried() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, s text, t text, PRIMARY KEY (pk, ck))");
        String index = createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        disableCompaction();
        writeRowsExpiringAfterFlush();

        // the rebuild numbers the rows differently from the flush, while a query reads the components of the flush
        Injections.Barrier query = Injections.newBarrier("held_query_" + UUID.randomUUID(), 2, false)
                                             .add(newInvokePoint().onClass(V1SearchableIndex.class).onMethod("search"))
                                             .build();
        Injections.inject(query);
        CompletableFuture<UntypedResultSet> results = CompletableFuture.supplyAsync(() -> {
            try
            {
                return execute("SELECT pk, ck FROM %s WHERE s = 's1'");
            }
            catch (Throwable t)
            {
                throw new RuntimeException(t);
            }
        });
        waitForAssert(() -> assertThat(query.getCount()).isEqualTo(1));
        query.disable();
        try
        {
            getCurrentColumnFamilyStore().indexManager.rebuildIndexesBlocking(Collections.singleton(index));
            assertThat(results.isDone()).isFalse();
        }
        finally
        {
            query.countDown();
        }

        assertRowCount(results.get(), 10);
        assertLiveRows("s");
    }

    @Test
    public void testRebuildOfOneOfTwoIndexes() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, s text, t text, PRIMARY KEY (pk, ck))");
        String s = createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        String t = createIndex("CREATE CUSTOM INDEX ON %s(t) USING 'StorageAttachedIndex'");
        disableCompaction();
        writeRowsExpiringAfterFlush();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        // a failed rebuild of one index leaves neither index on the rows the rewritten components number
        injectWriterFailure();
        assertThatThrownBy(() -> cfs.indexManager.rebuildIndexesBlocking(Collections.singleton(s))).hasStackTraceContaining("Injected failure");
        for (String index : new String[]{ s, t })
            assertThat(cfs.indexManager.isIndexQueryable(cfs.indexManager.getIndexByName(index))).as(index).isFalse();

        // a rebuild of one index rebuilds the other too, on the rows the rewritten components number
        cfs.indexManager.rebuildIndexesBlocking(Set.of(s, t));
        assertLiveRows("s", "t");
        cfs.indexManager.rebuildIndexesBlocking(Collections.singleton(s));
        assertLiveRows("s", "t");
    }

    @Test
    public void testRebuildOfIndexLeftEmpty() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, s text)");
        String index = createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        for (int pk = 0; pk < 10; pk++)
            execute("INSERT INTO %s (pk, s) VALUES (?, 's') USING TTL 2", pk);
        flush();
        SSTableReader sstable = Iterables.getOnlyElement(getCurrentColumnFamilyStore().getLiveSSTables());
        assertThat(sstable.components().stream().map(component -> component.name)).contains(APACHE_PREFIX + index + "+TermsData.db");
        Thread.sleep(3000);

        // a failed rebuild, then one that writes no terms: the TOC lists only the files it writes
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        injectWriterFailure();
        assertThatThrownBy(() -> cfs.indexManager.rebuildIndexesBlocking(Collections.singleton(index))).hasStackTraceContaining("Injected failure");
        cfs.indexManager.rebuildIndexesBlocking(Collections.singleton(index));
        assertThat(sstable.components().stream().map(component -> component.name)).doesNotContain(APACHE_PREFIX + index + "+TermsData.db");
        assertThat(sstable.components().stream().map(component -> component.name)).contains(APACHE_PREFIX + index + "+ColumnComplete.db");
        assertRowCount(execute("SELECT pk FROM %s WHERE s = 's'"), 0);
    }

    /** Fails the next index writer when it completes. */
    private static void injectWriterFailure() throws Throwable
    {
        failWriter.set(true);
        Injections.inject(Injections.newCustom("fail_rebuild_" + UUID.randomUUID())
                                    .add(newInvokePoint().onClass(StorageAttachedIndexWriter.class).onMethod("complete"))
                                    .add(ActionBuilder.newActionBuilder()
                                                      .conditions()
                                                      .when(Expression.expr(Cassandra5ModeIndexLifecycleTest.class.getName()).method("failWriter").args())
                                                      .toActionBuilder()
                                                      .actions()
                                                      .doThrow(RuntimeException.class, Expression.quote("Injected failure")))
                                    .build());
    }

    private static final AtomicBoolean failWriter = new AtomicBoolean();

    // called by byteman
    @SuppressWarnings("unused")
    public static boolean failWriter()
    {
        return failWriter.getAndSet(false);
    }

    /**
     * Writes and flushes rows of {@code s = 's<ck>'} and {@code t = 't<ck>'}, live when flushed, 60 of which stay live,
     * and waits until the others have expired.
     */
    private void writeRowsExpiringAfterFlush() throws Throwable
    {
        for (int pk = 0; pk < 10; pk++)
        {
            for (int ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, s, t) VALUES (?, ?, ?, ?) USING TTL " + (ck % 3 == 0 ? 3 : 0), pk, ck, "s" + ck, "t" + ck);
        }
        assertRowCount(execute("SELECT pk, ck FROM %s"), 100);
        flush();
        Thread.sleep(4000);
    }

    /** Checks that each of the provided columns finds the live rows of each value, and no expired one. */
    private void assertLiveRows(String... columns) throws Throwable
    {
        for (String column : columns)
        {
            for (int ck = 0; ck < 10; ck++)
                assertRowCount(execute("SELECT pk, ck FROM %s WHERE " + column + " = ?", column + ck), ck % 3 == 0 ? 0 : 10);
        }
    }

    /** The names of the files of the provided index on the provided sstable, without the sstable's prefix. */
    private static Set<String> filesOf(SSTableReader sstable, String index)
    {
        String prefix = sstable.descriptor.baseFile().name() + '-';
        return Arrays.stream(sstable.descriptor.directory.tryListNames())
                     .filter(name -> name.startsWith(prefix + APACHE_PREFIX + index + '+'))
                     .map(name -> name.substring(prefix.length()))
                     .collect(Collectors.toSet());
    }
}
