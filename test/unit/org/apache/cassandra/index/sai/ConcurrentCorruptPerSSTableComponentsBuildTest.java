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
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;

import static org.apache.cassandra.config.CassandraRelevantProperties.IMMUTABLE_SAI_COMPONENTS;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VALIDATE_PER_SSTABLE_CHECKSUM_ON_PARTIAL_BUILD;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Two partial builds of one SSTable whose per-SSTable components are corrupt, with checksum validation on partial
 * builds (CASSANDRA-18490): the one that gets the per-SSTable latch rewrites them, and the other one builds on its
 * result.
 */
public class ConcurrentCorruptPerSSTableComponentsBuildTest extends SAITester
{
    private static final int ROWS = 1000;

    private static final AtomicInteger writers = new AtomicInteger();
    private static final AtomicInteger maxWriters = new AtomicInteger();

    private static final AtomicInteger validations = new AtomicInteger();
    private static final AtomicInteger lookupFailures = new AtomicInteger();
    private static final AtomicInteger failures = new AtomicInteger();
    private static volatile CountDownLatch firstValidated;
    private static volatile CountDownLatch releaseFirstValidation;

    private String v;
    private String w;
    private boolean defaultImmutableSetting;

    // called by byteman
    @SuppressWarnings("unused")
    public static void startWriting()
    {
        maxWriters.accumulateAndGet(writers.incrementAndGet(), Math::max);
    }

    // called by byteman
    @SuppressWarnings("unused")
    public static void stopWriting()
    {
        writers.decrementAndGet();
    }

    // called by byteman
    @SuppressWarnings("unused")
    public static void holdFirstValidation()
    {
        if (validations.getAndIncrement() > 0)
            return;
        firstValidated.countDown();
        Uninterruptibles.awaitUninterruptibly(releaseFirstValidation, 3, TimeUnit.MINUTES);
    }

    // called by byteman
    @SuppressWarnings("unused")
    public static boolean failLookup()
    {
        return lookupFailures.getAndDecrement() > 0;
    }

    // called by byteman
    @SuppressWarnings("unused")
    public static boolean failOnce()
    {
        return failures.getAndDecrement() > 0;
    }

    @Before
    public void saveImmutableSetting()
    {
        defaultImmutableSetting = IMMUTABLE_SAI_COMPONENTS.getBoolean();
    }

    @After
    public void cleanUp() throws Throwable
    {
        Injections.deleteAll();
        SAI_VALIDATE_PER_SSTABLE_CHECKSUM_ON_PARTIAL_BUILD.reset();
        IMMUTABLE_SAI_COMPONENTS.setBoolean(defaultImmutableSetting);
    }

    @Test
    public void testBuildThatLostTheLatchWaitsForTheRewrite() throws Throwable
    {
        SSTableReader sstable = createSSTableWithCorruptPerSSTableComponents();

        // both builders find the per-SSTable components corrupt before either takes the per-SSTable latch...
        Injections.inject(Injections.newBarrier("corrupt_found_" + UUID.randomUUID(), 2, false)
                                    .add(InvokePointBuilder.newInvokePoint()
                                                           .onClass(StorageAttachedIndexBuilder.class)
                                                           .onMethod("hasCorruptPerSSTableComponents")
                                                           .atExit())
                                    .build());
        countWriters();

        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(getCurrentColumnFamilyStore());
        CompletableFuture<Throwable> buildV = CompletableFuture.supplyAsync(() -> partialBuild(group, sstable, v));
        CompletableFuture<Throwable> buildW = CompletableFuture.supplyAsync(() -> partialBuild(group, sstable, w));
        assertThat(buildV.get(3, TimeUnit.MINUTES)).isNull();
        assertThat(buildW.get(3, TimeUnit.MINUTES)).isNull();
        Injections.deleteAll();

        // the build without the latch waited for the one rewriting the components (which rebuilt both indexes)
        assertRebuiltOnce(sstable);
    }

    /**
     * The second build looks at the SSTable once the first one has deleted the corrupt components to rewrite them: it
     * finds them missing rather than corrupt, and waits for the rewrite all the same.
     */
    @Test
    public void testBuildThatFindsTheComponentsDeletedWaitsForTheRewrite() throws Throwable
    {
        SSTableReader sstable = createSSTableWithCorruptPerSSTableComponents();

        Injections.Barrier deleted = Injections.newBarrier("corrupt_deleted_" + UUID.randomUUID(), 2, false)
                                               .add(InvokePointBuilder.newInvokePoint()
                                                                      .onClass(StorageAttachedIndexBuilder.class)
                                                                      .onMethod("indexSSTable")
                                                                      .after("INVOKE prepareForRebuild"))
                                               .build();
        Injections.Counter waiting = Injections.newCounter("waiting_" + UUID.randomUUID())
                                               .add(InvokePointBuilder.newInvokePoint()
                                                                      .onClass(StorageAttachedIndexBuilder.class)
                                                                      .onMethod("indexSSTable")
                                                                      .atInvoke("await"))
                                               .build();
        Injections.inject(deleted, waiting);
        countWriters();

        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(getCurrentColumnFamilyStore());
        CompletableFuture<Throwable> buildV = CompletableFuture.supplyAsync(() -> partialBuild(group, sstable, v));
        CompletableFuture<Throwable> buildW;
        try
        {
            waitForAssert(() -> assertThat(deleted.getCount()).isEqualTo(1), 10, TimeUnit.SECONDS);
            buildW = CompletableFuture.supplyAsync(() -> partialBuild(group, sstable, w));
            waitForAssert(() -> assertThat(waiting.get() + writers.get()).isPositive(), 10, TimeUnit.SECONDS);
        }
        finally
        {
            deleted.countDown();
        }
        assertThat(buildV.get(3, TimeUnit.MINUTES)).isNull();
        assertThat(buildW.get(3, TimeUnit.MINUTES)).isNull();
        Injections.deleteAll();

        assertRebuiltOnce(sstable);
    }

    /**
     * The first build finds the components corrupt, and only takes the per-SSTable latch once the second one has
     * rewritten them: it validates them again, and builds on them rather than rewrite them a second time.
     */
    @Test
    public void testBuildThatFoundTheComponentsCorruptBeforeTheirRewriteValidatesThemAgain() throws Throwable
    {
        testBuildThatFoundTheComponentsCorruptBeforeTheirRewrite(false);
    }

    /**
     * The same, when the descriptor through which the first build found the components corrupt is evicted before the
     * second build rewrites them (the SSTable was removed and added again): the second build rewrites them through
     * another descriptor.
     */
    @Test
    public void testBuildThatFoundTheComponentsCorruptThroughAnEvictedDescriptorValidatesThemAgain() throws Throwable
    {
        testBuildThatFoundTheComponentsCorruptBeforeTheirRewrite(true);
    }

    private void testBuildThatFoundTheComponentsCorruptBeforeTheirRewrite(boolean evictDescriptor) throws Throwable
    {
        SSTableReader sstable = createSSTableWithCorruptPerSSTableComponents();

        validations.set(0);
        firstValidated = new CountDownLatch(1);
        releaseFirstValidation = new CountDownLatch(1);
        Injections.Counter rewrites = Injections.newCounter("rewrites_" + UUID.randomUUID())
                                                .add(InvokePointBuilder.newInvokePoint()
                                                                       .onClass(StorageAttachedIndexBuilder.class)
                                                                       .onMethod("withAllIndexes")
                                                                       .atEntry())
                                                .build();
        Injections.inject(rewrites,
                          Injections.newCustom("validated_" + UUID.randomUUID())
                                    .add(InvokePointBuilder.newInvokePoint()
                                                           .onClass(StorageAttachedIndexBuilder.class)
                                                           .onMethod("hasCorruptPerSSTableComponents")
                                                           .atExit())
                                    .add(ActionBuilder.newActionBuilder()
                                                      .actions()
                                                      .doAction(Expression.expr(ConcurrentCorruptPerSSTableComponentsBuildTest.class.getName())
                                                                          .method("holdFirstValidation")
                                                                          .args()))
                                    .build());
        countWriters();

        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(getCurrentColumnFamilyStore());
        CompletableFuture<Throwable> buildV = CompletableFuture.supplyAsync(() -> partialBuild(group, sstable, v));
        try
        {
            assertThat(firstValidated.await(1, TimeUnit.MINUTES)).isTrue();
            if (evictDescriptor)
            {
                IndexDescriptor validatedThrough = group.descriptorFor(sstable);
                group.onSSTableChanged(Collections.singleton(sstable), Collections.emptyList(), group.getIndexes(), false);
                group.onSSTableChanged(Collections.emptyList(), Collections.singleton(sstable), group.getIndexes(), false);
                assertThat(group.descriptorFor(sstable)).isNotSameAs(validatedThrough);
            }
            assertThat(partialBuild(group, sstable, w)).isNull();
        }
        finally
        {
            releaseFirstValidation.countDown();
        }
        assertThat(buildV.get(3, TimeUnit.MINUTES)).isNull();
        assertThat(rewrites.get()).as("rewrites of the per-SSTable components").isEqualTo(1);
        Injections.deleteAll();

        assertRebuiltOnce(sstable);
    }

    /**
     * A build that fails once it holds the per-SSTable latch, here looking up the descriptor cached for the SSTable,
     * releases it: the next build of the SSTable rewrites the components.
     */
    @Test
    public void testBuildThatFailsWithTheLatchReleasesIt() throws Throwable
    {
        SSTableReader sstable = createSSTableWithCorruptPerSSTableComponents();

        lookupFailures.set(1);
        Injections.inject(Injections.newCustom("fail_lookup_" + UUID.randomUUID())
                                    .add(InvokePointBuilder.newInvokePoint()
                                                           .onClass(StorageAttachedIndexGroup.class)
                                                           .onMethod("cachedDescriptorFor")
                                                           .atEntry())
                                    .add(ActionBuilder.newActionBuilder()
                                                      .conditions()
                                                      .when(ConcurrentCorruptPerSSTableComponentsBuildTest.class.getName() + ".failLookup()")
                                                      .actions()
                                                      .doThrow(RuntimeException.class, Expression.quote("Injected descriptor lookup failure")))
                                    .build());

        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(getCurrentColumnFamilyStore());
        assertThat(partialBuild(group, sstable, v)).hasStackTraceContaining("Injected descriptor lookup failure");
        assertThat(lookupFailures.get()).isNotPositive();
        Injections.deleteAll();

        CompletableFuture<Throwable> next = CompletableFuture.supplyAsync(() -> partialBuild(group, sstable, w));
        assertThat(next.get(1, TimeUnit.MINUTES)).isNull();
        assertThat(loadDescriptor(sstable, getCurrentColumnFamilyStore()).perSSTableComponents().isValid(true)).isTrue();
    }

    /**
     * The build that rewrites the components fails once it has deleted them, before its writer exists, and later as it
     * creates its writer: both indexes, which used them, become non-queryable, and the next build of the SSTable
     * rewrites them.
     */
    @Test
    public void testBuildFailingOnceItDeletedTheComponentsMakesTheIndexesNonQueryable() throws Throwable
    {
        testBuildFailingBeforeItsWriterMakesTheIndexesNonQueryable(InvokePointBuilder.newInvokePoint()
                                                                                    .onClass(StorageAttachedIndexBuilder.class)
                                                                                    .onMethod("prepareForRebuild")
                                                                                    .atExit());
        testBuildFailingBeforeItsWriterMakesTheIndexesNonQueryable(InvokePointBuilder.newInvokePoint()
                                                                                    .onClass(StorageAttachedIndexWriter.class)
                                                                                    .onMethod("<init>")
                                                                                    .atEntry());
    }

    /**
     * The same, when the build fails as it unregisters the per-index components from the TOC.
     */
    @Test
    public void testBuildFailingToUnregisterComponentsMakesTheIndexesNonQueryable() throws Throwable
    {
        testBuildFailingBeforeItsWriterMakesTheIndexesNonQueryable(InvokePointBuilder.newInvokePoint()
                                                                                    .onClass(SSTable.class)
                                                                                    .onMethod("unregisterComponents")
                                                                                    .atEntry());
    }

    private void testBuildFailingBeforeItsWriterMakesTheIndexesNonQueryable(InvokePointBuilder failurePoint) throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        SSTableReader sstable = createSSTableWithCorruptPerSSTableComponents();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        File corrupt = largestPerSSTableComponent(loadDescriptor(sstable, cfs)).file();

        failures.set(1);
        Injections.inject(Injections.newCustom("fail_" + UUID.randomUUID())
                                    .add(failurePoint)
                                    .add(ActionBuilder.newActionBuilder()
                                                      .conditions()
                                                      .when(ConcurrentCorruptPerSSTableComponentsBuildTest.class.getName() + ".failOnce()")
                                                      .actions()
                                                      .doThrow(RuntimeException.class, Expression.quote("Injected build failure")))
                                    .build());

        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        assertThat(partialBuild(group, sstable, v)).hasStackTraceContaining("Injected build failure");
        assertThat(failures.get()).isNotPositive();
        Injections.deleteAll();
        assertThat(corrupt.exists()).isFalse();
        assertThat(cfs.indexManager.isIndexQueryable(cfs.indexManager.getIndexByName(v))).isFalse();
        assertThat(cfs.indexManager.isIndexQueryable(cfs.indexManager.getIndexByName(w))).isFalse();

        CompletableFuture<Throwable> next = CompletableFuture.supplyAsync(() -> partialBuild(group, sstable, w));
        assertThat(next.get(1, TimeUnit.MINUTES)).isNull();
        assertThat(loadDescriptor(sstable, cfs).perSSTableComponents().isValid(true)).isTrue();
    }

    private SSTableReader createSSTableWithCorruptPerSSTableComponents() throws Exception
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        v = createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        w = createIndex("CREATE INDEX ON %s(w) USING 'sai'");
        for (int k = 0; k < ROWS; k++)
            execute("INSERT INTO %s (k, v, w) VALUES (?, ?, ?)", k, k % 10, k % 7);
        flush();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertThat(cfs.getLiveSSTables()).hasSize(1);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        corruptPerSSTableComponents(sstable, cfs);
        SAI_VALIDATE_PER_SSTABLE_CHECKSUM_ON_PARTIAL_BUILD.setBoolean(true);
        return sstable;
    }

    /**
     * Counts the builders writing per-index components of the SSTable at the same time: from the start of
     * indexPartitions to the end of completeSSTable.
     */
    private static void countWriters() throws Throwable
    {
        writers.set(0);
        maxWriters.set(0);
        Injections.inject(Injections.newCustom("writing_" + UUID.randomUUID())
                                    .add(InvokePointBuilder.newInvokePoint()
                                                           .onClass(StorageAttachedIndexBuilder.class)
                                                           .onMethod("indexPartitions")
                                                           .atEntry())
                                    .add(ActionBuilder.newActionBuilder()
                                                      .actions()
                                                      .doAction(Expression.expr(ConcurrentCorruptPerSSTableComponentsBuildTest.class.getName())
                                                                          .method("startWriting")
                                                                          .args()))
                                    .build());
        Injections.inject(Injections.newCustom("written_" + UUID.randomUUID())
                                    .add(InvokePointBuilder.newInvokePoint()
                                                           .onClass(StorageAttachedIndexBuilder.class)
                                                           .onMethod("completeSSTable")
                                                           .atExit())
                                    .add(ActionBuilder.newActionBuilder()
                                                      .actions()
                                                      .doAction(Expression.expr(ConcurrentCorruptPerSSTableComponentsBuildTest.class.getName())
                                                                          .method("stopWriting")
                                                                          .args()))
                                    .build());
    }

    private void assertRebuiltOnce(SSTableReader sstable) throws Throwable
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertThat(maxWriters.get()).as("builders writing per-index components of the SSTable at the same time").isEqualTo(1);

        assertThat(loadDescriptor(sstable, cfs).perSSTableComponents().isValid(true)).isTrue();
        assertThat(cfs.indexManager.isIndexQueryable(cfs.indexManager.getIndexByName(v))).isTrue();
        assertThat(cfs.indexManager.isIndexQueryable(cfs.indexManager.getIndexByName(w))).isTrue();
        assertQueries();

        // and once loaded again from disk
        simulateNodeRestart();
        assertQueries();
    }

    /** flips a byte in the middle of the largest per-SSTable component, which only checksum validation detects */
    private static void corruptPerSSTableComponents(SSTableReader sstable, ColumnFamilyStore cfs) throws Exception
    {
        flipMiddleByte(largestPerSSTableComponent(loadDescriptor(sstable, cfs)).file());
        assertThat(loadDescriptor(sstable, cfs).perSSTableComponents().isValid(true)).isFalse();
    }

    private static IndexComponent.ForRead largestPerSSTableComponent(IndexDescriptor descriptor)
    {
        return descriptor.perSSTableComponents()
                         .all()
                         .stream()
                         .filter(c -> c.componentType() != IndexComponentType.GROUP_COMPLETION_MARKER)
                         .max(Comparator.comparingLong(c -> c.file().length()))
                         .orElseThrow();
    }

    private void assertQueries()
    {
        assertThat(execute("SELECT k FROM %s WHERE v = 3")).hasSize(ROWS / 10);
        assertThat(execute("SELECT k FROM %s WHERE w = 3")).hasSize(143);
        assertThat(execute("SELECT k FROM %s WHERE v = 3 AND w = 3")).hasSize(15);
    }
}
