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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.zip.CRC32C;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reads the literal indexes Apache Cassandra 5.0 wrote (test/data/legacy-sai/apache-aa, written by Apache
 * cassandra-5.0 at fb7f1b7aee).
 */
public class ApacheLiteralIndexTest extends ApacheIndexTester
{
    private static final Map<String, AtomicInteger> WRITERS = new ConcurrentHashMap<>();
    private static final AtomicInteger MAX_CONCURRENT_WRITERS = new AtomicInteger();
    private static final AtomicInteger WRITERS_WHILE_PAUSED = new AtomicInteger();

    // called by byteman
    @SuppressWarnings("unused")
    public static void writerStarted(String sstable) throws InterruptedException
    {
        int writers = WRITERS.computeIfAbsent(sstable, k -> new AtomicInteger()).incrementAndGet();
        MAX_CONCURRENT_WRITERS.accumulateAndGet(writers, Math::max);
        Thread.sleep(1000);
    }

    // called by byteman
    @SuppressWarnings("unused")
    public static void writerDone(String sstable)
    {
        WRITERS.get(sstable).decrementAndGet();
    }

    // called by byteman
    @SuppressWarnings("unused")
    public static void writerBegan()
    {
        if (ownerPaused.getCount() == 0 && releaseOwner.getCount() > 0)
            WRITERS_WHILE_PAUSED.incrementAndGet();
    }

    // called by byteman
    @SuppressWarnings("unused")
    public static void pauseAfterFirstPublication(IndexContext context) throws InterruptedException
    {
        if ((context.getIndexName().equals("types_t") || context.getIndexName().equals("types_a"))
            && context.getView().getIndexes().stream().anyMatch(i -> !i.getVersion().isApache())
            && pauses.incrementAndGet() == 1)
        {
            ownerPaused.countDown();
            releaseOwner.await(60, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSkinny() throws Throwable
    {
        createTable(SKINNY);
        disableCompaction();
        createIndex("CREATE INDEX skinny_s ON %s(s) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("skinny");

        assertThat(execute("SELECT pk FROM %s WHERE s = 's3'")).hasSize(10);
        assertSameResultsAfterRebuild(List.of("SELECT pk FROM %s WHERE s = 's3'",
                                              "SELECT pk FROM %s WHERE s = 's10'",
                                              "SELECT pk FROM %s WHERE s = 's'",
                                              "SELECT pk FROM %s WHERE s = 's3' AND pk > 40 ALLOW FILTERING",
                                              "SELECT pk FROM %s WHERE s = 's3' AND n > 50 ALLOW FILTERING",
                                              "SELECT pk FROM %s WHERE s = 's3' AND token(pk) > 0",
                                              "SELECT pk FROM %s WHERE s LIKE 's%%'",
                                              "SELECT pk FROM %s WHERE s LIKE 's7%%'",
                                              "SELECT pk FROM %s WHERE s LIKE 't%%'",
                                              "SELECT pk FROM %s WHERE s LIKE '%%3'",
                                              "SELECT pk FROM %s WHERE s LIKE 's_'",
                                              "SELECT pk, s FROM %s WHERE s LIKE 's%%' ORDER BY s LIMIT 5",
                                              "SELECT pk, s FROM %s ORDER BY s LIMIT 15",
                                              "SELECT s FROM %s ORDER BY s DESC LIMIT 15"),
                                      "skinny_s");
    }

    @Test
    public void testWide() throws Throwable
    {
        createTable(WIDE);
        disableCompaction();
        createIndex("CREATE INDEX wide_s ON %s(s) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("wide");

        assertThat(execute("SELECT pk, ck FROM %s WHERE s = 'w1'")).hasSize(100);
        assertSameResultsAfterRebuild(List.of("SELECT pk, ck FROM %s WHERE s = 'w1'",
                                              "SELECT pk, ck FROM %s WHERE s = 'w1' AND pk = 42",
                                              "SELECT pk, ck FROM %s WHERE s = 'w1' AND pk = 42 AND ck > 0",
                                              "SELECT pk, ck FROM %s WHERE s LIKE 'w%%'",
                                              "SELECT pk, ck FROM %s WHERE s LIKE '%%2'",
                                              "SELECT pk, ck, st FROM %s WHERE s = 'w0' AND st = 3 ALLOW FILTERING",
                                              "SELECT pk, ck FROM %s WHERE s = 'w2' LIMIT 7",
                                              "SELECT pk FROM %s WHERE s = 'w2' PER PARTITION LIMIT 1"),
                                      "wide_s");
    }

    /**
     * SSTables imported into a table that already has the index (or streamed in) use their Apache components too.
     */
    @Test
    public void testImportedIntoIndexedTable() throws Throwable
    {
        createTable(SKINNY);
        disableCompaction();
        createIndex("CREATE INDEX skinny_s ON %s(s) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("skinny");
        waitForTableIndexesQueryable();

        assertSameResultsAfterRebuild(List.of("SELECT pk FROM %s WHERE s = 's3'", "SELECT pk FROM %s WHERE s LIKE 's8%%'"), "skinny_s");
    }

    /**
     * A restart keeps reading the Apache components, without rebuilding anything.
     */
    @Test
    public void testRestart() throws Throwable
    {
        createTable(WIDE);
        disableCompaction();
        createIndex("CREATE INDEX wide_s ON %s(s) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("wide");
        assertReadFromApacheComponents("wide_s");

        simulateNodeRestart();
        assertReadFromApacheComponents("wide_s");
        assertThat(execute("SELECT pk, ck FROM %s WHERE s = 'w1'")).hasSize(100);

        // validation as on a real restart, or streaming
        reloadSSTableIndexInPlace();
        assertReadFromApacheComponents("wide_s");
        assertThat(execute("SELECT pk, ck FROM %s WHERE s = 'w1'")).hasSize(100);
    }

    /**
     * Creating an index Apache didn't write replaces the Apache components of the sstables, so the indexes that used
     * them are rebuilt too, and every Apache file goes.
     */
    @Test
    public void testNewIndexReplacesApacheComponents() throws Throwable
    {
        createTable(SKINNY);
        disableCompaction();
        createIndex("CREATE INDEX skinny_s ON %s(s) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("skinny");
        assertReadFromApacheComponents("skinny_s");
        List<String> queries = List.of("SELECT pk FROM %s WHERE s = 's3'", "SELECT pk FROM %s WHERE s = 's3' AND n = 13 ALLOW FILTERING");
        var before = runQueries(queries);

        createIndex("CREATE INDEX skinny_x ON %s(x) USING 'sai'");
        waitForTableIndexesQueryable();
        assertNoApacheComponents("skinny_s", "skinny_x");
        assertThat(apacheFiles()).isEmpty();
        assertThat(runQueries(queries)).isEqualTo(before);
        assertThat(execute("SELECT pk FROM %s WHERE x = 42")).isEmpty();
    }

    /**
     * Creating an index Apache didn't write while an entire-SSTable stream holds the sstables replaces their Apache
     * components with ours but leaves the Apache files in place; the next build of the sstables removes them.
     */
    @Test
    public void testNewIndexWhileStreaming() throws Throwable
    {
        createTable(SKINNY);
        disableCompaction();
        createIndex("CREATE INDEX skinny_s ON %s(s) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("skinny");
        assertReadFromApacheComponents("skinny_s");
        List<String> queries = List.of("SELECT pk FROM %s WHERE s = 's3'", "SELECT pk FROM %s WHERE s LIKE 's7%%'");
        var before = runQueries(queries);
        Set<String> apacheFiles = apacheFiles();
        assertThat(apacheFiles).isNotEmpty();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        for (SSTableReader sstable : sstables)
            assertThat(sstable.streamRebuildState().tryBeginStreaming()).isTrue();
        try
        {
            createIndex("CREATE INDEX skinny_x ON %s(x) USING 'sai'");
            waitForTableIndexesQueryable();

            // our components are used...
            for (SSTableReader sstable : sstables)
            {
                IndexDescriptor descriptor = group.descriptorFor(sstable);
                assertThat(descriptor.perSSTableComponents().version()).isEqualTo(Version.current(KEYSPACE));
                assertThat(descriptor.perIndexComponents(getIndexContext("skinny_s")).version()).isEqualTo(Version.current(KEYSPACE));
                assertThat(descriptor.perIndexComponents(getIndexContext("skinny_x")).version()).isEqualTo(Version.current(KEYSPACE));
            }
            assertThat(runQueries(queries)).isEqualTo(before);

            // ...but the Apache files are all still there, and in the TOC
            assertThat(apacheFiles()).isEqualTo(apacheFiles);
            for (SSTableReader sstable : sstables)
                assertThat(Files.readAllLines(sstable.descriptor.fileFor(SSTableFormat.Components.TOC).toPath()))
                .anyMatch(c -> c.startsWith(APACHE_PREFIX));

            // a restart uses our components, doesn't rebuild anything, and leaves the Apache files alone too
            simulateNodeRestart();
            for (SSTableReader sstable : sstables)
                assertThat(group.descriptorFor(sstable).perIndexComponents(getIndexContext("skinny_s")).version()).isEqualTo(Version.current(KEYSPACE));
            assertThat(apacheFiles()).isEqualTo(apacheFiles);
            assertThat(runQueries(queries)).isEqualTo(before);
        }
        finally
        {
            for (SSTableReader sstable : sstables)
                sstable.streamRebuildState().endStreaming();
        }

        // once the stream is over, the Apache files are removed
        waitForAssert(() -> assertThat(apacheFiles()).isEmpty(), 30, TimeUnit.SECONDS);
        assertNoApacheComponents("skinny_s", "skinny_x");
        assertThat(runQueries(queries)).isEqualTo(before);
        createIndex("CREATE INDEX skinny_n ON %s(n) USING 'sai'");
        waitForTableIndexesQueryable();
        assertThat(execute("SELECT pk FROM %s WHERE n > 89")).hasSize(10);
    }

    /**
     * A rebuild of an index that uses the Apache components waits for a concurrent build that replaces them (here,
     * the initial build of a new index), rather than write the same files at the same time.
     */
    @Test
    public void testConcurrentRebuildWaitsForApacheReplacement() throws Throwable
    {
        createTable(SKINNY);
        disableCompaction();
        createIndex("CREATE INDEX skinny_s ON %s(s) USING 'sai'");
        waitForTableIndexesQueryable();
        // A single sstable: the index build executor has a thread for each concurrent build.
        loadSingleApacheSSTable("skinny");
        assertReadFromApacheComponents("skinny_s");
        List<String> queries = List.of("SELECT pk FROM %s WHERE s = 's3'", "SELECT pk FROM %s WHERE s LIKE 's7%%'");
        var before = runQueries(queries);
        assertThat(execute("SELECT pk FROM %s WHERE s = 's3'")).hasSize(5);

        // Every index writer pauses a little once started, so that two builds of an sstable would overlap if allowed
        // to, and we track how many write each sstable at the same time.
        String helper = ApacheLiteralIndexTest.class.getName();
        String writerSSTable = "$0.indexDescriptor.descriptor.toString()";
        Injection trackWriters = Injections.newCustom("track_sai_writers")
                                           .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexWriter.class).onMethod("begin").atEntry())
                                           .add(ActionBuilder.newActionBuilder().actions().doAction(helper + ".writerStarted(" + writerSSTable + ')'))
                                           .build();
        Injection untrackWriters = Injections.newCustom("untrack_sai_writers")
                                             .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexWriter.class).onMethod("complete").atExit())
                                             .add(ActionBuilder.newActionBuilder().actions().doAction(helper + ".writerDone(" + writerSSTable + ')'))
                                             .build();
        WRITERS.clear();
        MAX_CONCURRENT_WRITERS.set(0);
        Injections.inject(trackWriters, untrackWriters);
        try
        {
            String index = createIndexAsync("CREATE INDEX skinny_x ON %s(x) USING 'sai'");
            // the initial build of skinny_x has started (and replaces the Apache components)
            waitForAssert(() -> assertThat(WRITERS).isNotEmpty(), 30, TimeUnit.SECONDS);

            rebuildIndexes("skinny_s");
            waitForIndexQueryable(index);
            waitForTableIndexesQueryable();
        }
        finally
        {
            Injections.deleteAll();
        }
        assertThat(MAX_CONCURRENT_WRITERS.get()).as("concurrent builds of an sstable").isEqualTo(1);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IndexDescriptor descriptor = group.descriptorFor(sstable);
            assertThat(descriptor.perSSTableComponents().isValid(true)).isTrue();
            for (String index : List.of("skinny_s", "skinny_x"))
            {
                IndexComponents.ForRead components = descriptor.perIndexComponents(getIndexContext(index));
                assertThat(components.version()).isEqualTo(Version.current(KEYSPACE));
                assertThat(components.isValid(true)).as("%s on %s", index, sstable).isTrue();
            }
        }
        assertNoApacheComponents("skinny_s", "skinny_x");
        assertThat(runQueries(queries)).isEqualTo(before);
    }

    @Test
    public void testSegmentedChecksumValidation() throws Throwable
    {
        createTable(WIDE);
        disableCompaction();
        createIndex("CREATE INDEX wide_s ON %s(s) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("wide");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IndexComponents.ForRead components = group.descriptorFor(sstable).perIndexComponents(getIndexContext("wide_s"));
            assertThat(components.isValid(false)).isTrue();
            assertThat(components.isValid(true)).isTrue();

            flipMiddleByte(components.get(IndexComponentType.POSTING_LISTS).file());
            assertThat(components.isValid(false)).isTrue();
            assertThat(components.isValid(true)).isFalse();
        }
    }

    /**
     * A failure to validate an Apache component is reported as corruption, not thrown: here, garbage with a valid
     * checksum in the segment metadata, which the per-segment checksum validation of the other components reads.
     */
    @Test
    public void testValidationFailureIsCorruption() throws Throwable
    {
        createTable(WIDE);
        disableCompaction();
        createIndex("CREATE INDEX wide_s ON %s(s) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("wide");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        IndexComponents.ForRead components = group.descriptorFor(cfs.getLiveSSTables().iterator().next()).perIndexComponents(getIndexContext("wide_s"));
        assertThat(components.isValid(true)).isTrue();
        Path meta = components.get(IndexComponentType.META).file().toPath();
        byte[] original = Files.readAllBytes(meta);
        int invalid = 0;
        // every 8-byte window between the header and the footer
        for (int from = 32; from < original.length - 24; from += 8)
        {
            byte[] bytes = original.clone();
            for (int i = from; i < from + 8; i++)
                bytes[i] = (byte) 0x7f;
            CRC32C crc = new CRC32C();
            crc.update(bytes, 0, bytes.length - 8);
            long checksum = crc.getValue();
            for (int i = 0; i < 8; i++)
                bytes[bytes.length - 8 + i] = (byte) (checksum >>> (56 - 8 * i));
            Files.write(meta, bytes);
            // doesn't throw
            if (!components.isValid(true))
                invalid++;
        }
        Files.write(meta, original);
        assertThat(invalid).isGreaterThan(0);
        assertThat(components.isValid(true)).isTrue();
    }

    /**
     * Our per-index components are never used with Apache's per-SSTable components, whose row ids differ (Apache skips
     * rows without live data; in "mismatch" the first row by token only has a deleted s). A build while a stream holds
     * the sstable keeps its Apache files in the TOC, then our per-SSTable components are invalidated.
     */
    @Test
    public void testOurIndexComponentsNotUsedWithApachePerSSTableComponents() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, s text, v int, x text)");
        disableCompaction();
        createIndex("CREATE INDEX mismatch_s ON %s(s) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("mismatch");
        assertReadFromApacheComponents("mismatch_s");
        List<String> queries = List.of("SELECT pk FROM %s WHERE s = 'match'", "SELECT pk FROM %s WHERE s = 'match' AND v = 1 ALLOW FILTERING");
        var before = runQueries(queries);
        assertThat(execute("SELECT pk FROM %s WHERE s = 'match'")).hasSize(2);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        for (SSTableReader sstable : sstables)
            assertThat(sstable.streamRebuildState().tryBeginStreaming()).isTrue();
        try
        {
            createIndex("CREATE INDEX mismatch_x ON %s(x) USING 'sai'");
            waitForTableIndexesQueryable();
            assertThat(runQueries(queries)).isEqualTo(before);

            for (SSTableReader sstable : sstables)
            {
                group.descriptorFor(sstable).perSSTableComponents().invalidate(sstable, cfs.getTracker());
                IndexDescriptor loaded = loadDescriptor(sstable, cfs);
                assertThat(loaded.perSSTableComponents().version()).isEqualTo(Version.APACHE_AA);
                for (String index : List.of("mismatch_s", "mismatch_x"))
                    assertThat(loaded.perIndexComponents(getIndexContext(index)).isComplete()).as(index).isFalse();
            }

            // so a restart rebuilds the indexes
            simulateNodeRestart();
            for (SSTableReader sstable : sstables)
            {
                IndexDescriptor descriptor = group.descriptorFor(sstable);
                assertThat(descriptor.perSSTableComponents().version()).isEqualTo(Version.current(KEYSPACE));
                assertThat(descriptor.perIndexComponents(getIndexContext("mismatch_s")).version()).isEqualTo(Version.current(KEYSPACE));
            }
            assertThat(runQueries(queries)).isEqualTo(before);
        }
        finally
        {
            for (SSTableReader sstable : sstables)
                sstable.streamRebuildState().endStreaming();
        }
        waitForAssert(() -> assertThat(apacheFiles()).isEmpty(), 30, TimeUnit.SECONDS);
        assertNoApacheComponents("mismatch_s", "mismatch_x");
        assertThat(runQueries(queries)).isEqualTo(before);
    }

    /**
     * A build that replaces the Apache components of an sstable owns it until it has published every index it built:
     * a rebuild arriving after our per-SSTable components are written, while per-index ones still are, waits.
     */
    @Test
    public void testRebuildWaitsForTheWholeApacheReplacement() throws Throwable
    {
        createTable(SKINNY);
        disableCompaction();
        createIndex("CREATE INDEX skinny_s ON %s(s) USING 'sai'");
        waitForTableIndexesQueryable();
        loadSingleApacheSSTable("skinny");
        assertReadFromApacheComponents("skinny_s");
        List<String> queries = List.of("SELECT pk FROM %s WHERE s = 's3'", "SELECT pk FROM %s WHERE s LIKE 's7%%'");
        var before = runQueries(queries);

        String helper = ApacheLiteralIndexTest.class.getName();
        Injection pause = pauseAfterPerSSTableComplete();
        Injection began = Injections.newCustom("track_writer_begin")
                                    .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexWriter.class).onMethod("begin").atEntry())
                                    .add(ActionBuilder.newActionBuilder().actions().doAction(helper + ".writerBegan()"))
                                    .build();
        WRITERS_WHILE_PAUSED.set(0);
        Injections.inject(pause, began);
        try
        {
            String index = createIndexAsync("CREATE INDEX skinny_x ON %s(x) USING 'sai'");
            assertThat(ownerPaused.await(30, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Void> rebuild = CompletableFuture.runAsync(() -> rebuildIndexes("skinny_s"));
            Thread.sleep(2000);
            releaseOwner.countDown();
            rebuild.get(60, TimeUnit.SECONDS);
            waitForIndexQueryable(index);
            waitForTableIndexesQueryable();
        }
        finally
        {
            releaseOwner.countDown();
            Injections.deleteAll();
        }
        assertThat(WRITERS_WHILE_PAUSED.get()).as("writers started while the owner was replacing the Apache components").isEqualTo(0);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IndexDescriptor descriptor = group.descriptorFor(sstable);
            assertThat(descriptor.perSSTableComponents().isValid(true)).isTrue();
            for (String name : List.of("skinny_s", "skinny_x"))
                assertThat(descriptor.perIndexComponents(getIndexContext(name)).isValid(true)).as(name).isTrue();
        }
        assertNoApacheComponents("skinny_s", "skinny_x");
        assertThat(runQueries(queries)).isEqualTo(before);
    }

    /**
     * While a build replaces the Apache components of an sstable, a query can use the Apache components of one index
     * and ours of another one: their row ids differ (Apache skips rows without live data), so they aren't compared.
     */
    @Test
    public void testQueryWhileReplacingApacheComponents() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, i int, bi bigint, si smallint, ti tinyint, f float, d double, " +
                    "dec decimal, vi varint, dt date, tm time, ts timestamp, u uuid, tu timeuuid, ip inet, b boolean, t text, a ascii, x text)");
        disableCompaction();
        createIndex("CREATE INDEX types_t ON %s(t) USING 'sai'");
        createIndex("CREATE INDEX types_a ON %s(a) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("types");
        assertReadFromApacheComponents("types_t", "types_a");
        List<String> queries = List.of("SELECT pk FROM %s WHERE t = 't2' AND a = 'a2'",
                                       "SELECT pk FROM %s WHERE t = 't3' AND a IN ('a3', 'a13')",
                                       "SELECT pk FROM %s WHERE t = 't7' OR a = 'a1'",
                                       "SELECT pk FROM %s WHERE t = '' AND a = 'a5'");
        var before = runQueries(queries);
        assertThat(execute("SELECT pk FROM %s WHERE t = 't2' AND a = 'a2'")).isNotEmpty();

        String helper = ApacheLiteralIndexTest.class.getName();
        Injection pause = Injections.newCustom("pause_publication")
                                    .add(InvokePointBuilder.newInvokePoint().onClass(IndexContext.class)
                                                           .onMethod("onSSTableChanged").atExit())
                                    .add(ActionBuilder.newActionBuilder().actions().doAction(helper + ".pauseAfterFirstPublication($0)"))
                                    .build();
        ownerPaused = new CountDownLatch(1);
        releaseOwner = new CountDownLatch(1);
        pauses.set(0);
        Map<String, Object> during;
        Injections.inject(pause);
        try
        {
            String index = createIndexAsync("CREATE INDEX types_x ON %s(x) USING 'sai'");
            assertThat(ownerPaused.await(30, TimeUnit.SECONDS)).isTrue();
            // one of the two indexes uses our components on an sstable, the other one still Apache's
            Set<Version> versions = new HashSet<>();
            for (String name : List.of("types_t", "types_a"))
                getIndexContext(name).getView().getIndexes().forEach(i -> versions.add(i.getVersion()));
            assertThat(versions).contains(Version.APACHE_AA, Version.current(KEYSPACE));
            during = runQueries(queries);
            releaseOwner.countDown();
            waitForIndexQueryable(index);
        }
        finally
        {
            releaseOwner.countDown();
            Injections.deleteAll();
        }
        assertThat(during).isEqualTo(before);
        assertNoApacheComponents("types_t", "types_a", "types_x");
        assertThat(runQueries(queries)).isEqualTo(before);
    }

    /**
     * A failed build that replaces the Apache components of an sstable (here, the initial build of a new index) only
     * makes the index it was asked to build non-queryable: the other indexes keep using the Apache components.
     */
    @Test
    public void testFailedBuildKeepsApacheIndexesQueryable() throws Throwable
    {
        createTable(SKINNY);
        disableCompaction();
        createIndex("CREATE INDEX skinny_s ON %s(s) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("skinny");
        assertReadFromApacheComponents("skinny_s");
        List<String> queries = List.of("SELECT pk FROM %s WHERE s = 's3'", "SELECT pk FROM %s WHERE s LIKE 's7%%'");
        var before = runQueries(queries);
        Set<String> apacheFiles = apacheFiles();

        Injection fail = Injections.newCustom("fail_sai_writer")
                                   .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexWriter.class).onMethod("addRow"))
                                   .add(ActionBuilder.newActionBuilder().actions()
                                                     .doThrow(RuntimeException.class,
                                                              Expression.quote("Injected failure!")))
                                   .build();
        Injections.inject(fail);
        try
        {
            createIndexAsync("CREATE INDEX skinny_x ON %s(x) USING 'sai'");
            waitForIndexBuilds("skinny_x");
        }
        finally
        {
            Injections.deleteAll();
        }
        assertThat(isIndexQueryable(KEYSPACE, "skinny_x")).isFalse();
        assertThat(isIndexQueryable(KEYSPACE, "skinny_s")).isTrue();
        assertReadFromApacheComponents("skinny_s");
        assertThat(apacheFiles()).isEqualTo(apacheFiles);
        assertThat(runQueries(queries)).isEqualTo(before);

        // a rebuild of the failed index replaces the Apache components
        rebuildIndexes("skinny_x");
        waitForTableIndexesQueryable();
        assertNoApacheComponents("skinny_s", "skinny_x");
        assertThat(runQueries(queries)).isEqualTo(before);
    }

    /**
     * NOT CONTAINS finds the rows whose value was deleted in an Apache sstable: in "negative", the second sstable
     * deletes the set of pk 1 (Apache doesn't index it: no live data) that the first one has containing 'x'.
     */
    @Test
    public void testNegativeExpressionWithDeletedValue() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, st set<text>, v int)");
        disableCompaction();
        createIndex("CREATE INDEX negative_st ON %s(st) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("negative");
        assertReadFromApacheComponents("negative_st");
        assertNegativeQueries();
        rebuildIndexes("negative_st");
        waitForTableIndexesQueryable();
        assertNoApacheComponents("negative_st");
        assertNegativeQueries();
    }

    /**
     * Same as {@link #testNegativeExpressionWithDeletedValue}, with the first sstable written by us.
     */
    @Test
    public void testNegativeExpressionWithDeletedValueAfterOurSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, st set<text>, v int)");
        disableCompaction();
        createIndex("CREATE INDEX negative_st ON %s(st) USING 'sai'");
        execute("INSERT INTO %s (pk, st, v) VALUES (1, {'x'}, 1) USING TIMESTAMP 1000");
        execute("INSERT INTO %s (pk, st, v) VALUES (2, {'y'}, 1) USING TIMESTAMP 1000");
        flush();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> ours = cfs.getLiveSSTables();
        Path target = cfs.getDirectories().getDirectoryForNewSSTables().toPath();
        try (Stream<Path> files = Files.list(Paths.get("test/data/legacy-sai/apache-aa/negative")))
        {
            for (Path file : (Iterable<Path>) files::iterator)
                if (file.getFileName().toString().startsWith("nb-2-"))
                    Files.copy(file, target.resolve(file.getFileName()));
        }
        cfs.loadNewSSTables();
        waitForTableIndexesQueryable();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertThat(group.descriptorFor(sstable).perIndexComponents(getIndexContext("negative_st")).version())
            .isEqualTo(ours.contains(sstable) ? Version.current(KEYSPACE) : Version.APACHE_AA);
        assertNegativeQueries();
        rebuildIndexes("negative_st");
        waitForTableIndexesQueryable();
        assertNoApacheComponents("negative_st");
        assertNegativeQueries();
    }

    private void assertNegativeQueries()
    {
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE st NOT CONTAINS 'x'"), row(1), row(2), row(3));
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE st NOT CONTAINS 'y'"), row(1), row(3));
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE st CONTAINS 'x'"));
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE st NOT CONTAINS 'x' AND v = 1 ALLOW FILTERING"), row(1), row(2), row(3));
    }
}
