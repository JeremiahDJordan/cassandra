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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.FileUtils;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadataRef;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The life of indexes over the SAI components Apache Cassandra 5.0 wrote, next to ours: static column intersections
 * over both kinds of SSTables, dropping and re-creating indexes, and builds that replace the Apache components.
 */
public class ApacheIndexLifecycleTest extends ApacheIndexTester
{
    private static final String ANALYZED = "CREATE TABLE %s (pk int PRIMARY KEY, t_ci text, t_norm text, " +
                                           "t_ascii text, t_all text, a_ci ascii)";

    private static volatile boolean failArmed;
    private static volatile ColumnFamilyStore touchedTable;

    // called by byteman
    @SuppressWarnings("unused")
    public static void failOnce()
    {
        if (failArmed)
        {
            failArmed = false;
            throw new RuntimeException("Injected failure after the per-SSTable components!");
        }
    }

    // called by byteman: what any other thread that looks at the descriptors of the table does (another index's
    // initialization or pre-join task, a build of another index...)
    @SuppressWarnings("unused")
    public static void touchDescriptors()
    {
        ColumnFamilyStore cfs = touchedTable;
        if (cfs == null)
            return;
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        if (group == null)
            return;
        for (SSTableReader sstable : cfs.getLiveSSTables())
            group.descriptorFor(sstable);
    }

    @After
    public void resetOptimizerAndInjections()
    {
        CassandraRelevantProperties.SAI_QUERY_OPT_LEVEL.setInt(1);
        Injections.deleteAll();
    }

    /**
     * Intersections of a static column index with other indexes over the Apache sstables of "wide", then with our
     * rows in a memtable and in a flushed sstable, with the query optimizer and without.
     */
    @Test
    public void testStaticColumnIntersectionsOverMixedFormats() throws Throwable
    {
        for (int stage = 0; stage < 3; stage++)
        {
            String plain = createTable(WIDE);
            disableCompaction();
            loadApacheSSTables(plain, "wide");
            String indexed = createTable(WIDE);
            disableCompaction();
            for (String column : new String[]{ "s", "n", "st" })
                createIndex("CREATE INDEX wide_" + column + " ON %s(" + column + ") USING 'sai'");
            waitForTableIndexesQueryable();
            loadApacheSSTables(indexed, "wide");
            assertReadFromApacheComponents("wide_s", "wide_n", "wide_st");

            if (stage >= 1)
            {
                for (int pk = 0; pk < 130; pk += 7)
                {
                    both(indexed, plain, "INSERT INTO %s (pk, ck, st, s, n) VALUES (?, ?, ?, ?, ?)", pk, 1, 2, "w1", (long) (pk * 10));
                    both(indexed, plain, "UPDATE %s SET s = 'w0' WHERE pk = ? AND ck = 2", pk + 1);
                    both(indexed, plain, "UPDATE %s SET st = 7 WHERE pk = ?", pk + 2);
                    both(indexed, plain, "DELETE FROM %s WHERE pk = ? AND ck = 0", pk + 3);
                }
                if (stage == 2)
                {
                    flush(KEYSPACE, indexed);
                    flush(KEYSPACE, plain);
                }
            }
            String s = "SELECT pk, ck FROM %s WHERE ";
            assertSameRowsAsFiltering(List.of(s + "st = 2 AND s = 'w1'",
                                              s + "st = 2 AND n > 300",
                                              s + "st = 7 AND s = 'w0'",
                                              s + "st = 0 AND s = 'w2' AND n < 600",
                                              s + "st = 2 OR s = 'w1'",
                                              s + "st = 7 OR n < 20",
                                              s + "st = 2",
                                              s + "s = 'w1' AND n > 400 AND n < 800",
                                              s + "pk = 42 AND st = 2 AND s = 'w1'",
                                              s + "pk = 9 AND st = 7 AND n >= 0",
                                              s + "token(pk) > 0 AND st = 2 AND s = 'w1'",
                                              s + "st != 2 AND s = 'w1'",
                                              s + "st = 2 AND s = 'w1' LIMIT 9",
                                              "SELECT pk FROM %s WHERE st = 2 AND s = 'w1' PER PARTITION LIMIT 1"),
                                      indexed, plain);
            for (String column : new String[]{ "s", "n", "st" })
                dropIndex("DROP INDEX %s.wide_" + column);
        }
    }

    /**
     * The same with a composite partition key, two static columns and partitions that only have a static row
     * ("statics").
     */
    @Test
    public void testStaticColumnIntersectionsOverMixedFormatsWithCompositeKey() throws Throwable
    {
        for (int stage = 0; stage < 3; stage++)
        {
            String plain = createTable(STATICS);
            disableCompaction();
            loadApacheSSTables(plain, "statics");
            String indexed = createTable(STATICS);
            disableCompaction();
            for (String column : new String[]{ "st", "sti", "v", "ck" })
                createIndex("CREATE INDEX statics_" + column + " ON %s(" + column + ") USING 'sai'");
            waitForTableIndexesQueryable();
            loadApacheSSTables(indexed, "statics");
            assertReadFromApacheComponents("statics_st", "statics_sti", "statics_v", "statics_ck");

            if (stage >= 1)
            {
                // the statics data has partitions (pk1, 'p<n>') with pk1 in [0, 9], st = 'st<0-3>', sti in [0, 5]
                for (int p = 0; p < 100; p += 5)
                {
                    both(indexed, plain, "UPDATE %s SET sti = 2 WHERE pk1 = ? AND pk2 = ?", p / 10, "p" + p % 10);
                    both(indexed, plain, "INSERT INTO %s (pk1, pk2, ck, v) VALUES (?, ?, ?, ?)", p / 10, "p" + p % 10, 77, 5);
                }
                if (stage == 2)
                {
                    flush(KEYSPACE, indexed);
                    flush(KEYSPACE, plain);
                }
            }
            String s = "SELECT pk1, pk2, ck FROM %s WHERE ";
            assertSameRowsAsFiltering(List.of(s + "st = 'st1' AND v = 5",
                                              s + "sti = 2 AND v = 5",
                                              s + "sti = 2 AND v >= 5",
                                              s + "sti = 2 AND st = 'st1'",
                                              s + "sti >= 2 AND v = 5 AND ck > 1",
                                              s + "sti = 2 OR v = 5",
                                              s + "sti = 2",
                                              s + "sti = 2 AND ck = 77",
                                              s + "sti != 2 AND v = 5"),
                                      indexed, plain);
            for (String column : new String[]{ "st", "sti", "v", "ck" })
                dropIndex("DROP INDEX %s.statics_" + column);
        }
    }

    /**
     * An index dropped after a failed rebuild, then re-created under its name on another column, doesn't read the
     * dropped index's Apache components.
     */
    @Test
    public void testRecreateIndexAfterFailedRebuild() throws Throwable
    {
        String plain = createTable(TYPES);
        disableCompaction();
        loadApacheSSTables(plain, "types");
        String indexed = createTable(TYPES);
        disableCompaction();
        createIndex("CREATE INDEX types_t ON %s(t) USING 'sai'");
        createIndex("CREATE INDEX types_i ON %s(i) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables(indexed, "types");
        assertReadFromApacheComponents("types_t", "types_i");

        Injections.inject(Injections.newCustom("fail_sai_writer")
                                    .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexWriter.class).onMethod("addRow"))
                                    .add(ActionBuilder.newActionBuilder().actions().doThrow(RuntimeException.class, Expression.quote("Injected failure!")))
                                    .build());
        try
        {
            rebuildIndexes("types_t");
        }
        catch (Throwable t)
        {
            // expected
        }
        finally
        {
            Injections.deleteAll();
        }
        assertThat(isIndexQueryable(KEYSPACE, "types_t")).isFalse();
        assertThat(apacheFilesOf("types_t")).isNotEmpty();

        dropIndex("DROP INDEX %s.types_t");
        assertNotListedAndDeleted("types_t");
        createIndex("CREATE INDEX types_t ON %s(a) USING 'sai'");
        waitForTableIndexesQueryable();
        assertSameRowsAsFiltering(List.of("SELECT pk FROM %s WHERE a = 'a3'",
                                          "SELECT pk FROM %s WHERE a = 'a13'",
                                          "SELECT pk FROM %s WHERE i > 400",
                                          "SELECT pk FROM %s WHERE a = 'a3' AND i > 0"),
                                  indexed, plain);
    }

    /**
     * The same, with the index dropped while it is rebuilt (so it isn't in its view either).
     */
    @Test
    public void testRecreateIndexDroppedDuringItsRebuild() throws Throwable
    {
        String plain = createTable(TYPES);
        disableCompaction();
        loadApacheSSTables(plain, "types");
        String indexed = createTable(TYPES);
        disableCompaction();
        createIndex("CREATE INDEX types_t ON %s(t) USING 'sai'");
        createIndex("CREATE INDEX types_i ON %s(i) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables(indexed, "types");
        assertReadFromApacheComponents("types_t", "types_i");

        Injections.inject(pauseAfterPerSSTableComplete());
        try
        {
            CompletableFuture<Void> rebuild = CompletableFuture.runAsync(() -> rebuildIndexes("types_t"));
            assertThat(ownerPaused.await(30, TimeUnit.SECONDS)).isTrue();
            dropIndex("DROP INDEX %s.types_t");
            // unregistered right away, deleted once the rebuild (which holds the sstables) is over
            for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
                assertThat(toc(sstable)).noneMatch(c -> c.contains("+types_t+"));
            releaseOwner.countDown();
            try
            {
                rebuild.get(60, TimeUnit.SECONDS);
            }
            catch (Throwable t)
            {
                // the rebuild of a dropped index stops
            }
        }
        finally
        {
            releaseOwner.countDown();
            Injections.deleteAll();
        }
        assertNotListedAndDeleted("types_t");
        createIndex("CREATE INDEX types_t ON %s(a) USING 'sai'");
        waitForTableIndexesQueryable();
        assertSameRowsAsFiltering(List.of("SELECT pk FROM %s WHERE a = 'a3'",
                                          "SELECT pk FROM %s WHERE a = 'a13'",
                                          "SELECT pk FROM %s WHERE i > 400",
                                          "SELECT pk FROM %s WHERE a = 'a3' AND i > 0"),
                                  indexed, plain);
    }

    /**
     * CREATE INDEX builds the index rather than read the Apache components already on disk under its name; the Apache
     * components of sstables loaded afterwards are read.
     */
    @Test
    public void testCreateIndexDoesNotReadApacheComponentsAlreadyOnDisk() throws Throwable
    {
        createTable(SKINNY);
        disableCompaction();
        loadApacheSSTables("skinny");
        assertThat(apacheFilesOf("skinny_s")).isNotEmpty();
        createIndex("CREATE INDEX skinny_s ON %s(s) USING 'sai'");
        waitForTableIndexesQueryable();
        assertNoApacheComponents("skinny_s");
        assertThat(execute("SELECT pk FROM %s WHERE s = 's3'")).hasSize(10);
        dropIndex("DROP INDEX %s.skinny_s");

        createTable(SKINNY);
        disableCompaction();
        createIndex("CREATE INDEX skinny_s ON %s(s) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("skinny");
        assertReadFromApacheComponents("skinny_s");
        assertThat(execute("SELECT pk FROM %s WHERE s = 's3'")).hasSize(10);
    }

    /**
     * A rebuild of one index replaces the Apache components, so it also builds the other indexes: one of those is
     * dropped meanwhile, which must not fail the rebuild.
     */
    @Test
    public void testDropOtherIndexDuringRebuild() throws Throwable
    {
        createTable(SKINNY);
        disableCompaction();
        createIndex("CREATE INDEX skinny_s ON %s(s) USING 'sai'");
        createIndex("CREATE INDEX skinny_n ON %s(n) USING 'sai'");
        waitForTableIndexesQueryable();
        loadSingleApacheSSTable("skinny");
        assertReadFromApacheComponents("skinny_s", "skinny_n");
        List<String> queries = List.of("SELECT pk FROM %s WHERE s = 's3'", "SELECT pk FROM %s WHERE s LIKE 's7%%'");
        Map<String, Object> before = runQueries(queries);

        Injections.inject(pauseAfterPerSSTableComplete());
        Throwable failure = null;
        try
        {
            CompletableFuture<Void> rebuild = CompletableFuture.runAsync(() -> rebuildIndexes("skinny_s"));
            assertThat(ownerPaused.await(30, TimeUnit.SECONDS)).isTrue();
            dropIndex("DROP INDEX %s.skinny_n");
            releaseOwner.countDown();
            try
            {
                rebuild.get(60, TimeUnit.SECONDS);
            }
            catch (Throwable t)
            {
                failure = t;
            }
        }
        finally
        {
            releaseOwner.countDown();
            Injections.deleteAll();
        }
        assertThat(failure).as("rebuild of skinny_s").isNull();
        waitForTableIndexesQueryable();
        assertThat(isIndexQueryable(KEYSPACE, "skinny_s")).isTrue();
        assertNoApacheComponents("skinny_s");
        assertThat(runQueries(queries)).isEqualTo(before);
        waitForAssert(() -> assertThat(sstableFiles("-SAI")).noneMatch(n -> n.contains("skinny_n")),
                      30, TimeUnit.SECONDS);
    }

    /**
     * The removal of replaced Apache components leaves alone our per-SSTable components that a build has written but
     * not registered yet.
     */
    @Test
    public void testApacheComponentsNotRemovedBeforeTheirReplacementIsRegistered() throws Throwable
    {
        createTable(SKINNY);
        disableCompaction();
        createIndex("CREATE INDEX skinny_s ON %s(s) USING 'sai'");
        createIndex("CREATE INDEX skinny_n ON %s(n) USING 'sai'");
        waitForTableIndexesQueryable();
        loadSingleApacheSSTable("skinny");
        assertReadFromApacheComponents("skinny_s", "skinny_n");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        Set<String> before = apacheFiles();

        Injections.inject(pauseAfterPerSSTableComplete());
        Set<String> during;
        try
        {
            // the initial build of a new index doesn't hold the stream/rebuild reservation
            String index = createIndexAsync("CREATE INDEX skinny_x ON %s(x) USING 'sai'");
            assertThat(ownerPaused.await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(group.descriptorFor(sstable).perSSTableComponents().version().isApache()).isFalse();
            var removal = StorageAttachedIndexGroup.class.getDeclaredMethod("removeReplacedApacheComponents", SSTableReader.class);
            removal.setAccessible(true);
            removal.invoke(group, sstable);
            during = apacheFiles();
            releaseOwner.countDown();
            waitForIndexQueryable(index);
        }
        finally
        {
            releaseOwner.countDown();
            Injections.deleteAll();
        }
        assertThat(during).isEqualTo(before);
        assertNoApacheComponents("skinny_s", "skinny_n", "skinny_x");
    }

    /**
     * A build that replaces the Apache components and fails once our per-SSTable components are complete leaves the
     * Apache components in use, and its retry rebuilds every index.
     */
    @Test
    public void testRetryAfterFailureOnceOurPerSSTableComponentsAreComplete() throws Throwable
    {
        createTable(WIDE);
        disableCompaction();
        for (String column : new String[]{ "s", "n", "st" })
            createIndex("CREATE INDEX wide_" + column + " ON %s(" + column + ") USING 'sai'");
        waitForTableIndexesQueryable();
        loadSingleApacheSSTable("wide");
        assertReadFromApacheComponents("wide_s", "wide_n", "wide_st");
        List<String> queries = List.of("SELECT pk, ck FROM %s WHERE s = 'w1'",
                                       "SELECT pk, ck FROM %s WHERE n > 400 AND n < 800",
                                       "SELECT pk, ck FROM %s WHERE st = 2",
                                       "SELECT pk, ck FROM %s WHERE s = 'w1' AND n < 500");
        Map<String, Object> before = runQueries(queries);
        Set<String> apacheBefore = apacheFiles();

        failArmed = true;
        Injections.inject(Injections.newCustom("fail_after_per_sstable_complete")
                                    .add(afterPerSSTableWriterComplete())
                                    .add(ActionBuilder.newActionBuilder().actions().doAction(ApacheIndexLifecycleTest.class.getName() + ".failOnce()"))
                                    .build());
        try
        {
            rebuildIndexes("wide_s");
        }
        catch (Throwable t)
        {
            // expected
        }
        finally
        {
            Injections.deleteAll();
        }
        // the other indexes, and the descriptors, still use the Apache components, and none of ours is left
        assertReadFromApacheComponents("wide_n", "wide_st");
        assertThat(apacheFiles()).isEqualTo(apacheBefore);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            assertThat(group.descriptorFor(sstable).perSSTableComponents().version()).isEqualTo(Version.APACHE_AA);
            String ours = sstable.descriptor.baseFile().name() + "-SAI+" +
                          Version.current(KEYSPACE).versionString() + '+';
            assertThat(sstableFiles("-SAI")).noneMatch(n -> n.startsWith(ours));
        }

        rebuildIndexes("wide_s");
        waitForTableIndexesQueryable();
        assertNoApacheComponents("wide_s", "wide_n", "wide_st");
        assertThat(runQueries(queries)).isEqualTo(before);
        simulateNodeRestart();
        assertNoApacheComponents("wide_s", "wide_n", "wide_st");
        assertThat(runQueries(queries)).isEqualTo(before);
    }

    /**
     * An index created on another column under the name of Apache files in the sstables ("types_t"), whose initial
     * build fails, still doesn't use those files after a restart.
     */
    @Test
    public void testRestartAfterFailedInitialBuild() throws Throwable
    {
        String plain = createTable(TYPES);
        disableCompaction();
        loadApacheSSTables(plain, "types");
        String indexed = createTable(TYPES);
        disableCompaction();
        loadApacheSSTables(indexed, "types");
        assertThat(apacheFilesOf("types_t")).isNotEmpty();

        String index = createIndexWithFailedInitialBuild("CREATE INDEX types_t ON %s(a) USING 'sai'");
        assertNotListedAndDeleted("types_t");

        simulateNodeRestart();
        assertThat(isIndexQueryable(KEYSPACE, index)).isTrue();
        assertSameRowsAsFiltering(List.of("SELECT pk FROM %s WHERE a = 'a3'", "SELECT pk FROM %s WHERE a = 'a13'"), indexed, plain);
    }

    /**
     * The same with an index of the same name on the same column, with other options (Apache's "analyzed_ci" was
     * case-insensitive, the new one is case-sensitive).
     */
    @Test
    public void testRestartAfterFailedInitialBuildOtherOptions() throws Throwable
    {
        String plain = createTable(ANALYZED);
        disableCompaction();
        loadApacheSSTables(plain, "analyzed");
        String indexed = createTable(ANALYZED);
        disableCompaction();
        loadApacheSSTables(indexed, "analyzed");
        assertThat(apacheFilesOf("analyzed_ci")).isNotEmpty();

        String index = createIndexWithFailedInitialBuild("CREATE INDEX analyzed_ci ON %s(t_ci) USING 'sai'");
        assertNotListedAndDeleted("analyzed_ci");

        simulateNodeRestart();
        assertThat(isIndexQueryable(KEYSPACE, index)).isTrue();
        assertSameRowsAsFiltering(List.of("SELECT pk FROM %s WHERE t_ci = 'Alpha'",
                                          "SELECT pk FROM %s WHERE t_ci = 'alpha'",
                                          "SELECT pk FROM %s WHERE t_ci = 'ALPHA'",
                                          "SELECT pk FROM %s WHERE t_ci = 'Beta'"),
                                  indexed, plain);
    }

    /**
     * {@link #testRestartAfterFailedInitialBuild} with the table loaded again from its directory, the way a node loads
     * it when it starts (a new index instance, created with the table).
     */
    @Test
    public void testTableLoadedAgainAfterFailedInitialBuild() throws Throwable
    {
        createTable(TYPES);
        disableCompaction();
        loadApacheSSTables(currentTable(), "types");
        String index = createIndexWithFailedInitialBuild("CREATE INDEX types_t ON %s(a) USING 'sai'");
        assertNotListedAndDeleted("types_t");

        ColumnFamilyStore loaded = loadTableAgain();
        try
        {
            StorageAttachedIndex reloaded = (StorageAttachedIndex) loaded.indexManager.getIndexByName(index);
            assertThat(reloaded.getIndexContext().getView().getIndexes()).noneMatch(i -> i.getVersion().isApache());
        }
        finally
        {
            loaded.unloadCf();
        }
    }

    /**
     * The in-place upgrade: the Apache sstables are in the table's directory when the table and its indexes are
     * loaded, and the indexes use the Apache files without changing any.
     */
    @Test
    public void testInPlaceUpgrade() throws Throwable
    {
        createTable(SKINNY);
        disableCompaction();
        createIndex("CREATE INDEX skinny_s ON %s(s) USING 'sai'");
        createIndex("CREATE INDEX skinny_n ON %s(n) USING 'sai'");
        waitForTableIndexesQueryable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        FileUtils.copySSTablesAndIndexes(dir.toJavaIOFile(), "apache-aa/skinny");
        Set<String> before = Arrays.stream(dir.tryList())
                                   .map(File::name)
                                   .filter(n -> n.contains("-SAI"))
                                   .collect(Collectors.toSet());
        assertThat(before).isNotEmpty();

        ColumnFamilyStore loaded = loadTableAgain();
        try
        {
            assertThat(loaded.getLiveSSTables()).hasSize(2);
            for (String name : List.of("skinny_s", "skinny_n"))
            {
                StorageAttachedIndex index = (StorageAttachedIndex) loaded.indexManager.getIndexByName(name);
                assertThat(loaded.indexManager.isIndexQueryable(index)).as(name).isTrue();
                assertThat(index.getIndexContext().getView().getIndexes()).hasSize(2)
                                                                         .allMatch(i -> i.getVersion().isApache());
            }
            Set<String> after = Arrays.stream(dir.tryList())
                                      .map(File::name)
                                      .filter(n -> n.contains("-SAI"))
                                      .collect(Collectors.toSet());
            assertThat(after).isEqualTo(before);
        }
        finally
        {
            loaded.unloadCf();
        }
    }

    /**
     * The descriptors loaded when an index is added to an existing table don't include that index yet, or they would
     * keep the Apache group of its name. The load of the descriptors is injected.
     */
    @Test
    public void testDescriptorsLoadedWhenTheIndexIsCreated() throws Throwable
    {
        String plain = createTable(TYPES);
        disableCompaction();
        loadApacheSSTables(plain, "types");
        String indexed = createTable(TYPES);
        disableCompaction();
        // another index, reading the Apache components, so that the group and its cached descriptors exist
        createIndex("CREATE INDEX types_i ON %s(i) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables(indexed, "types");
        assertReadFromApacheComponents("types_i");
        assertThat(apacheFilesOf("types_t")).isNotEmpty();

        touchedTable = getCurrentColumnFamilyStore();
        Injections.inject(Injections.newCustom("touch_descriptors")
                                    .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndex.class).onMethod("onAddedToExistingTable"))
                                    .add(ActionBuilder.newActionBuilder().actions().doAction(ApacheIndexLifecycleTest.class.getName() + ".touchDescriptors()"))
                                    .build());
        try
        {
            createIndex("CREATE INDEX types_t ON %s(a) USING 'sai'");
            waitForTableIndexesQueryable();
        }
        finally
        {
            touchedTable = null;
            Injections.deleteAll();
        }
        assertSameRowsAsFiltering(List.of("SELECT pk FROM %s WHERE a = 'a3'", "SELECT pk FROM %s WHERE a = 'a13'"), indexed, plain);
    }

    /**
     * An index in its view is dropped while an entire-sstable stream holds its sstables: its Apache files, which the
     * stream may still send, are only deleted once the stream is over.
     */
    @Test
    public void testDropInViewWhileStreamed() throws Throwable
    {
        createTable(TYPES);
        disableCompaction();
        createIndex("CREATE INDEX types_t ON %s(t) USING 'sai'");
        createIndex("CREATE INDEX types_i ON %s(i) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables(currentTable(), "types");
        assertReadFromApacheComponents("types_t", "types_i");
        Set<String> before = apacheFilesOf("types_t");

        List<SSTableReader> streamed = new ArrayList<>(getCurrentColumnFamilyStore().getLiveSSTables());
        for (SSTableReader sstable : streamed)
            assertThat(sstable.streamRebuildState().tryBeginStreaming()).isTrue();
        Set<String> during;
        try
        {
            dropIndex("DROP INDEX %s.types_t");
            for (SSTableReader sstable : streamed)
                assertThat(toc(sstable)).noneMatch(c -> c.contains("+types_t+"));
            Thread.sleep(3000);
            during = apacheFilesOf("types_t");
        }
        finally
        {
            streamed.forEach(s -> s.streamRebuildState().endStreaming());
        }
        assertThat(during).as("Apache files of the dropped index while a stream holds the sstables").isEqualTo(before);
        waitForAssert(() -> assertThat(apacheFilesOf("types_t")).isEmpty(), 30, TimeUnit.SECONDS);
        assertReadFromApacheComponents("types_i");
    }

    /**
     * CREATE INDEX deletes the Apache files of an index of its name right away, also while an entire-sstable stream
     * holds the sstables.
     */
    @Test
    public void testCreateIndexWhileStreamed() throws Throwable
    {
        createTable(SKINNY);
        disableCompaction();
        loadApacheSSTables("skinny");
        assertThat(apacheFilesOf("skinny_s")).isNotEmpty();

        List<SSTableReader> streamed = new ArrayList<>(getCurrentColumnFamilyStore().getLiveSSTables());
        for (SSTableReader sstable : streamed)
            assertThat(sstable.streamRebuildState().tryBeginStreaming()).isTrue();
        try
        {
            createIndexAsync("CREATE INDEX skinny_s ON %s(s) USING 'sai'");
            for (SSTableReader sstable : streamed)
                assertThat(toc(sstable)).noneMatch(c -> c.startsWith(APACHE_PREFIX + "skinny_s+"));
            assertThat(apacheFilesOf("skinny_s")).as("Apache files of the index's name while a stream holds the sstables").isEmpty();
        }
        finally
        {
            streamed.forEach(s -> s.streamRebuildState().endStreaming());
        }
        waitForTableIndexesQueryable();
        assertThat(execute("SELECT pk FROM %s WHERE s = 's3'")).hasSize(10);
    }

    /**
     * A query holds the readers of an index over Apache components across its DROP and the CREATE of an index of the
     * same name: CREATE INDEX deletes the files that DROP INDEX left to the readers.
     */
    @Test
    public void testReaderHeldAcrossDropAndCreate() throws Throwable
    {
        testReaderHeldAcrossDropAndCreate("CREATE INDEX types_t ON %s(t) USING 'sai'");
    }

    /**
     * Same, when the index created with that name isn't a storage-attached index, so nothing deletes the Apache files on
     * CREATE: the release of the readers must delete them.
     */
    @Test
    public void testReaderHeldAcrossDropAndCreateOfLegacyIndex() throws Throwable
    {
        testReaderHeldAcrossDropAndCreate("CREATE INDEX types_t ON %s(t)");
    }

    private void testReaderHeldAcrossDropAndCreate(String create) throws Throwable
    {
        boolean sai = create.contains("'sai'");
        createTable(TYPES);
        disableCompaction();
        createIndex("CREATE INDEX types_t ON %s(t) USING 'sai'");
        createIndex("CREATE INDEX types_i ON %s(i) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables(currentTable(), "types");
        assertReadFromApacheComponents("types_t", "types_i");

        List<SSTableIndex> held = new ArrayList<>(getIndexContext("types_t").getView().getIndexes());
        assertThat(held).isNotEmpty();
        for (SSTableIndex index : held)
            assertThat(index.reference()).isTrue();
        try
        {
            dropIndex("DROP INDEX %s.types_t");
            assertThat(apacheFilesOf("types_t")).as("Apache files of the dropped index, left to its readers").isNotEmpty();
            createIndexAsync(create);
            if (sai)
                assertThat(apacheFilesOf("types_t")).as("Apache files of the index's name once it is created").isEmpty();
        }
        finally
        {
            held.forEach(SSTableIndex::release);
        }
        waitForTableIndexesQueryable();
        waitForAssert(() -> assertThat(apacheFilesOf("types_t")).isEmpty(), 30, TimeUnit.SECONDS);
    }

    /** Creates an index with the provided statement while its initial build fails, and returns its name. */
    private String createIndexWithFailedInitialBuild(String statement) throws Throwable
    {
        Injections.inject(Injections.newCustom("fail_initial_build")
                                    .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexWriter.class).onMethod("addRow"))
                                    .add(ActionBuilder.newActionBuilder().actions().doThrow(RuntimeException.class, Expression.quote("Injected failure!")))
                                    .build());
        try
        {
            String index = createIndexAsync(statement);
            waitForIndexBuilds(index);
            assertThat(isIndexQueryable(KEYSPACE, index)).isFalse();
            return index;
        }
        finally
        {
            Injections.deleteAll();
        }
    }

    /**
     * Loads the current table again from its directory, the way a node does when it starts: its indexes are created
     * with the table, and their initial builds are awaited. The caller unloads it.
     */
    private ColumnFamilyStore loadTableAgain() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        TableMetadataRef metadata = Schema.instance.getTableMetadataRef(KEYSPACE, currentTable());
        ColumnFamilyStore loaded = ColumnFamilyStore.createColumnFamilyStore(keyspace, currentTable(), metadata, new Directories(metadata.get()), true, false, true);
        try
        {
            loaded.awaitInitialIndexBuilds(60, TimeUnit.SECONDS);
        }
        catch (Throwable t)
        {
            // checked by the callers
        }
        return loaded;
    }

    /**
     * Checks the provided queries return the same rows from the indexed table as from the plain one with ALLOW
     * FILTERING, with the query optimizer and without.
     */
    private void assertSameRowsAsFiltering(List<String> queries, String indexed, String plain)
    {
        for (int level : new int[]{ 1, 0 })
        {
            CassandraRelevantProperties.SAI_QUERY_OPT_LEVEL.setInt(level);
            for (String query : queries)
            {
                List<List<Object>> expected = rows(executeFormattedQuery(String.format(query + " ALLOW FILTERING", KEYSPACE + '.' + plain)));
                List<List<Object>> actual = rows(executeFormattedQuery(String.format(query, KEYSPACE + '.' + indexed)));
                assertThat(actual).as("%s at optimization level %s", query, level).isEqualTo(expected);
            }
        }
        CassandraRelevantProperties.SAI_QUERY_OPT_LEVEL.setInt(1);
    }

    private void both(String indexed, String plain, String statement, Object... values)
    {
        executeFormattedQuery(String.format(statement, KEYSPACE + '.' + indexed), values);
        executeFormattedQuery(String.format(statement, KEYSPACE + '.' + plain), values);
    }

    private static List<List<Object>> rows(UntypedResultSet resultSet)
    {
        List<List<Object>> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : resultSet)
        {
            List<Object> values = new ArrayList<>();
            for (var column : row.getColumns())
                values.add(row.getBytes(column.name.toString()));
            rows.add(values);
        }
        return rows;
    }

    private Set<String> apacheFilesOf(String index)
    {
        return apacheFiles().stream().filter(n -> n.contains('+' + index + '+')).collect(Collectors.toSet());
    }

    /**
     * Checks no live sstable lists Apache components of the provided index in its TOC, and that their files are
     * (eventually) deleted.
     */
    private void assertNotListedAndDeleted(String index) throws Exception
    {
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
            assertThat(toc(sstable)).noneMatch(c -> c.startsWith(APACHE_PREFIX + index + '+'));
        waitForAssert(() -> assertThat(apacheFilesOf(index)).isEmpty(), 30, TimeUnit.SECONDS);
    }

    private static List<String> toc(SSTableReader sstable) throws Exception
    {
        return Files.readAllLines(sstable.descriptor.fileFor(SSTableFormat.Components.TOC).toPath());
    }
}
