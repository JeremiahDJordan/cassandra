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

package org.apache.cassandra.index.sai.functional;

import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.TOCComponent;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.streaming.StreamState;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The TOC and the reader of an SSTable written by an operation other than flush or compaction list only the SAI
 * components that were written.
 */
public class IndexComponentsInTOCTest extends SAITester
{
    @BeforeClass
    public static void setUpNetwork()
    {
        requireNetwork();
    }

    @Test
    public void testStreamWithEmptyIndex() throws Throwable
    {
        ColumnFamilyStore cfs = createTableWithEmptyIndex();
        Set<Descriptor> before = liveDescriptors(cfs);
        StaleSameNameIndexComponentsStreamingTest.streamHalfToSelf(cfs).get(60, TimeUnit.SECONDS);
        assertThat(liveDescriptors(cfs)).hasSize(2).containsAll(before);
        assertOnlyWrittenComponentsListed();
    }

    @Test
    public void testStreamWithIndexDroppedWhileWritten() throws Throwable
    {
        ColumnFamilyStore cfs = createTable(false);
        Set<Descriptor> before = liveDescriptors(cfs);

        Injections.Hold writerHold = Injections.newHold("writer").add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexWriter.class).onMethod("complete").atEntry()).build();
        Injections.inject(writerHold);
        try
        {
            Future<StreamState> stream = StaleSameNameIndexComponentsStreamingTest.streamHalfToSelf(cfs);
            writerHold.awaitHeld();
            dropIndex("DROP INDEX %s.t_idx");
            writerHold.release();
            stream.get(60, TimeUnit.SECONDS);
        }
        finally
        {
            Injections.deleteAll();
            Injections.Hold.releaseAll();
        }

        assertThat(liveDescriptors(cfs)).hasSize(2).containsAll(before);
        assertThat(indexFiles()).noneMatch(file -> file.name().contains("t_idx"));
        assertOnlyWrittenComponentsListed();
    }

    @Test
    public void testUpgradeSSTablesWithEmptyIndex() throws Throwable
    {
        ColumnFamilyStore cfs = createTableWithEmptyIndex();
        Set<Descriptor> before = liveDescriptors(cfs);
        upgradeSSTables();
        assertThat(liveDescriptors(cfs)).hasSize(1).doesNotContainAnyElementsOf(before);
        assertOnlyWrittenComponentsListed();
    }

    @Test
    public void testGarbageCollectWithEmptyIndex() throws Throwable
    {
        ColumnFamilyStore cfs = createTableWithEmptyIndex();
        Set<Descriptor> before = liveDescriptors(cfs);
        CompactionManager.instance.performGarbageCollection(cfs, CompactionParams.TombstoneOption.ROW, 1);
        assertThat(liveDescriptors(cfs)).hasSize(1).doesNotContainAnyElementsOf(before);
        assertOnlyWrittenComponentsListed();
    }

    private ColumnFamilyStore createTableWithEmptyIndex() throws Throwable
    {
        return createTable(true);
    }

    /** Table with indexes on columns t and o, and one SSTable of 100 rows, possibly with an index on e, never written. */
    private ColumnFamilyStore createTable(boolean emptyIndex) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, t text, o int, e int)");
        disableCompaction();
        createIndex("CREATE INDEX t_idx ON %s(t) USING 'sai'");
        createIndex("CREATE INDEX o_idx ON %s(o) USING 'sai'");
        if (emptyIndex)
            createIndex("CREATE INDEX e_idx ON %s(e) USING 'sai'");
        for (int pk = 0; pk < 100; pk++)
            execute("INSERT INTO %s (pk, t, o) VALUES (?, ?, ?)", pk, "t" + pk, pk);
        flush();
        return getCurrentColumnFamilyStore();
    }

    /** The TOC of every live SSTable lists exactly the SAI files on disk, and its reader the components of its TOC. */
    private void assertOnlyWrittenComponentsListed() throws Throwable
    {
        LifecycleTransaction.waitForDeletions();
        assertIndexFilesInToc(indexFiles());
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
            assertThat(sstable.components()).isEqualTo(TOCComponent.loadTOC(sstable.descriptor, false));
        assertRows(execute("SELECT pk FROM %s WHERE o = 3"), row(3));
    }

    private static Set<Descriptor> liveDescriptors(ColumnFamilyStore cfs)
    {
        return cfs.getLiveSSTables().stream().map(s -> s.descriptor).collect(Collectors.toSet());
    }
}
