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
package org.apache.cassandra.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.function.Function;

import com.google.common.collect.Iterables;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RepairedDataTombstonesTest extends CQLTester
{
    @Test
    public void compactionTest() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        // insert a live row to make sure that the sstables are not dropped (we test dropping in compactionDropExpiredSSTableTest() below)
        execute("insert into %s (id, id2, t) values (999,999,'live')");
        for (int i = 0; i < 10; i++)
        {
            execute("delete from %s where id=? and id2=?", 1, i);
        }
        flush();
        SSTableReader repairedSSTable = getCurrentColumnFamilyStore().getSSTables(SSTableSet.LIVE).iterator().next();
        repair(getCurrentColumnFamilyStore(), repairedSSTable);
        Thread.sleep(2000);
        execute("insert into %s (id, id2, t) values (999,999,'live')");
        for (int i = 10; i < 20; i++)
        {
            execute("delete from %s where id=? and id2=?", 1, i);
        }
        flush();
        Thread.sleep(1000);
        // at this point we have 2 sstables, one repaired and one unrepaired. Both sstables contain expired tombstones, but we should only drop the tombstones from the repaired sstable.
        getCurrentColumnFamilyStore().forceMajorCompaction();
        verifyIncludingPurgeable();
        verify2IncludingPurgeable(1);
        assertEquals(2, Iterables.size(getCurrentColumnFamilyStore().getSSTables(SSTableSet.LIVE)));
    }

    @Test
    public void compactionDropExpiredSSTableTest() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        for (int i = 0; i < 10; i++)
        {
            execute("delete from %s where id=? and id2=?", 1, i);
        }
        flush();
        SSTableReader repairedSSTable = getCurrentColumnFamilyStore().getSSTables(SSTableSet.LIVE).iterator().next();
        repair(getCurrentColumnFamilyStore(), repairedSSTable);
        Thread.sleep(2000);
        for (int i = 10; i < 20; i++)
        {
            execute("delete from %s where id=? and id2=?", 1, i);
        }
        flush();
        Thread.sleep(1000);
        getCurrentColumnFamilyStore().forceMajorCompaction();
        verifyIncludingPurgeable();
        verify2IncludingPurgeable(1);
        assertEquals(1, Iterables.size(getCurrentColumnFamilyStore().getSSTables(SSTableSet.LIVE)));
        assertFalse(getCurrentColumnFamilyStore().getSSTables(SSTableSet.LIVE).iterator().next().isRepaired());

    }

    @Test
    public void compactionIndexTableTest() throws Throwable
    {
        // index sstables are never repaired, so the index table purges its tombstones whatever the base table's option
        createTable("create table %s (id int primary key, t int) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        String index = createIndex("create index on %s (t) using 'legacy_local_table'");
        ColumnFamilyStore indexCfs = getCurrentColumnFamilyStore().indexManager.getIndexByName(index).getBackingTable().get();
        execute("insert into %s (id, t) values (1, 1)");
        // deletes the index entry of t=1
        execute("update %s set t=2 where id=1");
        flush();
        Thread.sleep(1000);
        indexCfs.forceMajorCompaction();
        SSTableReader sstable = Iterables.getOnlyElement(indexCfs.getSSTables(SSTableSet.LIVE));
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            assertTrue(scanner.hasNext());
            try (UnfilteredRowIterator partition = scanner.next())
            {
                assertEquals(2, ByteBufferUtil.toInt(partition.partitionKey().getKey()));
            }
            assertFalse(scanner.hasNext());
        }
    }

    @Test
    public void readTest() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        for (int i = 0; i < 10; i++)
        {
            execute("update %s set t2=null where id=? and id2=?", 123, i);
        }
        flush();
        SSTableReader repairedSSTable = getCurrentColumnFamilyStore().getSSTables(SSTableSet.LIVE).iterator().next();
        repair(getCurrentColumnFamilyStore(), repairedSSTable);
        Thread.sleep(2000);
        for (int i = 10; i < 20; i++)
        {
            execute("update %s set t2=null where id=? and id2=?", 123, i);
        }
        flush();
        // allow gcgrace to properly expire:
        Thread.sleep(1000);
        // make sure we only see the unrepaired tombstones, the other ones are expired and can be purged
        verify();
        verify2(123);
    }

    @Test
    public void readOnlyUnrepairedTest() throws Throwable
    {
        // make sure we keep all tombstones if we only have unrepaired data
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        for (int i = 10; i < 20; i++)
        {
            execute("update %s set t2=null where id=? and id2=?", 123, i);
        }
        flush();

        // allow gcgrace to properly expire:
        Thread.sleep(1000);
        verifyIncludingPurgeable();
        verify2IncludingPurgeable(123);
    }

    @Test
    public void readTestRowTombstones() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        for (int i = 0; i < 10; i++)
        {
            execute("delete from %s where id=? and id2=?", 1, i);
        }
        flush();
        SSTableReader repairedSSTable = getCurrentColumnFamilyStore().getSSTables(SSTableSet.LIVE).iterator().next();
        repair(getCurrentColumnFamilyStore(), repairedSSTable);
        Thread.sleep(2000);
        for (int i = 10; i < 20; i++)
        {
            execute("delete from %s where id=? and id2=?", 1, i);
        }
        flush();
        Thread.sleep(1000);
        verify();
        verify2(1);
    }

    @Test
    public void readTestWithSkipListMemtableData() throws Throwable
    {
        readTestWithMemtableData("skiplist");
    }

    @Test
    public void readTestWithShardedSkipListMemtableData() throws Throwable
    {
        StorageCompatibilityMode mode = DatabaseDescriptor.getStorageCompatibilityMode();
        // sharded memtables need storage compatibility with Cassandra 5.0
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
        try
        {
            readTestWithMemtableData("skiplist_sharded");
        }
        finally
        {
            DatabaseDescriptor.setStorageCompatibilityMode(mode);
        }
    }

    @Test
    public void readTestWithTrieMemtableData() throws Throwable
    {
        readTestWithMemtableData("trie");
    }

    private void readTestWithMemtableData(String memtable) throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true} and memtable = '" + memtable + "'");
        for (int i = 0; i < 10; i++)
        {
            execute("delete from %s where id=? and id2=?", 1, i);
        }
        flush();
        SSTableReader repairedSSTable = getCurrentColumnFamilyStore().getSSTables(SSTableSet.LIVE).iterator().next();
        repair(getCurrentColumnFamilyStore(), repairedSSTable);
        Thread.sleep(2000);
        for (int i = 10; i < 20; i++)
        {
            execute("delete from %s where id=? and id2=?", 1, i);
        }
        flush();
        // writes without deletions in the memtable don't hold back the purge of the repaired tombstones
        execute("insert into %s (id, id2, t) values (999,999,'live')");
        execute("insert into %s (id, id2, t) values (1,15,'live')");
        Thread.sleep(1000);
        verify();
        verify2(1);
        verifyRow(1, 5, 0);
        verifyRow(1, 12, 1);
    }

    @Test
    public void readTestPartitionTombstones() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        for (int i = 0; i < 10; i++)
        {
            execute("delete from %s where id=?", i);
        }
        flush();
        SSTableReader repairedSSTable = getCurrentColumnFamilyStore().getSSTables(SSTableSet.LIVE).iterator().next();
        repair(getCurrentColumnFamilyStore(), repairedSSTable);
        Thread.sleep(2000);
        for (int i = 10; i < 20; i++)
        {
            execute("delete from %s where id=?", i);
        }
        flush();

        Thread.sleep(1000);
        ReadCommand cmd = Util.cmd(getCurrentColumnFamilyStore()).build();
        int partitionsFound = 0;
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
        {
            while (iterator.hasNext())
            {
                partitionsFound++;
                try (UnfilteredRowIterator rowIter = iterator.next())
                {
                    int val = ByteBufferUtil.toInt(rowIter.partitionKey().getKey());
                    assertTrue("val=" + val, val >= 10 && val < 20);
                }
            }
        }
        assertEquals(10, partitionsFound);
    }

    @Test
    public void readTestNamesUnrepairedPartitionTombstone() throws Throwable
    {
        readTestNamesUnrepairedPartitionTombstone(false);
    }

    @Test
    public void readTestNamesUnrepairedPartitionTombstoneWithOtherRow() throws Throwable
    {
        readTestNamesUnrepairedPartitionTombstone(true);
    }

    private void readTestNamesUnrepairedPartitionTombstone(boolean otherRow) throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        getCurrentColumnFamilyStore().disableAutoCompaction();
        execute("insert into %s (id, id2, t) values (1, 5, 'live')");
        flush();
        repair(getCurrentColumnFamilyStore(), getCurrentColumnFamilyStore().getLiveSSTables().iterator().next());
        execute("delete from %s where id=1");
        if (otherRow)
            execute("insert into %s (id, id2, t) values (1, 7, 'live')");
        flush();
        Thread.sleep(2000);
        // the unrepaired sstable holds the partition deletion but not the requested row
        ReadCommand cmd = Util.cmd(getCurrentColumnFamilyStore(), Util.dk(ByteBufferUtil.bytes(1))).includeRow(5).columns("t").build();
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
        {
            assertTrue(iterator.hasNext());
            try (UnfilteredRowIterator rowIter = iterator.next())
            {
                assertFalse(rowIter.partitionLevelDeletion().isLive());
                assertFalse(rowIter.hasNext());
            }
        }
    }

    @Test
    public void readTestNamesStaticUnrepairedMemtablePartitionTombstone() throws Throwable
    {
        readTestNamesStaticUnrepairedMemtableTombstone(true);
    }

    @Test
    public void readTestNamesStaticUnrepairedMemtableCellTombstone() throws Throwable
    {
        readTestNamesStaticUnrepairedMemtableTombstone(false);
    }

    private void readTestNamesStaticUnrepairedMemtableTombstone(boolean partitionDeletion) throws Throwable
    {
        createTable("create table %s (id int, id2 int, s text static, t text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("insert into %s (id, id2, s, t) values (1, 5, 'live', 'live')");
        flush();
        repair(cfs, cfs.getLiveSSTables().iterator().next());
        if (partitionDeletion)
            execute("delete from %s where id=1");
        else
            execute("delete s from %s where id=1");
        Thread.sleep(2000);
        // a names read without clusterings reads only the static row and partition deletion of the memtable
        ColumnMetadata s = cfs.metadata().getColumn(ByteBufferUtil.bytes("s"));
        ReadCommand cmd = SinglePartitionReadCommand.create(cfs.metadata(),
                                                            FBUtilities.nowInSeconds(),
                                                            ColumnFilter.selection(RegularAndStaticColumns.of(s)),
                                                            RowFilter.none(),
                                                            DataLimits.NONE,
                                                            Util.dk(ByteBufferUtil.bytes(1)),
                                                            new ClusteringIndexNamesFilter(new TreeSet<>(cfs.metadata().comparator), false));
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
        {
            assertTrue(iterator.hasNext());
            try (UnfilteredRowIterator rowIter = iterator.next())
            {
                if (partitionDeletion)
                    assertFalse(rowIter.partitionLevelDeletion().isLive());
                else
                    assertTrue(rowIter.staticRow().getCell(s).isTombstone());
            }
        }
    }

    @Test
    public void readTestRowCacheHitRepaired() throws Throwable
    {
        CacheService.instance.setRowCacheCapacityInMB(1);
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'} and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        long now = FBUtilities.nowInSeconds();
        for (int i = 0; i < 10; i++)
            RowUpdateBuilder.deleteRowAt(cfs.metadata(), 1, now - 200, 1, i).apply();
        flush();
        repair(cfs, cfs.getLiveSSTables().iterator().next());
        for (int i = 10; i < 20; i++)
            RowUpdateBuilder.deleteRowAt(cfs.metadata(), 1, now - 100, 1, i).apply();
        flush();
        // the first read caches the partition, the next ones read it from the row cache: the repaired deletions, older
        // than the unrepaired ones, are purged
        long hits = cfs.metric.rowCacheHit.getCount();
        verify2(1);
        verify2(1);
        verifyRow(1, 15, 1);
        assertEquals(hits + 2, cfs.metric.rowCacheHit.getCount());
    }

    @Test
    public void readTestRangeAfterRowCacheFill() throws Throwable
    {
        CacheService.instance.setRowCacheCapacityInMB(1);
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'} and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("insert into %s (id, id2, t) values (1, 5, 'live') using timestamp 1");
        flush();
        execute("insert into %s (id, id2, t) values (2, 5, 'live') using timestamp 1");
        ReadCommand cmd = Util.cmd(cfs).withNowInSeconds(FBUtilities.nowInSeconds() + 10).build();
        boolean found = false;
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
        {
            // a point read caches partition 1 with a deletion that the sources of the range read don't hold
            flush();
            execute("delete from %s using timestamp 2 where id=1");
            Util.getAll(Util.cmd(cfs, Util.dk(ByteBufferUtil.bytes(1))).build());
            assertNotNull(cfs.getRawCachedPartition(Util.dk(ByteBufferUtil.bytes(1))));
            while (iterator.hasNext())
            {
                try (UnfilteredRowIterator rowIter = iterator.next())
                {
                    if (ByteBufferUtil.toInt(rowIter.partitionKey().getKey()) == 1)
                    {
                        found = true;
                        assertTrue(rowIter.hasNext() || !rowIter.partitionLevelDeletion().isLive());
                    }
                }
            }
        }
        assertTrue(found);
    }

    @Test
    public void readTestLegacyIndexSearch() throws Throwable
    {
        readTestIndexSearch("create index on %s (t)", false, false, false);
    }

    @Test
    public void readTestLegacyIndexSearchSinglePartition() throws Throwable
    {
        readTestIndexSearch("create index on %s (t)", true, false, false);
    }

    @Test
    public void readTestSAISearch() throws Throwable
    {
        readTestIndexSearch("create custom index on %s (t) using 'StorageAttachedIndex'", false, false, false);
    }

    @Test
    public void readTestSAISearchSinglePartition() throws Throwable
    {
        readTestIndexSearch("create custom index on %s (t) using 'StorageAttachedIndex'", true, false, false);
    }

    @Test
    public void readTestSAISearchMultiRange() throws Throwable
    {
        readTestIndexSearch("create custom index on %s (t) using 'StorageAttachedIndex'", false, true, false);
    }

    @Test
    public void readTestSAISearchRepaired() throws Throwable
    {
        readTestIndexSearch("create custom index on %s (t) using 'StorageAttachedIndex'", false, false, true);
    }

    private void readTestIndexSearch(String index, boolean singlePartition, boolean multiRange, boolean repaired) throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        createIndex(index);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("delete from %s using timestamp 1 where id=1");
        if (repaired)
        {
            flush();
            repair(cfs, cfs.getLiveSSTables().iterator().next());
        }
        execute("insert into %s (id, id2, t) values (1, 5, 'live') using timestamp 2");
        flush();
        // the partition deletion shadows older writes that other replicas may still hold until it is repaired
        AbstractReadCommandBuilder builder = singlePartition ? Util.cmd(cfs, Util.dk(ByteBufferUtil.bytes(1))) : Util.cmd(cfs);
        // the deletion is gc-able for a read 10 seconds later
        ReadCommand cmd = builder.filterOn("t", Operator.EQ, "live").withNowInSeconds(FBUtilities.nowInSeconds() + 10).build();
        if (multiRange)
        {
            // the partition is in the second range
            IPartitioner partitioner = cfs.getPartitioner();
            Token split = partitioner.midpoint(partitioner.getMinimumToken(), Util.dk(ByteBufferUtil.bytes(1)).getToken());
            List<AbstractBounds<PartitionPosition>> ranges = Arrays.asList(new Range<>(partitioner.getMinimumToken().maxKeyBound(), split.maxKeyBound()),
                                                                           new Range<>(split.maxKeyBound(), partitioner.getMaximumToken().maxKeyBound()));
            cmd = MultiRangeReadCommand.create((PartitionRangeReadCommand) cmd, ranges, true);
        }
        assertNotNull(cmd.indexQueryPlan());
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
        {
            assertTrue(iterator.hasNext());
            try (UnfilteredRowIterator rowIter = iterator.next())
            {
                assertEquals(repaired, rowIter.partitionLevelDeletion().isLive());
            }
        }
    }

    @Test
    public void readTestLegacyIndexSearchOldDeletionBetweenPartitions() throws Throwable
    {
        readTestIndexSearchOldDeletionBetweenPartitions("create index on %s (t)");
    }

    @Test
    public void readTestSAISearchOldDeletionBetweenPartitions() throws Throwable
    {
        readTestIndexSearchOldDeletionBetweenPartitions("create custom index on %s (t) using 'StorageAttachedIndex'");
    }

    @Test
    public void readTestLegacyIndexSearchGroupOldDeletionBetweenPartitions() throws Throwable
    {
        readTestIndexSearchOldDeletionBetweenPartitions("create index on %s (t)", RepairedDataTombstonesTest::groupSearch);
    }

    @Test
    public void readTestSAISearchGroupOldDeletionBetweenPartitions() throws Throwable
    {
        readTestIndexSearchOldDeletionBetweenPartitions("create custom index on %s (t) using 'StorageAttachedIndex'", RepairedDataTombstonesTest::groupSearch);
    }

    @Test
    public void readTestSAISearchMultiRangeOldDeletionBetweenPartitions() throws Throwable
    {
        readTestIndexSearchOldDeletionBetweenPartitions("create custom index on %s (t) using 'StorageAttachedIndex'", RepairedDataTombstonesTest::multiRangeSearch);
    }

    private void readTestIndexSearchOldDeletionBetweenPartitions(String index) throws Throwable
    {
        readTestIndexSearchOldDeletionBetweenPartitions(index, RepairedDataTombstonesTest::rangeSearch);
    }

    private void readTestIndexSearchOldDeletionBetweenPartitions(String index, Function<ColumnFamilyStore, ReadQuery> search) throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        createIndex(index);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("insert into %s (id, id2, t, t2) values (1, 5, 'live', 'old') using timestamp 1");
        execute("insert into %s (id, id2, t, t2) values (2, 5, 'live', 'old') using timestamp 1");
        flush();
        ReadQuery query = search.apply(cfs);
        try (ReadExecutionController executionController = query.executionController();
             UnfilteredPartitionIterator iterator = query.executeLocally(executionController))
        {
            int first;
            assertTrue(iterator.hasNext());
            try (UnfilteredRowIterator partition = iterator.next())
            {
                first = ByteBufferUtil.toInt(partition.partitionKey().getKey());
                while (partition.hasNext())
                    partition.next();
            }
            // the deletion reaches the memtable after the search read the first partition and before the second
            int second = first == 1 ? 2 : 1;
            deleteT2WithOldLocalDeletionTime(second, 5);
            assertTrue(iterator.hasNext());
            try (UnfilteredRowIterator partition = iterator.next())
            {
                assertEquals(second, ByteBufferUtil.toInt(partition.partitionKey().getKey()));
                assertT2Deleted((Row) partition.next());
            }
        }
    }

    private static ReadQuery rangeSearch(ColumnFamilyStore cfs)
    {
        ReadCommand cmd = Util.cmd(cfs).filterOn("t", Operator.EQ, "live").build();
        assertNotNull(cmd.indexQueryPlan());
        return cmd;
    }

    // single partition searches of partitions 1 and 2
    private static ReadQuery groupSearch(ColumnFamilyStore cfs)
    {
        List<SinglePartitionReadCommand> commands = new ArrayList<>();
        for (int id = 1; id <= 2; id++)
        {
            ReadCommand cmd = Util.cmd(cfs, Util.dk(ByteBufferUtil.bytes(id))).filterOn("t", Operator.EQ, "live").build();
            assertNotNull(cmd.indexQueryPlan());
            commands.add((SinglePartitionReadCommand) cmd);
        }
        return SinglePartitionReadCommand.Group.create(commands, DataLimits.NONE);
    }

    // a search of partitions 1 and 2, each in its own range
    private static ReadQuery multiRangeSearch(ColumnFamilyStore cfs)
    {
        IPartitioner partitioner = cfs.getPartitioner();
        Token first = Util.dk(ByteBufferUtil.bytes(1)).getToken();
        Token second = Util.dk(ByteBufferUtil.bytes(2)).getToken();
        Token split = first.compareTo(second) < 0 ? partitioner.midpoint(first, second) : partitioner.midpoint(second, first);
        List<AbstractBounds<PartitionPosition>> ranges = Arrays.asList(new Range<>(partitioner.getMinimumToken().maxKeyBound(), split.maxKeyBound()),
                                                                       new Range<>(split.maxKeyBound(), partitioner.getMaximumToken().maxKeyBound()));
        ReadCommand cmd = MultiRangeReadCommand.create((PartitionRangeReadCommand) rangeSearch(cfs), ranges, true);
        assertNotNull(cmd.indexQueryPlan());
        return cmd;
    }

    @Test
    public void readTestLegacyIndexSearchSSTableAdded() throws Throwable
    {
        readTestIndexSearchSSTableAdded("create index on %s (t)");
    }

    private void readTestIndexSearchSSTableAdded(String index) throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        createIndex(index);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        SSTableReader deletion = removedSSTableOfOldDeletion(cfs, 1, 5);
        execute("insert into %s (id, id2, t, t2) values (1, 5, 'live', 'old') using timestamp 1");
        flush();
        ReadCommand cmd = Util.cmd(cfs).filterOn("t", Operator.EQ, "live").build();
        assertNotNull(cmd.indexQueryPlan());
        Thread publisher;
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
        {
            // the sstable of the deletion is added, as by an import or a stream, after the search started
            publisher = addSSTable(cfs, deletion);
            assertTrue(iterator.hasNext());
            try (UnfilteredRowIterator partition = iterator.next())
            {
                assertT2Deleted((Row) partition.next());
            }
        }
        publisher.join();
    }

    @Test
    public void readTestSAISearchOldDeletionBetweenBatches() throws Throwable
    {
        ColumnFamilyStore cfs = createWidePartitionForBatches();
        ReadCommand cmd = Util.cmd(cfs).filterOn("t", Operator.EQ, "live").withLimit(2).build();
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
        {
            assertTrue(iterator.hasNext());
            try (UnfilteredRowIterator partition = iterator.next())
            {
                assertEquals(5, clustering((Row) partition.next()));
                // the deletion reaches the memtable after the first batch of keys of the partition
                deleteT2WithOldLocalDeletionTime(1, 7);
                Row row = (Row) partition.next();
                assertEquals(7, clustering(row));
                assertNotNull("t2 has neither its value nor its deletion", t2(row));
            }
        }
    }

    @Test
    public void readTestSAISearchSSTableAddedBetweenBatches() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        createIndex("create custom index on %s (t) using 'StorageAttachedIndex'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        SSTableReader deletion = removedSSTableOfOldDeletion(cfs, 1, 7);
        for (int i = 5; i < 8; i++)
            execute("insert into %s (id, id2, t, t2) values (1, ?, 'live', 'old') using timestamp 1", i);
        flush();
        execute("update %s using timestamp 2 set t = 'other' where id=1 and id2=6");
        // with a limit of 2, the search reads rows 5 and 6 of the partition, then row 7 once row 6 doesn't match
        ReadCommand cmd = Util.cmd(cfs).filterOn("t", Operator.EQ, "live").withLimit(2).build();
        Thread publisher;
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
        {
            assertTrue(iterator.hasNext());
            try (UnfilteredRowIterator partition = iterator.next())
            {
                assertEquals(5, clustering((Row) partition.next()));
                publisher = addSSTable(cfs, deletion);
                Row row = (Row) partition.next();
                assertEquals(7, clustering(row));
                assertT2Deleted(row);
            }
        }
        publisher.join();
    }

    @Test
    public void readTestSAISearchSSTableOfSecondBatch() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        createIndex("create custom index on %s (t) using 'StorageAttachedIndex'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int i = 5; i < 8; i++)
            execute("insert into %s (id, id2, t, t2) values (1, ?, 'live', 'old') using timestamp 1", i);
        flush();
        deleteT2WithOldLocalDeletionTime(1, 7);
        flush();
        execute("update %s using timestamp 2 set t = 'other' where id=1 and id2=6");
        // the batch of rows 5 and 6 skips the sstable of the deletion, the batch of row 7 reads it
        ReadCommand cmd = Util.cmd(cfs).filterOn("t", Operator.EQ, "live").withLimit(2).build();
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
        {
            assertTrue(iterator.hasNext());
            try (UnfilteredRowIterator partition = iterator.next())
            {
                assertEquals(5, clustering((Row) partition.next()));
                Row row = (Row) partition.next();
                assertEquals(7, clustering(row));
                assertT2Deleted(row);
            }
        }
    }

    @Test
    public void readTestSAISearchOldRangeDeletionAroundRowBetweenBatches() throws Throwable
    {
        // the search reads the partition in batches of keys, and the deleted row starts the third batch
        int batchSize = CassandraRelevantProperties.SAI_PARTITION_ROW_BATCH_SIZE.getInt();
        int deleted = 2 * batchSize;
        ColumnFamilyStore cfs = createWidePartitionWithStaticRow(deleted + 50);
        ReadCommand cmd = staticExpressionSearch(cfs);
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
        {
            assertTrue(iterator.hasNext());
            try (UnfilteredRowIterator partition = iterator.next())
            {
                assertEquals(0, clustering((Row) partition.next()));
                // the range deletion reaches the memtable after the first batch of keys; it is older than the row
                deleteRangeWithOldLocalDeletionTime(2, 1, deleted);
                assertEquals(Arrays.asList("row " + (deleted - 1), "open", "row " + deleted, "close", "row " + (deleted + 1)),
                             around(partition, deleted));
            }
        }
    }

    @Test
    public void readTestRowCacheHitAfterRepairAndCompaction() throws Throwable
    {
        CacheService.instance.setRowCacheCapacityInMB(1);
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'} and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int i = 0; i < 10; i++)
            RowUpdateBuilder.deleteRowAt(cfs.metadata(), 1, FBUtilities.nowInSeconds() - 100, 1, i).apply();
        execute("insert into %s (id, id2, t) values (1, 10, 'live') using timestamp 1");
        flush();
        DecoratedKey key = Util.dk(ByteBufferUtil.bytes(1));
        // the read caches the partition with its unrepaired row deletions
        verify2(1, 11, 0, 11, false);
        assertNotNull(cfs.getRawCachedPartition(key));
        // repair, then a compaction that purges the deletions; the cached partition still holds them
        repair(cfs, cfs.getLiveSSTables().iterator().next());
        cfs.forceMajorCompaction();
        assertNotNull(cfs.getRawCachedPartition(key));
        long hits = cfs.metric.rowCacheHit.getCount();
        verify2(1, 1, 10, 11, false);
        assertEquals(hits + 1, cfs.metric.rowCacheHit.getCount());
        verify(1, 10, 11, false);
    }

    @Test
    public void readTestNamesUnrepairedDeletionAtEncodingEpoch() throws Throwable
    {
        readTestUnrepairedDeletionAtEncodingEpoch(null);
    }

    private void readTestUnrepairedDeletionAtEncodingEpoch(String index) throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        if (index != null)
            createIndex(index);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("insert into %s (id, id2, t, t2) values (1, 5, 'live', 'old') using timestamp 1");
        // a deletion at the local deletion time that EncodingStats also uses for no deletion time
        new RowUpdateBuilder(cfs.metadata(), EncodingStats.NO_STATS.minLocalDeletionTime, 2L, 1).clustering(5).delete("t2").build().apply();
        flush();
        assertEquals(EncodingStats.NO_STATS.minLocalDeletionTime, cfs.getLiveSSTables().iterator().next().getMinLocalDeletionTime());
        ReadCommand cmd = index == null ? Util.cmd(cfs, Util.dk(ByteBufferUtil.bytes(1))).includeRow(5).columns("t2").build()
                                        : Util.cmd(cfs).filterOn("t", Operator.EQ, "live").build();
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
        {
            assertTrue(iterator.hasNext());
            try (UnfilteredRowIterator partition = iterator.next())
            {
                assertT2Deleted((Row) partition.next());
            }
        }
    }

    @Test
    public void readTestSAISearchRepairedDeletionBeforeOlderUnrepairedOne() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        createIndex("create custom index on %s (t) using 'StorageAttachedIndex'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        int first = Util.dk(ByteBufferUtil.bytes(1)).compareTo(Util.dk(ByteBufferUtil.bytes(2))) < 0 ? 1 : 2;
        int second = 3 - first;
        execute("insert into %s (id, id2, t, t2) values (?, 5, 'live', 'old') using timestamp 1", first);
        execute("insert into %s (id, id2, t, t2) values (?, 5, 'live', 'old') using timestamp 1", second);
        new RowUpdateBuilder(cfs.metadata(), FBUtilities.nowInSeconds() - 100, 2L, first).clustering(5).delete("t2").build().apply();
        flush();
        repair(cfs, cfs.getLiveSSTables().iterator().next());
        // the partition read second has an unrepaired deletion, older than the repaired deletion of the first one
        new RowUpdateBuilder(cfs.metadata(), FBUtilities.nowInSeconds() - 200, 2L, second).clustering(5).delete("t2").build().apply();
        flush();
        ReadCommand cmd = Util.cmd(cfs).filterOn("t", Operator.EQ, "live").build();
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
        {
            for (int id : new int[]{ first, second })
            {
                assertTrue(iterator.hasNext());
                try (UnfilteredRowIterator partition = iterator.next())
                {
                    assertEquals(id, ByteBufferUtil.toInt(partition.partitionKey().getKey()));
                    assertT2Deleted((Row) partition.next());
                }
            }
        }
    }

    @Test
    public void readTestCustomIndexReadingAnotherPartitionInRangeTombstone() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        createIndex("create custom index on %s (t) using '" + ReadingAheadIndex.class.getName() + "'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        SSTableReader deletion = removedSSTableOfOldDeletion(cfs, 2, 1);
        execute("insert into %s (id, id2, t, t2) values (1, 1, 'live', 'old') using timestamp 10");
        execute("insert into %s (id, id2, t, t2) values (2, 1, 'live', 'old') using timestamp 10");
        // newer than the deletion of partition 2
        new RowUpdateBuilder(cfs.metadata(), FBUtilities.nowInSeconds() - 50, 5L, 1).addRangeTombstone(0, 2).build().apply();
        flush();
        repair(cfs, cfs.getLiveSSTables().iterator().next());
        ReadCommand cmd = Util.cmd(cfs).filterOn("t", Operator.EQ, "live").build();
        int rows = 0;
        Thread publisher;
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
        {
            // the searcher reads partition 2, from this sstable, while it returns the range tombstone of partition 1
            publisher = addSSTable(cfs, deletion);
            assertTrue(iterator.hasNext());
            try (UnfilteredRowIterator partition = iterator.next())
            {
                while (partition.hasNext())
                {
                    if (partition.next().isRow())
                        rows++;
                }
            }
        }
        publisher.join();
        assertEquals(1, rows);
    }

    @Test
    public void readTestCustomIndexReadingSSTable() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        createIndex("create custom index on %s (t) using '" + SSTableReadingIndex.class.getName() + "'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("insert into %s (id, id2, t, t2) values (1, 1, 'live', 'old') using timestamp 1");
        deleteT2WithOldLocalDeletionTime(1, 1);
        flush();
        ReadCommand cmd = Util.cmd(cfs).filterOn("t", Operator.EQ, "live").build();
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
        {
            assertTrue(iterator.hasNext());
            try (UnfilteredRowIterator partition = iterator.next())
            {
                assertT2Deleted((Row) partition.next());
            }
        }
    }

    // Returns partition 1, and reads partition 2 through the controller of the search when it returns a row
    public static class ReadingAheadIndex extends StubIndex
    {
        private final ColumnFamilyStore cfs;

        public ReadingAheadIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
            cfs = baseCfs;
        }

        @Override
        public RowFilter getPostIndexQueryFilter(RowFilter filter)
        {
            return RowFilter.none();
        }

        @Override
        public Searcher searcherFor(ReadCommand command)
        {
            return new Searcher(command)
            {
                @Override
                public UnfilteredPartitionIterator search(ReadExecutionController controller)
                {
                    UnfilteredRowIterator partition = SinglePartitionReadCommand.fullPartitionRead(cfs.metadata(), command.nowInSec(), Util.dk(ByteBufferUtil.bytes(1)))
                                                                                .queryMemtableAndDisk(cfs, controller);
                    return new SingletonUnfilteredPartitionIterator(new WrappingUnfilteredRowIterator()
                    {
                        public UnfilteredRowIterator wrapped()
                        {
                            return partition;
                        }

                        public Unfiltered next()
                        {
                            Unfiltered next = partition.next();
                            if (next.isRow())
                            {
                                try (UnfilteredRowIterator other = SinglePartitionReadCommand.fullPartitionRead(cfs.metadata(), command.nowInSec(), Util.dk(ByteBufferUtil.bytes(2)))
                                                                                             .queryMemtableAndDisk(cfs, controller))
                                {
                                    while (other.hasNext())
                                        other.next();
                                }
                            }
                            return next;
                        }
                    });
                }
            };
        }
    }

    // Returns partition 1 from the first live sstable, without the controller of the search
    public static class SSTableReadingIndex extends StubIndex
    {
        private final ColumnFamilyStore cfs;

        public SSTableReadingIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
            cfs = baseCfs;
        }

        @Override
        public RowFilter getPostIndexQueryFilter(RowFilter filter)
        {
            return RowFilter.none();
        }

        @Override
        public Searcher searcherFor(ReadCommand command)
        {
            return new Searcher(command)
            {
                @Override
                public UnfilteredPartitionIterator search(ReadExecutionController controller)
                {
                    SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
                    return new SingletonUnfilteredPartitionIterator(sstable.rowIterator(Util.dk(ByteBufferUtil.bytes(1)), Slices.ALL, command.columnFilter(), false, SSTableReadsListener.NOOP_LISTENER));
                }
            };
        }
    }

    @Test
    public void readTestRangeOldDeletionWrittenAfterOpenSkipList() throws Throwable
    {
        readTestRangeOldDeletionWrittenAfterOpen(" and memtable = 'skiplist'");
    }

    @Test
    public void readTestReadDuringOldDeletionWriteSkipList() throws Throwable
    {
        readTestReadDuringOldDeletionWrite(" and memtable = 'skiplist'", "org.apache.cassandra.db.memtable.SkipListMemtable", "addAll");
    }

    @Test
    public void readTestRangeOldDeletionWrittenAfterOpenTrie() throws Throwable
    {
        readTestRangeOldDeletionWrittenAfterOpen(" and memtable = 'trie'");
    }

    @Test
    public void readTestReadDuringOldDeletionWriteTrie() throws Throwable
    {
        readTestReadDuringOldDeletionWrite(" and memtable = 'trie'", "org.apache.cassandra.db.memtable.TrieMemtable$MemtableShard", "mergeUpdate");
    }

    @Test
    public void readTestRangeOldDeletionWrittenAfterOpenTrieStage1() throws Throwable
    {
        readTestRangeOldDeletionWrittenAfterOpen(" and memtable = 'trie_stage1'");
    }

    @Test
    public void readTestReadDuringOldDeletionWriteTrieStage1() throws Throwable
    {
        readTestReadDuringOldDeletionWrite(" and memtable = 'trie_stage1'", "org.apache.cassandra.db.memtable.AbstractTrieMemtable$AbstractMemtableShard", "applyUpdate");
    }

    @Test
    public void readTestReadDuringOldDeletionWriteShardedSkipList() throws Throwable
    {
        StorageCompatibilityMode mode = DatabaseDescriptor.getStorageCompatibilityMode();
        // sharded memtables need storage compatibility with Cassandra 5.0
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
        try
        {
            readTestReadDuringOldDeletionWrite(" and memtable = 'skiplist_sharded'", "org.apache.cassandra.db.memtable.ShardedSkipListMemtable$MemtableShard", "addAll");
        }
        finally
        {
            DatabaseDescriptor.setStorageCompatibilityMode(mode);
        }
    }

    private void readTestRangeOldDeletionWrittenAfterOpen(String memtableOption) throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}" + memtableOption);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int i = 0; i < 10; i++)
            execute("insert into %s (id, id2, t, t2) values (?, 5, 'live', 'old') using timestamp 1", i);
        ReadCommand cmd = Util.cmd(cfs).build();
        int partitions = 0;
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
        {
            // the deletions reach the memtable after the read opened it, and before it reads their partitions
            for (int i = 0; i < 10; i++)
                deleteT2WithOldLocalDeletionTime(i, 5);
            while (iterator.hasNext())
            {
                try (UnfilteredRowIterator partition = iterator.next())
                {
                    partitions++;
                    // the row has either the value of t2 or its deletion
                    assertNotNull(t2((Row) partition.next()));
                }
            }
        }
        assertEquals(10, partitions);
    }

    private void readTestReadDuringOldDeletionWrite(String memtableOption, String memtableClass, String publishingMethod) throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}" + memtableOption);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("insert into %s (id, id2, t, t2) values (1, 5, 'live', 'old') using timestamp 1");
        // the write of the deletion stops once its data is visible in the memtable
        Injections.Barrier published = Injections.newBarrier("published", 2, false)
                                                 .add(InvokePointBuilder.newInvokePoint().onClass(memtableClass).onMethod("put").after("INVOKE " + publishingMethod))
                                                 .build();
        Injections.inject(published);
        Thread writer = new Thread(() -> deleteT2WithOldLocalDeletionTime(1, 5));
        try
        {
            writer.start();
            Util.spinAssertEquals(1L, published::getCount, 10);
            ReadCommand cmd = Util.cmd(cfs, Util.dk(ByteBufferUtil.bytes(1))).build();
            try (ReadExecutionController executionController = cmd.executionController();
                 UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
            {
                assertTrue(iterator.hasNext());
                try (UnfilteredRowIterator partition = iterator.next())
                {
                    // the row has either the value of t2 or its deletion
                    assertNotNull(t2((Row) partition.next()));
                }
            }
        }
        finally
        {
            published.countDown();
            writer.join();
            Injections.deleteAll();
        }
    }

    private ColumnFamilyStore createWidePartitionForBatches() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        createIndex("create custom index on %s (t) using 'StorageAttachedIndex'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int i = 5; i < 8; i++)
            execute("insert into %s (id, id2, t, t2) values (1, ?, 'live', 'old') using timestamp 1", i);
        flush();
        // with a limit of 2, the search reads rows 5 and 6 of the partition, then row 7 once row 6 doesn't match
        execute("update %s using timestamp 2 set t = 'other' where id=1 and id2=6");
        return cfs;
    }

    // a deletion of t2 with a local deletion time in the past, as a read repair writes it
    private void deleteT2WithOldLocalDeletionTime(int id, int id2)
    {
        new RowUpdateBuilder(getCurrentColumnFamilyStore().metadata(), FBUtilities.nowInSeconds() - 100, 2L, id).clustering(id2).delete("t2").build().apply();
    }

    // a partition with a static row, which an SAI search with a static column expression reads in batches of keys
    private ColumnFamilyStore createWidePartitionWithStaticRow(int rows) throws Throwable
    {
        createTable("create table %s (id int, id2 int, s text static, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        createIndex("create custom index on %s (t) using 'StorageAttachedIndex'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("insert into %s (id, s) values (1, 'x') using timestamp 10");
        for (int i = 0; i < rows; i++)
            execute("insert into %s (id, id2, t, t2) values (1, ?, 'live', 'old') using timestamp 10", i);
        flush();
        return cfs;
    }

    private ReadCommand staticExpressionSearch(ColumnFamilyStore cfs)
    {
        ReadCommand cmd = Util.cmd(cfs).filterOn("t", Operator.EQ, "live").filterOn("s", Operator.EQ, "x").build();
        assertNotNull(cmd.indexQueryPlan());
        assertTrue(cmd.rowFilter().hasStaticExpression());
        return cmd;
    }

    // a deletion of one clustering by a range tombstone with a local deletion time in the past
    private void deleteRangeWithOldLocalDeletionTime(long timestamp, int id, int id2)
    {
        new RowUpdateBuilder(getCurrentColumnFamilyStore().metadata(), FBUtilities.nowInSeconds() - 100, timestamp, id).addRangeTombstone(id2, id2).build().apply();
    }

    // what the read returns from the row before the given clustering to the row after it
    private static List<String> around(UnfilteredRowIterator partition, int clustering)
    {
        List<String> unfiltereds = new ArrayList<>();
        boolean collect = false;
        while (partition.hasNext())
        {
            Unfiltered unfiltered = partition.next();
            if (unfiltered.isRow() && clustering((Row) unfiltered) == clustering - 1)
                collect = true;
            if (collect)
                unfiltereds.add(unfiltered.isRow() ? "row " + clustering((Row) unfiltered) : ((RangeTombstoneMarker) unfiltered).isOpen(false) ? "open" : "close");
            if (unfiltered.isRow() && clustering((Row) unfiltered) == clustering + 1)
                collect = false;
        }
        return unfiltereds;
    }

    // an sstable that holds only a deletion of t2 with an old local deletion time, flushed and removed from the table
    private SSTableReader removedSSTableOfOldDeletion(ColumnFamilyStore cfs, int id, int id2)
    {
        deleteT2WithOldLocalDeletionTime(id, id2);
        flush();
        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());
        cfs.getTracker().removeUnsafe(Collections.singleton(sstable));
        return sstable;
    }

    // adds the sstable to the table as a stream or an import does; a legacy index builds its entries for it, which
    // waits for the running read, so it is added from another thread
    private static Thread addSSTable(ColumnFamilyStore cfs, SSTableReader sstable)
    {
        // the flush already linked the sstable into the backups directory
        boolean incrementalBackups = DatabaseDescriptor.isIncrementalBackupsEnabled();
        DatabaseDescriptor.setIncrementalBackupsEnabled(false);
        Thread publisher = new Thread(() -> {
            try
            {
                cfs.addSSTable(sstable);
            }
            finally
            {
                DatabaseDescriptor.setIncrementalBackupsEnabled(incrementalBackups);
            }
        });
        publisher.start();
        Util.spinAssertEquals(true, () -> cfs.getLiveSSTables().contains(sstable), 10);
        return publisher;
    }

    private Cell<?> t2(Row row)
    {
        return row.getCell(getCurrentColumnFamilyStore().metadata().getColumn(ByteBufferUtil.bytes("t2")));
    }

    private void assertT2Deleted(Row row)
    {
        assertNotNull("t2 has neither its value nor its deletion", t2(row));
        assertTrue(t2(row).isTombstone());
    }

    private static int clustering(Row row)
    {
        return ByteBufferUtil.toInt(row.clustering().bufferAt(0));
    }

    @Test
    public void readTestOldUnrepaired() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, t2 text, primary key (id, id2)) with gc_grace_seconds=0 and compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");
        getCurrentColumnFamilyStore().disableAutoCompaction();
        for (int i = 0; i < 10; i++)
        {
            execute("delete from %s where id=1 and id2=?", i);
        }
        flush();
        SSTableReader oldSSTable = getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();
        Thread.sleep(2000);
        for (int i = 10; i < 20; i++)
        {
            execute("delete from %s where id=1 and id2=?", i);
        }
        flush();
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
            if (sstable != oldSSTable)
                repair(getCurrentColumnFamilyStore(), sstable);
        Thread.sleep(2000);
        for (int i = 20; i < 30; i++)
        {
            execute("delete from %s where id=1 and id2=?", i);
        }
        flush();

        Thread.sleep(2000);
        // we will keep all tombstones since the oldest tombstones are unrepaired:
        verify(30, 0, 30, false);
        verify2(1, 30, 0, 30, false);
    }

    private void verify()
    {
        verify(10, 10, 20, false);
    }

    private void verifyIncludingPurgeable()
    {
        verify(10, 10, 20, true);
    }

    private void verify(int expectedRows, int minVal, int maxVal, boolean includePurgeable)
    {
        ReadCommand cmd = Util.cmd(getCurrentColumnFamilyStore()).build();
        int foundRows = 0;
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator =
             includePurgeable ? cmd.queryStorage(getCurrentColumnFamilyStore(), executionController) :
                                cmd.executeLocally(executionController))
        {
            while (iterator.hasNext())
            {
                try (UnfilteredRowIterator rowIter = iterator.next())
                {
                    if (!rowIter.partitionKey().equals(Util.dk(ByteBufferUtil.bytes(999)))) // partition key 999 is 'live' and used to avoid sstables from being dropped
                    {
                        while (rowIter.hasNext())
                        {
                            AbstractRow row = (AbstractRow) rowIter.next();
                            for (int i = 0; i < row.clustering().size(); i++)
                            {
                                foundRows++;
                                int val = ByteBufferUtil.toInt(row.clustering().bufferAt(i));
                                assertTrue("val=" + val, val >= minVal && val < maxVal);
                            }
                        }
                    }
                    else
                    {
                        while (rowIter.hasNext()) rowIter.next();
                    }
                }
            }
        }
        assertEquals(expectedRows, foundRows);
    }

    private void verify2(int key)
    {
        verify2(key, 10, 10, 20, false);
    }

    private void verify2IncludingPurgeable(int key)
    {
        verify2(key, 10, 10, 20, true);
    }

    private void verify2(int key, int expectedRows, int minVal, int maxVal, boolean includePurgeable)
    {
        ReadCommand cmd = Util.cmd(getCurrentColumnFamilyStore(), Util.dk(ByteBufferUtil.bytes(key))).build();
        int foundRows = 0;
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator =
             includePurgeable ? cmd.queryStorage(getCurrentColumnFamilyStore(), executionController) :
                                cmd.executeLocally(executionController))
        {
            while (iterator.hasNext())
            {
                try (UnfilteredRowIterator rowIter = iterator.next())
                {
                    while (rowIter.hasNext())
                    {
                        AbstractRow row = (AbstractRow) rowIter.next();
                        for (int i = 0; i < row.clustering().size(); i++)
                        {
                            foundRows++;
                            int val = ByteBufferUtil.toInt(row.clustering().bufferAt(i));
                            assertTrue("val=" + val, val >= minVal && val < maxVal);
                        }
                    }
                }
            }
        }
        assertEquals(expectedRows, foundRows);
    }

    private void verifyRow(int key, int id2, int expectedRows)
    {
        ReadCommand cmd = Util.cmd(getCurrentColumnFamilyStore(), Util.dk(ByteBufferUtil.bytes(key))).includeRow(id2).columns("t").build();
        int foundRows = 0;
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
        {
            while (iterator.hasNext())
            {
                try (UnfilteredRowIterator rowIter = iterator.next())
                {
                    while (rowIter.hasNext())
                    {
                        rowIter.next();
                        foundRows++;
                    }
                }
            }
        }
        assertEquals(expectedRows, foundRows);
    }

    public static void repair(ColumnFamilyStore cfs, SSTableReader sstable) throws IOException
    {
        sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, 1, null, false);
        sstable.reloadSSTableMetadata();
        cfs.getTracker().notifySSTableRepairedStatusChanged(Collections.singleton(sstable));
    }
}