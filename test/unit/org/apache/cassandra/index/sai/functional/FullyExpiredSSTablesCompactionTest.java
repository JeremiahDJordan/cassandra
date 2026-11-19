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

import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.NoopIndex;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Compaction drops fully expired SSTables without reading them, unless an index asks to be told about their rows
 * (CASSANDRA-20829). SSTable-attached indexes don't need that, so they must not make compaction read those SSTables.
 */
public class FullyExpiredSSTablesCompactionTest extends SAITester
{
    private static final String TWCS = "{'class': 'TimeWindowCompactionStrategy', " +
                                       " 'compaction_window_size': 1, " +
                                       " 'compaction_window_unit': 'MINUTES', " +
                                       " 'expired_sstable_check_frequency_seconds': 1}";

    // Counts the full scans of an SSTable, which is how expired SSTables are read to notify indexes about their rows.
    private static final Injections.Counter FULL_SCANS = Injections.newCounter("FullSSTableScans")
                                                                   .add(InvokePointBuilder.newInvokePoint()
                                                                                          .onClass(SSTableReader.class)
                                                                                          .onMethod("getScanner()"))
                                                                   .build();

    @After
    public void removeInjections()
    {
        Injections.deleteAll();
    }

    @Test
    public void shouldNotReadFullyExpiredSSTablesForStorageAttachedIndex() throws Throwable
    {
        assertFullyExpiredSSTableIsDroppedWithoutReading("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
    }

    @Test
    public void shouldNotReadFullyExpiredSSTablesForNoopIndex() throws Throwable
    {
        assertFullyExpiredSSTableIsDroppedWithoutReading("CREATE CUSTOM INDEX ON %s(v) USING '" + NoopIndex.class.getName() + '\'');
    }

    private void assertFullyExpiredSSTableIsDroppedWithoutReading(String createIndex) throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH compaction = " + TWCS + " AND gc_grace_seconds = 0");
        disableCompaction(KEYSPACE);
        createIndex(createIndex);

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, v) VALUES (?, ?) USING TTL 1", i, i);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertEquals(1, cfs.getLiveSSTables().size());
        awaitFullyExpired(cfs);

        Injections.inject(FULL_SCANS);
        FULL_SCANS.reset();
        compact();

        assertTrue("The fully expired SSTable should have been dropped", cfs.getLiveSSTables().isEmpty());
        assertEquals("The fully expired SSTable should have been dropped without reading it", 0, FULL_SCANS.get());
    }

    private void awaitFullyExpired(ColumnFamilyStore cfs)
    {
        assertFalse(cfs.getLiveSSTables().isEmpty());
        long maxLocalDeletionTime = cfs.getLiveSSTables().stream()
                                       .mapToLong(SSTableReader::getMaxLocalDeletionTime)
                                       .max()
                                       .orElseThrow(AssertionError::new);

        waitForAssert(() -> assertTrue("Timed out waiting for the SSTables to become fully expired",
                                       FBUtilities.nowInSeconds() > maxLocalDeletionTime),
                      30, TimeUnit.SECONDS);
    }
}
