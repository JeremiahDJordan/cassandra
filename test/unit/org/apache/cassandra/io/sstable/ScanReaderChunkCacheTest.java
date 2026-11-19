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
package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.compaction.IteratorFromCursor;
import org.apache.cassandra.io.sstable.compaction.SortedStringTableCursor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Compaction, verification, key iteration for a scan, filter rebuilds and index builds read sstables with scan readers,
 * which bypass the chunk cache; range reads and the other key readers use it. Index files are read through the chunk
 * cache too (standard access mode), and the table has partitions with and without a row index entry.
 */
@RunWith(Parameterized.class)
public class ScanReaderChunkCacheTest extends CQLTester
{
    private static Config.DiskAccessMode savedIndexAccessMode;
    private static SSTableFormat<?, ?> savedSSTableFormat;

    @Parameterized.Parameters(name = "format={0}")
    public static Collection<Object> generateParameters()
    {
        // We need to set up the class here, as the parameterized test runner will not call the @BeforeClass method
        paramaterizedSetUpClass();

        return Lists.newArrayList(DatabaseDescriptor.getSSTableFormats().values());
    }

    public static void paramaterizedSetUpClass()
    {
        CQLTester.setUpClass();
        savedIndexAccessMode = DatabaseDescriptor.getIndexAccessMode();
        savedSSTableFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setIndexAccessMode(Config.DiskAccessMode.standard);
    }

    @BeforeClass
    public static void setUpClass() // override CQLTester's setUpClass
    {
        // No-op, as initialization was done in paramaterizedSetUpClass, and we don't want to call CQLTester.setUpClass again
    }

    @AfterClass
    public static void restoreConfiguration()
    {
        DatabaseDescriptor.setIndexAccessMode(savedIndexAccessMode);
        DatabaseDescriptor.setSelectedSSTableFormat(savedSSTableFormat);
    }

    @Parameterized.Parameter
    public SSTableFormat<?, ?> format = DatabaseDescriptor.getSelectedSSTableFormat();

    private ColumnFamilyStore cfs;
    private SSTableReader sstable;

    @Before
    public void createSSTable() throws Throwable
    {
        assertTrue(ChunkCache.instance.isEnabled());
        DatabaseDescriptor.setSelectedSSTableFormat(format);
        createTable("CREATE TABLE %s (pk int, ck int, v blob, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': 16}");
        cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int pk = 0; pk < 100; pk++)
            for (int ck = 0; ck < 50; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, ByteBuffer.wrap(new byte[200]));
        for (int pk = 100; pk < 400; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 0, ByteBuffer.wrap(new byte[200]));
        flush();
        sstable = cfs.getLiveSSTables().iterator().next();
        assertEquals(format, sstable.descriptor.version.format);
        ChunkCache.instance.invalidateFileNow(sstable.getDataFile());
        ChunkCache.instance.invalidateFileNow(indexFile());
    }

    @Test
    public void testCompactionCursorBypassesChunkCache()
    {
        readAll(new IteratorFromCursor(sstable.metadata(), new SortedStringTableCursor(sstable)));
        assertEquals(0, ChunkCache.instance.sizeOfFile(sstable.getDataFile()));
    }

    @Test
    public void testScannerBypassesChunkCache()
    {
        readAll(sstable.getScanner());
        assertEquals(0, ChunkCache.instance.sizeOfFile(sstable.getDataFile()));
    }

    @Test
    public void testVerifierBypassesChunkCache()
    {
        Token min = sstable.getPartitioner().getMinimumToken();
        IVerifier.Options options = IVerifier.options()
                                             .extendedVerification(true)
                                             .checkOwnsTokens(true)
                                             .tokenLookup(keyspace -> Collections.singletonList(new Range<>(min, min)))
                                             .build();
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, options))
        {
            verifier.verify();
        }
        assertEquals(0, ChunkCache.instance.sizeOfFile(sstable.getDataFile()));
        assertEquals(0, ChunkCache.instance.sizeOfFile(indexFile()));
    }

    @Test
    public void testKeyReaderForScanBypassesChunkCache() throws IOException
    {
        assertEquals(400, readAll(sstable.keyReaderForScan()));
        assertEquals(0, ChunkCache.instance.sizeOfFile(sstable.getDataFile()));
        assertEquals(0, ChunkCache.instance.sizeOfFile(indexFile()));
    }

    @Test
    public void testKeyReaderUsesChunkCache() throws IOException
    {
        assertEquals(400, readAll(sstable.keyReader()));
        if (format instanceof BtiFormat)
            assertTrue(ChunkCache.instance.sizeOfFile(sstable.getDataFile()) > 0);
        assertTrue(ChunkCache.instance.sizeOfFile(indexFile()) > 0);
    }

    @Test
    public void testFilterRebuildOnLoadBypassesChunkCache()
    {
        sstable.descriptor.fileFor(SSTableFormat.Components.FILTER).delete();
        if (format instanceof BigFormat)
            sstable.descriptor.fileFor(BigFormat.Components.SUMMARY).delete();
        SSTableReader reopened = SSTableReader.open(cfs, sstable.descriptor);
        try
        {
            assertTrue(sstable.descriptor.fileFor(SSTableFormat.Components.FILTER).exists());
            assertEquals(0, ChunkCache.instance.sizeOfFile(sstable.getDataFile()));
            assertEquals(0, ChunkCache.instance.sizeOfFile(indexFile()));
        }
        finally
        {
            reopened.selfRef().release();
        }
    }

    @Test
    public void testIndexBuildBypassesChunkCache()
    {
        createIndex("CREATE CUSTOM INDEX ON %s(ck) USING 'StorageAttachedIndex'");
        assertEquals(0, ChunkCache.instance.sizeOfFile(sstable.getDataFile()));
        assertEquals(0, ChunkCache.instance.sizeOfFile(indexFile()));
    }

    @Test
    public void testRangeReadUsesChunkCache()
    {
        readAll(sstable.partitionIterator(ColumnFilter.all(sstable.metadata()),
                                          DataRange.allData(sstable.getPartitioner()),
                                          SSTableReadsListener.NOOP_LISTENER));
        assertTrue(ChunkCache.instance.sizeOfFile(sstable.getDataFile()) > 0);
    }

    private File indexFile()
    {
        return sstable.descriptor.fileFor(format instanceof BigFormat ? BigFormat.Components.PRIMARY_INDEX
                                                                      : BtiFormat.Components.ROW_INDEX);
    }

    private static int readAll(KeyReader reader) throws IOException
    {
        int keys = 0;
        try (reader)
        {
            for (boolean more = !reader.isExhausted(); more; more = reader.advance())
                keys++;
        }
        return keys;
    }

    private static void readAll(UnfilteredPartitionIterator partitions)
    {
        try (partitions)
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    partition.forEachRemaining(unfiltered -> {});
                }
            }
        }
    }
}
