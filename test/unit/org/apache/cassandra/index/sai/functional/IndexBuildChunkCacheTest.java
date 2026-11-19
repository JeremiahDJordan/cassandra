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

package org.apache.cassandra.index.sai.functional;

import java.nio.ByteBuffer;

import com.google.common.collect.Iterators;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.service.CacheService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An index build over an sstable with partitions with and without a row index entry, with index files in standard
 * access mode, puts no chunk of the data file and of the primary index (BIG) or row index (BTI) into the chunk cache,
 * and no key into the key cache.
 */
public class IndexBuildChunkCacheTest extends SAITester
{
    private Config.DiskAccessMode savedIndexAccessMode;
    private SSTableFormat<?, ?> savedSSTableFormat;

    @Before
    public void setIndexAccessMode()
    {
        assertThat(ChunkCache.instance).isNotNull();
        savedIndexAccessMode = DatabaseDescriptor.getIndexAccessMode();
        savedSSTableFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setIndexAccessMode(Config.DiskAccessMode.standard);
    }

    @After
    public void restoreConfiguration()
    {
        DatabaseDescriptor.setIndexAccessMode(savedIndexAccessMode);
        DatabaseDescriptor.setSelectedSSTableFormat(savedSSTableFormat);
    }

    @Test
    public void testIndexBuildOnBigSSTableBypassesCaches()
    {
        testIndexBuildBypassesCaches(BigFormat.NAME, BigFormat.Components.PRIMARY_INDEX);
    }

    @Test
    public void testIndexBuildOnBtiSSTableBypassesCaches()
    {
        testIndexBuildBypassesCaches(BtiFormat.NAME, BtiFormat.Components.ROW_INDEX);
    }

    private void testIndexBuildBypassesCaches(String formatName, Component indexComponent)
    {
        DatabaseDescriptor.setSelectedSSTableFormat(DatabaseDescriptor.getSSTableFormats().get(formatName));
        createTable("CREATE TABLE %s (pk int, ck int, v blob, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': 16}");
        disableCompaction();
        for (int pk = 0; pk < 100; pk++)
            for (int ck = 0; ck < 50; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, ByteBuffer.wrap(new byte[200]));
        for (int pk = 100; pk < 400; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 0, ByteBuffer.wrap(new byte[200]));
        flush();

        SSTableReader sstable = getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();
        assertThat(sstable.descriptor.version.format.name()).isEqualTo(formatName);
        String dataFile = sstable.getFilename();
        String indexFile = sstable.descriptor.fileFor(indexComponent).path();
        ChunkCache.instance.invalidateFile(dataFile);
        ChunkCache.instance.invalidateFile(indexFile);
        CacheService.instance.invalidateKeyCacheForCf(sstable.metadata());

        createIndex("CREATE INDEX ON %s(ck) USING 'sai'");

        assertThat(ChunkCache.instance.sizeOfFile(dataFile)).as("an index build must not populate the chunk cache with data file chunks").isZero();
        assertThat(ChunkCache.instance.sizeOfFile(indexFile)).as("an index build must not populate the chunk cache with index file chunks").isZero();
        assertThat(keyCacheSize(sstable)).as("an index build must not populate the key cache").isZero();

        execute("SELECT * FROM %s WHERE pk = 0");
        if (formatName.equals(BigFormat.NAME) && CacheService.instance.keyCache.getCapacity() > 0)
            assertThat(keyCacheSize(sstable)).as("a read populates the key cache").isEqualTo(1);
    }

    private static int keyCacheSize(SSTableReader sstable)
    {
        return Iterators.size(Iterators.filter(CacheService.instance.keyCache.keyIterator(), key -> key.desc.equals(sstable.descriptor)));
    }
}
