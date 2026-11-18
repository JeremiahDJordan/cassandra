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
package org.apache.cassandra.index.sai.cql;

import java.io.RandomAccessFile;
import java.util.List;
import java.util.zip.CRC32;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.Segment;
import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Checksum validation of a vector index with several segments, each of which has its own header and footer in each
 * component file: small segments written by the initial build of the index, and segments large enough for product
 * quantization written by compaction.
 */
public class VectorChecksumValidationTest extends VectorTester.Versioned
{
    private static final IndexComponentType[] COMPONENTS = { IndexComponentType.TERMS_DATA,
                                                             IndexComponentType.PQ,
                                                             IndexComponentType.POSTING_LISTS };

    private static final int DIMENSION = 16;
    private static final int ROWS_PER_COMPACTED_SEGMENT = CassandraOnHeapGraph.MIN_PQ_ROWS + 100;

    @After
    public void resetSegmentRowIds()
    {
        SegmentBuilder.updateLastValidSegmentRowId(-1);
    }

    @Test
    public void testMultiSegmentIndexPassesChecksumValidation()
    {
        IndexContext context = createMultiSegmentIndex();
        assertTrue(verifyChecksum(context));
    }

    @Test
    public void testCorruptSegmentFailsChecksumValidation() throws Throwable
    {
        for (IndexComponentType component : COMPONENTS)
        {
            for (boolean lastSegment : new boolean[]{ false, true })
            {
                IndexContext context = createMultiSegmentIndex();
                flipByteInSegment(context, component, lastSegment ? -1 : 0);
                assertFalse(component + (lastSegment ? " last segment" : " first segment"), verifyChecksum(context));
            }
        }
    }

    @Test
    public void testGraphFileChecksumsAcceptedBeforeEC() throws Throwable
    {
        IndexContext context = createMultiSegmentIndex();
        writeFileChecksumsInGraphFooters(context);
        boolean accepted = verifyChecksum(context);
        assertEquals(!version.onOrAfter(Version.EC), accepted);

        if (accepted)
        {
            flipByteInSegment(context, IndexComponentType.TERMS_DATA, -1);
            assertFalse(verifyChecksum(context));
        }
    }

    @Test
    public void testCompactedMultiSegmentIndexPassesChecksumValidation()
    {
        IndexContext context = createCompactedMultiSegmentIndex();
        assertTrue(verifyChecksum(context));
    }

    @Test
    public void testCorruptCompactedSegmentFailsChecksumValidation() throws Throwable
    {
        for (IndexComponentType component : COMPONENTS)
        {
            IndexContext context = createCompactedMultiSegmentIndex();
            flipByteInSegment(context, component, 1);
            assertFalse(component + " middle segment", verifyChecksum(context));
        }
    }

    private IndexContext createMultiSegmentIndex()
    {
        SegmentBuilder.updateLastValidSegmentRowId(-1);
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val vector<float, 3>)");
        disableCompaction();
        for (int pk = 0; pk < 100; pk++)
            execute("INSERT INTO %s (pk, val) VALUES (?, ?)", pk, vector((float) pk, (float) pk + 1, (float) pk + 2));
        flush();

        SegmentBuilder.updateLastValidSegmentRowId(17); // 17 rows per segment
        IndexContext context = getIndexContext(createVectorIndex("%s", "val"));
        assertSegments(context, 2);
        return context;
    }

    private IndexContext createCompactedMultiSegmentIndex()
    {
        SegmentBuilder.updateLastValidSegmentRowId(-1);
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val vector<float, " + DIMENSION + ">)");
        IndexContext context = getIndexContext(createVectorIndex("%s", "val"));
        disableCompaction();
        int pk = 0;
        for (int sstable = 0; sstable < 3; sstable++)
        {
            for (int row = 0; row < ROWS_PER_COMPACTED_SEGMENT; row++)
                execute("INSERT INTO %s (pk, val) VALUES (?, ?)", pk++, vector(randomVector(DIMENSION)));
            flush();
        }

        SegmentBuilder.updateLastValidSegmentRowId(ROWS_PER_COMPACTED_SEGMENT - 1);
        compact();
        assertSegments(context, 3);
        return context;
    }

    private static void assertSegments(IndexContext context, int minSegments)
    {
        assertFalse(context.getView().getIndexes().isEmpty());
        for (SSTableIndex index : context.getView().getIndexes())
            assertTrue("segments " + index.getSegments().size(), index.getSegments().size() >= minSegments);
    }

    /**
     * Writes in the footer of each segment of the graph file of each SSTable index the checksum of the file up to that
     * checksum, as the graph writers of earlier releases did.
     */
    private void writeFileChecksumsInGraphFooters(IndexContext context) throws Exception
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        for (SSTableIndex index : context.getView().getIndexes())
        {
            List<Segment> segments = index.getSegments();
            File file = loadDescriptor(index.getSSTable(), cfs).perIndexComponents(context).get(IndexComponentType.TERMS_DATA).file();
            try (RandomAccessFile raf = new RandomAccessFile(file.toJavaIOFile(), "rw"))
            {
                for (int i = 0; i < segments.size(); i++)
                {
                    long segmentEnd = i + 1 < segments.size()
                                      ? segments.get(i + 1).metadata.componentMetadatas.get(IndexComponentType.TERMS_DATA).offset - SAICodecUtils.headerSize()
                                      : raf.length();
                    long checksumPosition = segmentEnd - Long.BYTES;
                    byte[] bytes = new byte[(int) checksumPosition];
                    raf.seek(0);
                    raf.readFully(bytes);
                    CRC32 checksum = new CRC32();
                    checksum.update(bytes);
                    raf.seek(checksumPosition);
                    raf.writeLong(checksum.getValue());
                }
            }
        }
    }

    /**
     * Flips a byte in the middle of the given segment (from the end if negative) of the component of each SSTable index.
     */
    private void flipByteInSegment(IndexContext context, IndexComponentType component, int segment) throws Exception
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        for (SSTableIndex index : context.getView().getIndexes())
        {
            int segmentIndex = segment < 0 ? index.getSegments().size() + segment : segment;
            SegmentMetadata.ComponentMetadata metadata = index.getSegments().get(segmentIndex).metadata.componentMetadatas.get(component);
            SSTableReader sstable = index.getSSTable();
            File file = loadDescriptor(sstable, cfs).perIndexComponents(context).get(component).file();
            try (RandomAccessFile raf = new RandomAccessFile(file.toJavaIOFile(), "rw"))
            {
                long position = metadata.offset + metadata.length / 2;
                raf.seek(position);
                int b = raf.read();
                raf.seek(position);
                raf.write(~b);
            }
        }
    }
}
