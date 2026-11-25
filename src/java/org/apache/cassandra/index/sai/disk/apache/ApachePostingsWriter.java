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

package org.apache.cassandra.index.sai.disk.apache;

import java.io.Closeable;
import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.collections.LongArrayList;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.io.IndexOutput;
import org.apache.cassandra.index.sai.disk.oldlucene.DirectWriterAdapter;
import org.apache.cassandra.index.sai.disk.oldlucene.LuceneCompat;
import org.apache.cassandra.index.sai.disk.oldlucene.ResettableByteBuffersIndexOutput;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.max;

/**
 * Writes posting lists the way Apache Cassandra 5.0 does (cassandra-5.0.9 {@code PostingsWriter}): blocks of 128
 * deltas, the first block of a list led by its first posting, every block padded to 128 values, then a summary with
 * the skip table of block offsets and block maximums.
 */
@NotThreadSafe
public class ApachePostingsWriter implements Closeable
{
    private static final int BLOCK_SIZE = 128;

    private static final String POSTINGS_MUST_BE_SORTED_ERROR_MSG = "Postings must be sorted ascending, got [%s] after [%s]";

    private final IndexOutput dataOutput;
    private final long[] deltaBuffer = new long[BLOCK_SIZE];
    private final LongArrayList blockOffsets = new LongArrayList();
    private final LongArrayList blockMaximumPostings = new LongArrayList();
    private final ResettableByteBuffersIndexOutput inMemoryOutput;

    private final long startOffset;

    private int bufferUpto;
    private long firstPosting = Long.MIN_VALUE;
    private long lastPosting = Long.MIN_VALUE;
    private long maxDelta;

    public ApachePostingsWriter(IndexComponents.ForWrite components) throws IOException
    {
        this(components.addOrGet(IndexComponentType.POSTING_LISTS).openOutput(true));
    }

    public ApachePostingsWriter(IndexOutput dataOutput) throws IOException
    {
        this.dataOutput = dataOutput;
        this.startOffset = dataOutput.getFilePointer();
        this.inMemoryOutput = LuceneCompat.getResettableByteBuffersIndexOutput(dataOutput.order(), 1024, "blockOffsets", dataOutput.version());
        SAICodecUtils.writeHeader(dataOutput);
    }

    public long getFilePointer()
    {
        return dataOutput.getFilePointer();
    }

    /**
     * @return the file pointer where this segment's postings start (before the header)
     */
    public long getStartOffset()
    {
        return startOffset;
    }

    public void complete() throws IOException
    {
        SAICodecUtils.writeFooter(dataOutput);
    }

    @Override
    public void close() throws IOException
    {
        dataOutput.close();
    }

    /**
     * Writes a non-empty posting list and returns the file offset of its summary.
     */
    public long write(PostingList postings) throws IOException
    {
        checkArgument(postings != null, "Expected non-null posting list.");
        checkArgument(postings.size() > 0, "Expected non-empty posting list.");

        lastPosting = Long.MIN_VALUE;
        resetBlockCounters();
        blockOffsets.clear();
        blockMaximumPostings.clear();

        // a merged posting list knows only an upper bound of its size
        int size = 0;
        int posting;
        while ((posting = postings.nextPosting()) != PostingList.END_OF_STREAM)
        {
            writePosting(posting);
            size++;
        }
        assert size > 0 : "No postings were written";

        finish();

        long summaryOffset = dataOutput.getFilePointer();
        writeSummary(size);
        return summaryOffset;
    }

    private void writePosting(long posting) throws IOException
    {
        if (lastPosting == Long.MIN_VALUE)
        {
            firstPosting = posting;
            deltaBuffer[bufferUpto++] = 0;
        }
        else
        {
            if (posting < lastPosting)
                throw new IllegalArgumentException(String.format(POSTINGS_MUST_BE_SORTED_ERROR_MSG, posting, lastPosting));
            long delta = posting - lastPosting;
            maxDelta = max(maxDelta, delta);
            deltaBuffer[bufferUpto++] = delta;
        }
        lastPosting = posting;

        if (bufferUpto == BLOCK_SIZE)
        {
            addBlockToSkipTable();
            writePostingsBlock();
            resetBlockCounters();
        }
    }

    private void finish() throws IOException
    {
        if (bufferUpto > 0)
        {
            addBlockToSkipTable();
            writePostingsBlock();
        }
    }

    private void resetBlockCounters()
    {
        firstPosting = Long.MIN_VALUE;
        bufferUpto = 0;
        maxDelta = 0;
    }

    private void addBlockToSkipTable()
    {
        blockOffsets.add(dataOutput.getFilePointer());
        blockMaximumPostings.add(lastPosting);
    }

    private void writeSummary(int exactSize) throws IOException
    {
        dataOutput.writeVInt(BLOCK_SIZE);
        dataOutput.writeVInt(exactSize);
        writeSkipTable();
    }

    private void writeSkipTable() throws IOException
    {
        assert blockOffsets.size() == blockMaximumPostings.size();
        dataOutput.writeVInt(blockOffsets.size());

        // the offsets are written in memory first, to know their exact length with padding
        inMemoryOutput.reset();
        writeSortedFoRBlock(blockOffsets, inMemoryOutput);
        dataOutput.writeVLong(inMemoryOutput.getFilePointer());
        inMemoryOutput.copyTo(dataOutput);
        writeSortedFoRBlock(blockMaximumPostings, dataOutput);
    }

    private void writePostingsBlock() throws IOException
    {
        int bitsPerValue = maxDelta == 0 ? 0 : LuceneCompat.directWriterUnsignedBitsRequired(dataOutput.order(), maxDelta);

        // the first block of a posting list starts with its first posting
        if (firstPosting != Long.MIN_VALUE)
            dataOutput.writeVLong(firstPosting);

        dataOutput.writeByte((byte) bitsPerValue);
        if (bitsPerValue > 0)
        {
            DirectWriterAdapter writer = LuceneCompat.directWriterGetInstance(dataOutput.order(), dataOutput, BLOCK_SIZE, bitsPerValue);
            for (int index = 0; index < bufferUpto; ++index)
                writer.add(deltaBuffer[index]);
            // padding, so that no value of an earlier block is read
            for (int index = bufferUpto; index < BLOCK_SIZE; index++)
                writer.add(0);
            writer.finish();
        }
    }

    private void writeSortedFoRBlock(LongArrayList values, IndexOutput output) throws IOException
    {
        assert !values.isEmpty();
        long maxValue = values.getLong(values.size() - 1);
        int bitsPerValue = maxValue == 0 ? 0 : LuceneCompat.directWriterUnsignedBitsRequired(output.order(), maxValue);
        output.writeByte((byte) bitsPerValue);
        if (bitsPerValue > 0)
        {
            DirectWriterAdapter writer = LuceneCompat.directWriterGetInstance(output.order(), output, values.size(), bitsPerValue);
            for (int i = 0; i < values.size(); ++i)
                writer.add(values.getLong(i));
            writer.finish();
        }
    }
}
