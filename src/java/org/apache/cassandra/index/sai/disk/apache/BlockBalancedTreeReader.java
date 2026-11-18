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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.Nullable;

import com.google.common.base.Stopwatch;
import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntLongHashMap;
import com.carrotsearch.hppc.IntLongMap;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.index.sai.disk.io.IndexInput;
import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.disk.oldlucene.LuceneCompat;
import org.apache.cassandra.index.sai.disk.v1.postings.FilteringPostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.MergePostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsReader;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.RowIdWithByteComparable;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongValues;

/**
 * Searches a block balanced tree written by Apache Cassandra 5.0's {@code BlockBalancedTreeWriter}: the tree and its
 * leaves in the {@code BalancedTree} component, the postings of its leaves and of some of its internal nodes in the
 * {@code PostingLists} component, along with the index of those postings.
 * <p>
 * Port of Apache's {@code org.apache.cassandra.index.sai.disk.v1.bbtree.BlockBalancedTreeReader} (5.0), reading
 * Apache's postings with our {@link PostingsReader}.
 */
class BlockBalancedTreeReader extends BlockBalancedTreeWalker
{
    private static final Logger logger = LoggerFactory.getLogger(BlockBalancedTreeReader.class);

    private final IndexContext indexContext;
    private final FileHandle postingsFile;
    private final IntLongMap postingsIndex;
    private final int leafOrderMapBitsRequired;

    /**
     * Takes ownership of the provided files: they are closed with this reader, and if creating it fails, the tree
     * index file is closed, but not the postings file.
     */
    BlockBalancedTreeReader(IndexContext indexContext,
                            FileHandle treeIndexFile,
                            long treeIndexRoot,
                            FileHandle postingsFile,
                            long treePostingsRoot) throws IOException
    {
        super(treeIndexFile, treeIndexRoot);
        this.indexContext = indexContext;
        this.postingsFile = postingsFile;
        try
        {
            this.postingsIndex = readPostingsIndex(postingsFile, treePostingsRoot);
        }
        catch (Throwable t)
        {
            super.close();
            throw t;
        }
        // the leaf order maps are unsigned values in [0, maxValuesInLeafNode)
        leafOrderMapBitsRequired = LuceneCompat.directWriterUnsignedBitsRequired(ByteOrder.LITTLE_ENDIAN, maxValuesInLeafNode - 1);
    }

    /**
     * Reads Apache's {@code BlockBalancedTreePostingsIndex}: the file pointer of the postings of each node that has
     * some (every leaf, and some internal nodes).
     */
    private static IntLongMap readPostingsIndex(FileHandle postingsFile, long filePosition) throws IOException
    {
        try (IndexInputReader input = IndexInputReader.create(postingsFile.createReader()))
        {
            SAICodecUtils.validate(input, Version.APACHE_AA);
            input.seek(filePosition);

            int size = input.readVInt();
            IntLongMap index = new IntLongHashMap(size);
            for (int x = 0; x < size; x++)
            {
                int node = input.readVInt();
                long filePointer = input.readVLong();
                index.put(node, filePointer);
            }
            return index;
        }
    }

    public int getBytesPerValue()
    {
        return bytesPerValue;
    }

    public long getPointCount()
    {
        return valueCount;
    }

    @Override
    public void close()
    {
        super.close();
        FileUtils.closeQuietly(postingsFile);
    }

    /**
     * @return the postings of the values matching the provided visitor, or {@code null} if there are none
     */
    public PostingList intersect(IntersectVisitor visitor, QueryEventListener.BKDIndexEventListener listener, QueryContext context)
    {
        Relation relation = visitor.compare(minPackedValue, maxPackedValue);

        if (relation == Relation.CELL_OUTSIDE_QUERY)
        {
            listener.onIntersectionEarlyExit();
            return null;
        }

        listener.onSegmentHit();
        IndexInput treeInput = IndexFileUtils.instance().openInput(treeIndexFile);
        IndexInput postingsInput = IndexFileUtils.instance().openInput(postingsFile);
        IndexInput postingsSummaryInput = IndexFileUtils.instance().openInput(postingsFile);

        Intersection intersection = relation == Relation.CELL_INSIDE_QUERY
                                    ? new Intersection(treeInput, postingsInput, postingsSummaryInput, listener, context)
                                    : new FilteringIntersection(treeInput, postingsInput, postingsSummaryInput, visitor, listener, context);

        return intersection.execute();
    }

    /**
     * Iterates the values of the tree accepted by the provided visitor, with their row id, in the order of the values.
     *
     * @param visitor the values to iterate, or {@code null} for all of them
     * @param ascending whether to iterate in increasing order of the values, or decreasing
     * @param termMapper maps the bytes of a value, as stored in the tree, to the term to return for it
     */
    public CloseableIterator<RowIdWithByteComparable> iterate(@Nullable IntersectVisitor visitor, boolean ascending, Function<byte[], ByteComparable> termMapper)
    {
        List<long[]> leaves = new ArrayList<>(); // {nodeID, leafBlockFP}, in increasing order of their values
        collectLeaves(newTraversalState(), visitor, minPackedValue, maxPackedValue, leaves);
        if (!ascending)
            Collections.reverse(leaves);
        return new LeafIterator(leaves, visitor, ascending, termMapper);
    }

    private void collectLeaves(TraversalState state, @Nullable IntersectVisitor visitor, byte[] minPackedValue, byte[] maxPackedValue, List<long[]> leaves)
    {
        if (visitor != null && visitor.compare(minPackedValue, maxPackedValue) == Relation.CELL_OUTSIDE_QUERY)
            return;

        if (state.atLeafNode())
        {
            if (state.nodeExists())
                leaves.add(new long[]{ state.nodeID, state.getLeafBlockFP() });
            return;
        }

        byte[] splitValue = state.getSplitValue().clone();
        state.pushLeft();
        collectLeaves(state, visitor, minPackedValue, splitValue, leaves);
        state.pop();

        state.pushRight();
        collectLeaves(state, visitor, splitValue, maxPackedValue, leaves);
        state.pop();
    }

    /**
     * Iterates the values of leaves, one leaf at a time: the values of a leaf are stored in increasing order, along
     * with the order of their row ids in the postings of the leaf.
     */
    private class LeafIterator extends AbstractIterator<RowIdWithByteComparable> implements CloseableIterator<RowIdWithByteComparable>
    {
        private final List<long[]> leaves;
        @Nullable
        private final IntersectVisitor visitor;
        private final boolean ascending;
        private final Function<byte[], ByteComparable> termMapper;
        private final IndexInput treeInput;
        private final IndexInput postingsInput;
        private final IndexInput postingsSummaryInput;

        private int nextLeaf = 0;
        private final List<RowIdWithByteComparable> current = new ArrayList<>();
        private int currentIndex = 0;

        LeafIterator(List<long[]> leaves, @Nullable IntersectVisitor visitor, boolean ascending, Function<byte[], ByteComparable> termMapper)
        {
            this.leaves = leaves;
            this.visitor = visitor;
            this.ascending = ascending;
            this.termMapper = termMapper;
            this.treeInput = IndexFileUtils.instance().openInput(treeIndexFile);
            this.postingsInput = IndexFileUtils.instance().openInput(postingsFile);
            this.postingsSummaryInput = IndexFileUtils.instance().openInput(postingsFile);
        }

        @Override
        protected RowIdWithByteComparable computeNext()
        {
            try
            {
                while (currentIndex >= current.size())
                {
                    if (nextLeaf >= leaves.size())
                        return endOfData();
                    readLeaf(leaves.get(nextLeaf++));
                }
                return current.get(currentIndex++);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }

        private void readLeaf(long[] leaf) throws IOException
        {
            current.clear();
            currentIndex = 0;

            int nodeID = Math.toIntExact(leaf[0]);
            treeInput.seek(leaf[1]);
            int count = treeInput.readVInt();
            int orderMapLength = treeInput.readVInt();
            long orderMapPointer = treeInput.getFilePointer();
            LongValues orderMap = LuceneCompat.directReaderGetInstance(new SeekingRandomAccessInput(treeInput), leafOrderMapBitsRequired, orderMapPointer);
            int[] postingOrdinals = new int[count];
            for (int i = 0; i < count; i++)
                postingOrdinals[i] = Math.toIntExact(orderMap.get(i));
            treeInput.seek(orderMapPointer + orderMapLength);
            byte[][] values = readLeafValues(treeInput, count);

            if (!postingsIndex.containsKey(nodeID))
                throw new CorruptIndexException(indexContext.logMessage(String.format("Leaf node %s does not have balanced tree postings.", nodeID)), "");
            int[] rowIds = new int[count];
            PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(postingsSummaryInput, postingsIndex.get(nodeID), PostingsReader.InputCloser.NOOP);
            try (PostingsReader postings = new PostingsReader(postingsInput, summary, false, QueryEventListener.PostingListEventListener.NO_OP, PostingsReader.InputCloser.NOOP, true))
            {
                for (int i = 0; i < count; i++)
                    rowIds[i] = postings.nextPosting();
            }

            for (int i = 0; i < count; i++)
            {
                int index = ascending ? i : count - 1 - i;
                if (visitor == null || visitor.contains(values[index]))
                    current.add(new RowIdWithByteComparable(rowIds[postingOrdinals[index]], termMapper.apply(values[index])));
            }
        }

        private byte[][] readLeafValues(IndexInput in, int count) throws IOException
        {
            byte[][] values = new byte[count][];
            byte[] packedValue = new byte[bytesPerValue];
            int commonPrefixLength = in.readVInt();
            if (commonPrefixLength > 0)
                in.readBytes(packedValue, 0, commonPrefixLength);

            if (commonPrefixLength == bytesPerValue)
            {
                for (int i = 0; i < count; i++)
                    values[i] = packedValue;
                return values;
            }

            // the byte at `compressedByteOffset` is compressed using run-length compression,
            // other suffix bytes are stored verbatim
            int compressedByteOffset = commonPrefixLength;
            int suffixOffset = commonPrefixLength + 1;
            int i = 0;
            while (i < count)
            {
                packedValue[compressedByteOffset] = in.readByte();
                int runLength = Byte.toUnsignedInt(in.readByte());
                for (int j = 0; j < runLength; j++)
                {
                    in.readBytes(packedValue, suffixOffset, bytesPerValue - suffixOffset);
                    values[i + j] = packedValue.clone();
                }
                i += runLength;
            }
            if (i != count)
                throw new CorruptIndexException(String.format("Expected %d sub-blocks but read %d.", count, i), in);
            return values;
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(treeInput, postingsInput, postingsSummaryInput);
        }
    }

    /**
     * Synchronous intersection of a point or point range with the tree.
     */
    private class Intersection
    {
        private final Stopwatch queryExecutionTimer = Stopwatch.createStarted();
        final QueryContext context;

        final TraversalState state;
        final IndexInput treeInput;
        final IndexInput postingsInput;
        final IndexInput postingsSummaryInput;
        final QueryEventListener.BKDIndexEventListener listener;
        final List<PostingList> postingLists;

        Intersection(IndexInput treeInput, IndexInput postingsInput, IndexInput postingsSummaryInput,
                     QueryEventListener.BKDIndexEventListener listener, QueryContext context)
        {
            this.state = newTraversalState();
            this.treeInput = treeInput;
            this.postingsInput = postingsInput;
            this.postingsSummaryInput = postingsSummaryInput;
            this.listener = listener;
            this.context = context;
            this.postingLists = new ArrayList<>();
        }

        public PostingList execute()
        {
            try
            {
                executeInternal();

                FileUtils.closeQuietly(treeInput);

                return mergePostings();
            }
            catch (Throwable t)
            {
                if (!(t instanceof AbortedOperationException))
                    logger.error(indexContext.logMessage("Balanced tree intersection failed on {}"), treeIndexFile.path(), t);

                closeOnException();
                throw Throwables.cleaned(t);
            }
        }

        protected void executeInternal() throws IOException
        {
            collectPostingLists();
        }

        protected void closeOnException()
        {
            FileUtils.closeQuietly(postingLists);
            FileUtils.closeQuietly(treeInput);
            FileUtils.closeQuietly(postingsInput);
            FileUtils.closeQuietly(postingsSummaryInput);
        }

        protected PostingList mergePostings() throws IOException
        {
            final long elapsedMicros = queryExecutionTimer.stop().elapsed(TimeUnit.MICROSECONDS);

            listener.onIntersectionComplete(elapsedMicros, TimeUnit.MICROSECONDS);
            listener.postingListsHit(postingLists.size());

            if (postingLists.isEmpty())
            {
                FileUtils.closeQuietly(postingsInput);
                FileUtils.closeQuietly(postingsSummaryInput);
                return null;
            }

            if (logger.isTraceEnabled())
                logger.trace(indexContext.logMessage("[{}] Intersection completed in {} microseconds. {} leaf and internal posting lists hit."),
                             treeIndexFile.path(), elapsedMicros, postingLists.size());
            return MergePostingList.merge(postingLists).onClose(() -> FileUtils.close(postingsInput, postingsSummaryInput));
        }

        void collectPostingLists() throws IOException
        {
            context.checkpoint();

            // This will return true if the node is a child leaf that has postings or if there is postings for the
            // entire subtree under a leaf
            if (postingsIndex.containsKey(state.nodeID))
            {
                postingLists.add(initPostingReader(postingsIndex.get(state.nodeID)));
                return;
            }

            if (state.atLeafNode())
                throw new CorruptIndexException(indexContext.logMessage(String.format("Leaf node %s does not have balanced tree postings.", state.nodeID)), "");

            // Recurse on left subtree:
            state.pushLeft();
            collectPostingLists();
            state.pop();

            // Recurse on right subtree:
            state.pushRight();
            collectPostingLists();
            state.pop();
        }

        private PostingList initPostingReader(long offset) throws IOException
        {
            PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(postingsSummaryInput, offset, PostingsReader.InputCloser.NOOP);
            return new PostingsReader(postingsInput, summary, false, listener.postingListEventListener(), PostingsReader.InputCloser.NOOP, true);
        }
    }

    private class FilteringIntersection extends Intersection
    {
        private final IntersectVisitor visitor;
        private final byte[] packedValue;
        private final short[] origIndex;

        FilteringIntersection(IndexInput treeInput, IndexInput postingsInput, IndexInput postingsSummaryInput,
                              IntersectVisitor visitor, QueryEventListener.BKDIndexEventListener listener, QueryContext context)
        {
            super(treeInput, postingsInput, postingsSummaryInput, listener, context);
            this.visitor = visitor;
            this.packedValue = new byte[bytesPerValue];
            this.origIndex = new short[maxValuesInLeafNode];
        }

        @Override
        public void executeInternal() throws IOException
        {
            collectPostingLists(minPackedValue, maxPackedValue);
        }

        private void collectPostingLists(byte[] minPackedValue, byte[] maxPackedValue) throws IOException
        {
            context.checkpoint();

            final Relation r = visitor.compare(minPackedValue, maxPackedValue);

            // This value range is fully outside the query shape: stop recursing
            if (r == Relation.CELL_OUTSIDE_QUERY)
                return;

            if (r == Relation.CELL_INSIDE_QUERY)
            {
                // This value range is fully inside the query shape: recursively add all points from this node without filtering
                super.collectPostingLists();
                return;
            }

            if (state.atLeafNode())
            {
                if (state.nodeExists())
                    filterLeaf();
                return;
            }

            visitNode(minPackedValue, maxPackedValue);
        }

        private void filterLeaf() throws IOException
        {
            treeInput.seek(state.getLeafBlockFP());

            int count = treeInput.readVInt();
            int orderMapLength = treeInput.readVInt();
            long orderMapPointer = treeInput.getFilePointer();

            SeekingRandomAccessInput randomAccessInput = new SeekingRandomAccessInput(treeInput);
            LongValues leafOrderMapReader = LuceneCompat.directReaderGetInstance(randomAccessInput, leafOrderMapBitsRequired, orderMapPointer);
            for (int index = 0; index < count; index++)
                origIndex[index] = (short) Math.toIntExact(leafOrderMapReader.get(index));

            // seek beyond the ordermap
            treeInput.seek(orderMapPointer + orderMapLength);

            FixedBitSet fixedBitSet = buildPostingsFilter(treeInput, count, visitor, origIndex);

            if (postingsIndex.containsKey(state.nodeID) && fixedBitSet.cardinality() > 0)
            {
                long pointer = postingsIndex.get(state.nodeID);
                postingLists.add(initFilteringPostingReader(pointer, fixedBitSet));
            }
        }

        void visitNode(byte[] minPackedValue, byte[] maxPackedValue) throws IOException
        {
            assert !state.atLeafNode() : "Cannot recurse down tree because nodeID " + state.nodeID + " is a leaf node";

            byte[] splitValue = state.getSplitValue();

            // Recurse on left subtree:
            state.pushLeft();
            collectPostingLists(minPackedValue, splitValue);
            state.pop();

            // Recurse on right subtree:
            state.pushRight();
            collectPostingLists(splitValue, maxPackedValue);
            state.pop();
        }

        private PostingList initFilteringPostingReader(long offset, FixedBitSet filter) throws IOException
        {
            PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(postingsSummaryInput, offset, PostingsReader.InputCloser.NOOP);
            PostingsReader postingsReader = new PostingsReader(postingsInput, summary, false, listener.postingListEventListener(), PostingsReader.InputCloser.NOOP, true);
            return new FilteringPostingList(filter, postingsReader);
        }

        private FixedBitSet buildPostingsFilter(IndexInput in, int count, IntersectVisitor visitor, short[] origIndex) throws IOException
        {
            int commonPrefixLength = readCommonPrefixLength(in);
            return commonPrefixLength == bytesPerValue ? buildPostingsFilterForSingleValueLeaf(count, visitor, origIndex)
                                                       : buildPostingsFilterForMultiValueLeaf(commonPrefixLength, in, count, visitor, origIndex);
        }

        private FixedBitSet buildPostingsFilterForMultiValueLeaf(int commonPrefixLength,
                                                                 IndexInput in,
                                                                 int count,
                                                                 IntersectVisitor visitor,
                                                                 short[] origIndex) throws IOException
        {
            // the byte at `compressedByteOffset` is compressed using run-length compression,
            // other suffix bytes are stored verbatim
            int compressedByteOffset = commonPrefixLength;
            commonPrefixLength++;
            int i;

            FixedBitSet fixedBitSet = new FixedBitSet(maxValuesInLeafNode);

            for (i = 0; i < count; )
            {
                packedValue[compressedByteOffset] = in.readByte();
                final int runLen = Byte.toUnsignedInt(in.readByte());
                for (int j = 0; j < runLen; ++j)
                {
                    in.readBytes(packedValue, commonPrefixLength, bytesPerValue - commonPrefixLength);
                    final int rowIDIndex = origIndex[i + j];
                    if (visitor.contains(packedValue))
                        fixedBitSet.set(rowIDIndex);
                }
                i += runLen;
            }
            if (i != count)
                throw new CorruptIndexException(String.format("Expected %d sub-blocks but read %d.", count, i), in);

            return fixedBitSet;
        }

        private FixedBitSet buildPostingsFilterForSingleValueLeaf(int count, IntersectVisitor visitor, final short[] origIndex)
        {
            FixedBitSet fixedBitSet = new FixedBitSet(maxValuesInLeafNode);

            // All the values in the leaf are the same, so we only
            // need to visit once then set the bits for the relevant indexes
            if (visitor.contains(packedValue))
            {
                for (int i = 0; i < count; ++i)
                    fixedBitSet.set(origIndex[i]);
            }
            return fixedBitSet;
        }

        private int readCommonPrefixLength(IndexInput in) throws IOException
        {
            int prefixLength = in.readVInt();
            if (prefixLength > 0)
                in.readBytes(packedValue, 0, prefixLength);
            return prefixLength;
        }
    }

    /**
     * Guides the recursion through the tree.
     */
    interface IntersectVisitor
    {
        /**
         * Called for all values in a leaf cell that crosses the query. Values are visited in increasing order, and
         * in the case of ties, in increasing order by segment row ID.
         */
        boolean contains(byte[] packedValue);

        /**
         * Called for non-leaf cells to test how the cell relates to the query, to determine how to further recurse
         * down the tree.
         */
        Relation compare(byte[] minPackedValue, byte[] maxPackedValue);
    }
}
