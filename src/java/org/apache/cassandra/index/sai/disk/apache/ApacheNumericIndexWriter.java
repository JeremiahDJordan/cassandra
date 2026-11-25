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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.io.IndexOutput;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.postings.IntArrayPostingList;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_MINIMUM_POSTINGS_LEAVES;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_POSTINGS_SKIP;

/**
 * Writes the balanced tree and posting lists of a numeric index segment the way Apache Cassandra 5.0 does
 * (cassandra-5.0.9 {@code NumericIndexWriter} and {@code BlockBalancedTreePostingsWriter}): the posting list of each
 * leaf block, and the merged posting list of each internal node at every third level with at least 64 leaves below it
 * (see {@code cassandra.sai.postings_skip} and {@code cassandra.sai.minimum_postings_leaves}), then the map of node ids
 * to posting lists.
 */
public class ApacheNumericIndexWriter
{
    private static final int MAX_POINTS_IN_LEAF_NODE = BlockBalancedTreeWriter.DEFAULT_MAX_POINTS_IN_LEAF_NODE;

    private ApacheNumericIndexWriter()
    {
    }

    /**
     * Writes the terms, each of {@code bytesPerValue} bytes and sorted, with their postings.
     */
    public static SegmentMetadata.ComponentMetadataMap writeCompleteSegment(IndexComponents.ForWrite components, int bytesPerValue, TermsIterator terms) throws IOException
    {
        SegmentMetadata.ComponentMetadataMap metadata = new SegmentMetadata.ComponentMetadataMap();
        BlockBalancedTreeWriter writer = new BlockBalancedTreeWriter(bytesPerValue, MAX_POINTS_IN_LEAF_NODE);
        List<int[]> leafPostings = new ArrayList<>();

        long treePosition;
        try (IndexOutput treeOutput = components.addOrGet(IndexComponentType.BALANCED_TREE).openOutput(true))
        {
            // The balanced tree component file is opened in append mode, so our offset is the current file pointer.
            long treeOffset = treeOutput.getFilePointer();

            treePosition = writer.write(treeOutput, terms, (rowIDAndIndexes, offset, count) -> {
                int[] rowIds = new int[count - offset];
                for (int i = offset; i < count; i++)
                    rowIds[i - offset] = Math.toIntExact(rowIDAndIndexes[i].rowID);
                leafPostings.add(rowIds);
            });

            if (treePosition < 0)
                return metadata;

            long treeLength = treeOutput.getFilePointer() - treeOffset;

            Map<String, String> attributes = new LinkedHashMap<>();
            attributes.put("max_points_in_leaf_node", Integer.toString(writer.getMaxPointsInLeafNode()));
            attributes.put("num_leaves", Integer.toString(leafPostings.size()));
            attributes.put("num_values", Long.toString(writer.getValueCount()));
            attributes.put("bytes_per_value", Long.toString(writer.getBytesPerValue()));

            metadata.put(IndexComponentType.BALANCED_TREE, treePosition, treeOffset, treeLength, attributes);
        }

        try (BlockBalancedTreeWalker walker = new BlockBalancedTreeWalker(components.get(IndexComponentType.BALANCED_TREE).createIndexBuildTimeFileHandle(), treePosition);
             IndexOutput postingsOutput = components.addOrGet(IndexComponentType.POSTING_LISTS).openOutput(true))
        {
            long postingsOffset = postingsOutput.getFilePointer();

            NodePostings nodePostings = new NodePostings();
            walker.traverse(nodePostings);
            long postingsPosition = nodePostings.write(postingsOutput, leafPostings);

            Map<String, String> attributes = new LinkedHashMap<>();
            attributes.put("num_leaf_postings", Integer.toString(nodePostings.numLeafPostings));
            attributes.put("num_non_leaf_postings", Integer.toString(nodePostings.numNonLeafPostings));

            long postingsLength = postingsOutput.getFilePointer() - postingsOffset;
            metadata.put(IndexComponentType.POSTING_LISTS, postingsPosition, postingsOffset, postingsLength, attributes);
        }
        return metadata;
    }

    /**
     * Apache's {@code BlockBalancedTreePostingsWriter}: collects the leaves below the internal nodes that get posting
     * lists while the tree is traversed, then writes the posting lists.
     */
    private static class NodePostings implements BlockBalancedTreeWalker.TraversalCallback
    {
        private final int minimumPostingsLeaves = SAI_MINIMUM_POSTINGS_LEAVES.getInt();
        private final int postingsSkip = SAI_POSTINGS_SKIP.getInt();
        private final TreeMap<Long, Integer> leafOffsetToNodeID = new TreeMap<>(Long::compareTo);
        private final Multimap<Integer, Integer> nodeToChildLeaves = HashMultimap.create();

        int numNonLeafPostings = 0;
        int numLeafPostings = 0;

        @Override
        public void onLeaf(int leafNodeID, long leafBlockFP, IntArrayList pathToRoot)
        {
            leafOffsetToNodeID.put(leafBlockFP, leafNodeID);
            for (int i = 0; i < pathToRoot.size(); i++)
            {
                int level = i + 1;
                if (level > 1 && level % postingsSkip == 0)
                    nodeToChildLeaves.put(pathToRoot.get(i), leafNodeID);
            }
        }

        /**
         * Writes the posting lists of the internal nodes, then of the leaves, then the map of node ids to posting list
         * file pointers, and returns the file pointer of the map.
         */
        long write(IndexOutput out, List<int[]> leafPostings) throws IOException
        {
            ApachePostingsWriter postingsWriter = new ApachePostingsWriter(out);
            checkState(leafPostings.size() == leafOffsetToNodeID.size(),
                       "Expected equal number of postings lists (%s) and leaf offsets (%s).",
                       leafPostings.size(), leafOffsetToNodeID.size());

            Map<Integer, int[]> leafToPostings = new HashMap<>();
            var postingsIterator = leafPostings.iterator();
            leafOffsetToNodeID.forEach((fp, nodeID) -> leafToPostings.put(nodeID, postingsIterator.next()));

            List<Integer> internalNodeIDs = nodeToChildLeaves.keySet()
                                                             .stream()
                                                             .filter(i -> nodeToChildLeaves.get(i).size() >= minimumPostingsLeaves)
                                                             .collect(Collectors.toList());
            Collection<Integer> leafNodeIDs = leafOffsetToNodeID.values();

            TreeMap<Integer, Long> nodeIDToPostingsFilePointer = new TreeMap<>();
            for (int nodeID : Iterables.concat(internalNodeIDs, leafNodeIDs))
            {
                Collection<Integer> leaves = nodeToChildLeaves.get(nodeID);
                PostingList postings;
                if (leaves.isEmpty())
                {
                    // a leaf's posting list as it is, an internal node's merged without repeated row ids
                    postings = new IntArrayPostingList(leafToPostings.get(nodeID));
                    numLeafPostings++;
                }
                else
                {
                    postings = new MergedPostings(leaves.stream().map(leafToPostings::get).collect(Collectors.toList()));
                    numNonLeafPostings++;
                }
                nodeIDToPostingsFilePointer.put(nodeID, postingsWriter.write(postings));
            }

            long indexFilePointer = out.getFilePointer();
            out.writeVInt(nodeIDToPostingsFilePointer.size());
            for (Map.Entry<Integer, Long> e : nodeIDToPostingsFilePointer.entrySet())
            {
                out.writeVInt(e.getKey());
                out.writeVLong(e.getValue());
            }
            postingsWriter.complete();
            return indexFilePointer;
        }

    }

    /**
     * The row ids of the leaves below an internal node, sorted and each once, merged as they are read (Apache's
     * {@code MergePostingList}).
     */
    private static class MergedPostings implements PostingList
    {
        private final PriorityQueue<Cursor> cursors = new PriorityQueue<>(Comparator.comparingInt(Cursor::rowId));
        private final int size;
        private int lastRowId = -1;

        MergedPostings(Collection<int[]> postings)
        {
            int size = 0;
            for (int[] rowIds : postings)
            {
                if (rowIds.length > 0)
                    cursors.add(new Cursor(rowIds));
                size += rowIds.length;
            }
            this.size = size;
        }

        @Override
        public int nextPosting()
        {
            while (!cursors.isEmpty())
            {
                Cursor cursor = cursors.poll();
                int rowId = cursor.rowId();
                if (cursor.advance())
                    cursors.add(cursor);
                if (rowId > lastRowId)
                {
                    lastRowId = rowId;
                    return rowId;
                }
            }
            return END_OF_STREAM;
        }

        @Override
        public int size()
        {
            return size;
        }

        @Override
        public int advance(int targetRowID)
        {
            throw new UnsupportedOperationException();
        }

        private static class Cursor
        {
            private final int[] rowIds;
            private int position;

            Cursor(int[] rowIds)
            {
                this.rowIds = rowIds;
            }

            int rowId()
            {
                return rowIds[position];
            }

            boolean advance()
            {
                return ++position < rowIds.length;
            }
        }
    }
}
