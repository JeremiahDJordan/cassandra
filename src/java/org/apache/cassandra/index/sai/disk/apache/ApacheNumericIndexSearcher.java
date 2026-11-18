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
import java.nio.ByteBuffer;
import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.v1.IndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.metrics.MulticastQueryEventListeners;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.PointValues.Relation;

/**
 * Searches a segment of a numeric index written by Apache Cassandra 5.0: a block balanced tree of Apache's fixed-length
 * terms (see {@link ApacheTermType#asNumericTerm}), with Apache's postings. Apache never stores inet, varint, decimal
 * and bigint terms reversed (CASSANDRA-20100), so the bounds of a DESC column of those types are put back in term
 * order.
 */
public class ApacheNumericIndexSearcher extends IndexSearcher
{
    private final BlockBalancedTreeReader treeReader;
    private final ApacheTermType termType;
    private final QueryEventListener.BKDIndexEventListener perColumnEventListener;

    ApacheNumericIndexSearcher(SSTableContext sstableContext,
                               PerIndexFiles perIndexFiles,
                               SegmentMetadata segmentMetadata,
                               IndexContext indexContext) throws IOException
    {
        super(sstableContext.primaryKeyMapFactory(), perIndexFiles, segmentMetadata, indexContext);
        this.termType = ApacheTermType.of(indexContext.getValidator());
        this.perColumnEventListener = (QueryEventListener.BKDIndexEventListener) indexContext.getColumnQueryMetrics();

        long treePosition = segmentMetadata.componentMetadatas.get(IndexComponentType.BALANCED_TREE).root;
        if (treePosition < 0)
            throw new CorruptIndexException(indexContext.logMessage("The tree position is less than zero."), IndexComponentType.BALANCED_TREE.name());
        long postingsPosition = segmentMetadata.componentMetadatas.get(IndexComponentType.POSTING_LISTS).root;
        if (postingsPosition < 0)
            throw new CorruptIndexException(indexContext.logMessage("The postings position is less than zero."), IndexComponentType.POSTING_LISTS.name());

        FileHandle postings = perIndexFiles.postingLists();
        try
        {
            this.treeReader = new BlockBalancedTreeReader(indexContext,
                                                          perIndexFiles.getFile(IndexComponentType.BALANCED_TREE).sharedCopy(),
                                                          treePosition,
                                                          postings,
                                                          postingsPosition);
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(postings);
            throw t;
        }
        if (treeReader.getBytesPerValue() != termType.fixedSizeOf())
        {
            treeReader.close();
            throw new CorruptIndexException(indexContext.logMessage(String.format("The terms of the balanced tree have %d bytes, expected %d",
                                                                                  treeReader.getBytesPerValue(), termType.fixedSizeOf())),
                                            IndexComponentType.BALANCED_TREE.name());
        }
    }

    @Override
    public long indexFileCacheSize()
    {
        return treeReader.memoryUsage();
    }

    @Override
    public KeyRangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange, QueryContext context, boolean defer) throws IOException
    {
        if (!expression.getOp().isEqualityOrRange())
            throw new IllegalArgumentException(indexContext.logMessage("Unsupported expression during index query: " + expression));

        QueryEventListener.BKDIndexEventListener listener = MulticastQueryEventListeners.of(context, perColumnEventListener);
        return toPrimaryKeyIterator(treeReader.intersect(query(expression), listener, context), context);
    }

    @Override
    public CloseableIterator<PrimaryKeyWithSortKey> orderBy(Orderer orderer, Expression slice, AbstractBounds<PartitionPosition> keyRange, QueryContext context, int limit) throws IOException
    {
        BlockBalancedTreeReader.IntersectVisitor query = slice != null && slice.getOp().isEqualityOrRange() ? query(slice) : null;
        return toMetaSortedIterator(treeReader.iterate(query, orderer.isAscending(), this::asOrderingTerm), context);
    }

    // The TypeUtil#toComparableBytes form of the value, as for our numeric indexes; the tree holds it in full for the
    // types that can be ordered by.
    private ByteComparable asOrderingTerm(byte[] treeTerm)
    {
        AbstractType<?> type = indexContext.getValidator();
        ByteBuffer value;
        if (type.unwrap() instanceof LongType)
        {
            // Apache's optionalSignedFixedLengthNumber: the value with its sign bit flipped
            byte[] bytes = treeTerm.clone();
            bytes[0] ^= (byte) 0x80;
            value = ByteBuffer.wrap(bytes);
        }
        else
        {
            value = type.fromComparableBytes(ByteSource.peekable(ByteSource.preencoded(treeTerm)), ApacheTermType.VERSION);
        }
        byte[] term = new byte[TypeUtil.fixedSizeOf(type)];
        TypeUtil.toComparableBytes(TypeUtil.asIndexBytes(value, type), type, term);
        return ByteComparable.preencoded(TypeUtil.BYTE_COMPARABLE_VERSION, term);
    }

    private BlockBalancedTreeReader.IntersectVisitor query(Expression expression)
    {
        Expression.Bound lower = expression.lower;
        Expression.Bound upper = expression.upper;
        // Put the bounds in the order of the terms (see the class javadoc).
        if (expression.hasReversedBounds() != termType.isReversed())
        {
            Expression.Bound tmp = lower;
            lower = upper;
            upper = tmp;
        }
        // Truncated values (varint, decimal) can't be excluded on their terms: the query filters them.
        boolean rounding = termType.supportsRounding();
        return new RangeQueryVisitor(lower == null ? null : termType.asNumericTerm(lower.value.raw), lower != null && !lower.inclusive && !rounding,
                                     upper == null ? null : termType.asNumericTerm(upper.value.raw), upper != null && !upper.inclusive && !rounding);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("indexContext", indexContext)
                          .add("count", treeReader.getPointCount())
                          .add("bytesPerValue", treeReader.getBytesPerValue())
                          .toString();
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(treeReader);
    }

    /**
     * Apache's {@code BlockBalancedTreeQueries.RangeQueryVisitor}: matches the terms between the bounds, if any.
     */
    private static class RangeQueryVisitor implements BlockBalancedTreeReader.IntersectVisitor
    {
        @Nullable
        private final byte[] lower, upper;
        private final boolean lowerExclusive, upperExclusive;

        RangeQueryVisitor(@Nullable byte[] lower, boolean lowerExclusive, @Nullable byte[] upper, boolean upperExclusive)
        {
            this.lower = lower;
            this.lowerExclusive = lowerExclusive;
            this.upper = upper;
            this.upperExclusive = upperExclusive;
        }

        // whether the lower bound excludes the value
        private boolean lowerExcludes(byte[] value)
        {
            int cmp = ByteArrayUtil.compareUnsigned(lower, 0, value, 0, lower.length);
            return cmp > 0 || (cmp == 0 && lowerExclusive);
        }

        // whether the upper bound excludes the value
        private boolean upperExcludes(byte[] value)
        {
            int cmp = ByteArrayUtil.compareUnsigned(upper, 0, value, 0, upper.length);
            return cmp < 0 || (cmp == 0 && upperExclusive);
        }

        @Override
        public boolean contains(byte[] value)
        {
            return (lower == null || !lowerExcludes(value)) && (upper == null || !upperExcludes(value));
        }

        @Override
        public Relation compare(byte[] minValue, byte[] maxValue)
        {
            boolean crosses = false;

            if (lower != null)
            {
                if (lowerExcludes(maxValue))
                    return Relation.CELL_OUTSIDE_QUERY;
                crosses = lowerExcludes(minValue);
            }

            if (upper != null)
            {
                if (upperExcludes(minValue))
                    return Relation.CELL_OUTSIDE_QUERY;
                crosses |= upperExcludes(maxValue);
            }

            return crosses ? Relation.CELL_CROSSES_QUERY : Relation.CELL_INSIDE_QUERY;
        }
    }
}
