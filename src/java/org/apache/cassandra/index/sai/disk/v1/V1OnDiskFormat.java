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

package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.EmptyIndex;
import org.apache.cassandra.index.sai.disk.PerIndexWriter;
import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.SearchableIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.format.OnDiskFormat;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.metrics.AbstractMetrics;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;

import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemory;

/**
 * The original SAI OnDiskFormat, found in DSE.  Because it has a simple token -> offsets map, queries
 * against "wide partitions" are slow in proportion to the partition size, since we have to read
 * the whole partition and post-filter the rows
 */
public class V1OnDiskFormat implements OnDiskFormat
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final Set<IndexComponentType> PER_SSTABLE_COMPONENTS = EnumSet.of(IndexComponentType.GROUP_COMPLETION_MARKER,
                                                                                     IndexComponentType.GROUP_META,
                                                                                     IndexComponentType.TOKEN_VALUES,
                                                                                     IndexComponentType.OFFSETS_VALUES);

    private static final Set<IndexComponentType> LITERAL_COMPONENTS = EnumSet.of(IndexComponentType.COLUMN_COMPLETION_MARKER,
                                                                                 IndexComponentType.META,
                                                                                 IndexComponentType.TERMS_DATA,
                                                                                 IndexComponentType.POSTING_LISTS);

    public static final Set<IndexComponentType> NUMERIC_COMPONENTS = EnumSet.of(IndexComponentType.COLUMN_COMPLETION_MARKER,
                                                                                IndexComponentType.META,
                                                                                IndexComponentType.KD_TREE,
                                                                                IndexComponentType.KD_TREE_POSTING_LISTS);

    /**
     * Global limit on heap consumed by all index segment building that occurs outside the context of Memtable flush.
     *
     * Note that to avoid flushing extremely small index segments, a segment is only flushed when
     * both the global size of all building segments has breached the limit and the size of the
     * segment in question reaches (segment_write_buffer_space_mb / # currently building column indexes).
     *
     * ex. If there is only one column index building, it can buffer up to segment_write_buffer_space_mb.
     *
     * ex. If there is one column index building per table across 8 compactors, each index will be
     *     eligible to flush once it reaches (segment_write_buffer_space_mb / 8) MBs.
     */
    public static final long SEGMENT_BUILD_MEMORY_LIMIT = 1024L * 1024L * DatabaseDescriptor.getSAISegmentWriteBufferSpace();

    public static final NamedMemoryLimiter SEGMENT_BUILD_MEMORY_LIMITER =
    new NamedMemoryLimiter(SEGMENT_BUILD_MEMORY_LIMIT, "SSTable-attached Index Segment Builder");

    static
    {
        logger.debug("Segment build memory limit set to {} bytes", prettyPrintMemory(SEGMENT_BUILD_MEMORY_LIMIT));

        CassandraMetricsRegistry.MetricName bufferSpaceUsed = DefaultNameFactory.createMetricName(AbstractMetrics.TYPE, "SegmentBufferSpaceUsedBytes", null);
        CassandraMetricsRegistry.Metrics.register(bufferSpaceUsed, (Gauge<Long>) SEGMENT_BUILD_MEMORY_LIMITER::currentBytesUsed);

        CassandraMetricsRegistry.MetricName bufferSpaceLimit = DefaultNameFactory.createMetricName(AbstractMetrics.TYPE, "SegmentBufferSpaceLimitBytes", null);
        CassandraMetricsRegistry.Metrics.register(bufferSpaceLimit, (Gauge<Long>) () -> SEGMENT_BUILD_MEMORY_LIMIT);

        // Note: The active builder count starts at 1 to avoid dividing by zero.
        CassandraMetricsRegistry.MetricName buildsInProgress = DefaultNameFactory.createMetricName(AbstractMetrics.TYPE, "ColumnIndexBuildsInProgress", null);
        CassandraMetricsRegistry.Metrics.register(buildsInProgress, (Gauge<Long>) () -> SegmentBuilder.ACTIVE_BUILDER_COUNT.get() - 1);
    }

    public static final V1OnDiskFormat instance = new V1OnDiskFormat();

    private static final IndexFeatureSet v1IndexFeatureSet = new IndexFeatureSet()
    {
        @Override
        public boolean isRowAware()
        {
            return false;
        }

        @Override
        public boolean hasTermsHistogram()
        {
            return false;
        }
    };

    protected V1OnDiskFormat()
    {}

    @Override
    public IndexFeatureSet indexFeatureSet()
    {
        return v1IndexFeatureSet;
    }

    @Override
    public PrimaryKey.Factory newPrimaryKeyFactory(ClusteringComparator comparator)
    {
        return new PartitionAwarePrimaryKeyFactory();
    }

    @Override
    public PrimaryKeyMap.Factory newPrimaryKeyMapFactory(IndexComponents.ForRead perSSTableComponents, PrimaryKey.Factory primaryKeyFactory, SSTableReader sstable)
    {
        return new PartitionAwarePrimaryKeyMap.PartitionAwarePrimaryKeyMapFactory(perSSTableComponents, sstable, primaryKeyFactory);
    }

    @Override
    public SearchableIndex newSearchableIndex(SSTableContext sstableContext, IndexComponents.ForRead perIndexComponents)
    {
        return perIndexComponents.isEmpty()
               ? new EmptyIndex()
               : new V1SearchableIndex(sstableContext, perIndexComponents);
    }

    @Override
    public IndexSearcher newIndexSearcher(SSTableContext sstableContext,
                                          IndexContext indexContext,
                                          PerIndexFiles indexFiles,
                                          SegmentMetadata segmentMetadata) throws IOException
    {
        if (indexContext.isLiteral())
            // We filter because the CA format wrote maps according to a different order than their abstract type.
            return new InvertedIndexSearcher(sstableContext, indexFiles, segmentMetadata, indexContext, Version.AA, true);
        return new KDTreeIndexSearcher(sstableContext.primaryKeyMapFactory(), indexFiles, segmentMetadata, indexContext);
    }

    @Override
    public PerSSTableWriter newPerSSTableWriter(IndexDescriptor indexDescriptor) throws IOException
    {
        return new V1SSTableComponentsWriter(indexDescriptor.newPerSSTableComponentsForWrite());
    }

    @Override
    public PerIndexWriter newPerIndexWriter(StorageAttachedIndex index,
                                            IndexDescriptor indexDescriptor,
                                            LifecycleNewTracker tracker,
                                            RowMapping rowMapping,
                                            long keyCount)
    {
        IndexContext context = index.getIndexContext();
        IndexComponents.ForWrite perIndexComponents = indexDescriptor.newPerIndexComponentsForWrite(context);
        // If we're not flushing or we haven't yet started the initialization build, flush from SSTable contents.
        if (tracker.opType() != OperationType.FLUSH || !index.canFlushFromMemtableIndex())
        {
            NamedMemoryLimiter limiter = SEGMENT_BUILD_MEMORY_LIMITER;
            logger.debug(index.getIndexContext().logMessage("Starting a compaction index build. Global segment memory usage: {}"),
                         prettyPrintMemory(limiter.currentBytesUsed()));

            return new SSTableIndexWriter(perIndexComponents, limiter, index.isDropped(), index.isUnloaded(), keyCount);
        }

        return new MemtableIndexWriter(context.getPendingMemtableIndex(tracker),
                                       perIndexComponents,
                                       context.keyFactory(),
                                       rowMapping,
                                       index.isDropped());
    }

    protected Version getExpectedEarliestVersion(IndexContext context, IndexComponentType indexComponentType)
    {
        Version earliest = Version.EARLIEST;
        if (isVectorDataComponent(context, indexComponentType))
        {
            if (!context.version().onOrAfter(Version.VECTOR_EARLIEST))
                throw new IllegalStateException("Configured current version " + context.version() + " is not compatible with vector index");
            earliest = Version.VECTOR_EARLIEST;
        }
        return earliest;
    }

    @Override
    public boolean validateIndexComponent(IndexComponent.ForRead component, boolean checksum)
    {
        if (component.isCompletionMarker())
            return true;

        IndexContext context = component.parent().context();
        boolean isVectorData = isVectorDataComponent(context, component.componentType());
        // The vector components only have headers and footers from version CA (jvector) on
        if (isVectorData && !component.parent().version().onOrAfter(Version.JVECTOR_EARLIEST))
            return true;

        Version earliest = getExpectedEarliestVersion(context, component.componentType());
        try (IndexInput input = component.openInput())
        {
            if (checksum && isVectorData && component.componentType() == IndexComponentType.TERMS_DATA)
                validateGraphChecksums(component, input, earliest);
            else if (checksum)
                SAICodecUtils.validateChecksum(input);
            else
                SAICodecUtils.validate(input, earliest);
        }
        catch (Throwable e)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug(component.parent().logMessage("{} failed for index component {} on SSTable {}"),
                             (checksum ? "Checksum validation" : "Validation"),
                             component,
                             component.parent().descriptor(),
                             e);
            }
            return false;
        }
        return true;
    }

    @Override
    public Set<IndexComponentType> perSSTableComponentTypes(boolean hasClustering)
    {
        return PER_SSTABLE_COMPONENTS;
    }

    @Override
    public Set<IndexComponentType> perIndexComponentTypes(AbstractType<?> validator)
    {
        if (TypeUtil.isLiteral(validator))
            return LITERAL_COMPONENTS;
        return NUMERIC_COMPONENTS;
    }

    @Override
    public int openFilesPerSSTable(boolean hasClustering)
    {
        return 2;
    }

    @Override
    public int openFilesPerIndex(IndexContext indexContext)
    {
        // For the V1 format there are always 2 open files per index:
        // - index (balanced tree or terms)
        // - auxiliary postings for the balanced tree and postings for the literal terms
        return 2;
    }

    @Override
    public ByteOrder byteOrderFor(IndexComponentType indexComponentType, IndexContext context)
    {
        return ByteOrder.BIG_ENDIAN;
    }

    @Override
    public ByteComparable encodeForTrie(ByteBuffer input, AbstractType<?> type)
    {
        return TypeUtil.isLiteral(type) ? v -> ByteSource.preencoded(input)
                                        : TypeUtil.asComparableBytes(input, type);
    }

    @Override
    public ByteBuffer decodeFromTrie(ByteComparable value, AbstractType<?> type)
    {
        return TypeUtil.isLiteral(type)
               ? ByteBuffer.wrap(ByteSourceInverse.readBytes(value.asComparableBytes(ByteComparable.Version.OSS41)))
               : TypeUtil.fromComparableBytes(value, type, ByteComparable.Version.OSS41);
    }

    /**
     * Validates each segment of the graph file (TERMS_DATA) of a vector index. The graph writer gives each segment its
     * own header and footer, and the checksum of its footer covers only the graph of the segment, between them (see
     * {@code CassandraOnHeapGraph#flush}). Before version EC, a footer may instead have the checksum of the file up to
     * it, as the other components have, which the graph writers of earlier releases wrote.
     */
    private static void validateGraphChecksums(IndexComponent.ForRead component, IndexInput input, Version earliest) throws IOException
    {
        IndexComponents.ForRead parent = component.parent();
        List<SegmentMetadata> segments = SegmentMetadata.loadWithoutFullResolutionBounds(MetadataSource.loadMetadata(parent),
                                                                                       parent.context());
        boolean acceptFileChecksum = !parent.version().onOrAfter(Version.EC);
        long fileLength = input.length();
        CRC32 fileChecksum = new CRC32();
        byte[] buffer = new byte[64 * 1024];
        long segmentStart = 0;
        for (int i = 0; i < segments.size(); i++)
        {
            long graphStart = graphOffset(segments.get(i));
            long segmentEnd = i + 1 < segments.size() ? graphOffset(segments.get(i + 1)) - SAICodecUtils.headerSize() : fileLength;
            long footerStart = segmentEnd - CodecUtil.footerLength();
            if (graphStart - SAICodecUtils.headerSize() != segmentStart || footerStart < graphStart || segmentEnd > fileLength)
                throw new CorruptIndexException(String.format("Segment %d of the graph at [%d, %d) is inconsistent with the previous " +
                                                              "segment, which ends at %d, or with the file length %d",
                                                              i, graphStart, segmentEnd, segmentStart, fileLength),
                                                input);

            input.seek(segmentStart);
            SAICodecUtils.checkHeader(input, earliest);
            if (input.getFilePointer() != graphStart)
                throw new CorruptIndexException("Unexpected header length " + (input.getFilePointer() - segmentStart) + " of segment " + i, input);

            // the checksum of the graph, and the checksum of the file up to the footer's checksum
            input.seek(segmentStart);
            CRC32 graphChecksum = new CRC32();
            long checksumStart = footerStart + 8;
            for (long position = segmentStart; position < checksumStart; )
            {
                int length = (int) Math.min(buffer.length, checksumStart - position);
                input.readBytes(buffer, 0, length);
                fileChecksum.update(buffer, 0, length);
                if (position + length > graphStart && position < footerStart)
                {
                    int from = (int) Math.max(0, graphStart - position);
                    int to = (int) Math.min(length, footerStart - position);
                    graphChecksum.update(buffer, from, to - from);
                }
                position += length;
            }

            input.seek(footerStart);
            SAICodecUtils.validateFooter(input, true);
            long expected = SAICodecUtils.readCRC(input);
            if (expected != graphChecksum.getValue() && !(acceptFileChecksum && expected == fileChecksum.getValue()))
                throw new CorruptIndexException(String.format("Checksum failed for segment %d of the graph: expected %x, actual %x",
                                                              i, expected, graphChecksum.getValue()),
                                                input);

            // the checksum of the file up to the next segment's footer includes this footer's checksum
            input.seek(checksumStart);
            input.readBytes(buffer, 0, 8);
            fileChecksum.update(buffer, 0, 8);
            segmentStart = segmentEnd;
        }

        if (segmentStart != fileLength)
            throw new CorruptIndexException("The segments of the graph end at " + segmentStart + ", before the end of the file at " + fileLength, input);
    }

    private static long graphOffset(SegmentMetadata segment)
    {
        return segment.componentMetadatas.get(IndexComponentType.TERMS_DATA).offset;
    }

    /** vector data components (that did not have checksums before v3) */
    private boolean isVectorDataComponent(IndexContext context, IndexComponentType indexComponentType)
    {
        if (context == null || !context.isVector())
            return false;

        return indexComponentType == IndexComponentType.VECTOR ||
               indexComponentType == IndexComponentType.PQ ||
               indexComponentType == IndexComponentType.TERMS_DATA ||
               indexComponentType == IndexComponentType.POSTING_LISTS;
    }

    @Override
    public int jvectorFileFormatVersion()
    {
        throw new UnsupportedOperationException("JVector is not supported in V2OnDiskFormat");
    }
}
