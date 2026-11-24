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

package org.apache.cassandra.index.sai.disk.apache;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.EmptyIndex;
import org.apache.cassandra.index.sai.disk.PerIndexWriter;
import org.apache.cassandra.index.sai.disk.SearchableIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.IndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SSTableIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.V1SearchableIndex;
import org.apache.cassandra.index.sai.disk.v3.V3VectorIndexSearcher;
import org.apache.cassandra.index.sai.disk.v9.V9OnDiskFormat;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;

/**
 * The SAI on-disk format of Apache Cassandra 5.0 (its version "aa", {@link Version#APACHE_AA}).
 * The row ids of Apache's per-index components are only valid against Apache's per-SSTable components (Apache does
 * not index rows without live data), so the Apache components of an sstable are used, and replaced, as a unit:
 * see {@link IndexDescriptor} and {@code StorageAttachedIndexBuilder}.
 */
public class ApacheOnDiskFormat extends V9OnDiskFormat
{
    private static final Logger logger = LoggerFactory.getLogger(ApacheOnDiskFormat.class);

    public static final ApacheOnDiskFormat instance = new ApacheOnDiskFormat();

    private static final Set<IndexComponentType> SKINNY_PER_SSTABLE_COMPONENTS = EnumSet.of(IndexComponentType.GROUP_COMPLETION_MARKER,
                                                                                            IndexComponentType.GROUP_META,
                                                                                            IndexComponentType.ROW_TO_TOKEN,
                                                                                            IndexComponentType.ROW_TO_PARTITION,
                                                                                            IndexComponentType.PARTITION_KEY_BLOCKS,
                                                                                            IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS);

    private static final Set<IndexComponentType> WIDE_PER_SSTABLE_COMPONENTS = EnumSet.of(IndexComponentType.GROUP_COMPLETION_MARKER,
                                                                                          IndexComponentType.GROUP_META,
                                                                                          IndexComponentType.ROW_TO_TOKEN,
                                                                                          IndexComponentType.ROW_TO_PARTITION,
                                                                                          IndexComponentType.PARTITION_TO_SIZE,
                                                                                          IndexComponentType.PARTITION_KEY_BLOCKS,
                                                                                          IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS,
                                                                                          IndexComponentType.CLUSTERING_KEY_BLOCKS,
                                                                                          IndexComponentType.CLUSTERING_KEY_BLOCK_OFFSETS);

    private static final Set<IndexComponentType> LITERAL_COMPONENTS = EnumSet.of(IndexComponentType.COLUMN_COMPLETION_MARKER,
                                                                                 IndexComponentType.META,
                                                                                 IndexComponentType.TERMS_DATA,
                                                                                 IndexComponentType.POSTING_LISTS);

    private static final Set<IndexComponentType> NUMERIC_COMPONENTS = EnumSet.of(IndexComponentType.COLUMN_COMPLETION_MARKER,
                                                                                 IndexComponentType.META,
                                                                                 IndexComponentType.BALANCED_TREE,
                                                                                 IndexComponentType.POSTING_LISTS);

    // written in append mode: one codec footer per segment
    private static final Set<IndexComponentType> SEGMENTED_COMPONENTS = EnumSet.of(IndexComponentType.BALANCED_TREE,
                                                                                   IndexComponentType.POSTING_LISTS,
                                                                                   IndexComponentType.TERMS_DATA,
                                                                                   IndexComponentType.PQ);

    // Apache's COMPRESSED_VECTORS component is our PQ.
    private static final Set<IndexComponentType> VECTOR_COMPONENTS = EnumSet.of(IndexComponentType.COLUMN_COMPLETION_MARKER,
                                                                                IndexComponentType.META,
                                                                                IndexComponentType.PQ,
                                                                                IndexComponentType.TERMS_DATA,
                                                                                IndexComponentType.POSTING_LISTS);

    private static final Map<IndexComponentType, String> FILE_NAMES = new EnumMap<>(IndexComponentType.class);
    private static final Map<String, IndexComponentType> BY_FILE_NAME = new HashMap<>();
    static
    {
        FILE_NAMES.put(IndexComponentType.META, "Meta");
        FILE_NAMES.put(IndexComponentType.BALANCED_TREE, "BalancedTree");
        FILE_NAMES.put(IndexComponentType.TERMS_DATA, "TermsData");
        FILE_NAMES.put(IndexComponentType.PQ, "CompressedVectors");
        FILE_NAMES.put(IndexComponentType.POSTING_LISTS, "PostingLists");
        FILE_NAMES.put(IndexComponentType.COLUMN_COMPLETION_MARKER, "ColumnComplete");
        FILE_NAMES.put(IndexComponentType.ROW_TO_TOKEN, "RowToToken");
        FILE_NAMES.put(IndexComponentType.ROW_TO_PARTITION, "RowToPartition");
        FILE_NAMES.put(IndexComponentType.PARTITION_TO_SIZE, "PartitionToSize");
        FILE_NAMES.put(IndexComponentType.PARTITION_KEY_BLOCKS, "PartitionKeyBlocks");
        FILE_NAMES.put(IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS, "PartitionKeyBlockOffsets");
        FILE_NAMES.put(IndexComponentType.CLUSTERING_KEY_BLOCKS, "ClusteringKeyBlocks");
        FILE_NAMES.put(IndexComponentType.CLUSTERING_KEY_BLOCK_OFFSETS, "ClusteringKeyBlockOffsets");
        FILE_NAMES.put(IndexComponentType.GROUP_META, "GroupMeta");
        FILE_NAMES.put(IndexComponentType.GROUP_COMPLETION_MARKER, "GroupComplete");
        FILE_NAMES.forEach((type, name) -> BY_FILE_NAME.put(name, type));
    }

    private static final IndexFeatureSet FEATURE_SET = new IndexFeatureSet()
    {
        @Override
        public boolean isRowAware()
        {
            return true;
        }

        @Override
        public boolean hasTermsHistogram()
        {
            return false;
        }
    };

    protected ApacheOnDiskFormat()
    {}

    /**
     * Formats the component part of a file name the way Apache Cassandra 5.0 does:
     * {@code SAI+aa(+<index name>)+<component name>.db}.
     */
    public static String fileNameFormat(IndexComponentType indexComponentType, @Nullable String indexName, int generation)
    {
        if (generation != 0)
            throw new IllegalArgumentException("Apache Cassandra SAI components have no generation, got " + generation);

        String name = FILE_NAMES.get(indexComponentType);
        if (name == null)
            throw new IllegalArgumentException("Apache Cassandra SAI has no " + indexComponentType + " component");

        StringBuilder builder = new StringBuilder(Version.APACHE_COMPONENT_PREFIX);
        if (indexName != null)
            builder.append(indexName).append(Version.SAI_SEPARATOR);
        return builder.append(name).append(Version.EXTENSION).toString();
    }

    /**
     * @return the component type of the provided Apache component file name part ({@code PartitionKeyBlocks}, ...), or
     * {@code null} if it isn't one.
     */
    public static @Nullable IndexComponentType componentTypeFor(String fileNamePart)
    {
        return BY_FILE_NAME.get(fileNamePart);
    }

    /**
     * Whether we can read the Apache per-index components of the index; a group we can't read is treated as missing
     * (see {@link IndexDescriptor}). All the kinds Apache Cassandra 5.0 writes are readable.
     */
    public boolean isReadable(IndexContext context)
    {
        return true;
    }

    /**
     * Whether the min and max terms of an Apache index of the type compare with our query bounds. Not for reversed
     * columns (Apache doesn't reverse inet, varint, decimal and bigint, CASSANDRA-20100) and decimals, whose terms are
     * encoded differently.
     */
    public static boolean canPruneByTermRange(AbstractType<?> type)
    {
        return !type.isReversed() && !(type.unwrap() instanceof DecimalType);
    }

    @Override
    public IndexFeatureSet indexFeatureSet()
    {
        return FEATURE_SET;
    }

    @Override
    public SearchableIndex newSearchableIndex(SSTableContext sstableContext, IndexComponents.ForRead perIndexComponents)
    {
        // V1SearchableIndex reads Apache's segment metadata (see SegmentMetadata#load) and uses newIndexSearcher.
        return perIndexComponents.isEmpty()
               ? new EmptyIndex()
               : new V1SearchableIndex(sstableContext, perIndexComponents);
    }

    @Override
    public IndexSearcher newIndexSearcher(SSTableContext sstableContext, IndexContext indexContext, PerIndexFiles indexFiles, SegmentMetadata segmentMetadata) throws IOException
    {
        if (!isReadable(indexContext))
            throw new UnsupportedOperationException("Cannot read the Apache Cassandra SAI components of " + indexContext);
        // Apache's vector indexes are read by the searcher of our CA version: jvector reads their graphs and PQ
        // through its compatibility paths, and their postings have the CA layout.
        if (indexContext.isVector())
            return new V3VectorIndexSearcher(sstableContext, indexFiles, segmentMetadata, indexContext);
        return TypeUtil.isLiteral(indexContext.getValidator())
               ? new ApacheLiteralIndexSearcher(sstableContext, indexFiles, segmentMetadata, indexContext)
               : new ApacheNumericIndexSearcher(sstableContext, indexFiles, segmentMetadata, indexContext);
    }

    /**
     * Apache's segments are built from the sstable rows, at flush too: the row ids of a memtable index count the rows
     * without live data, which Apache's row ids leave out.
     */
    @Override
    public PerIndexWriter newPerIndexWriter(StorageAttachedIndex index, IndexDescriptor indexDescriptor, LifecycleNewTracker tracker, RowMapping rowMapping, long keyCount)
    {
        IndexContext context = index.getIndexContext();
        if (context.isVector() || !TypeUtil.isLiteral(context.getValidator()))
            throw new UnsupportedOperationException("Only literal indexes are written in the Apache Cassandra SAI format, not " + context.getIndexName());
        return new SSTableIndexWriter(indexDescriptor.newPerIndexComponentsForWrite(context),
                                      SEGMENT_BUILD_MEMORY_LIMITER,
                                      index.isDropped(),
                                      index.isUnloaded(),
                                      keyCount);
    }

    @Override
    public Set<IndexComponentType> perSSTableComponentTypes(boolean hasClustering)
    {
        // Apache writes a PartitionToSize component for skinny tables too, but doesn't list it in the TOC nor uses it.
        return hasClustering ? WIDE_PER_SSTABLE_COMPONENTS : SKINNY_PER_SSTABLE_COMPONENTS;
    }

    @Override
    public Set<IndexComponentType> perIndexComponentTypes(AbstractType<?> validator)
    {
        if (validator.isVector())
            return VECTOR_COMPONENTS;
        return TypeUtil.isLiteral(validator) ? LITERAL_COMPONENTS : NUMERIC_COMPONENTS;
    }

    @Override
    public int openFilesPerSSTable(boolean hasClustering)
    {
        // Everything but the completion marker.
        return perSSTableComponentTypes(hasClustering).size() - 1;
    }

    @Override
    public int openFilesPerIndex(IndexContext indexContext)
    {
        // The terms dictionary or balanced tree, and the postings.
        return 2;
    }

    @Override
    public ByteOrder byteOrderFor(IndexComponentType indexComponentType, IndexContext context)
    {
        // Apache writes through Lucene 9's DataOutput (little-endian), except for what goes directly through a
        // SequentialWriter (big-endian): the terms trie, and the vector graph, compressed vectors and postings.
        switch (indexComponentType)
        {
            case TERMS_DATA:
            case PQ:
                return ByteOrder.BIG_ENDIAN;
            case POSTING_LISTS:
                return context != null && context.isVector() ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
            default:
                return ByteOrder.LITTLE_ENDIAN;
        }
    }

    @Override
    public void validateIndexComponent(IndexComponent.ForRead component, boolean checksum)
    {
        if (component.isCompletionMarker())
            return;

        try (IndexInput input = component.openInput())
        {
            if (checksum && component.parent().isPerIndexGroup() && SEGMENTED_COMPONENTS.contains(component.componentType()))
                validateSegmentedChecksums(component, input);
            else if (checksum)
                SAICodecUtils.validateChecksum(input, Version.APACHE_AA);
            else
                SAICodecUtils.validate(input, Version.APACHE_AA);
        }
        catch (Throwable e)
        {
            logger.warn(component.parent().logMessage("{} failed for Apache Cassandra index component {} on SSTable {}"),
                        checksum ? "Checksum validation" : "Validation",
                        component,
                        component.parent().descriptor(),
                        e);
            // Any failure means the component can't be read (e.g. a bad header or segment metadata, or a missing
            // component), so report it as corruption, which makes the caller rebuild the index.
            if (e instanceof Error && !(e instanceof AssertionError))
                throw (Error) e;
            if (e instanceof IOException)
                throw new UncheckedIOException((IOException) e);
            if (e.getCause() instanceof IOException)
                throw new UncheckedIOException((IOException) e.getCause());
            throw new UncheckedIOException(new IOException("Invalid Apache Cassandra index component " + component, e));
        }
    }

    @Override
    public ByteComparable encodeForTrie(ByteBuffer input, AbstractType<?> type)
    {
        ApacheTermType termType = ApacheTermType.of(type);
        if (!termType.isLiteral())
            throw new UnsupportedOperationException("Apache Cassandra only has tries for literal terms, not for " + type.asCQL3Type());
        return termType.asComparableBytes(input);
    }

    @Override
    public ByteBuffer decodeFromTrie(ByteComparable value, AbstractType<?> type)
    {
        ApacheTermType termType = ApacheTermType.of(type);
        if (!termType.isLiteral())
            throw new UnsupportedOperationException("Apache Cassandra only has tries for literal terms, not for " + type.asCQL3Type());
        return termType.decodeLiteral(ByteSourceInverse.readBytes(value.asComparableBytes(ApacheTermType.VERSION)));
    }

    /**
     * Validates the checksum of each frame (header, segment data, footer) of a component written in append mode; the
     * frames are found from the segment metadata (vector indexes record the payload only).
     */
    private static void validateSegmentedChecksums(IndexComponent.ForRead component, IndexInput input) throws IOException
    {
        IndexComponents.ForRead parent = component.parent();
        IndexContext context = parent.context();
        List<SegmentMetadata> segments = ApacheSegmentMetadata.load(MetadataSource.loadMetadata(parent), context, null);
        boolean payloadOnly = context.isVector();
        String name = component.fileNamePart();

        long fileLength = input.length();
        long frameStart = 0;
        for (SegmentMetadata segment : segments)
        {
            SegmentMetadata.ComponentMetadata metadata = segment.componentMetadatas.get(component.componentType());
            long frameEnd = payloadOnly ? metadata.offset + metadata.length + CodecUtil.footerLength()
                                        : metadata.offset + metadata.length;
            if (frameEnd > fileLength || frameEnd < frameStart)
                throw new CorruptIndexException(String.format("Segment frame [%d, %d) is inconsistent with component file length %d",
                                                              frameStart, frameEnd, fileLength),
                                                name + '@' + frameStart);

            SAICodecUtils.validateChecksum(input.slice(name + '@' + frameStart, frameStart, frameEnd - frameStart), Version.APACHE_AA);
            frameStart = frameEnd;
        }

        if (frameStart != fileLength)
            throw new CorruptIndexException(String.format("Component file length %d does not match combined frame length of all segments %d",
                                                          fileLength, frameStart),
                                            name);
    }

    @Override
    public int jvectorFileFormatVersion()
    {
        // jvector 1.0.2's graphs are read through jvector's headerless "version 2" path.
        return 2;
    }
}
