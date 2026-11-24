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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.io.IndexInput;
import org.apache.cassandra.index.sai.disk.io.IndexOutput;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.index.CorruptIndexException;

/**
 * Reads and writes the segment metadata of Apache Cassandra 5.0 (Apache's {@code SegmentMetadata#write}) as our
 * {@link SegmentMetadata}: int-prefixed keys and terms, OSS50 byte-comparable primary keys, no histograms.
 */
public class ApacheSegmentMetadata
{
    private static final String NAME = "SegmentMetadata";

    /**
     * Apache's {@code IndexComponent} names, as written in the component metadata map, to our component types.
     */
    private static final Map<String, IndexComponentType> COMPONENTS = new HashMap<>();
    static
    {
        COMPONENTS.put("META", IndexComponentType.META);
        COMPONENTS.put("BALANCED_TREE", IndexComponentType.BALANCED_TREE);
        COMPONENTS.put("TERMS_DATA", IndexComponentType.TERMS_DATA);
        COMPONENTS.put("COMPRESSED_VECTORS", IndexComponentType.PQ);
        COMPONENTS.put("POSTING_LISTS", IndexComponentType.POSTING_LISTS);
    }

    /**
     * @param sstableContext the sstable, whose per-sstable components must be Apache's, to resolve the min and max
     * primary keys from their row ids; if null, those are partition keys only, not for query processing.
     */
    public static List<SegmentMetadata> load(MetadataSource source, IndexContext context, @Nullable SSTableContext sstableContext) throws IOException
    {
        IndexInput input = source.get(NAME);
        int segmentCount = checkCount(input.readVInt(), input);
        List<SegmentMetadata> segments = new ArrayList<>(segmentCount);
        for (int i = 0; i < segmentCount; i++)
            segments.add(read(input, context, sstableContext));
        return segments;
    }

    /**
     * Writes the segment metadata the way Apache Cassandra 5.0 does.
     */
    public static void write(MetadataWriter writer, List<SegmentMetadata> segments, ClusteringComparator comparator) throws IOException
    {
        try (IndexOutput output = writer.builder(NAME))
        {
            output.writeVInt(segments.size());
            for (SegmentMetadata metadata : segments)
            {
                output.writeLong(metadata.segmentRowIdOffset);
                output.writeLong(metadata.numRows);
                output.writeLong(metadata.minSSTableRowId);
                output.writeLong(metadata.maxSSTableRowId);
                writeBytes(keyBytes(metadata.minKey, comparator), output);
                writeBytes(keyBytes(metadata.maxKey, comparator), output);
                writeBytes(ByteBufferUtil.getArray(metadata.minTerm), output);
                writeBytes(ByteBufferUtil.getArray(metadata.maxTerm), output);
                metadata.componentMetadatas.write(output);
            }
        }
    }

    /**
     * Apache's {@code PrimaryKey#asComparableBytes}: the partition key, and for a table with clustering columns the
     * clustering too, null for a static row.
     */
    private static byte[] keyBytes(PrimaryKey key, ClusteringComparator comparator)
    {
        ByteSource partitionKey = ByteSource.of(key.partitionKey().getKey(), ByteComparable.Version.OSS50);
        if (comparator.size() == 0)
            return ByteSourceInverse.readBytes(partitionKey);

        ByteSource clustering = key.isStaticRow() ? null : comparator.asByteComparable(key.clustering()).asComparableBytes(ByteComparable.Version.OSS50);
        return ByteSourceInverse.readBytes(ByteSource.withTerminator(ByteComparable.Version.OSS50, ByteSource.TERMINATOR, partitionKey, clustering));
    }

    private static void writeBytes(byte[] bytes, IndexOutput output) throws IOException
    {
        output.writeInt(bytes.length);
        output.writeBytes(bytes, 0, bytes.length);
    }

    private static SegmentMetadata read(IndexInput input, IndexContext context, @Nullable SSTableContext sstableContext) throws IOException
    {
        long rowIdOffset = input.readLong();
        long numRows = input.readLong();
        long minSSTableRowId = input.readLong();
        long maxSSTableRowId = input.readLong();
        ByteBuffer minKeyBytes = readBytes(input);
        ByteBuffer maxKeyBytes = readBytes(input);

        PrimaryKey minKey, maxKey;
        if (sstableContext != null)
        {
            // Like we do for our segments: the fully resolved primary keys from the row ids.
            try (PrimaryKeyMap map = sstableContext.primaryKeyMapFactory().newPerSSTablePrimaryKeyMap())
            {
                PrimaryKey min = map.primaryKeyFromRowId(minSSTableRowId).loadDeferred();
                PrimaryKey max = map.primaryKeyFromRowId(maxSSTableRowId).loadDeferred();
                minKey = map.primaryKeyFromRowId(minSSTableRowId, min, max).loadDeferred();
                maxKey = map.primaryKeyFromRowId(maxSSTableRowId, min, max).loadDeferred();
            }
        }
        else
        {
            boolean hasClustering = context.comparator().size() > 0;
            IPartitioner partitioner = context.getPartitioner();
            minKey = context.keyFactory().createPartitionKeyOnly(partitionKey(minKeyBytes, hasClustering, partitioner));
            maxKey = context.keyFactory().createPartitionKeyOnly(partitionKey(maxKeyBytes, hasClustering, partitioner));
        }

        ByteBuffer minTerm = readBytes(input);
        ByteBuffer maxTerm = readBytes(input);

        SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();
        int size = checkCount(input.readInt(), input);
        for (int i = 0; i < size; i++)
        {
            String name = input.readString();
            IndexComponentType type = COMPONENTS.get(name);
            if (type == null)
                throw new CorruptIndexException("Unknown Apache Cassandra SAI component " + name, input);

            long root = input.readLong();
            long offset = input.readLong();
            long length = input.readLong();
            int attributeCount = checkCount(input.readInt(), input);
            Map<String, String> attributes = new HashMap<>(attributeCount);
            for (int a = 0; a < attributeCount; a++)
                attributes.put(input.readString(), input.readString());
            components.put(type, root, offset, length, attributes);
        }

        return new SegmentMetadata(rowIdOffset,
                                   numRows,
                                   minSSTableRowId,
                                   maxSSTableRowId,
                                   minKey,
                                   maxKey,
                                   minTerm,
                                   maxTerm,
                                   null,
                                   components,
                                   SegmentMetadata.INVALID_TOTAL_TERM_COUNT,
                                   Version.APACHE_AA);
    }

    private static DecoratedKey partitionKey(ByteBuffer bytes, boolean hasClustering, IPartitioner partitioner) throws IOException
    {
        ByteSource.Peekable source = ByteSource.peekable(ByteSource.preencoded(bytes));
        if (hasClustering)
            source = ByteSourceInverse.nextComponentSource(source);
        if (source == null)
            throw new CorruptIndexException("Apache Cassandra SAI segment key has no partition key", NAME);
        ByteBuffer key = ByteBuffer.wrap(ByteSourceInverse.getUnescapedBytes(source));
        return new BufferDecoratedKey(partitioner.getToken(key), key);
    }

    // A count the remaining bytes can't hold means corrupt metadata, which must fail as such, not allocate memory.
    private static int checkCount(int count, IndexInput input) throws IOException
    {
        if (count < 0 || count > input.length() - input.getFilePointer())
            throw new CorruptIndexException("Invalid count or length " + count + " in Apache Cassandra SAI segment metadata", input);
        return count;
    }

    private static ByteBuffer readBytes(IndexInput input) throws IOException
    {
        int length = checkCount(input.readInt(), input);
        byte[] bytes = new byte[length];
        input.readBytes(bytes, 0, length);
        return ByteBuffer.wrap(bytes);
    }
}
