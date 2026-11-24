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

package org.apache.cassandra.db.partitions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.apache.cassandra.db.partitions.TrieBackedPartition.BYTE_COMPARABLE_VERSION;
import static org.github.jamm.MemoryMeterStrategy.MEMORY_LAYOUT;

/// The values of clusterings and range tombstone bounds of tables whose clusterings decoding their byte-comparable
/// form may not give back byte for byte (see [ClusteringComparator#decodesComparableBytesExactly()]), for tries that
/// hold clusterings only as paths. They are kept in an on-heap trie under the same paths as the data, the first ones
/// written for a path (as a `BTreePartition` keeps the clustering of the first row written), and used instead of
/// decoding the path. Values that decoding gives back are kept as [#DECODES_EXACTLY], without a copy.
public final class OriginalClusterings
{
    /// Kept for a path whose first values decoding gives back.
    private static final byte[][] DECODES_EXACTLY = new byte[0][];

    private OriginalClusterings()
    {
    }

    /// A builder of the values of a partition with the given comparator, or null if the comparator decodes every
    /// clustering exactly.
    @Nullable
    public static Builder builder(ClusteringComparator comparator)
    {
        return comparator.decodesComparableBytesExactly() ? null : new Builder(comparator);
    }

    /// Collects the values of the clusterings and range tombstone bounds of a partition, keeping the first values
    /// added for a path. One value, e.g. of a single-row update, is kept in a singleton trie.
    public static class Builder
    {
        private final ClusteringComparator comparator;
        private ByteComparable singlePath;
        private byte[][] singleValues;
        private InMemoryTrie<byte[][]> trie;

        private Builder(ClusteringComparator comparator)
        {
            this.comparator = comparator;
        }

        public <V> void add(ClusteringPrefix<V> prefix)
        {
            byte[][] values = DECODES_EXACTLY;
            if (!comparator.decodesComparableBytesExactly(prefix))
            {
                values = new byte[prefix.size()][];
                for (int i = 0; i < values.length; i++)
                {
                    V value = prefix.get(i);
                    if (value != null)
                        values[i] = prefix.accessor().toArray(value);
                }
            }
            ByteComparable path = comparator.asByteComparable(prefix);
            if (singleValues == null && trie == null)
            {
                singlePath = path;
                singleValues = values;
                return;
            }

            try
            {
                if (trie == null)
                {
                    trie = InMemoryTrie.shortLived(BYTE_COMPARABLE_VERSION);
                    put(trie, singlePath, singleValues);
                }
                put(trie, path, values);
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new AssertionError(e);
            }
        }

        /// The values collected, by path, or null if there are none.
        @Nullable
        public Trie<byte[][]> build()
        {
            if (trie != null)
                return trie;
            return singleValues != null ? Trie.singleton(singlePath, BYTE_COMPARABLE_VERSION, singleValues) : null;
        }
    }

    /// The entries one write adds to the trie of a memtable shard, under its write lock. They are removed again if the
    /// write fails, and the trie keeps the space of removed entries, so [#size] measures the space the whole attempt
    /// takes whether it succeeds or not.
    public static class Staging
    {
        private final InMemoryTrie<byte[][]> trie;
        private final long usedBefore;
        private final List<ByteComparable> added = new ArrayList<>();
        private long valuesSize;
        private int removed;

        public Staging(InMemoryTrie<byte[][]> trie)
        {
            this.trie = trie;
            this.usedBefore = trie.usedSizeOnHeap();
        }

        /// Adds the values in the given trie of a partition under the paths of the partition with the given key, unless
        /// values are already there.
        public void addAll(DecoratedKey key, Trie<byte[][]> partitionValues)
        {
            try
            {
                for (Map.Entry<ByteComparable.Preencoded, byte[][]> entry : partitionValues.entrySet())
                {
                    ByteComparable.Preencoded path = entry.getKey();
                    add(v -> ByteSource.concat(key.asComparableBytes(v), path.getPreencodedBytes()), entry.getValue());
                }
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new AssertionError(e);
            }
        }

        private void add(ByteComparable path, byte[][] values) throws TrieSpaceExhaustedException
        {
            int valuesCount = trie.valuesCount();
            put(trie, path, values);
            if (trie.valuesCount() == valuesCount)
                return;
            added.add(path);
            if (values != DECODES_EXACTLY)
            {
                valuesSize += ObjectSizes.sizeOfReferenceArray(values.length);
                for (byte[] value : values)
                    if (value != null)
                        valuesSize += ObjectSizes.sizeOfArray(value);
            }
        }

        /// Removes the entries added. A failure to remove them is added to the given failure of the write.
        public void rollback(Throwable failure)
        {
            try
            {
                for (ByteComparable path : added)
                {
                    trie.putSingleton(path, DECODES_EXACTLY, (existing, update) -> null);
                    ++removed;
                }
            }
            catch (Throwable t)
            {
                if (t != failure)
                    failure.addSuppressed(t);
            }
        }

        /// The on-heap space the trie took since this staging started, counted as the trie counts its used space,
        /// with the content slots of removed entries, which it doesn't reuse, and the values added.
        public long size()
        {
            return trie.usedSizeOnHeap() - usedBefore + removed * MEMORY_LAYOUT.getReferenceSize() + valuesSize;
        }
    }

    private static void put(InMemoryTrie<byte[][]> trie, ByteComparable path, byte[][] values) throws TrieSpaceExhaustedException
    {
        trie.putSingleton(path, values, (existing, update) -> existing != null ? existing : update);
    }

    /// The clustering at the given path, with the values written if they are in the given trie.
    public static <V> Clustering<V> clustering(@Nullable Trie<byte[][]> originals, ClusteringComparator comparator, ValueAccessor<V> accessor, ByteComparable path)
    {
        byte[][] values = originals != null ? originals.get(path) : null;
        return values != null && values != DECODES_EXACTLY
               ? accessor.factory().clustering(toValues(accessor, values))
               : (Clustering<V>) comparator.clusteringFromByteComparable(accessor, path, BYTE_COMPARABLE_VERSION);
    }

    /// The clustering written at the given path if it is in the given trie, otherwise null.
    @Nullable
    public static <V> Clustering<V> written(@Nullable Trie<byte[][]> originals, ClusteringComparator comparator, ValueAccessor<V> accessor, ByteComparable path)
    {
        byte[][] values = originals != null ? originals.get(path) : null;
        if (values == null)
            return null;
        return values != DECODES_EXACTLY
               ? accessor.factory().clustering(toValues(accessor, values))
               : (Clustering<V>) comparator.clusteringFromByteComparable(accessor, path, BYTE_COMPARABLE_VERSION);
    }

    /// The bound at the given path, with the values written if they are in the given trie.
    public static ClusteringBound<byte[]> bound(@Nullable Trie<byte[][]> originals, ClusteringComparator comparator, ByteComparable path, boolean isEnd)
    {
        ClusteringBound<byte[]> bound = comparator.boundFromByteComparable(ByteArrayAccessor.instance, path, isEnd);
        byte[][] values = originals != null ? originals.get(path) : null;
        return values == null || values == DECODES_EXACTLY ? bound : ByteArrayAccessor.instance.factory().bound(bound.kind(), values);
    }

    /// The given marker decoded from the given path, with the values written if they are in the given trie.
    public static RangeTombstoneMarker withOriginalValues(@Nullable RangeTombstoneMarker marker, @Nullable Trie<byte[][]> originals, ByteComparable path)
    {
        byte[][] values = marker != null && originals != null ? originals.get(path) : null;
        if (values == null || values == DECODES_EXACTLY)
            return marker;

        if (marker.isBoundary())
        {
            RangeTombstoneBoundaryMarker boundary = (RangeTombstoneBoundaryMarker) marker;
            return new RangeTombstoneBoundaryMarker(ByteArrayAccessor.instance.factory().boundary(marker.clustering().kind(), values),
                                                    boundary.endDeletionTime(),
                                                    boundary.startDeletionTime());
        }
        return new RangeTombstoneBoundMarker(ByteArrayAccessor.instance.factory().bound(marker.clustering().kind(), values),
                                             ((RangeTombstoneBoundMarker) marker).deletionTime());
    }

    private static <V> V[] toValues(ValueAccessor<V> accessor, byte[][] values)
    {
        V[] result = accessor.createArray(values.length);
        for (int i = 0; i < values.length; i++)
            result[i] = values[i] != null ? accessor.valueOf(values[i]) : null;
        return result;
    }
}
