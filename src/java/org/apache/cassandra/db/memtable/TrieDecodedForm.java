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

package org.apache.cassandra.db.memtable;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringBoundOrBoundary;
import org.apache.cassandra.db.ClusteringBoundary;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MultiCellCapableType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.TrieBackedPartition;
import org.apache.cassandra.db.partitions.TrieBackedPartitionStage2;
import org.apache.cassandra.db.partitions.TrieBackedPartitionStage3;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/// The form in which a trie memtable returns clustering keys, range tombstone bounds and cell paths: the ones it
/// keeps in its trie are decoded from their byte-comparable form. Most decoded values are other forms of equal values
/// (a decimal with another scale), but an empty value of a fixed-length type inside a tuple or user type comes back as
/// a null component, which sorts differently. [TrieMemtableStage2] decodes only clustering keys, [TrieMemtableStage3]
/// also range tombstone bounds, and [TrieMemtableStage1] keeps all of them as written.
///
/// On request, scrub and compaction rewrite sstables into this form, one partition at a time, so that their keys match
/// the keys of writes through the memtable. A partition can't be rewritten when its rewritten keys no longer sort
/// strictly in the written order, or when a range tombstone bound moves in byte-comparable order: it then holds two
/// forms of a key, or a range of one form and a row of the other, written while the table used another memtable. The
/// rewrite refuses it with a [RefusedException].
public class TrieDecodedForm
{
    /// Types whose values decode to the same bytes.
    private static final Set<AbstractType<?>> DECODED_EXACTLY = ImmutableSet.of(Int32Type.instance, LongType.instance,
                                                                                ShortType.instance, ByteType.instance,
                                                                                TimestampType.instance, SimpleDateType.instance,
                                                                                TimeType.instance, UUIDType.instance,
                                                                                TimeUUIDType.instance);

    private final ByteComparable.Version version;
    private final ClusteringComparator comparator;
    private final boolean decodesClusterings;
    private final boolean decodesBounds;
    private final boolean decodesCellPaths;

    private TrieDecodedForm(ByteComparable.Version version,
                            ClusteringComparator comparator,
                            boolean decodesClusterings,
                            boolean decodesBounds,
                            boolean decodesCellPaths)
    {
        this.version = version;
        this.comparator = comparator;
        this.decodesClusterings = decodesClusterings;
        this.decodesBounds = decodesBounds;
        this.decodesCellPaths = decodesCellPaths;
    }

    /// The form of the table's memtable for keys of the table's current types, or null if the memtable keeps keys as
    /// written or none of the types can decode to other bytes.
    public static TrieDecodedForm of(TableMetadata metadata)
    {
        return of(metadata.params.memtable.factory(), metadata.comparator, metadata.regularAndStaticColumns());
    }

    /// The form of the memtables of the given factory for keys of the given clustering types and columns, or null if
    /// those memtables keep keys as written or none of the types can decode to other bytes.
    public static TrieDecodedForm of(Memtable.Factory factory, ClusteringComparator comparator, RegularAndStaticColumns columns)
    {
        ByteComparable.Version version;
        boolean decodesBounds;
        boolean decodesCellPaths;
        if (factory instanceof TrieMemtableFactory)
        {
            version = TrieBackedPartition.BYTE_COMPARABLE_VERSION;
            decodesBounds = true;
            decodesCellPaths = true;
        }
        else if (factory instanceof TrieMemtableStage3.Factory)
        {
            version = TrieBackedPartitionStage3.BYTE_COMPARABLE_VERSION;
            decodesBounds = true;
            decodesCellPaths = false;
        }
        else if (factory instanceof TrieMemtableStage2.Factory)
        {
            version = TrieBackedPartitionStage2.BYTE_COMPARABLE_VERSION;
            decodesBounds = false;
            decodesCellPaths = false;
        }
        else
        {
            return null;
        }

        boolean decodesClusterings = !Iterables.all(comparator.subtypes(), TrieDecodedForm::decodesExactly);
        decodesCellPaths &= Iterables.any(columns, TrieDecodedForm::hasCellPathsToDecode);
        if (!decodesClusterings && !decodesCellPaths)
            return null;

        return new TrieDecodedForm(version, comparator, decodesClusterings, decodesClusterings && decodesBounds, decodesCellPaths);
    }

    private static boolean decodesExactly(AbstractType<?> type)
    {
        AbstractType<?> unwrapped = type instanceof ReversedType ? ((ReversedType<?>) type).baseType : type;
        return unwrapped.isByteOrderComparable || DECODED_EXACTLY.contains(unwrapped);
    }

    private static boolean hasCellPathsToDecode(ColumnMetadata column)
    {
        return column.isComplex() && !decodesExactly(nameComparator(column));
    }

    private static AbstractType<?> nameComparator(ColumnMetadata column)
    {
        return ((MultiCellCapableType<?>) column.type).nameComparator();
    }

    /// Rewrites the partition, read from `source`, into this form.
    public UnfilteredRowIterator apply(UnfilteredRowIterator partition, Object source)
    {
        Rewrite rewrite = new Rewrite(partition.metadata(), partition, source);
        UnfilteredRowIterator rewritten = Transformation.apply(partition, rewrite);
        return new WrappingUnfilteredRowIterator()
        {
            @Override
            public UnfilteredRowIterator wrapped()
            {
                return rewritten;
            }

            @Override
            public boolean hasNext()
            {
                if (rewritten.hasNext())
                    return true;
                rewrite.checkEnd();
                return false;
            }
        };
    }

    /// Rewrites the partitions of each scanner into this form.
    public List<ISSTableScanner> apply(List<ISSTableScanner> scanners)
    {
        return scanners.stream().map(RewritingScanner::new).collect(Collectors.toList());
    }

    /// Thrown for a partition whose keys can't be rewritten into the trie memtable's form.
    public static class RefusedException extends RuntimeException
    {
        RefusedException(String message)
        {
            super(message);
        }
    }

    private class Rewrite extends Transformation<UnfilteredRowIterator>
    {
        private final TableMetadata metadata;
        private final UnfilteredRowIterator partition;
        private final Object source;
        private Unfiltered previous;
        private Unfiltered previousWritten;
        private boolean rangeOpen;

        Rewrite(TableMetadata metadata, UnfilteredRowIterator partition, Object source)
        {
            this.metadata = metadata;
            this.partition = partition;
            this.source = source;
        }

        @Override
        protected Row applyToStatic(Row row)
        {
            return rewrite(row, row.clustering());
        }

        @Override
        protected Row applyToRow(Row row)
        {
            Clustering<?> clustering = decodesClusterings
                                       ? comparator.clusteringFromByteComparable(ByteBufferAccessor.instance,
                                                                                 comparator.asByteComparable(row.clustering()),
                                                                                 version)
                                       : row.clustering();
            return checkOrder(rewrite(row, clustering), row);
        }

        @Override
        protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
        {
            if (marker.isClose(false) != rangeOpen)
                throw refused(String.format("the range tombstone marker %s doesn't %s a range", marker.toString(metadata),
                                            rangeOpen ? "close" : "open"));
            rangeOpen = marker.isOpen(false);

            if (!decodesBounds)
                return checkOrder(marker, marker);

            ClusteringBoundOrBoundary<?> bound = marker.clustering();
            ClusteringBoundOrBoundary<?> decoded = bound.isBoundary()
                                                   ? comparator.boundaryFromByteComparable(ByteBufferAccessor.instance, comparator.asByteComparable(bound))
                                                   : comparator.boundFromByteComparable(ByteBufferAccessor.instance, comparator.asByteComparable(bound), bound.kind().isEnd());
            if (decoded.equals(bound))
                return checkOrder(marker, marker);
            if (ByteComparable.compare(comparator.asByteComparable(bound), comparator.asByteComparable(decoded), version) != 0)
                throw refused(String.format("the range tombstone bound %s decodes to %s, which has another position",
                                            bound.toString(metadata), decoded.toString(metadata)));

            RangeTombstoneMarker rewritten = marker.isBoundary()
                                             ? new RangeTombstoneBoundaryMarker((ClusteringBoundary<?>) decoded,
                                                                                ((RangeTombstoneBoundaryMarker) marker).endDeletionTime(),
                                                                                ((RangeTombstoneBoundaryMarker) marker).startDeletionTime())
                                             : new RangeTombstoneBoundMarker((ClusteringBound<?>) decoded, ((RangeTombstoneBoundMarker) marker).deletionTime());
            return checkOrder(rewritten, marker);
        }

        void checkEnd()
        {
            if (rangeOpen)
                throw refused("a range tombstone is open at the end of the partition");
        }

        private <U extends Unfiltered> U checkOrder(U rewritten, Unfiltered written)
        {
            if (previous != null && comparator.compare(previous, rewritten) >= 0)
                throw refused(String.format("%s and %s are no longer in order",
                                            previousWritten.clustering().toString(metadata), written.clustering().toString(metadata)));
            previous = rewritten;
            previousWritten = written;
            return rewritten;
        }

        private Row rewrite(Row row, Clustering<?> clustering)
        {
            boolean pathsChange = decodesCellPaths && cellPathsChange(row);
            if (!pathsChange && clustering.equals(row.clustering()))
                return row;

            Row.Builder builder = BTreeRow.sortedBuilder();
            builder.newRow(clustering);
            builder.addPrimaryKeyLivenessInfo(row.primaryKeyLivenessInfo());
            builder.addRowDeletion(row.deletion());
            for (ColumnData cd : row)
            {
                if (cd.column().isSimple())
                {
                    builder.addCell((Cell<?>) cd);
                    continue;
                }

                ComplexColumnData complex = (ComplexColumnData) cd;
                if (!complex.complexDeletion().isLive())
                    builder.addComplexDeletion(complex.column(), complex.complexDeletion());
                boolean decodesPaths = pathsChange && hasCellPathsToDecode(complex.column());
                for (Cell<?> cell : complex)
                    builder.addCell(decodesPaths ? cell.withPath(decode(cell.column(), cell.path())) : cell);
            }
            return builder.build();
        }

        private boolean cellPathsChange(Row row)
        {
            boolean changed = false;
            for (ColumnData cd : row)
            {
                if (!hasCellPathsToDecode(cd.column()))
                    continue;

                CellPath previousPath = null;
                for (Cell<?> cell : (ComplexColumnData) cd)
                {
                    CellPath path = decode(cell.column(), cell.path());
                    if (previousPath != null && cd.column().cellPathComparator().compare(previousPath, path) >= 0)
                        throw refused(String.format("two elements of %s in row %s are no longer in order",
                                                    cd.column().name, row.clustering().toString(metadata)));
                    previousPath = path;
                    changed |= !path.equals(cell.path());
                }
            }
            return changed;
        }

        private RefusedException refused(String reason)
        {
            return new RefusedException(String.format("Cannot rewrite partition %s of %s.%s in %s into the form of the " +
                                                      "table's trie memtable: %s. The sstable is kept; other sstables " +
                                                      "of the same run may already be rewritten. To rewrite the " +
                                                      "partition in the live table, delete it, run nodetool flush and " +
                                                      "nodetool garbagecollect on the table, write its rows again, and " +
                                                      "run the rewrite again; until every sstable of the table is " +
                                                      "rewritten, compactions without the rewrite, including that " +
                                                      "garbagecollect, can make rows of other partitions hidden or " +
                                                      "shown for good. For an sstable of a snapshot, leave the live " +
                                                      "table as it is and upgrade the snapshot without the rewrite.",
                                                      metadata.partitionKeyType.getString(partition.partitionKey().getKey()),
                                                      metadata.keyspace, metadata.name, source, reason));
        }
    }

    private CellPath decode(ColumnMetadata column, CellPath path)
    {
        AbstractType<?> type = nameComparator(column);
        return CellPath.create(type.fromComparableBytes(ByteSource.peekable(type.asComparableBytes(path.get(0), version)), version));
    }

    private class RewritingScanner implements ISSTableScanner
    {
        private final ISSTableScanner scanner;

        RewritingScanner(ISSTableScanner scanner)
        {
            this.scanner = scanner;
        }

        @Override
        public UnfilteredRowIterator next()
        {
            return apply(scanner.next(), scanner.getBackingSSTables());
        }

        @Override
        public boolean hasNext()
        {
            return scanner.hasNext();
        }

        @Override
        public TableMetadata metadata()
        {
            return scanner.metadata();
        }

        @Override
        public void close()
        {
            scanner.close();
        }

        @Override
        public long getLengthInBytes()
        {
            return scanner.getLengthInBytes();
        }

        @Override
        public long getCompressedLengthInBytes()
        {
            return scanner.getCompressedLengthInBytes();
        }

        @Override
        public long getCurrentPosition()
        {
            return scanner.getCurrentPosition();
        }

        @Override
        public long getBytesScanned()
        {
            return scanner.getBytesScanned();
        }

        @Override
        public Set<SSTableReader> getBackingSSTables()
        {
            return scanner.getBackingSSTables();
        }

        @Override
        public int level()
        {
            return scanner.level();
        }
    }
}
