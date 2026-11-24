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

package org.apache.cassandra.db.streaming;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingDataOutputPlus;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.StorageCompatibilityMode;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Ref;

/**
 * used to transfer the part(or whole) of a SSTable data file
 */
public class CassandraOutgoingFile implements OutgoingStream
{
    private final Ref<? extends SSTableReader> ref;
    private final long estimatedKeys;
    private final List<SSTableReader.PartitionPositionBounds> sections;
    private final String filename;
    private final boolean shouldStreamEntireSSTable;
    private final StreamOperation operation;
    private final BtiDaReceiver receiver;
    private final CassandraStreamHeader header;

    public CassandraOutgoingFile(StreamOperation operation, Ref<? extends SSTableReader> ref,
                                 List<SSTableReader.PartitionPositionBounds> sections, List<Range<Token>> normalizedRanges,
                                 long estimatedKeys)
    {
        this(operation, ref, sections, normalizedRanges, estimatedKeys, BtiDaReceiver.ENTIRE);
    }

    /**
     * @param receiver what the peer reads of bti sstables of major version "d"
     */
    public CassandraOutgoingFile(StreamOperation operation, Ref<? extends SSTableReader> ref,
                                 List<SSTableReader.PartitionPositionBounds> sections, List<Range<Token>> normalizedRanges,
                                 long estimatedKeys, BtiDaReceiver receiver)
    {
        Preconditions.checkNotNull(ref.get());
        Range.assertNormalized(normalizedRanges);
        this.operation = operation;
        this.receiver = receiver;
        this.ref = ref;
        this.estimatedKeys = estimatedKeys;
        this.sections = sections;

        SSTableReader sstable = ref.get();

        this.filename = sstable.getFilename();
        this.shouldStreamEntireSSTable = computeShouldStreamEntireSSTables();
        ComponentManifest manifest = ComponentManifest.create(sstable);
        this.header = makeHeader(sstable, operation, sections, estimatedKeys, shouldStreamEntireSSTable, manifest);
    }

    private static CassandraStreamHeader makeHeader(SSTableReader sstable,
                                                    StreamOperation operation,
                                                    List<SSTableReader.PartitionPositionBounds> sections,
                                                    long estimatedKeys,
                                                    boolean shouldStreamEntireSSTable,
                                                    ComponentManifest manifest)
    {
        CompressionInfo compressionInfo = sstable.compression
                ? CompressionInfo.newLazyInstance(sstable.getCompressionMetadata(), sections)
                : null;

        return CassandraStreamHeader.builder()
                                    .withSSTableVersion(sstable.descriptor.version)
                                    .withSSTableLevel(operation.keepSSTableLevel() ? sstable.getSSTableLevel() : 0)
                                    .withEstimatedKeys(estimatedKeys)
                                    .withSections(sections)
                                    .withCompressionInfo(compressionInfo)
                                    .withSerializationHeader(sstable.header.toComponent())
                                    .isEntireSSTable(shouldStreamEntireSSTable)
                                    .withComponentManifest(manifest)
                                    .withFirstKey(sstable.getFirst())
                                    .withTableId(sstable.metadata().id)
                                    .build();
    }

    @VisibleForTesting
    public static CassandraOutgoingFile fromStream(OutgoingStream stream)
    {
        Preconditions.checkArgument(stream instanceof CassandraOutgoingFile);
        return (CassandraOutgoingFile) stream;
    }

    @VisibleForTesting
    public Ref<? extends SSTableReader> getRef()
    {
        return ref;
    }

    @Override
    public String getName()
    {
        return filename;
    }

    @Override
    public long getEstimatedSize()
    {
        return header.size();
    }

    @Override
    public TableId getTableId()
    {
        return ref.get().metadata().id;
    }

    @Override
    public int getNumFiles()
    {
        return shouldStreamEntireSSTable ? header.componentManifest.components().size() : 1;
    }

    @Override
    public long getRepairedAt()
    {
        return ref.get().getRepairedAt();
    }

    @Override
    public TimeUUID getPendingRepair()
    {
        return ref.get().getPendingRepair();
    }

    @Override
    public void write(StreamSession session, StreamingDataOutputPlus out, int version) throws IOException
    {
        SSTableReader sstable = ref.get();

        if (shouldStreamEntireSSTable)
        {
            // Acquire lock to avoid concurrent sstable component mutation because of stats update or index summary
            // redistribution, otherwise file sizes recorded in component manifest will be different from actual
            // file sizes.
            // Recreate the latest manifest and hard links for mutatable components in case they are modified.
            try (ComponentContext context = sstable.runWithLock(ignored -> ComponentContext.create(sstable)))
            {
                CassandraStreamHeader current = makeHeader(sstable, operation, sections, estimatedKeys, true, context.manifest());
                CassandraStreamHeader.serializer.serialize(current, out, version);
                out.flush();

                CassandraEntireSSTableStreamWriter writer = new CassandraEntireSSTableStreamWriter(sstable, session, context);
                writer.write(out);
            }
        }
        else
        {
            // legacy streaming is not affected by stats metadata mutation and index sumary redistribution
            CassandraStreamHeader.serializer.serialize(header, out, version);
            out.flush();

            if (header.isCompressed())
            {
                CassandraCompressedStreamWriter writer = new CassandraCompressedStreamWriter(sstable, header, session);
                writer.write(out);
            }
            else
            {
                CassandraStreamWriter writer = new CassandraStreamWriter(sstable, header, session);
                writer.write(out, version);
            }
        }
    }

    @VisibleForTesting
    public boolean computeShouldStreamEntireSSTables()
    {
        // don't stream if full sstable transfers are disabled, legacy counter shards are present,
        // or sstable uses old bloom filter format (pre-4.0) which is incompatible with zero-copy streaming
        if (!DatabaseDescriptor.streamEntireSSTables() ||
            ref.get().getSSTableMetadata().hasLegacyCounterShards ||
            ref.get().descriptor.version.hasOldBfFormat())
            return false;

        // an sstable of a version newer than the one written is streamed by partitions, so the receiver writes the
        // data in its own version; only HCD_1 and an explicit bti version write a version older than one they read
        Version version = ref.get().descriptor.version;
        if ((DatabaseDescriptor.getStorageCompatibilityModeForWrites() == StorageCompatibilityMode.HCD_1
             || BtiFormat.is(version.format) && CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION.isPresent())
            && version.version.compareTo(version.format.getLatestVersion().version) > 0)
            return false;

        if (!receiver.entire && isBtiD(version))
            return false;

        return contained(sections, ref.get());
    }

    @Override
    public void checkCanSend()
    {
        if (receiver.refusal != null && isBtiD(ref.get().descriptor.version))
            throw new IllegalStateException(String.format(BtiDaReceiver.REFUSAL, ref.get().descriptor, receiver.peer, receiver.refusal,
                                                          BtiFormat.STORAGE_COMPATIBILITY_HCD_1_VERSION,
                                                          CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION.getKey(),
                                                          CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION.getKey(),
                                                          BtiFormat.STORAGE_COMPATIBILITY_HCD_1_VERSION));
    }

    private static boolean isBtiD(Version version)
    {
        return BtiFormat.is(version.format) && version.version.charAt(0) > BtiFormat.STORAGE_COMPATIBILITY_HCD_1_VERSION.charAt(0);
    }

    @VisibleForTesting
    public boolean contained(List<SSTableReader.PartitionPositionBounds> sections, SSTableReader sstable)
    {
        if (sections == null || sections.isEmpty())
            return false;

        // if transfer sections contain entire sstable
        long transferLength = sections.stream().mapToLong(p -> p.upperPosition - p.lowerPosition).sum();
        return transferLength == sstable.uncompressedLength();
    }

    @Override
    public void finish()
    {
        ref.release();
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CassandraOutgoingFile that = (CassandraOutgoingFile) o;
        return estimatedKeys == that.estimatedKeys &&
               Objects.equals(ref, that.ref) &&
               Objects.equals(sections, that.sections);
    }

    public int hashCode()
    {
        return Objects.hash(ref, estimatedKeys, sections);
    }

    @Override
    public String toString()
    {
        return "CassandraOutgoingFile{" + filename + '}';
    }

    /**
     * What a peer reads of bti sstables of major version "d", from one snapshot of its messaging version.
     */
    public static final class BtiDaReceiver
    {
        // a peer they are sent to entire: this node, an Apache Cassandra node or a DataStax 5.0 node, though the
        // earlier 5.0 releases, which don't read them, have the same messaging version
        public static final BtiDaReceiver ENTIRE = new BtiDaReceiver(true, null, null);
        // a peer that may not read them entire, as for sstableloader, which doesn't know the versions of its targets
        public static final BtiDaReceiver PARTITIONS = new BtiDaReceiver(false, null, null);

        private static final String REFUSAL = "Cannot stream %s to %s, %s: make the sending node write bti %s " +
                                              "(storage_compatibility_mode HCD_1 without a later -D%s, or -D%s=%s), " +
                                              "then rewrite it with nodetool upgradesstables -a";

        @VisibleForTesting
        final boolean entire;
        @Nullable
        private final InetAddressAndPort peer;
        // why the peer can't receive an sstable of major version "d" at all, if it can't
        @VisibleForTesting
        @Nullable
        final String refusal;

        private BtiDaReceiver(boolean entire, @Nullable InetAddressAndPort peer, @Nullable String refusal)
        {
            this.entire = entire;
            this.peer = peer;
            this.refusal = refusal;
        }

        /**
         * The DataStax nodes before 5.0 (DS 10 to DS 12, DSE 6.8) can't decode the partitions of a bti sstable of major
         * version "d", nor can a node whose version isn't known yet be shown not to be one of them.
         */
        public static BtiDaReceiver of(InetAddressAndPort peer)
        {
            if (peer.equals(FBUtilities.getBroadcastAddressAndPort()))
                return ENTIRE;
            Integer version = MessagingService.instance().versions.getRawIfKnown(peer);
            if (version != null && (version < MessagingService.VERSION_DS_10 || MessagingService.isVersion50OrLater(version)))
                return ENTIRE;
            return new BtiDaReceiver(false, peer, version == null ? "whose messaging version isn't known yet"
                                                                  : "whose messaging version " + version + " doesn't read it");
        }
    }
}
