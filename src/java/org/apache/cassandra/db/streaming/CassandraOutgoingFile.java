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
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.sstable.ApacheReadability;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ApacheUpgradePath;
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
@NotThreadSafe
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

    // guards against releasing the SSTable streaming status more than once
    private boolean streamRebuildStatusReleased = false;

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

        // If the SSTable is currently being rebuilt, fall back to legacy streaming.
        boolean entire = computeShouldStreamEntireSSTables() && sstable.streamRebuildState().tryBeginStreaming();
        this.shouldStreamEntireSSTable = entire;
        try
        {
            ComponentManifest manifest = ComponentManifest.create(sstable);
            this.header = makeHeader(sstable, operation, sections, estimatedKeys, entire, manifest);
        }
        catch (RuntimeException | Error e)
        {
            // Release the status we just acquired if construction fails.
            if (entire)
                sstable.streamRebuildState().endStreaming();
            throw e;
        }
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

        // an Apache Cassandra 5.0 node fails on an sstable without a complete group of its own for one of its indexes,
        // and keeps SAI components it didn't write
        if (receiver.apache && (!receiver.schemaAgrees
                                || !ApacheReadability.hasCompleteAaGroups(ref.get())
                                || !ApacheReadability.dataStaxSAIComponents(ref.get().components()).isEmpty()))
            return false;

        return contained(sections, ref.get());
    }

    @Override
    public void checkCanSend()
    {
        if (receiver.apache)
        {
            List<String> unreadable = ApacheReadability.unreadable(ref.get());
            if (!unreadable.isEmpty())
                throw new IllegalStateException(String.format("Cannot stream %s to %s, %s, which reads only what Apache Cassandra 5.0 reads: " +
                                                              "%s. Rewrite it with nodetool upgradesstables -a with settings that write what " +
                                                              "Apache Cassandra 5.0 reads",
                                                              ref.get().descriptor, receiver.peer, receiver.description, String.join("; ", unreadable)));
        }
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
        releaseStreamRebuildStatus();
        ref.release();
    }

    /**
     * Releases the entire-sstable streaming status this stream reserved at construction, exactly once, without
     * releasing the SSTable reference. Used both by {@link #finish()} and by callers that must clean up a
     * constructed-but-never-transferred stream.
     */
    @Override
    public void releaseStreamRebuildStatus()
    {
        if (shouldStreamEntireSSTable && !streamRebuildStatusReleased)
        {
            streamRebuildStatusReleased = true;
            ref.get().streamRebuildState().endStreaming();
        }
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
     * What a peer reads of bti sstables of major version "d", and whether it takes only what Apache Cassandra 5.0 reads,
     * from one snapshot of its messaging version.
     */
    public static final class BtiDaReceiver
    {
        // a peer that reads them, entire: this node or a DataStax node of this release's messaging version
        public static final BtiDaReceiver ENTIRE = new BtiDaReceiver(true, null, null, false, null, false);
        // a peer that may not read them entire, as for sstableloader, which doesn't know the versions of its targets
        public static final BtiDaReceiver PARTITIONS = new BtiDaReceiver(false, null, null, false, null, false);

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
        // whether the peer is below DS 10 (an Apache Cassandra node or a node in storage_compatibility_mode CASSANDRA_5),
        // or of an unknown version while an Apache Cassandra node may be in the ring, which takes only what Apache
        // Cassandra 5.0 reads
        @VisibleForTesting
        final boolean apache;
        // the peer's messaging version, as the refusals of an apache peer name it
        @Nullable
        private final String description;
        // whether the peer had this node's schema version when the session was planned
        private final boolean schemaAgrees;

        private BtiDaReceiver(boolean entire, @Nullable InetAddressAndPort peer, @Nullable String refusal, boolean apache, @Nullable String description, boolean schemaAgrees)
        {
            this.entire = entire;
            this.peer = peer;
            this.refusal = refusal;
            this.apache = apache;
            this.description = description;
            this.schemaAgrees = schemaAgrees;
        }

        /**
         * The DataStax nodes of earlier 5.0 messaging versions accept an entire bti sstable of major version "d" but
         * don't read it, so they receive it by partitions. The DataStax nodes before 5.0 (DS 10 to DS 12, DSE 6.8) can't
         * decode its partitions either, nor can a node whose version isn't known yet be shown not to be one of them,
         * unless an Apache Cassandra node may be in the ring: then such a node takes only what Apache Cassandra 5.0 reads.
         */
        public static BtiDaReceiver of(InetAddressAndPort peer)
        {
            if (peer.equals(FBUtilities.getBroadcastAddressAndPort()))
                return ENTIRE;
            Integer version = MessagingService.instance().versions.getRawIfKnown(peer);
            if (version != null && version < MessagingService.VERSION_DS_10)
                return new BtiDaReceiver(true, peer, null, true, "a node at messaging version " + version, schemaAgrees(peer));
            if (version == null && ApacheUpgradePath.checksSchemaChanges())
                return new BtiDaReceiver(true, peer, null, true, "whose messaging version isn't known yet", schemaAgrees(peer));
            if (version != null && MessagingService.isDataStax21OrLater(version))
                return ENTIRE;
            if (version != null && MessagingService.isDataStax50OrLater(version))
                return PARTITIONS;
            return new BtiDaReceiver(false, peer, version == null ? "whose messaging version isn't known yet"
                                                                  : "whose messaging version " + version + " doesn't read it", false, null, false);
        }

        private static boolean schemaAgrees(InetAddressAndPort peer)
        {
            EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(peer);
            VersionedValue schema = state == null ? null : state.getApplicationState(ApplicationState.SCHEMA);
            return schema != null && schema.value.equals(Schema.instance.getVersion().toString());
        }
    }
}
