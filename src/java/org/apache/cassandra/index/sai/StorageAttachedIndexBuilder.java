/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.cassandra.index.sai;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.index.sai.disk.PerIndexFilesOwner;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.ComponentsBuildId;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableSimpleIterator;
import org.apache.cassandra.io.sstable.SSTableWatcher;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.ReadPattern;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ApproximateTime;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.db.compaction.TableOperation.StopTrigger.TRUNCATE;

/**
 * Multiple storage-attached indexes can start building concurrently. We need to make sure:
 * 1. Per-SSTable index files are built only once
 *      a. Per-SSTable index files already built, do nothing
 *      b. Per-SSTable index files are currently building, we need to wait until it's built in order to consider index built.
 * 2. Per-column index files are built for each column index
 */
public class StorageAttachedIndexBuilder extends SecondaryIndexBuilder
{
    protected static final Logger logger = LoggerFactory.getLogger(StorageAttachedIndexBuilder.class);

    // make sure only one builder can write to per sstable files when multiple storage-attached indexes are created simultaneously.
    private static final Map<SSTableReader, CountDownLatch> inProgress = Maps.newConcurrentMap();

    /**
     * How long, at most, a build stopped by a truncate waits for the truncate to be over (see
     * {@link #removedByTruncate}): longer than a truncate waits for the operations it stops.
     */
    @VisibleForTesting
    static volatile long TRUNCATED_SSTABLES_WAIT_MILLIS =
        TimeUnit.SECONDS.toMillis(CassandraRelevantProperties.CESSATION_WAIT_SECONDS.getInt(60) + 60);

    private final StorageAttachedIndexGroup group;
    private final TableMetadata metadata;
    private final Tracker tracker;
    private final UUID compactionId = UUIDGen.getTimeUUID();
    private final boolean isFullRebuild;
    private final boolean isInitialBuild;

    private final SortedMap<SSTableReader, Set<StorageAttachedIndex>> sstables;
    // the SSTables this build hasn't built yet
    private final Set<SSTableReader> unbuilt;

    private long bytesProcessed = 0;
    private final long totalSizeInBytes;

    StorageAttachedIndexBuilder(StorageAttachedIndexGroup group, SortedMap<SSTableReader, Set<StorageAttachedIndex>> sstables, boolean isFullRebuild, boolean isInitialBuild)
    {
        this.group = group;
        this.metadata = group.metadata();
        this.sstables = sstables;
        this.unbuilt = new HashSet<>(sstables.keySet());
        this.tracker = group.table().getTracker();
        this.isFullRebuild = isFullRebuild;
        this.isInitialBuild = isInitialBuild;
        this.totalSizeInBytes = sstables.keySet().stream().mapToLong(SSTableReader::uncompressedLength).sum();
    }

    @Override
    public void build()
    {
        logger.debug(logMessage("Starting full index build"));
        for (Map.Entry<SSTableReader, Set<StorageAttachedIndex>> e : sstables.entrySet())
        {
            SSTableReader sstable = e.getKey();
            Set<StorageAttachedIndex> indexes = e.getValue();

            Set<StorageAttachedIndex> existing = validateIndexes(indexes, sstable.descriptor);
            if (existing.isEmpty())
            {
                logger.debug(logMessage("{} dropped during index build"), indexes);
                unbuilt.remove(sstable);
                continue;
            }

            if (indexSSTable(sstable, existing))
            {
                return;
            }
            unbuilt.remove(sstable);
        }
    }

    private String logMessage(String message)
    {
        return String.format("[%s.%s.*] %s", metadata.keyspace, metadata.name, message);
    }

    /**
     * @return true if index build should be stopped
     */
    private boolean indexSSTable(SSTableReader sstable, Set<StorageAttachedIndex> indexes)
    {
        logger.debug(logMessage("Starting index build on {}"), sstable.descriptor);
        long startTimeNanos = ApproximateTime.nanoTime();

        CountDownLatch perSSTableFileLock = null;
        StorageAttachedIndexWriter indexWriter = null;

        Ref<? extends SSTableReader> ref = sstable.tryRef();
        if (ref == null)
        {
            logger.warn(logMessage("Couldn't acquire reference to the SSTable {}. It may have been removed."), sstable.descriptor);
            return false;
        }

        SSTableWatcher.instance.onIndexBuild(sstable, indexes);

        IndexDescriptor indexDescriptor = group.descriptorFor(sstable);

        Set<Component> replacedComponents = new HashSet<>();
        List<PerIndexFilesOwner> owners = new ArrayList<>();

        try (RandomAccessReader dataFile = sstable.openDataReader(ReadPattern.SEQUENTIAL);
             LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.INDEX_BUILD, tracker.metadata))
        {
            ownFiles(sstable, indexes, owners);
            if (indexes.isEmpty())
                return false;

            perSSTableFileLock = shouldWritePerSSTableFiles(sstable, indexDescriptor, replacedComponents);
            // If we were unable to get the per-SSTable file lock it means that the
            // per-SSTable components are already being built so we only want to
            // build the per-index components
            boolean perIndexComponentsOnly = perSSTableFileLock == null;
            for (StorageAttachedIndex index : indexes)
            {
                replaceListedComponentsOfName(sstable, indexDescriptor, index.getIndexContext(), replacedComponents);
                prepareForRebuild(indexDescriptor.perIndexComponents(index.getIndexContext()), replacedComponents);
                deleteLeftoversUnderNewBuildNames(indexDescriptor, index.getIndexContext());
            }

            long keyCount = SSTableReader.getApproximateKeyCount(Set.of(sstable));
            indexWriter = new StorageAttachedIndexWriter(indexDescriptor, metadata, indexes, txn, keyCount, perIndexComponentsOnly, group.table().metric);

            long previousKeyPosition = 0;
            indexWriter.begin();

            try (PartitionIndexIterator keys = sstable.allKeysIterator())
            {
                while (!keys.isExhausted())
                {
                    if (isStopRequested())
                    {
                        logger.debug(indexDescriptor.logMessage("Index build has been stopped. Reason: {}"), trigger());
                        throw new CompactionInterruptedException(getProgress(), trigger());
                    }

                    final DecoratedKey key = sstable.decorateKey(keys.key());
                    final long keyPosition = keys.keyPosition();

                    indexWriter.startPartition(key, keyPosition);

                    RowIndexEntry indexEntry = sstable.getPosition(key, SSTableReader.Operator.EQ);
                    dataFile.seek(indexEntry.position);
                    ByteBufferUtil.skipShortLength(dataFile); // key

                    /*
                     * Not directly using {@link SSTableIdentityIterator#create(SSTableReader, FileDataInput, DecoratedKey)},
                     * because we need to get position of partition level deletion and static row.
                     */
                    long partitionDeletionPosition = dataFile.getFilePointer();
                    DeletionTime partitionLevelDeletion = DeletionTime.serializer.deserialize(dataFile);
                    long staticRowPosition = dataFile.getFilePointer();

                    indexWriter.partitionLevelDeletion(partitionLevelDeletion, partitionDeletionPosition);

                    DeserializationHelper helper = new DeserializationHelper(sstable.metadata(), sstable.descriptor.version.correspondingMessagingVersion(), DeserializationHelper.Flag.LOCAL);

                    try (SSTableSimpleIterator iterator = SSTableSimpleIterator.create(sstable.metadata(), dataFile, sstable.header, helper, partitionLevelDeletion);
                         SSTableIdentityIterator partition = new SSTableIdentityIterator(sstable, key, partitionLevelDeletion, sstable.getDataFile(), iterator))
                    {
                        // if the row has statics attached, it has to be indexed separately
                        if (metadata.hasStaticColumns())
                            indexWriter.nextUnfilteredCluster(partition.staticRow(), staticRowPosition);

                        while (partition.hasNext())
                        {
                            long unfilteredPosition = dataFile.getFilePointer();
                            indexWriter.nextUnfilteredCluster(partition.next(), unfilteredPosition);
                        }
                    }

                    keys.advance();
                    long dataPosition = keys.isExhausted() ? sstable.uncompressedLength() : keys.dataPosition();
                    bytesProcessed += dataPosition - previousKeyPosition;
                    previousKeyPosition = dataPosition;
                }

                completeSSTable(txn, indexWriter, indexDescriptor, sstable, indexes, perSSTableFileLock, replacedComponents);
            }
            long timeTaken = ApproximateTime.nanoTime() - startTimeNanos;
            group.table().metric.updateStorageAttachedIndexBuildTime(timeTaken);
            logger.trace("Completed indexing sstable {} in {} seconds", sstable.descriptor, TimeUnit.NANOSECONDS.toSeconds(timeTaken));

            return false;
        }
        catch (Throwable t)
        {
            if (indexWriter != null)
            {
                // A stop by a truncate that removed the SSTables this build hasn't built isn't a failure of the index
                boolean truncated = t instanceof CompactionInterruptedException && removedByTruncate();
                indexWriter.abort(t, !truncated);
            }

            if (t instanceof InterruptedException)
            {
                // TODO: Is there anything that makes more sense than just restoring the interrupt?
                logger.warn(logMessage("Interrupted while building indexes {} on SSTable {}"), indexes, sstable.descriptor);
                Thread.currentThread().interrupt();
                return true;
            }
            else if (t instanceof CompactionInterruptedException)
            {
                if (isInitialBuild && trigger() != TRUNCATE)
                {
                    logger.error(logMessage("Stop requested while building initial indexes {} on SSTable {}. {}"), indexes, sstable.descriptor, t.getMessage());
                    throw Throwables.unchecked(t);
                }
                logger.info(logMessage("Stop requested while building indexes {} on SSTable {}. {}"),
                            indexes, sstable.descriptor, t.getMessage());
                // Without writers, the stop came while waiting for a dropped index of the same name (see ownFiles):
                // the SSTable isn't built, unless a truncate removed it
                if (indexWriter == null && !removedByTruncate())
                    throw Throwables.unchecked(t);
                return true;
            }
            else
            {
                logger.error(logMessage("Unable to build indexes {} on SSTable {}. Cause: {}."), indexes, sstable, t.getMessage());
                throw Throwables.unchecked(t);
            }
        }
        finally
        {
            ref.release();
            // release current lock in case of error
            if (perSSTableFileLock != null)
            {
                inProgress.remove(sstable);
                perSSTableFileLock.countDown();
            }
            owners.forEach(PerIndexFilesOwner::close);
        }
    }

    /**
     * Takes, for each of the provided indexes, the ownership of its per-index files on the SSTable, once no build or
     * writer of a dropped index of the same name owns them, before any of them is deleted or written. The indexes
     * dropped by then are removed from the provided ones; a full rebuild stops instead.
     */
    private void ownFiles(SSTableReader sstable, Set<StorageAttachedIndex> indexes, List<PerIndexFilesOwner> owners)
    {
        Set<StorageAttachedIndex> dropped = new HashSet<>();
        // the files of every index are owned at once, and none while waiting, so that two builds of the SSTable taking
        // the indexes in different orders can't wait for each other
        synchronized (StorageAttachedIndex.SAME_NAME_FILES_LOCK)
        {
            boolean waits = false;
            try
            {
                String name;
                while ((name = nameOwnedByDroppedIndex(sstable, indexes)) != null)
                {
                    waits = true;
                    if (!PerIndexFilesOwner.awaitOwnersOfDroppedIndex(sstable.descriptor, name, 1, TimeUnit.SECONDS)
                        && isStopRequested())
                        throw new CompactionInterruptedException(getProgress(), trigger());
                }
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new CompactionInterruptedException(getProgress(), trigger());
            }
            // a stop requested while it waited, or an interrupt that came with the wakeup, whose flag stays set
            if (waits && (isStopRequested() || Thread.currentThread().isInterrupted()))
                throw new CompactionInterruptedException(getProgress(), trigger());

            for (StorageAttachedIndex index : indexes)
            {
                String name = index.getIndexContext().getIndexName();
                PerIndexFilesOwner owner = PerIndexFilesOwner.tryOwn(sstable.descriptor, name, index.isDropped());
                if (owner == null)
                    dropped.add(index);
                else
                    owners.add(owner);
            }
        }

        if (!dropped.isEmpty())
        {
            indexes.removeAll(dropped);
            validateIndexes(dropped, sstable.descriptor);
        }
    }

    /**
     * @return the name of one of the provided indexes, not dropped, whose files on the provided SSTable a dropped index
     * of that name owns, or null
     */
    private static String nameOwnedByDroppedIndex(SSTableReader sstable, Set<StorageAttachedIndex> indexes)
    {
        for (StorageAttachedIndex index : indexes)
        {
            String name = index.getIndexContext().getIndexName();
            if (!index.isDropped().getAsBoolean()
                && PerIndexFilesOwner.hasOwnerOfDroppedIndex(sstable.descriptor, name))
                return name;
        }
        return null;
    }

    /**
     * @return whether the build was stopped by a truncate that has removed all the SSTables it hasn't built, so the
     * build is complete. Waits for the truncate to be over (it runs with compactions disabled on the table) for at most
     * {@link #TRUNCATED_SSTABLES_WAIT_MILLIS}, as it may still fail to remove them.
     */
    private boolean removedByTruncate()
    {
        if (trigger() != TRUNCATE)
            return false;

        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TRUNCATED_SSTABLES_WAIT_MILLIS);
        while (true)
        {
            // Looked at before the SSTables: once the truncate is over, whatever it removed is removed.
            boolean truncating = group.table().isRunningWithCompactionsDisabled();

            // Removed means obsolete: an SSTable also leaves the live set for a while when an operation that
            // opens its results early has read it, and comes back if that operation aborts.
            Set<SSTableReader> live = tracker.getView().liveSSTables();
            if (unbuilt.stream().noneMatch(sstable -> live.contains(sstable) || !sstable.isMarkedCompacted()))
                return true;
            if (!truncating || System.nanoTime() - deadline > 0)
            {
                logger.warn(logMessage("The index build stopped by a truncate fails: {} of the SSTables it hasn't " +
                                       "built are still live, or may be again, {}"),
                            unbuilt.stream()
                                   .filter(sstable -> live.contains(sstable) || !sstable.isMarkedCompacted())
                                   .count(),
                            truncating ? "and the truncate isn't over" : "after the truncate");
                return false;
            }
            try
            {
                Thread.sleep(100);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    @Override
    public OperationProgress getProgress()
    {
        return new OperationProgress(metadata,
                                     OperationType.INDEX_BUILD,
                                     bytesProcessed,
                                     totalSizeInBytes,
                                     compactionId,
                                     sstables.keySet());
    }

    /**
     * if the per sstable index files are already created, not need to write it again, unless it's full rebuild.
     * if not created, try to acquire a lock, so only one builder will generate per sstable index files
     */
    private CountDownLatch shouldWritePerSSTableFiles(SSTableReader sstable, IndexDescriptor indexDescriptor, Set<Component> replacedComponents)
    {
        // if per-table files are incomplete or checksum failed during full rebuild.
        if (!indexDescriptor.perSSTableComponents().isComplete() || isFullRebuild)
        {
            CountDownLatch latch = new CountDownLatch(1);
            if (inProgress.putIfAbsent(sstable, latch) == null)
            {
                prepareForRebuild(indexDescriptor.perSSTableComponents(), replacedComponents);
                return latch;
            }
        }
        return null;
    }

    /**
     * The components named after the index that the TOC lists are those of previous builds of the name, possibly of
     * component types the new build doesn't write (e.g. of a dropped index of the same name on a column of another
     * type): the new build replaces them all. Without immutable components, their files are deleted before the build,
     * so they are unregistered now; otherwise, once the new build has registered its own.
     */
    private void replaceListedComponentsOfName(SSTableReader sstable,
                                               IndexDescriptor indexDescriptor,
                                               IndexContext context,
                                               Set<Component> replacedComponents)
    {
        Set<Component> listed = new HashSet<>();
        for (Component component : sstable.components())
        {
            Optional<Version.ParsedFileName> parsed = StorageAttachedIndex.tryParseFileName(component.name, context);
            if (parsed.isPresent() && context.getIndexName().equals(parsed.get().indexName))
                listed.add(component);
        }

        if (indexDescriptor.versionForNewComponents().useImmutableComponentFiles())
            replacedComponents.addAll(listed);
        else if (!listed.isEmpty())
            sstable.unregisterComponents(listed, tracker);
    }

    /**
     * Without immutable components, the new build of an index writes the files of generation 0, and most of them are
     * opened to append. Deletes any file there may be under those names, of any component type, so that the build
     * doesn't append to the files of another build (e.g. left by a crash, or by a dropped index of the same name), and
     * that its completion marker doesn't certify them.
     */
    private static void deleteLeftoversUnderNewBuildNames(IndexDescriptor indexDescriptor, IndexContext context)
    {
        Version version = indexDescriptor.versionForNewComponents();
        if (version.useImmutableComponentFiles())
            return;
        EnumSet<IndexComponentType> types = EnumSet.allOf(IndexComponentType.class);
        indexDescriptor.perIndexComponentsOfBuild(context, ComponentsBuildId.forNewSSTable(version), types)
                       .forceDeleteAllComponents();
    }

    private static void prepareForRebuild(IndexComponents.ForRead components, Set<Component> replacedComponents)
    {
        // The current components are "replaced" (by "other" components) if the build create different components than
        // the existing ones. This will happen in the following cases:
        // 1. if we use immutable components, that's the point of immutable components.
        // 2. when we do not use immutable components, the rebuild components will always be for the current version and
        // for generation 0, so if the current components are not for that specific built, then we won't be rebuilding
        // the exact same components, and we're "replacing", not "overwriting" ()
        //   a) the old components are from an older version: a new build will alawys be for `Version.current()` and
        //     so will create new files in that case (Note that "normally" we should not have non-0 generation in the
        //     first place if immutable components are not used, but we handle this case to better support "downgrades"
        //     where immutable components was enabled, but then disabled for some reason. If that happens, we still
        //     want to ensure a new build removes the old files both from disk (happens below) and from the sstable TOC
        //     (which is what `replacedComponents` is about)).
        if (components.version().useImmutableComponentFiles() || !components.buildId().equals(ComponentsBuildId.forNewSSTable(components.version())))
            replacedComponents.addAll(components.allAsCustomComponents());

        if (!components.version().useImmutableComponentFiles())
            components.forWrite().forceDeleteAllComponents();
    }

    /**
     * Completes the index build for an SSTable and registers the built components in its TOC.
     *
     * <p>The supplied {@code indexDescriptor} must be the descriptor used to create {@code indexWriter}. Looking up the
     * descriptor through the group at this point is unsafe because concurrent unloading can evict the cached descriptor;
     * rebuilding it from the SSTable TOC would skip the components produced by this build.</p>
     *
     * <p>If {@code latch} is non-null, this builder owns the per-SSTable component build and releases waiting builders.
     * Otherwise, this builder waits for an owner, if any, before publishing its per-index components.</p>
     */
    private void completeSSTable(LifecycleTransaction txn,
                                 StorageAttachedIndexWriter indexWriter,
                                 IndexDescriptor indexDescriptor,
                                 SSTableReader sstable,
                                 Set<StorageAttachedIndex> indexes,
                                 CountDownLatch latch,
                                 Set<Component> replacedComponents) throws InterruptedException
    {
        indexWriter.complete(sstable);

        if (latch != null)
        {
            // current builder owns the lock
            latch.countDown();
        }
        else
        {
            /*
             * When there is no lock, it means the per sstable index files are already created, just proceed to finish.
             * When there is a lock held by another builder, wait for it to finish before finishing marking current index built.
             */
            latch = inProgress.get(sstable);
            if (latch != null)
                latch.await();
        }

        Set<StorageAttachedIndex> existing = validateIndexes(indexes, sstable.descriptor);
        if (existing.isEmpty())
        {
            logger.debug(logMessage("{} dropped during index build"), indexes);
            return;
        }

        // The per-SSTable components are missing if the builder that held the lock failed or was stopped. A removed
        // SSTable needs no index.
        if (!sstable.isMarkedCompacted() && !indexDescriptor.perSSTableComponents().isComplete())
            throw new RuntimeException(logMessage("Failed to build indexes " + existing + " on SSTable " + sstable.descriptor + " as its per-SSTable components were not built."));

        // Register components from the descriptor that owns the writers. The context cache may have been
        // evicted before this point, and a reader lookup would reconstruct a descriptor from the pre-build TOC.
        Set<Component> activeComponents = group.activeComponents(indexDescriptor);
        sstable.registerComponents(activeComponents, tracker);
        // the new build may reuse names the TOC listed (e.g. files deleted while the TOC couldn't be rewritten)
        replacedComponents.removeAll(activeComponents);
        if (!replacedComponents.isEmpty())
            sstable.unregisterComponents(replacedComponents, tracker);

        /**
         * During memtable flush, it completes the transaction first which opens the flushed sstable,
         * and then notify the new sstable to SAI. Here we should do the same.
         */
        txn.trackNewAttachedIndexFiles(sstable);
        // there is nothing to commit. Close() effectively abort the transaction.
        txn.close();

        Set<StorageAttachedIndex> incomplete = group.onSSTableChanged(Collections.emptyList(), Collections.singleton(sstable), existing, false);
        if (!incomplete.isEmpty())
        {
            // If this occurs during an initial index build, there is only one index in play, and
            // throwing here to terminate makes sense. (This allows the initialization task to fail
            // correctly and be marked as failed by the SIM.) In other cases, such as rebuilding a
            // set of indexes for a new added/streamed SSTables, we terminate pessimistically. In
            // other words, we abort the SSTable index write across all column indexes and mark
            // then non-queryable until a restart or other incremental rebuild occurs.
            throw new RuntimeException(logMessage("Failed to update views on column indexes " + incomplete + " on indexes " + indexes + "."));
        }
    }

    /**
     *  In case of full rebuild, stop the index build if any index is dropped.
     *  Otherwise, skip dropped indexes to avoid exception during repair/streaming.
     */
    private Set<StorageAttachedIndex> validateIndexes(Set<StorageAttachedIndex> indexes, Descriptor descriptor)
    {
        Set<StorageAttachedIndex> existing = new HashSet<>();
        Set<StorageAttachedIndex> dropped = new HashSet<>();

        for (StorageAttachedIndex index : indexes)
        {
            if (group.containsIndex(index))
                existing.add(index);
            else
                dropped.add(index);
        }

        if (!dropped.isEmpty())
        {
            String droppedIndexes = dropped.stream().map(sai -> sai.getIndexContext().getIndexName()).collect(Collectors.toList()).toString();
            if (isFullRebuild)
                throw new RuntimeException(logMessage(String.format("%s are dropped, will stop index build.", droppedIndexes)));
            else
                logger.debug(logMessage("Skip building dropped index {} on sstable {}"), droppedIndexes, descriptor.baseFileUri());
        }

        return existing;
    }
}
