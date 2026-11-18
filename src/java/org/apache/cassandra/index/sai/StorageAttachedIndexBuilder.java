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

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.TimeUUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.ComponentsBuildId;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableWatcher;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.ReadPattern;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

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

    // Held by a build that rebuilds every index of an SSTable (Apache components replaced, or corrupt per-SSTable
    // components rewritten, see withAllIndexes) until it has published them; the other builders of the SSTable wait.
    private static final Map<SSTableReader, CountDownLatch> rebuildingAllIndexes = Maps.newConcurrentMap();

    /**
     * @return whether a build is replacing the components written by Apache Cassandra of the provided SSTable, or
     * rewriting its per-SSTable components after they failed checksum validation: either way, it rebuilds every index
     * of the SSTable, and owns its components until it has published them (see rebuildingAllIndexes)
     */
    static boolean isRebuildingAllIndexesOf(SSTableReader sstable)
    {
        return rebuildingAllIndexes.containsKey(sstable);
    }

    private final StorageAttachedIndexGroup group;
    private final TableMetadata metadata;
    private final Tracker tracker;
    private final TimeUUID compactionId = nextTimeUUID();
    private final boolean isFullRebuild;
    private final boolean isInitialBuild;
    /** Whether the built SSTables are added to the views of their indexes, see {@link #completeSSTable}. */
    private final boolean updateViews;

    // The reserved SSTables whose rebuild status this builder still owns and must release.
    private final Set<SSTableReader> outstandingReservations;

    private final SortedMap<SSTableReader, Set<StorageAttachedIndex>> sstables;

    // Whether the build of the current sstable has registered its components (see completeSSTable).
    private boolean registered;

    private long bytesProcessed = 0;
    private final long totalSizeInBytes;

    StorageAttachedIndexBuilder(StorageAttachedIndexGroup group, SortedMap<SSTableReader, Set<StorageAttachedIndex>> sstables, boolean isFullRebuild, boolean isInitialBuild)
    {
        this(group, sstables, isFullRebuild, isInitialBuild, false);
    }

    StorageAttachedIndexBuilder(StorageAttachedIndexGroup group,
                                SortedMap<SSTableReader, Set<StorageAttachedIndex>> sstables,
                                boolean isFullRebuild,
                                boolean isInitialBuild,
                                boolean ownsStreamRebuildStatus)
    {
        this(group, sstables, isFullRebuild, isInitialBuild, ownsStreamRebuildStatus, true);
    }

    /**
     * @param updateViews whether to add each SSTable to the views of the built indexes once it's built. This must be
     *                    {@code false} for SSTables that aren't live yet, which their indexes pick up when they are
     *                    added to the table: their views must not reference an SSTable that may never be added.
     */
    StorageAttachedIndexBuilder(StorageAttachedIndexGroup group,
                                SortedMap<SSTableReader, Set<StorageAttachedIndex>> sstables,
                                boolean isFullRebuild,
                                boolean isInitialBuild,
                                boolean ownsStreamRebuildStatus,
                                boolean updateViews)
    {
        this.group = group;
        this.metadata = group.metadata();
        this.sstables = sstables;
        this.tracker = group.table().getTracker();
        this.isFullRebuild = isFullRebuild;
        this.isInitialBuild = isInitialBuild;
        this.updateViews = updateViews;
        this.outstandingReservations = ownsStreamRebuildStatus ? new HashSet<>(sstables.keySet()) : Collections.emptySet();
        this.totalSizeInBytes = sstables.keySet().stream().mapToLong(SSTableReader::uncompressedLength).sum();
    }

    @Override
    public void build()
    {
        logger.debug(logMessage(String.format("Starting %s %s index build...",
                                              isInitialBuild ? "initial" : "non-initial",
                                              isFullRebuild ? "full" : "partial")));

        try
        {
            for (Map.Entry<SSTableReader, Set<StorageAttachedIndex>> e : sstables.entrySet())
            {
                SSTableReader sstable = e.getKey();
                Set<StorageAttachedIndex> indexes = e.getValue();

                Set<StorageAttachedIndex> existing = validateIndexes(indexes, sstable.descriptor);
                if (existing.isEmpty())
                {
                    logger.debug(logMessage("{} dropped during index build"), indexes);
                    // Nothing to build for this sstable, so release its reserved status right away.
                    releaseStreamRebuildStatus(sstable);
                    continue;
                }

                if (indexSSTable(sstable, existing))
                    return;

                // This SSTable's index build completed, so release its streaming status immediately, which allows
                // other streaming operations to operate on it.
                releaseStreamRebuildStatus(sstable);
            }
        }
        finally
        {
            // Release the rebuild status for any sstables not already released above.
            releaseStreamRebuildStatus();
        }
    }

    @Override
    public void onNotExecuted()
    {
        // If build() will never run, release here to avoid leaving the reserved sstables stuck in the REBUILDING state.
        releaseStreamRebuildStatus();
    }

    private void releaseStreamRebuildStatus()
    {
        outstandingReservations.forEach(sstable -> sstable.streamRebuildState().endRebuild());
        outstandingReservations.clear();
    }

    private void releaseStreamRebuildStatus(SSTableReader sstable)
    {
        // Only release a status this builder actually still owns, so we never clear a reservation that was
        // released earlier and possibly re-taken by another rebuild or stream.
        if (outstandingReservations.remove(sstable))
            sstable.streamRebuildState().endRebuild();
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
        long startTimeNanos = Clock.Global.nanoTime();

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
        // the indexes we were asked to build, without the ones we also rebuild with them (see withAllIndexes)
        Set<StorageAttachedIndex> requestedIndexes = indexes;
        CountDownLatch replacingApache = null;
        // set when we rewrite per-SSTable components that failed checksum validation (see hasCorruptPerSSTableComponents)
        CountDownLatch rewritingCorrupt = null;
        registered = false;
        boolean succeeded = false;

        try (RandomAccessReader dataFile = sstable.openDataReader(ReadPattern.SEQUENTIAL);
             LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.INDEX_BUILD, sstable))
        {
            while (true)
            {
                if (isStopRequested())
                    throw new CompactionInterruptedException(getProgress(), trigger());

                // Another builder is replacing the Apache Cassandra components of this SSTable, along with every index
                // that uses them (see below). Building any of those indexes at the same time would write the same
                // files, so we wait until it has published its components, and build on its result.
                CountDownLatch replacing = rebuildingAllIndexes.get(sstable);
                if (replacing != null)
                {
                    replacing.await(100, TimeUnit.MILLISECONDS);
                    continue;
                }

                indexDescriptor = group.descriptorFor(sstable);
                boolean corruptPerSSTable = hasCorruptPerSSTableComponents(indexDescriptor);
                perSSTableFileLock = shouldWritePerSSTableFiles(sstable, indexDescriptor, replacedComponents, corruptPerSSTable);
                if (perSSTableFileLock != null && hasApacheComponents(indexDescriptor.perSSTableComponents()))
                {
                    replacingApache = CountDownLatch.newCountDownLatch(1);
                    rebuildingAllIndexes.put(sstable, replacingApache);
                }
                else if (perSSTableFileLock != null && corruptPerSSTable)
                {
                    // Like for the Apache components above, the other builders of the SSTable wait for us, as we rebuild
                    // all of its indexes
                    rewritingCorrupt = CountDownLatch.newCountDownLatch(1);
                    rebuildingAllIndexes.put(sstable, rewritingCorrupt);
                }
                // Either we write the per-SSTable components, or they are ours (complete, or being written by
                // another builder, as usual). Otherwise, another builder has just got the latch to replace the
                // Apache Cassandra components, and is about to say so above.
                if (perSSTableFileLock != null || !hasApacheComponents(indexDescriptor.perSSTableComponents()))
                    break;
                Thread.sleep(10);
            }
            if (replacingApache != null)
            {
                // The per-SSTable components were written by Apache Cassandra, and we're replacing them with ours, so
                // no index can use its current components on this sstable afterwards: rebuild all of them. The Apache
                // files are removed once the build is complete (see StorageAttachedIndexGroup#removeApacheComponents).
                indexes = withAllIndexes(indexes, indexDescriptor, "Apache Cassandra components are replaced");
                Set<StorageAttachedIndex> others = Sets.difference(indexes, requestedIndexes);
                if (!others.isEmpty())
                    SSTableWatcher.instance.onIndexBuild(sstable, others);
            }
            else if (rewritingCorrupt != null)
            {
                // The per-SSTable components failed checksum validation, and every index of the SSTable uses them:
                // rebuild all of them on the rewritten ones. They were all reading corrupt components, so they are all
                // treated as requested: a failure of the build makes them all non-queryable.
                indexes = withAllIndexes(indexes, indexDescriptor,
                                         "per-SSTable components are rewritten after failing checksum validation");
                Set<StorageAttachedIndex> others = Sets.difference(indexes, requestedIndexes);
                if (!others.isEmpty())
                    SSTableWatcher.instance.onIndexBuild(sstable, others);
                requestedIndexes = indexes;
            }

            // If we were unable to get the per-SSTable file lock it means that the
            // per-SSTable components are already being built so we only want to
            // build the per-index components
            boolean perIndexComponentsOnly = perSSTableFileLock == null;
            for (StorageAttachedIndex index : indexes)
            {
                prepareForRebuild(indexDescriptor.perIndexComponents(index.getIndexContext()), replacedComponents);
                deleteLeftoversUnderNewBuildNames(indexDescriptor, index.getIndexContext());
            }

            long keyCount = SSTableReader.getApproximateKeyCount(Set.of(sstable));
            // If the build fails, only the indexes we were asked to build become non-queryable: the others keep using
            // the Apache Cassandra components, which are only removed once the build has succeeded.
            // None does for an SSTable that isn't live yet: its failed build fails the operation that was going to
            // add it (a stream or an import), and the indexes, which don't serve it, are fine. That includes the
            // failures the writer handles itself (adding a row, completing a segment), which abort it as failures
            // of the index.
            indexWriter = new StorageAttachedIndexWriter(indexDescriptor, metadata, indexes,
                                                         updateViews ? requestedIndexes : Collections.emptySet(),
                                                         txn, keyCount, perIndexComponentsOnly, group.table().metric);

            indexWriter.begin();

            long previousBytesRead = 0;

            try (KeyIterator keys = sstable.keyIterator())
            {
                while (keys.hasNext())
                {
                    if (isStopRequested())
                    {
                        logger.debug(indexDescriptor.logMessage("Index build has been stopped. Reason: {}"), trigger());
                        throw new CompactionInterruptedException(getProgress(), trigger());
                    }

                    DecoratedKey key = keys.next();
                    long position = sstable.getPosition(key, SSTableReader.Operator.EQ);

                    indexWriter.startPartition(key, position, position);

                    dataFile.seek(position);
                    ByteBufferUtil.readWithShortLength(dataFile); // key

                    try (SSTableIdentityIterator partition = SSTableIdentityIterator.create(sstable, dataFile, key))
                    {
                        // if the row has statics attached, it has to be indexed separately
                        if (metadata.hasStaticColumns())
                            indexWriter.nextUnfilteredCluster(partition.staticRow());

                        while (partition.hasNext())
                            indexWriter.nextUnfilteredCluster(partition.next());
                    }
                    long bytesRead = keys.getBytesRead();
                    bytesProcessed += bytesRead - previousBytesRead;
                    previousBytesRead = bytesRead;
                }

                completeSSTable(txn, indexWriter, indexDescriptor, sstable, requestedIndexes, indexes, perSSTableFileLock, replacedComponents);
            }
            long timeTaken = Clock.Global.nanoTime() - startTimeNanos;
            group.table().metric.updateStorageAttachedIndexBuildTime(timeTaken);
            logger.trace("Completed indexing sstable {} in {} seconds", sstable.descriptor, TimeUnit.NANOSECONDS.toSeconds(timeTaken));

            succeeded = true;
            return false;
        }
        catch (Throwable t)
        {
            if (indexWriter != null)
            {
                // The index is only affected if the SSTable is live: for one that isn't yet, the failure fails the
                // operation that was going to add it (a stream or import), and the index is fine without it. The
                // writer then neither makes the index non-queryable nor rethrows, and the failure is handled below.
                indexWriter.abort(t, updateViews);
            }

            if (t instanceof InterruptedException)
            {
                logger.warn(logMessage("Interrupted while building indexes {} on SSTable {}"), indexes, sstable.descriptor);
                Thread.currentThread().interrupt();
                return true;
            }
            else if (t instanceof CompactionInterruptedException)
            {
                //TODO Shouldn't do this if the stop was interrupted by a truncate
                if (isInitialBuild)
                {
                    logger.error(logMessage("Stop requested while building initial indexes {} on SSTable {}. {}"), indexes, sstable.descriptor, t.getMessage());
                    throw Throwables.unchecked(t);
                }
                else
                {
                    logger.info(logMessage("Stop requested while building indexes {} on SSTable {}. {}"), indexes, sstable.descriptor, t.getMessage());
                    // A stop fails the build of SSTables that aren't live yet, and so the operation that was going to
                    // add them, which can't add them without their indexes.
                    if (!updateViews)
                        throw Throwables.unchecked(t);
                    return true;
                }
            }
            else
            {
                logger.error(logMessage("Unable to build indexes {} on SSTable {}. Cause: {}."), indexes, sstable, t.getMessage());
                throw Throwables.unchecked(t);
            }
        }
        finally
        {
            // A build that replaces the Apache Cassandra components and fails before registering ours leaves them as
            // they were, so that their indexes keep using them, and the next build replaces them all again.
            if (replacingApache != null && !succeeded && !registered)
                restoreApacheComponents(sstable, indexDescriptor, indexes);
            ref.release();
            // release current lock in case of error
            if (perSSTableFileLock != null)
            {
                inProgress.remove(sstable);
                perSSTableFileLock.decrement();
            }
            if (replacingApache != null)
            {
                rebuildingAllIndexes.remove(sstable);
                replacingApache.decrement();
            }
            if (rewritingCorrupt != null)
            {
                rebuildingAllIndexes.remove(sstable);
                rewritingCorrupt.decrement();
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
    private CountDownLatch shouldWritePerSSTableFiles(SSTableReader sstable, IndexDescriptor indexDescriptor,
                                                      Set<Component> replacedComponents, boolean corrupt)
    {
        // if per-table files are incomplete or checksum failed during full rebuild, or if they were written by Apache
        // Cassandra (we never write new components in that format, see IndexDescriptor#versionForNewComponents), or if
        // they failed checksum validation during a partial build (see hasCorruptPerSSTableComponents).
        if (!indexDescriptor.perSSTableComponents().isComplete() || isFullRebuild
            || hasApacheComponents(indexDescriptor.perSSTableComponents()) || corrupt)
        {
            CountDownLatch latch = CountDownLatch.newCountDownLatch(1);
            if (inProgress.putIfAbsent(sstable, latch) == null)
            {
                prepareForRebuild(indexDescriptor.perSSTableComponents(), replacedComponents);
                return latch;
            }
        }
        return null;
    }

    /**
     * Whether our complete per-SSTable components, which a partial build would reuse, fail checksum validation, when
     * {@link CassandraRelevantProperties#SAI_VALIDATE_PER_SSTABLE_CHECKSUM_ON_PARTIAL_BUILD} is set (the default). Not
     * for a full rebuild, which rewrites them anyway.
     */
    private boolean hasCorruptPerSSTableComponents(IndexDescriptor indexDescriptor)
    {
        if (isFullRebuild || !CassandraRelevantProperties.SAI_VALIDATE_PER_SSTABLE_CHECKSUM_ON_PARTIAL_BUILD.getBoolean())
            return false;

        IndexComponents.ForRead perSSTable = indexDescriptor.perSSTableComponents();
        if (!perSSTable.isComplete() || perSSTable.version().isApache() || perSSTable.isValid(true))
            return false;

        logger.warn(indexDescriptor.logMessage("Per-SSTable index components failed checksum validation: rewriting them, " +
                                               "and rebuilding every index of the SSTable"));
        return true;
    }

    /**
     * Without immutable components, the new build of an index writes the files of generation 0, and most of them are
     * opened to append. Deletes any file there may be under those names, of any component type, so that the build doesn't
     * append to the files of another build (e.g. left by a crash, or by a dropped index of the same name).
     */
    private static void deleteLeftoversUnderNewBuildNames(IndexDescriptor indexDescriptor, IndexContext context)
    {
        Version version = indexDescriptor.versionForNewComponents();
        if (version.useImmutableComponentFiles())
            return;
        indexDescriptor.perIndexComponentsOfBuild(context, ComponentsBuildId.forNewSSTable(version), EnumSet.allOf(IndexComponentType.class))
                       .forceDeleteAllComponents();
    }

    private static void prepareForRebuild(IndexComponents.ForRead components, Set<Component> replacedComponents)
    {
        // Apache Cassandra's components are never overwritten (we write other file names), and they are streamable,
        // so they stay in the TOC and on disk until the build is complete, and StorageAttachedIndexGroup#removeApacheComponents removes them.
        if (components.version().isApache())
            return;

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
                                 Set<StorageAttachedIndex> requestedIndexes,
                                 Set<StorageAttachedIndex> indexes,
                                 CountDownLatch latch,
                                 Set<Component> replacedComponents) throws InterruptedException
    {
        indexWriter.complete(sstable);

        if (latch != null)
        {
            // current builder owns the lock
            latch.decrement();
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

        // A full rebuild stops if one of the indexes it was asked to build was dropped (see validateIndexes), but the
        // indexes we also built because they used the Apache Cassandra components we replace (see withAllIndexes)
        // can be dropped meanwhile: we just drop what we built for them.
        Set<StorageAttachedIndex> existing = validateIndexes(requestedIndexes, sstable.descriptor);
        for (StorageAttachedIndex index : indexes)
        {
            if (requestedIndexes.contains(index))
                continue;
            if (group.containsIndex(index))
                existing.add(index);
            else
                indexDescriptor.perIndexComponents(index.getIndexContext()).forWrite().forceDeleteAllComponents();
        }
        if (existing.isEmpty())
        {
            logger.debug(logMessage("{} dropped during index build"), indexes);
            return;
        }

        // Register components from the descriptor that owns the writers. The context cache may have been
        // evicted before this point, and a reader lookup would reconstruct a descriptor from the pre-build TOC.
        // The size of an SSTable that isn't live yet isn't tracked: it's accounted for, components included, when the
        // SSTable is added to the table.
        Tracker componentsTracker = updateViews ? tracker : null;
        sstable.registerComponents(group.activeComponents(indexDescriptor), componentsTracker);
        registered = true;
        Set<StorageAttachedIndex> incomplete;
        try
        {
            if (!replacedComponents.isEmpty())
                sstable.unregisterComponents(replacedComponents, componentsTracker);
            // Our components replace any Apache Cassandra ones now (see withAllIndexes).
            group.removeApacheComponents(sstable, outstandingReservations.contains(sstable), componentsTracker);

            /**
             * During memtable flush, it completes the transaction first which opens the flushed sstable,
             * and then notify the new sstable to SAI. Here we should do the same.
             */
            txn.trackNewAttachedIndexFiles(sstable);
            // there is nothing to commit. Close() effectively abort the transaction.
            txn.close();

            // A streamed or imported SSTable that isn't live yet joins the index views only once it is added to the table
            if (!tracker.getView().liveSSTables().contains(sstable))
                return;

            incomplete = group.onSSTableChanged(Collections.emptyList(), Collections.singleton(sstable), existing, false);
        }
        catch (Throwable t)
        {
            // Our components are registered, and may have replaced Apache Cassandra ones that the other indexes we
            // built used, so none of them can be trusted anymore. That's only for a live SSTable: the indexes don't use
            // one that isn't live yet, whose failure fails the operation that was going to add it.
            if (updateViews)
                existing.forEach(StorageAttachedIndex::makeIndexNonQueryable);
            throw t;
        }
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
     * Deletes what a build replacing the Apache Cassandra components of the SSTable wrote before it failed to register
     * ours, and reloads the descriptor from the TOC, so the indexes keep using the Apache components until the next
     * build replaces them all.
     */
    private void restoreApacheComponents(SSTableReader sstable, IndexDescriptor descriptor, Set<StorageAttachedIndex> indexes)
    {
        try
        {
            IndexComponents.ForRead perSSTable = descriptor.perSSTableComponents();
            if (!perSSTable.version().isApache())
                perSSTable.forWrite().forceDeleteAllComponents();
            for (StorageAttachedIndex index : indexes)
            {
                IndexComponents.ForRead perIndex = descriptor.perIndexComponents(index.getIndexContext());
                if (!perIndex.version().isApache())
                    perIndex.forWrite().forceDeleteAllComponents();
            }
            descriptor.reload(sstable, group.getIndexes().stream().map(StorageAttachedIndex::getIndexContext).collect(Collectors.toSet()));
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.warn(logMessage("Could not restore the Apache Cassandra SAI components of {} after a failed build"), sstable.descriptor, t);
        }
    }

    private static boolean hasApacheComponents(IndexComponents.ForRead components)
    {
        return components.isComplete() && components.version().isApache();
    }

    /**
     * @return the provided indexes, plus all the other indexes of the group, to build on the sstable of the provided
     * descriptor, whose per-SSTable components are replaced: no index can use its current components on it afterwards
     */
    private Set<StorageAttachedIndex> withAllIndexes(Set<StorageAttachedIndex> indexes, IndexDescriptor indexDescriptor,
                                                     String reason)
    {
        Set<StorageAttachedIndex> withAllIndexes = new HashSet<>(indexes);
        for (StorageAttachedIndex index : group.getIndexes())
        {
            if (withAllIndexes.add(index))
                logger.info(logMessage("Also rebuilding index {} on {}, whose {}"),
                            index.getIndexContext().getIndexName(), indexDescriptor.descriptor, reason);
        }
        return withAllIndexes;
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
