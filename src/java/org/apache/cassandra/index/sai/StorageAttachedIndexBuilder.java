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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.ComponentsBuildId;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.OnDiskFormat;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
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
import org.apache.cassandra.schema.DroppedColumn;
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

    /**
     * Every build of an SSTable holds its lock for the length of the build (see {@link #lockForBuild}): shared, except
     * a full rebuild, which holds it exclusively. Such a rebuild may have to rebuild every index of the SSTable (see
     * {@link #numbersEmptyStaticRows}), including one that another builder is building, and it decides from components
     * that a concurrent build may rely on.
     */
    private static final Map<SSTableReader, BuildLock> buildLocks = Maps.newConcurrentMap();

    private static final class BuildLock
    {
        // Fair, so that a waiting exclusive build isn't overtaken by new shared builds. A waiter gives up its place in
        // the queue every second, though, to look for a stop request (see lockForBuild), and joins it again at the
        // end: shared builds that came after it may then go first. That only delays it (each of them holds the lock
        // for one SSTable), it doesn't change what any build does.
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
        private int users;
    }

    private static final String REBUILD_ALL_INDEXES = "Rebuild all the SAI indexes of the table in one command, " +
                                                      "nodetool rebuild_index <keyspace> <table> <index> [<index> ...], " +
                                                      "which rewrites those components.";

    /**
     * Number the empty static row of every partition without a static row, as the builds of earlier releases did, to
     * write per-SSTable components like theirs in tests.
     */
    @VisibleForTesting
    public static volatile boolean NUMBER_EMPTY_STATIC_ROWS = false;

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

        Set<Component> replacedComponents = new HashSet<>();
        // the indexes we were asked to build, without the ones we also rebuild with them (see the numbering of the rows
        // below)
        Set<StorageAttachedIndex> requestedIndexes = indexes;
        // whether the per-SSTable components are lost to the indexes that use them (the build deletes them, or found them
        // unreadable or missing), and the indexes whose own components the build deletes: if it fails before the writer
        // exists, those of the indexes it builds that lost files become non-queryable (the writer's abort makes them all
        // non-queryable afterwards)
        boolean lostPerSSTable = false;
        Set<StorageAttachedIndex> lostPerIndex = new HashSet<>();
        Lock buildLock = null;
        Map<StorageAttachedIndex, PerIndexFilesOwner> owners = new HashMap<>();

        try
        {
            // A full rebuild replaces the per-SSTable components, and may then rebuild every index of the SSTable (see
            // below): it waits for the other builds of the SSTable, and they wait for it. (Whether the components are
            // complete can't decide the mode: another build may complete them between that look and the lock.)
            // The lock comes first, so that a builder waiting for it holds nothing else of the SSTable.
            buildLock = lockForBuild(sstable, isFullRebuild);
            ownFiles(sstable, indexes, owners);
            if (indexes.isEmpty())
                return false;

            try (RandomAccessReader dataFile = sstable.openDataReader(ReadPattern.SEQUENTIAL);
                 LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.INDEX_BUILD, tracker.metadata))
            {
                IndexDescriptor indexDescriptor = group.descriptorFor(sstable);

                // The per-SSTable components number the rows of the SSTable for all its indexes. Flushes, compactions
                // and builds give no row ID to the empty static row of a partition without a static row, but the builds
                // of earlier releases did. Find out how the existing components number them, before a full rebuild
                // deletes them (below, once it holds the latch).
                IndexComponents.ForRead perSSTable = indexDescriptor.perSSTableComponents();
                boolean numberEmptyStaticRows = NUMBER_EMPTY_STATIC_ROWS;
                boolean existingComponents = !inProgress.containsKey(sstable) && perSSTable.isComplete();
                boolean unreadableComponents = false;
                long existingRows = -1;
                if (existingComponents)
                {
                    try
                    {
                        numberEmptyStaticRows |= numbersEmptyStaticRows(sstable, dataFile, perSSTable);
                        existingRows = rowCount(perSSTable, sstable);
                    }
                    catch (IOException | RuntimeException e)
                    {
                        // A full rebuild replaces these components, and is how damaged ones are repaired: it must not
                        // fail because it can't read them. Their numbering is unknown, so every index is rebuilt.
                        if (!isFullRebuild || e instanceof CompactionInterruptedException)
                            throw e;
                        logger.warn(indexDescriptor.logMessage("Can't read the per-SSTable index components " +
                                                               "of {}, which this rebuild replaces: {}"),
                                    sstable.descriptor, e.toString());
                        unreadableComponents = true;
                        numberEmptyStaticRows = NUMBER_EMPTY_STATIC_ROWS;
                        existingRows = -1;
                    }
                }
                // Rewriting the existing per-SSTable components may also change the numbering if the build can't read
                // some of the rows they number: rows whose only cells belong to a column dropped since they were
                // written.
                boolean numberingMayChange = existingComponents && (numberEmptyStaticRows || unreadableComponents || mayHaveDroppedColumnCells(sstable));
                // A full rebuild that finds no complete per-SSTable components (a failed rebuild deleted them, or they
                // were invalidated) can't tell how they numbered the rows. The other indexes that still have components
                // on the SSTable were built on that numbering, so they are rebuilt too.
                boolean orphanedComponents = isFullRebuild && !existingComponents && hasComponentsOfOtherIndexes(indexDescriptor, indexes);
                if (orphanedComponents)
                    numberingMayChange = true;

                // the complete per-index components of the other indexes before we try to take the per-SSTable latch
                Map<StorageAttachedIndex, IndexComponents.ForRead> othersBeforeLatch = completeComponentsOfOtherIndexes(indexDescriptor, indexes);
                perSSTableFileLock = shouldWritePerSSTableFiles(sstable, indexDescriptor);

                // If we were unable to get the per-SSTable file lock it means that the
                // per-SSTable components are already being built so we only want to
                // build the per-index components
                boolean perIndexComponentsOnly = perSSTableFileLock == null;

                // whether every index of the SSTable is rebuilt
                boolean widened = false;
                if (isFullRebuild && numberingMayChange && !perIndexComponentsOnly && !NUMBER_EMPTY_STATIC_ROWS)
                {
                    // We rewrite per-SSTable components (a full rebuild) whose numbering ours may not match,
                    // so the row IDs of every other index of the SSTable could point to the wrong rows: rebuild all of
                    // them along with the requested ones. No other build of the SSTable runs meanwhile (buildLock). Their
                    // per-index components are replaced, so they become non-queryable with the requested ones if the
                    // build fails.
                    widened = true;
                    String reason = orphanedComponents ? "the components they replace are missing"
                                                       : unreadableComponents ? "the components they replace can't be read"
                                                       : numberEmptyStaticRows ? "they number the empty static rows of partitions"
                                                                               : "a column was dropped since it was written";
                    numberEmptyStaticRows = false;
                    indexes = new HashSet<>(indexes);
                    indexes.addAll(group.getIndexes());
                    Set<StorageAttachedIndex> others = Sets.difference(indexes, requestedIndexes);
                    if (!others.isEmpty())
                    {
                        logger.info(indexDescriptor.logMessage("Rewriting the per-SSTable index components of {} may change how they " +
                                                               "number its rows ({}): also rebuilding indexes {} on it"),
                                    sstable.descriptor, reason, others);
                        SSTableWatcher.instance.onIndexBuild(sstable, others);
                    }
                }
                else if (!perIndexComponentsOnly && !isFullRebuild && !NUMBER_EMPTY_STATIC_ROWS)
                {
                    // A partial build that writes the per-SSTable components: they were lost, maybe after we looked at
                    // them. We write them with the usual numbering, whatever the lost ones did, as the builds that wait
                    // for us do, and invalidate the components of the other indexes (below).
                    numberEmptyStaticRows = false;
                    existingRows = -1;
                }
                else if (numberEmptyStaticRows && !NUMBER_EMPTY_STATIC_ROWS)
                {
                    logger.info(indexDescriptor.logMessage("The per-SSTable index components of {} number the empty static rows of " +
                                                           "partitions; building indexes {} the same way"), sstable.descriptor, indexes);
                }
                if (widened)
                    ownFilesOfOtherIndexes(sstable, indexes, owners);
                // We write the per-SSTable components, deleting the current ones unless they are immutable (see
                // prepareForRebuild), once the indexes we rebuild with the requested ones are known
                if (perSSTableFileLock != null)
                {
                    lostPerSSTable = unreadableComponents || orphanedComponents || deletesOnRebuild(perSSTable);
                    prepareForRebuild(perSSTable, replacedComponents);
                }
                // A partial build that writes the per-SSTable components (they were lost) doesn't rebuild the other
                // indexes, whose components were built on the lost ones and may number the rows differently:
                // invalidate those they had before we took the latch (a build that has seen our latch builds on
                // ours). A full rebuild rebuilds them itself (see orphanedComponents).
                if (!perIndexComponentsOnly && !widened && !isFullRebuild && !NUMBER_EMPTY_STATIC_ROWS)
                    invalidateComponentsOfOtherIndexes(indexDescriptor, sstable, indexes, othersBeforeLatch);

                for (StorageAttachedIndex index : indexes)
                {
                    // recorded before the components are unlisted or deleted, which may fail part way
                    Set<Component> listed = listedComponentsOfName(sstable, index.getIndexContext());
                    IndexComponents.ForRead components = indexDescriptor.perIndexComponents(index.getIndexContext());
                    if (unlistsBeforeBuild(indexDescriptor, listed) || deletesOnRebuild(components))
                        lostPerIndex.add(index);
                    replaceListedComponents(sstable, indexDescriptor, listed, replacedComponents);
                    prepareForRebuild(components, replacedComponents);
                    deleteLeftoversUnderNewBuildNames(indexDescriptor, index.getIndexContext());
                }

                long keyCount = SSTableReader.getApproximateKeyCount(Set.of(sstable));
                indexWriter = new StorageAttachedIndexWriter(indexDescriptor, metadata, indexes, txn, keyCount, perIndexComponentsOnly, group.table().metric);

                indexWriter.begin();
                indexPartitions(sstable, dataFile, indexDescriptor, indexWriter, numberEmptyStaticRows);

                // Safety net: we replaced complete per-SSTable components without rebuilding every index of the
                // SSTable, because nothing said their numbering could change. If we numbered another number of rows
                // than they did, it changed all the same, and the indexes we didn't rebuild would point to the wrong
                // rows.
                if (!perIndexComponentsOnly && !widened && existingRows >= 0 && existingRows != indexWriter.rowCount()
                    && !NUMBER_EMPTY_STATIC_ROWS && !indexes.containsAll(group.getIndexes()))
                {
                    String message = String.format("Numbered %d rows of SSTable %s, but the per-SSTable index " +
                                                   "components this build replaces had %d. %s",
                                                   indexWriter.rowCount(), sstable.descriptor, existingRows, REBUILD_ALL_INDEXES);
                    // If the components we replace are gone already (they are deleted before the build, unless
                    // components are immutable), the components of the indexes we didn't rebuild are all that is left
                    // of their numbering: a later build (a restart, a rebuild of one index) would write per-SSTable
                    // components that number the rows differently, and those indexes would load with them. Don't leave
                    // them behind.
                    if (!indexDescriptor.perSSTableComponents().version().useImmutableComponentFiles())
                        invalidateComponentsOfOtherIndexes(indexDescriptor, sstable, indexes, null);
                    throw new IllegalStateException(indexDescriptor.logMessage(message));
                }

                completeSSTable(txn, indexWriter, indexDescriptor, sstable, requestedIndexes, indexes, perSSTableFileLock, replacedComponents);
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
                try
                {
                    indexWriter.abort(t, !truncated);
                }
                finally
                {
                    // once the abort has deleted what the writer wrote
                    if (!truncated)
                        removeFromViews(sstable, indexes, t);
                }
            }
            else
            {
                Set<StorageAttachedIndex> lostFiles = new HashSet<>();
                for (StorageAttachedIndex index : indexes)
                {
                    if (lostPerSSTable || lostPerIndex.contains(index))
                        lostFiles.add(index);
                }
                removeFromViews(sstable, makeNonQueryable(lostFiles, t), t);
            }

            if (t instanceof InterruptedException)
            {
                // TODO: Is there anything that makes more sense than just restoring the interrupt?
                logger.warn(logMessage("Interrupted while building indexes {} on SSTable {}"), indexes, sstable.descriptor);
                Thread.currentThread().interrupt();
                // The writer's abort makes the indexes non-queryable. Without a writer yet (e.g. interrupted while
                // waiting for the other builds of the SSTable), returning would end the build as a success.
                if (indexWriter == null)
                    throw Throwables.unchecked(t);
                return true;
            }
            else if (t instanceof CompactionInterruptedException)
            {
                if (isInitialBuild && trigger() != TRUNCATE)
                {
                    logger.error(logMessage("Stop requested while building initial indexes {} on SSTable {}. {}"), indexes, sstable.descriptor, t.getMessage());
                    throw Throwables.unchecked(t);
                }
                else
                {
                    logger.info(logMessage("Stop requested while building indexes {} on SSTable {}. {}"), indexes, sstable.descriptor, t.getMessage());
                    // A stop before the writer exists (while waiting for the other builds of the SSTable, or for a
                    // dropped index of the same name, see ownFiles) fails the build, unless a truncate removed the
                    // SSTable.
                    if (indexWriter == null && !removedByTruncate())
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
            ref.release();
            // release current lock in case of error
            if (perSSTableFileLock != null)
            {
                inProgress.remove(sstable, perSSTableFileLock);
                perSSTableFileLock.countDown();
            }
            if (buildLock != null)
                unlockForBuild(sstable, buildLock);
            owners.values().forEach(PerIndexFilesOwner::close);
        }
    }

    /** @return the provided indexes that this has made non-queryable */
    private static Set<StorageAttachedIndex> makeNonQueryable(Collection<StorageAttachedIndex> indexes, Throwable failure)
    {
        Set<StorageAttachedIndex> nonQueryable = new HashSet<>();
        for (StorageAttachedIndex index : indexes)
        {
            try
            {
                index.makeIndexNonQueryable();
                nonQueryable.add(index);
            }
            catch (Throwable e)
            {
                failure.addSuppressed(e);
            }
        }
        return nonQueryable;
    }

    /**
     * Removes the SSTable from the views of the provided indexes, which its failed build has made non-queryable, as it
     * may have deleted their files on it, and makes them non-queryable again: a build of one of them that looks at
     * its view after the removal builds the SSTable, and one that looked before (and so forgot that the index was made
     * non-queryable, see StorageAttachedIndex#buildSSTablesMissedByInitialBuild) fails, or leaves the index
     * non-queryable if it is over. Not for an index dropped meanwhile, whose drop cleans up the files of the SSTables
     * in its view.
     */
    private void removeFromViews(SSTableReader sstable, Collection<StorageAttachedIndex> indexes, Throwable failure)
    {
        for (StorageAttachedIndex index : indexes)
        {
            if (!group.containsIndex(index))
                continue;
            try
            {
                index.getIndexContext().prepareSSTablesForRebuild(Collections.singleton(sstable));
                index.makeIndexNonQueryable();
            }
            catch (Throwable e)
            {
                failure.addSuppressed(e);
            }
        }
    }

    /**
     * Takes, for each of the provided indexes, the ownership of its per-index files on the SSTable, once no build or
     * writer of a dropped index of the same name owns them, before any of them is deleted or written. The indexes
     * dropped by then are removed from the provided ones; a full rebuild stops instead.
     */
    private void ownFiles(SSTableReader sstable, Set<StorageAttachedIndex> indexes, Map<StorageAttachedIndex, PerIndexFilesOwner> owners)
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
                    owners.put(index, owner);
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
     * Takes, without waiting, the ownership of the files of the indexes this build also rebuilds (see the numbering of
     * the rows in {@link #indexSSTable}), as it holds what other builds of the SSTable may wait for. It leaves out an
     * index that is dropped by then, and one that a build or writer of a dropped index of the same name still owns the
     * files of: that is a new index, whose files on the SSTable were all discarded when it was created, and whose own
     * build, which waits for that owner, builds the SSTable on the components this build writes.
     */
    private void ownFilesOfOtherIndexes(SSTableReader sstable, Set<StorageAttachedIndex> indexes, Map<StorageAttachedIndex, PerIndexFilesOwner> owners)
    {
        synchronized (StorageAttachedIndex.SAME_NAME_FILES_LOCK)
        {
            for (StorageAttachedIndex index : Set.copyOf(indexes))
            {
                if (owners.containsKey(index))
                    continue;
                String name = index.getIndexContext().getIndexName();
                PerIndexFilesOwner owner = PerIndexFilesOwner.hasOwnerOfDroppedIndex(sstable.descriptor, name)
                                           ? null
                                           : PerIndexFilesOwner.tryOwn(sstable.descriptor, name, index.isDropped());
                if (owner == null)
                {
                    logger.debug(logMessage("Not rebuilding index {} on SSTable {}: it is dropped, or its own build builds it"),
                                 name, sstable.descriptor);
                    indexes.remove(index);
                }
                else
                {
                    owners.put(index, owner);
                }
            }
        }
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

    /**
     * Takes the lock of the given SSTable for a build of it (see {@link #buildLocks}). Waiting for it stops if the build
     * is stopped (nodetool stop), with a {@link CompactionInterruptedException}, or interrupted.
     *
     * @param exclusive whether the build replaces the per-SSTable components of the SSTable (a full rebuild)
     */
    private Lock lockForBuild(SSTableReader sstable, boolean exclusive) throws InterruptedException
    {
        BuildLock buildLock = buildLocks.compute(sstable, (s, existing) -> {
            BuildLock l = existing == null ? new BuildLock() : existing;
            l.users++;
            return l;
        });
        Lock lock = exclusive ? buildLock.lock.writeLock() : buildLock.lock.readLock();
        try
        {
            while (!lock.tryLock(1, TimeUnit.SECONDS))
            {
                if (isStopRequested())
                {
                    logger.debug(logMessage("Index build has been stopped while waiting for the other builds of {}. " +
                                            "Reason: {}"),
                                 sstable.descriptor, trigger());
                    throw new CompactionInterruptedException(getProgress(), trigger());
                }
            }
            return lock;
        }
        catch (Throwable t)
        {
            releaseBuildLock(sstable);
            throw t;
        }
    }

    private static void unlockForBuild(SSTableReader sstable, Lock lock)
    {
        lock.unlock();
        releaseBuildLock(sstable);
    }

    private static void releaseBuildLock(SSTableReader sstable)
    {
        buildLocks.computeIfPresent(sstable, (s, l) -> --l.users == 0 ? null : l);
    }

    /**
     * Invalidates the complete components that the indexes of the table that aren't in the given ones have for the
     * sstable, and makes those indexes non-queryable: they have to be built again on this sstable.
     *
     * @param only if not null, only the components it has for an index, if they are still those of the index
     */
    private void invalidateComponentsOfOtherIndexes(IndexDescriptor indexDescriptor, SSTableReader sstable, Set<StorageAttachedIndex> indexes,
                                                    @Nullable Map<StorageAttachedIndex, IndexComponents.ForRead> only)
    {
        for (StorageAttachedIndex index : group.getIndexes())
        {
            if (indexes.contains(index))
                continue;
            IndexComponents.ForRead components = indexDescriptor.perIndexComponents(index.getIndexContext());
            // Components that aren't complete can't be loaded, whatever they were built on.
            if (!components.isComplete())
                continue;
            // Another build has replaced them with its own since.
            if (only != null && only.get(index) != components)
                continue;
            logger.warn(indexDescriptor.logMessage("Invalidating the components of index {} on {}: the per-SSTable " +
                                                   "components they were built on are gone"),
                        index.getIndexContext().getIndexName(), sstable.descriptor);
            components.invalidate(sstable, tracker);
            index.makeIndexNonQueryable();
            index.getIndexContext().prepareSSTablesForRebuild(Collections.singleton(sstable));
            // as for a failed build, see removeFromViews
            index.makeIndexNonQueryable();
        }
    }

    /**
     * @return the complete per-index components that the indexes of the table that aren't in the given ones have for
     * the sstable of the provided descriptor
     */
    private Map<StorageAttachedIndex, IndexComponents.ForRead> completeComponentsOfOtherIndexes(
        IndexDescriptor indexDescriptor, Set<StorageAttachedIndex> indexes)
    {
        Map<StorageAttachedIndex, IndexComponents.ForRead> complete = new HashMap<>();
        for (StorageAttachedIndex index : group.getIndexes())
        {
            IndexComponents.ForRead components = indexDescriptor.perIndexComponents(index.getIndexContext());
            if (!indexes.contains(index) && components.isComplete())
                complete.put(index, components);
        }
        return complete;
    }

    /**
     * @return whether an index of the table that isn't in the given ones has complete components for the sstable
     */
    private boolean hasComponentsOfOtherIndexes(IndexDescriptor indexDescriptor, Set<StorageAttachedIndex> indexes)
    {
        for (StorageAttachedIndex index : group.getIndexes())
            if (!indexes.contains(index) && indexDescriptor.perIndexComponents(index.getIndexContext()).isComplete())
                return true;
        return false;
    }

    /**
     * Passes the rows of the given sstable to the index writer.
     *
     * @param numberEmptyStaticRows whether to give a row ID to the empty static row of a partition without a static
     *                              row, as the builds of earlier releases did
     */
    private void indexPartitions(SSTableReader sstable,
                                 RandomAccessReader dataFile,
                                 IndexDescriptor indexDescriptor,
                                 StorageAttachedIndexWriter indexWriter,
                                 boolean numberEmptyStaticRows) throws IOException
    {
        long previousKeyPosition = 0;

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
                    // If the row has statics attached, it has to be indexed separately. This goes through staticRow,
                    // like a flush or a compaction does, which skips an empty static row (the static row of a
                    // partition without one), unless the build has to number empty static rows like the existing
                    // per-SSTable components do.
                    if (metadata.hasStaticColumns())
                    {
                        if (numberEmptyStaticRows)
                            indexWriter.nextUnfilteredCluster(partition.staticRow(), staticRowPosition);
                        else
                            indexWriter.staticRow(partition.staticRow(), staticRowPosition);
                    }

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
        }
    }

    /**
     * Whether the given complete per-SSTable components give a row ID to the empty static row of the partitions without
     * one, as the builds of earlier releases did. The first partition with an empty static row decides: on row-aware
     * versions, by the key at its first row ID; on AA, by having one more row ID than rows.
     */
    private boolean numbersEmptyStaticRows(SSTableReader sstable,
                                           RandomAccessReader dataFile,
                                           IndexComponents.ForRead components) throws IOException
    {
        // Without static columns, there are no static rows to number. (If every static column of the table was
        // dropped, the build can't read the static rows any more: if the components numbered them, the row count
        // check fails the build.)
        if (!metadata.hasStaticColumns())
            return false;

        OnDiskFormat format = components.onDiskFormat();
        PrimaryKey.Factory keyFactory = format.newPrimaryKeyFactory(metadata.comparator);
        try (PartitionIndexIterator keys = sstable.allKeysIterator();
             PrimaryKeyMap.Factory mapFactory = format.newPrimaryKeyMapFactory(components, keyFactory, sstable);
             PrimaryKeyMap map = mapFactory.newPerSSTablePrimaryKeyMap())
        {
            for (; !keys.isExhausted(); keys.advance())
            {
                if (isStopRequested())
                    throw new CompactionInterruptedException(getProgress(), trigger());

                DecoratedKey key = sstable.decorateKey(keys.key());
                dataFile.seek(sstable.getPosition(key, SSTableReader.Operator.EQ).position);
                ByteBufferUtil.skipShortLength(dataFile); // key
                try (SSTableIdentityIterator partition = SSTableIdentityIterator.create(sstable, dataFile, key))
                {
                    if (!partition.staticRow().isEmpty())
                        continue;

                    // the first row ID of the partition, if it has any
                    long rowId = map.ceiling(keyFactory.createTokenOnly(key.getToken()));
                    while (rowId >= 0 && rowId < map.count() && !map.primaryKeyFromRowId(rowId).partitionKey().equals(key))
                    {
                        if (!map.primaryKeyFromRowId(rowId).token().equals(key.getToken()))
                            return false;
                        rowId++;
                    }
                    if (rowId < 0 || rowId >= map.count())
                        return false;

                    // (the key of a static row read from the components has no clustering)
                    if (format.indexFeatureSet().isRowAware())
                        return !map.primaryKeyFromRowId(rowId).hasClustering();

                    long rowIds = 0;
                    while (rowId + rowIds < map.count() && map.primaryKeyFromRowId(rowId + rowIds).partitionKey().equals(key))
                        rowIds++;
                    long rows = 0;
                    while (partition.hasNext())
                        if (partition.next().isRow())
                            rows++;
                    return rowIds == rows + 1;
                }
            }
        }
        return false;
    }

    /**
     * @return whether the sstable may have cells of a column of the table dropped since: the build can't read them, so
     * it can't number a row that only has such cells, which the per-SSTable components written with the sstable did
     */
    private boolean mayHaveDroppedColumnCells(SSTableReader sstable)
    {
        // once the table has no static column left, the build reads no static row at all, whatever their timestamps
        if (!metadata.hasStaticColumns() && sstable.header.hasStatic())
            return true;
        // a cell is dropped if its timestamp is at most the time of the drop
        long minTimestamp = sstable.getMinTimestamp();
        for (DroppedColumn dropped : metadata.droppedColumns.values())
            if (dropped.droppedTime >= minTimestamp)
                return true;
        return false;
    }

    /**
     * @return the number of rows of the given complete per-SSTable components
     */
    private static long rowCount(IndexComponents.ForRead components, SSTableReader sstable) throws IOException
    {
        OnDiskFormat format = components.onDiskFormat();
        PrimaryKey.Factory keyFactory = format.newPrimaryKeyFactory(sstable.metadata().comparator);
        try (PrimaryKeyMap.Factory factory = format.newPrimaryKeyMapFactory(components, keyFactory, sstable))
        {
            return factory.count();
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
    private CountDownLatch shouldWritePerSSTableFiles(SSTableReader sstable, IndexDescriptor indexDescriptor)
    {
        // if per-table files are incomplete or checksum failed during full rebuild.
        if (!indexDescriptor.perSSTableComponents().isComplete() || isFullRebuild)
        {
            CountDownLatch latch = new CountDownLatch(1);
            if (inProgress.putIfAbsent(sstable, latch) == null)
            {
                try
                {
                    // The builder that held the latch before us may have completed them between our look above and now
                    // (it releases the latch once they are complete, and published): use them, rather than write them
                    // again (and, see indexSSTable, invalidate the components of the indexes that it has just built on
                    // them).
                    if (!isFullRebuild && indexDescriptor.perSSTableComponents().isComplete())
                    {
                        // A builder that has seen our latch meanwhile may be waiting for it.
                        latch.countDown();
                        inProgress.remove(sstable, latch);
                        return null;
                    }
                    // A stop that came while our caller looked at the components (e.g. in numbersEmptyStaticRows) takes
                    // effect before it deletes them
                    if (isStopRequested())
                        throw new CompactionInterruptedException(getProgress(), trigger());
                    return latch;
                }
                catch (Throwable t)
                {
                    // our caller never gets the latch, so it can't release it: do it here, or the next builders of the
                    // SSTable would wait for it forever
                    latch.countDown();
                    inProgress.remove(sstable, latch);
                    throw t;
                }
            }
        }
        return null;
    }

    /**
     * @return the components named after the index that the TOC lists: those of previous builds of the name, possibly
     * of component types the new build doesn't write (e.g. of a dropped index of the same name on a column of another
     * type)
     */
    private static Set<Component> listedComponentsOfName(SSTableReader sstable, IndexContext context)
    {
        Set<Component> listed = new HashSet<>();
        for (Component component : sstable.components())
        {
            Optional<Version.ParsedFileName> parsed = StorageAttachedIndex.tryParseFileName(component.name, context);
            if (parsed.isPresent() && context.getIndexName().equals(parsed.get().indexName))
                listed.add(component);
        }
        return listed;
    }

    /** Whether {@link #replaceListedComponents} unregisters the provided components before the build. */
    private static boolean unlistsBeforeBuild(IndexDescriptor indexDescriptor, Set<Component> listed)
    {
        return !listed.isEmpty() && !indexDescriptor.versionForNewComponents().useImmutableComponentFiles();
    }

    /**
     * The new build replaces the provided listed components of the index's name (see {@link #listedComponentsOfName})
     * all. Without immutable components, their files are deleted before the build, so they are unregistered now;
     * otherwise, once the new build has registered its own.
     */
    private void replaceListedComponents(SSTableReader sstable,
                                         IndexDescriptor indexDescriptor,
                                         Set<Component> listed,
                                         Set<Component> replacedComponents)
    {
        if (unlistsBeforeBuild(indexDescriptor, listed))
            sstable.unregisterComponents(listed, tracker);
        else
            replacedComponents.addAll(listed);
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

        if (deletesOnRebuild(components))
            components.forWrite().forceDeleteAllComponents();
    }

    /** Whether {@link #prepareForRebuild} deletes the files of the provided components. */
    private static boolean deletesOnRebuild(IndexComponents.ForRead components)
    {
        return !components.version().useImmutableComponentFiles();
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
                                 Set<Component> replacedComponents) throws InterruptedException, IOException
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

            // The per-index components we built are only right if we numbered the rows as the per-SSTable components
            // they go with, as they are now (another build may have replaced them meanwhile). If we didn't, fail
            // rather than publish an index that points to the wrong rows.
            IndexComponents.ForRead perSSTableComponents = indexDescriptor.perSSTableComponents();
            if (perSSTableComponents.isComplete())
            {
                long rows = rowCount(perSSTableComponents, sstable);
                if (rows != indexWriter.rowCount())
                {
                    String message = String.format("Numbered %d rows of SSTable %s, but its per-SSTable index components have %d. %s",
                                                   indexWriter.rowCount(), sstable.descriptor, rows, REBUILD_ALL_INDEXES);
                    throw new IllegalStateException(indexDescriptor.logMessage(message));
                }
            }
        }

        // A full rebuild stops if one of the indexes it was asked to build was dropped (see validateIndexes), but the
        // indexes we also built because the numbering of the rows may change can be dropped meanwhile: we just drop
        // what we built for them.
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
