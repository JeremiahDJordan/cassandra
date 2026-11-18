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
package org.apache.cassandra.index.sai;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexerFailures;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.apache.ApacheOnDiskFormat;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.metrics.IndexGroupMetrics;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.index.sai.metrics.TableStateMetrics;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexQueryPlan;
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.SSTableWatcher;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.MemtableDiscardedNotification;
import org.apache.cassandra.notifications.MemtableRenewedNotification;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.lucene.index.CorruptIndexException;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_TABLE_STATE_METRICS_ENABLED;

/**
 * Orchestrates building of storage-attached indices, and manages lifecycle of resources shared between them.
 */
@ThreadSafe
public class StorageAttachedIndexGroup implements Index.Group, INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(StorageAttachedIndexGroup.class);

    public static final Index.Group.Key GROUP_KEY = new Index.Group.Key(StorageAttachedIndexGroup.class);

    /** How often an index that is being built is checked for the end of its build, see {@link #checkAfterBuild}. */
    private static final long CHECK_AFTER_BUILD_INTERVAL_MS = 100;

    private final TableQueryMetrics queryMetrics;
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<TableStateMetrics> stateMetrics;
    private final IndexGroupMetrics groupMetrics;

    private final Set<StorageAttachedIndex> indices = ConcurrentHashMap.newKeySet();
    // the indexes that a full rebuild (or several) is rebuilding, with the number of those rebuilds
    private final Map<StorageAttachedIndex, Integer> fullRebuilds = new ConcurrentHashMap<>();
    private final ColumnFamilyStore baseCfs;

    private final SSTableContextManager contextManager;
    private final Version version;



    StorageAttachedIndexGroup(ColumnFamilyStore baseCfs)
    {
        this.baseCfs = baseCfs;
        this.queryMetrics = new TableQueryMetrics(baseCfs.metadata());
        this.stateMetrics = SAI_TABLE_STATE_METRICS_ENABLED.getBoolean()
                            ? Optional.of(new TableStateMetrics(baseCfs.metadata(), this))
                            : Optional.empty();
        this.groupMetrics = new IndexGroupMetrics(baseCfs.metadata(), this);
        this.contextManager = new SSTableContextManager(baseCfs.getTracker());
        this.version = Version.current(baseCfs.keyspace.getName());

        Tracker tracker = baseCfs.getTracker();
        tracker.subscribe(this);
    }

    @Nullable
    public static StorageAttachedIndexGroup getIndexGroup(ColumnFamilyStore cfs)
    {
        return (StorageAttachedIndexGroup) cfs.indexManager.getIndexGroup(GROUP_KEY);
    }

    @Override
    public Set<StorageAttachedIndex> getIndexes()
    {
        return ImmutableSet.copyOf(indices);
    }

    @Override
    public void addIndex(Index index)
    {
        assert index instanceof StorageAttachedIndex;
        indices.add((StorageAttachedIndex) index);
    }

    @Override
    public void removeIndex(Index index)
    {
        assert index instanceof StorageAttachedIndex;
        boolean removed = indices.remove(index);
        assert removed : "Cannot remove non-existing index " + index;
        /*
         * per index files are dropped via {@link StorageAttachedIndex#getInvalidateTask()}
         */
        if (indices.isEmpty())
        {
            // We unregister the per-sstable components first, then we clear the context, which closes all the contexts
            // and unsure there is not more reference to it. When that's done, we can safely remove the component files
            // on disk. Note that we copy the contexts list because we're going to clear the manager, and we need to
            // make sure this does not clear the `contexts` collection below (since it exists to be used after the clear).
            Collection<SSTableContext> contexts = new ArrayList<>(contextManager.allContexts());
            contexts.forEach(context -> {
                var components = context.usedPerSSTableComponents();
                context.sstable.unregisterComponents(components.allAsCustomComponents(), baseCfs.getTracker());
            });

            contextManager.clear();

            contexts.forEach(context -> {
                SSTableWatcher.instance.onIndexDropped(baseCfs.metadata(), context.usedPerSSTableComponents().forWrite());
            });
        }
    }

    @Override
    public void invalidate()
    {
        // in case of dropping table, sstable contexts should already been removed by SSTableListChangedNotification.
        // in case of removing last index from group,  sstable contexts should already been removed by StorageAttachedIndexGroup#removeIndex
        queryMetrics.release();
        groupMetrics.release();
        stateMetrics.ifPresent(TableStateMetrics::release);
        baseCfs.getTracker().unsubscribe(this);
    }

    @Override
    public void unload()
    {
        baseCfs.getTracker().unsubscribe(this);

        contextManager.clear();
        queryMetrics.release();
        groupMetrics.release();
        stateMetrics.ifPresent(TableStateMetrics::release);
    }

    @Override
    public boolean supportsMultipleContains()
    {
        return true;
    }

    @Override
    public boolean supportsDisjunction()
    {
        return true;
    }

    @Override
    public boolean containsIndex(Index index)
    {
        return index instanceof StorageAttachedIndex && indices.contains(index);
    }

    @Override
    public Index.Indexer indexerFor(Predicate<Index> indexSelector,
                                    DecoratedKey key,
                                    RegularAndStaticColumns columns,
                                    long nowInSec,
                                    WriteContext ctx,
                                    IndexTransaction.Type transactionType,
                                    Memtable memtable)
    {
        final Set<Index.Indexer> indexers =
                indices.stream().filter(indexSelector)
                                .map(i -> i.indexerFor(key, columns, nowInSec, ctx, transactionType, memtable))
                                .filter(Objects::nonNull)
                                .collect(Collectors.toSet());

        // Only a memtable that applies updates atomically, always or on request (which this indexer makes, see
        // requiresAtomicUpdates), delivers events for an update it has already applied. On any other memtable an
        // indexer failure fails the update before it is applied, so stop at the first one.
        final boolean deliverToAll = memtable != null && (memtable.appliesUpdatesAtomically() || memtable.appliesUpdatesAtomicallyOnRequest());

        return indexers.isEmpty() ? null : new StorageAttachedIndex.IndexerAdapter()
        {
            // The memtable indexes must see each applied update once, in memtable order: they remove the old values of
            // a row on update, count the occurrences of a value and keep BM25 document lengths.
            @Override
            public boolean requiresAtomicUpdates()
            {
                return true;
            }

            @Override
            public void insertRow(Row row)
            {
                forEach(indexer -> indexer.insertRow(row));
            }

            @Override
            public void updateRow(Row oldRow, Row newRow)
            {
                forEach(indexer -> indexer.updateRow(oldRow, newRow));
            }

            @Override
            public void removeRow(Row row)
            {
                forEach(indexer -> indexer.removeRow(row));
            }

            @Override
            public void removeRowData(Row oldRowData)
            {
                forEach(indexer -> indexer.removeRowData(oldRowData));
            }

            @Override
            public void partitionDelete(DeletionTime deletionTime)
            {
                forEach(indexer -> indexer.partitionDelete(deletionTime));
            }

            @Override
            public void rangeTombstone(RangeTombstone tombstone)
            {
                forEach(indexer -> indexer.rangeTombstone(tombstone));
            }

            // On a memtable that applies updates atomically, delivers the event to every SAI index, then throws the
            // first failure; otherwise stops at the first failure, which fails the update before it is applied.
            private void forEach(Consumer<Index.Indexer> action)
            {
                if (!deliverToAll)
                {
                    indexers.forEach(action::accept);
                    return;
                }

                IndexerFailures failures = null;
                for (Index.Indexer indexer : indexers)
                {
                    try
                    {
                        action.accept(indexer);
                    }
                    catch (Throwable t)
                    {
                        if (failures == null)
                            failures = new IndexerFailures(baseCfs);
                        failures.add(t);
                    }
                }
                if (failures != null)
                    failures.maybeFail();
            }
        };
    }

    @Override
    public StorageAttachedIndexQueryPlan queryPlanFor(RowFilter rowFilter)
    {
        return StorageAttachedIndexQueryPlan.create(baseCfs, queryMetrics, indices, rowFilter);
    }

    @Override
    public SSTableFlushObserver getFlushObserver(Descriptor descriptor, LifecycleNewTracker tracker, TableMetadata tableMetadata, long keyCount)
    {
        IndexDescriptor indexDescriptor = IndexDescriptor.empty(descriptor, tableMetadata.comparator);
        try
        {
            return new StorageAttachedIndexWriter(indexDescriptor, tableMetadata, indices, tracker, keyCount, baseCfs.metric);
        }
        catch (Throwable t)
        {
            String message = "Unable to create storage-attached index writer on SSTable flush." +
                             " All indexes from this table are going to be marked as non-queryable and will need to be rebuilt.";
            logger.error(indexDescriptor.logMessage(message), t);
            indices.forEach(StorageAttachedIndex::makeIndexNonQueryable);
            return null;
        }
    }

    @Override
    public boolean handles(IndexTransaction.Type type)
    {
        // to skip CleanupGCTransaction and IndexGCTransaction
        return type == IndexTransaction.Type.UPDATE;
    }

    @Override
    public Set<Component> componentsForNewSSTable()
    {
        return IndexDescriptor.componentsForNewlyFlushedSSTable(indices, version, baseCfs.metadata().hasClustering());
    }

    @Override
    public Set<Component> activeComponents(SSTableReader sstable)
    {
        return activeComponents(descriptorFor(sstable));
    }

    /**
     * Returns the active components tracked by the supplied descriptor.
     */
    Set<Component> activeComponents(IndexDescriptor indexDescriptor)
    {
        Set<Component> components = indexDescriptor
                                    .perSSTableComponents()
                                    .all()
                                    .stream()
                                    .map(IndexComponent::asCustomComponent)
                                    .collect(Collectors.toSet());

        for (StorageAttachedIndex index : indices)
        {
            indexDescriptor.perIndexComponents(index.getIndexContext())
                           .all()
                           .stream()
                           .map(IndexComponent::asCustomComponent)
                           .forEach(components::add);
        }

        return components;
    }

    @Override
    public void handleNotification(INotification notification, Object sender)
    {
        // unfortunately, we can only check the type of notification via instanceof :(
        if (notification instanceof SSTableAddedNotification)
        {
            SSTableAddedNotification notice = (SSTableAddedNotification) notification;

            // Avoid validation for index files just written following Memtable flush. ZCS streaming should
            // validate index checksum. Also avoid validation for UNKNOWN operations (imports) as they
            // are already validated in SSTableImporter.
            boolean validate = notice.fromStreaming() || 
                              (!notice.memtable().isPresent() && notice.operationType != OperationType.UNKNOWN);
            onSSTableChanged(Collections.emptySet(), Lists.newArrayList(notice.added), indices, validate);
        }
        else if (notification instanceof SSTableListChangedNotification)
        {
            SSTableListChangedNotification notice = (SSTableListChangedNotification) notification;

            // Avoid validation for index files just written during compaction.
            onSSTableChanged(notice.removed, notice.added, indices, false);
        }
        else if (notification instanceof MemtableRenewedNotification)
        {
            indices.forEach(index -> index.getIndexContext().renewMemtable(((MemtableRenewedNotification) notification).renewed));
        }
        else if (notification instanceof MemtableDiscardedNotification)
        {
            indices.forEach(index -> index.getIndexContext().discardMemtable(((MemtableDiscardedNotification) notification).memtable));
        }
    }

    /**
     * Checks that every index of this group includes the specified SSTables, just added by a stream or import that
     * built the missing indexes first (see {@link #buildMissingSSTableAttachedIndexes}). An index created after that
     * build hands them to its running initial build, which builds them before the index becomes queryable; an index
     * that isn't being built is made non-queryable.
     */
    @Override
    public void checkSSTableAttachedIndexesAfterAdding(Collection<SSTableReader> added)
    {
        for (StorageAttachedIndex index : indices)
        {
            List<SSTableReader> missing = unindexedLiveSSTables(index, added);
            if (missing.isEmpty())
                continue;

            if (baseCfs.indexManager.isIndexBuilding(index.getIndexMetadata().name))
            {
                logger.info(index.getIndexContext().logMessage("SSTables {} were added without this index while it is being built. " +
                                                               "They will be built before it becomes queryable."), descriptors(missing));
                index.buildBeforeQueryable(missing);
                checkAfterBuild(index, missing);
            }
            else
            {
                // The initial build may have built them, and ended, since they were found missing
                missing = unindexedLiveSSTables(index, missing);
                if (!missing.isEmpty())
                    makeNonQueryable(index, missing);
            }
        }
    }

    /**
     * Once the build of the specified index is over, makes it non-queryable if it still doesn't include some of the
     * specified SSTables. That covers SSTables added just after its initial build checked the SSTables it had been
     * handed, which it would otherwise make queryable without them.
     */
    private void checkAfterBuild(StorageAttachedIndex index, List<SSTableReader> sstables)
    {
        ScheduledExecutors.optionalTasks.schedule(() -> {
            if (!indices.contains(index))
                return;

            if (baseCfs.indexManager.isIndexBuilding(index.getIndexMetadata().name))
            {
                checkAfterBuild(index, sstables);
                return;
            }

            index.forgetBuildBeforeQueryable(sstables);
            List<SSTableReader> missing = unindexedLiveSSTables(index, sstables);
            if (!missing.isEmpty())
                makeNonQueryable(index, missing);
        }, CHECK_AFTER_BUILD_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void makeNonQueryable(StorageAttachedIndex index, List<SSTableReader> missing)
    {
        logger.warn(index.getIndexContext().logMessage("SSTables {} were added without this index, " +
                                                       "which won't be able to return their rows until it's rebuilt."), descriptors(missing));
        index.makeIndexNonQueryable();
    }

    /**
     * @return those of the specified SSTables that are live, but not included by the specified index
     */
    List<SSTableReader> unindexedLiveSSTables(StorageAttachedIndex index, Collection<SSTableReader> sstables)
    {
        // Ignore the SSTables that a compaction has already replaced, as their replacements are indexed.
        Set<SSTableReader> live = baseCfs.getTracker().getView().liveSSTables();
        View view = index.getIndexContext().getView();
        return sstables.stream()
                       .filter(sstable -> !sstable.isMarkedCompacted() && live.contains(sstable))
                       .filter(sstable -> !view.containsSSTableIndex(sstable.descriptor))
                       .collect(Collectors.toList());
    }

    private static List<Descriptor> descriptors(Collection<SSTableReader> sstables)
    {
        return sstables.stream().map(sstable -> sstable.descriptor).collect(Collectors.toList());
    }

    /**
     * Builds the indexes of this group that are missing or incomplete on the specified SSTables, which aren't live yet,
     * without adding the SSTables to the views of the indexes: the indexes pick them up when they are added to the
     * table.
     * <p>
     * This repeats until no index is missing, in case an index is created while the missing ones are built. An index
     * that is still incomplete after it's been built (for instance, because its build was stopped) is an error.
     */
    @Override
    public void buildMissingSSTableAttachedIndexes(Collection<SSTableReader> sstables)
    {
        try
        {
            buildMissingIndexes(sstables);
        }
        finally
        {
            // The builds cache the descriptors of the SSTables, which may never be added (e.g. if the stream fails).
            contextManager.forgetUntrackedDescriptors(sstables);
        }
    }

    private void buildMissingIndexes(Collection<SSTableReader> sstables)
    {
        Map<SSTableReader, Set<StorageAttachedIndex>> built = Collections.emptyMap();
        while (true)
        {
            NavigableMap<SSTableReader, Set<StorageAttachedIndex>> missing = missingIndexes(sstables);
            if (missing.isEmpty())
                return;

            for (Map.Entry<SSTableReader, Set<StorageAttachedIndex>> entry : missing.entrySet())
            {
                Set<StorageAttachedIndex> justBuilt = built.getOrDefault(entry.getKey(), Collections.emptySet());
                if (!Collections.disjoint(justBuilt, entry.getValue()))
                    throw new IllegalStateException(String.format("Storage-attached indexes %s are still incomplete " +
                                                                  "on SSTable %s after building them",
                                                                  Sets.intersection(justBuilt, entry.getValue()), entry.getKey().descriptor));
            }

            if (logger.isInfoEnabled())
                logger.info("Building storage-attached indexes missing on {} SSTables of {} before adding them: {}",
                            missing.size(), baseCfs.metadata(),
                            missing.entrySet()
                                   .stream()
                                   .map(e -> e.getKey().descriptor + "=" + e.getValue().stream()
                                                                                .map(i -> i.getIndexContext().getIndexName())
                                                                                .sorted()
                                                                                .collect(Collectors.toList()))
                                   .collect(Collectors.joining(", ")));

            List<Future<?>> futures = new ArrayList<>();
            List<SSTableReader> toBuild = new ArrayList<>(missing.keySet());
            int parallelism = DatabaseDescriptor.getConcurrentCompactors();
            for (List<SSTableReader> group : StorageAttachedIndex.groupBySize(toBuild, parallelism))
            {
                SortedMap<SSTableReader, Set<StorageAttachedIndex>> current = new TreeMap<>(SSTableReader.idComparator);
                group.forEach(sstable -> current.put(sstable, missing.get(sstable)));
                futures.add(CompactionManager.instance.submitIndexBuild(new StorageAttachedIndexBuilder(this, current, false, false, false, false)));
            }
            FBUtilities.waitOnFutures(futures);

            built = missing;
        }
    }

    /**
     * @return for each of the specified SSTables that misses some of the indexes of this group, or has some of them
     * incomplete or written with an obsolete encoding (see {@link IndexDescriptor#hasObsoleteEncoding}), those indexes
     */
    private NavigableMap<SSTableReader, Set<StorageAttachedIndex>> missingIndexes(Collection<SSTableReader> sstables)
    {
        NavigableMap<SSTableReader, Set<StorageAttachedIndex>> missing = new TreeMap<>(SSTableReader.idComparator);
        Set<StorageAttachedIndex> indexes = ImmutableSet.copyOf(indices);
        if (indexes.isEmpty())
            return missing;

        Set<IndexContext> contexts = indexes.stream().map(StorageAttachedIndex::getIndexContext).collect(Collectors.toSet());
        for (SSTableReader sstable : sstables)
        {
            // Load a fresh descriptor, to see the components that are on disk now.
            IndexDescriptor descriptor = IndexDescriptor.load(sstable, contexts);
            boolean perSSTableComplete = descriptor.perSSTableComponents().isComplete();
            for (StorageAttachedIndex index : indexes)
            {
                IndexComponents.ForRead perIndex = descriptor.perIndexComponents(index.getIndexContext());
                if (!perSSTableComplete || !perIndex.isComplete() || IndexDescriptor.hasObsoleteEncoding(perIndex))
                    missing.computeIfAbsent(sstable, k -> new HashSet<>()).add(index);
            }
        }
        return missing;
    }

    void prepareIndexSSTablesForRebuild(Collection<SSTableReader> ss, StorageAttachedIndex index)
    {
        try
        {
            index.getIndexContext().prepareSSTablesForRebuild(ss);
        }
        catch (Throwable t)
        {
            // Mark the index non-queryable, as its view may be compromised.
            index.makeIndexNonQueryable();

            throw Throwables.unchecked(t);
        }
    }

    /**
     * This method is synchronized to avoid concurrent initialization tasks validating same per-SSTable files.
     *
     * @return the set of column indexes that were marked as non-queryable as a result of their per-SSTable index
     * files being corrupt or being unable to successfully update their views
     */
    public synchronized Set<StorageAttachedIndex> onSSTableChanged(Collection<SSTableReader> removed, Iterable<SSTableReader> added,
                                                            Set<StorageAttachedIndex> indexes, boolean validate)
    {
        // An SSTable can still list Apache Cassandra components that our components have replaced, if they couldn't
        // be removed then (e.g. a stream held the SSTable, and we restarted since).
        for (SSTableReader sstable : added)
        {
            if (!apacheComponents(sstable).isEmpty())
                ScheduledExecutors.optionalTasks.execute(() -> removeReplacedApacheComponents(sstable));
        }

        Map<StorageAttachedIndex, Set<SSTableReader>> unloadable = new HashMap<>();
        Optional<Set<SSTableContext>> optValid = contextManager.update(removed, added, validate, indices, unloadable);
        if (optValid.isEmpty())
        {
            // This means at least one sstable had invalid per-sstable components, so mark all indexes non-queryable.
            indices.forEach(StorageAttachedIndex::makeIndexNonQueryable);
            return indexes;
        }

        Set<StorageAttachedIndex> incomplete = new HashSet<>();

        for (StorageAttachedIndex index : indexes)
        {
            Set<SSTableContext> invalid = index.getIndexContext().onSSTableChanged(removed, optValid.get(), validate);

            if (!invalid.isEmpty())
            {
                // Mark the index non-queryable, as its view may be compromised, and incomplete, for our callers.
                index.makeIndexNonQueryable();
                incomplete.add(index);
            }
        }

        // Indexes with complete components on an SSTable that can't be loaded, as its per-SSTable components are
        // incomplete (see SSTableContextManager#update). Those that still have the SSTable in their view (now that the
        // views are updated) go on serving it from the components they have open: that is every other index of the
        // table while a full rebuild of one index rewrites the per-SSTable components, when something loads the
        // SSTables of the table (e.g. the initial build of a new index). Those that don't can't serve it, so they
        // can't stay queryable: their rebuild (rebuild_index, or the initial build at the next start) brings them back.
        for (Map.Entry<StorageAttachedIndex, Set<SSTableReader>> entry : unloadable.entrySet())
        {
            StorageAttachedIndex index = entry.getKey();
            // An index under a full rebuild is out of its own view for the SSTables being rebuilt: the rebuild
            // checks the views once it is over (see StorageAttachedIndexBuilder.FullRebuild). Only while the
            // manager counts the index as building, as a rebuild whose builders never ran never clears its record.
            // Lock order: the manager's monitor inside the group's; the only opposite path is createIndex at
            // startup, which has no full rebuild record, so take no other lock of the manager here.
            if (fullRebuilds.containsKey(index) && baseCfs.indexManager.isIndexBuilding(index.getIndexMetadata().name))
                continue;
            View view = index.getIndexContext().getView();
            List<Descriptor> missing = entry.getValue()
                                            .stream()
                                            .filter(sstable -> !view.containsSSTableIndex(sstable.descriptor))
                                            .map(sstable -> sstable.descriptor)
                                            .collect(Collectors.toList());
            if (missing.isEmpty())
                continue;
            logger.warn(index.getIndexContext().logMessage("Index has complete components on {} SSTable(s) whose per-SSTable index " +
                                                           "components are incomplete, so it can't serve them: making it " +
                                                           "non-queryable until it is rebuilt (nodetool rebuild_index {} {} {}): {}"),
                        missing.size(), baseCfs.keyspace.getName(), baseCfs.name, index.getIndexContext().getIndexName(),
                        missing.stream().limit(10).collect(Collectors.toList()));
            index.makeIndexNonQueryable();
        }
        return incomplete;
    }

    /**
     * A build that has written the components of a live SSTable through the given descriptor is about to update the
     * views: reloads the descriptor cached for the SSTable from the disk if it isn't that one (see
     * {@link SSTableContextManager#reloadDescriptorIfReplaced}). The caller must hold the SSTable's build lock
     * exclusively; the group's monitor keeps out the loads of the SSTable that update the views (see that method for
     * the one load outside it, which is harmless).
     */
    synchronized void reloadDescriptorIfReplaced(SSTableReader sstable, IndexDescriptor built)
    {
        if (contextManager.reloadDescriptorIfReplaced(sstable, built, indices))
            logger.debug(logMessage("Reloaded the index descriptor of {}, which was loaded again while it was being built"), sstable.descriptor);
    }

    /** A full rebuild of the given indexes starts (see {@link StorageAttachedIndexBuilder.FullRebuild}). */
    void fullRebuildStarted(Set<StorageAttachedIndex> indexes)
    {
        indexes.forEach(index -> fullRebuilds.merge(index, 1, Integer::sum));
    }

    /** A full rebuild of the given indexes is over. */
    void fullRebuildEnded(Set<StorageAttachedIndex> indexes)
    {
        indexes.forEach(index -> fullRebuilds.computeIfPresent(index, (i, n) -> n == 1 ? null : n - 1));
    }

    /**
     * Removes the SAI components written by Apache Cassandra 5.0 that the provided SSTable lists, once our components
     * have replaced them (see {@link StorageAttachedIndexBuilder}): unregisters them from the TOC, then deletes them.
     * They are streamable, so this runs under the SSTable's stream/rebuild reservation (CASSANDRA-21520), which the
     * caller may already hold; without it, the removal is retried once no stream or rebuild holds the SSTable.
     *
     * @param reserved whether the caller holds the stream/rebuild reservation of the SSTable
     * @param tracker the tracker accounting for the size of the SSTable's components, {@code null} for an SSTable
     *                that isn't live yet
     */
    void removeApacheComponents(SSTableReader sstable, boolean reserved, @Nullable Tracker tracker)
    {
        Set<Component> apacheComponents = apacheComponents(sstable);
        if (apacheComponents.isEmpty())
            return;

        if (!reserved && !sstable.streamRebuildState().tryBeginRebuild())
        {
            logger.debug(logMessage("Not removing the Apache Cassandra SAI components of {} while it is " +
                                    "being streamed or rebuilt; will retry when it is not"),
                         sstable.descriptor);
            sstable.streamRebuildState().runWhenNormal(() -> ScheduledExecutors.optionalTasks.execute(() -> removeReplacedApacheComponents(sstable)));
            return;
        }

        try
        {
            sstable.unregisterComponents(apacheComponents, tracker);
            // Apache Cassandra also writes a PartitionToSize component for tables without clustering columns, but
            // doesn't list it in the TOC.
            if (!baseCfs.metadata().hasClustering())
                apacheComponents.add(new Component(SSTableFormat.Components.Types.CUSTOM,
                                                   ApacheOnDiskFormat.fileNameFormat(IndexComponentType.PARTITION_TO_SIZE, null, 0)));
            // Another removal may have deleted them already, which is fine.
            deleteFiles(sstable, apacheComponents);
            logger.debug(logMessage("Removed the Apache Cassandra SAI components of {}"), sstable.descriptor);
        }
        finally
        {
            if (!reserved)
                sstable.streamRebuildState().endRebuild();
        }
    }

    /**
     * Removes the Apache Cassandra components of the provided SSTable if it is live and its TOC lists our complete
     * per-SSTable components, which replaced them (see {@link #removeApacheComponents}). Otherwise, they are either
     * still used, or the build that replaces them removes them. The TOC, not the cached descriptor, says so: a build
     * links our components to the descriptor as soon as it writes them, before registering them, and may still fail.
     * SSTables being replaced (see {@link StorageAttachedIndexBuilder#isRebuildingAllIndexesOf}) are left to the
     * build that replaces them.
     */
    private void removeReplacedApacheComponents(SSTableReader sstable)
    {
        Ref<? extends SSTableReader> ref = sstable.tryRef();
        if (ref == null)
            return; // gone
        try
        {
            if (apacheComponents(sstable).isEmpty()
                || sstable.isMarkedCompacted()
                || !baseCfs.getLiveSSTables().contains(sstable)
                || indices.isEmpty()
                || StorageAttachedIndexBuilder.isRebuildingAllIndexesOf(sstable)
                || !hasRegisteredPerSSTableComponents(sstable))
                return;
            removeApacheComponents(sstable, false, baseCfs.getTracker());
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.warn(logMessage("Failed to remove the replaced Apache Cassandra SAI components of {}"), sstable.descriptor, t);
        }
        finally
        {
            ref.release();
        }
    }

    /**
     * @return whether the TOC of the provided SSTable lists the completion marker of per-SSTable components of ours
     */
    private static boolean hasRegisteredPerSSTableComponents(SSTableReader sstable)
    {
        for (Component component : sstable.components())
        {
            Optional<Version.ParsedFileName> parsed = Version.tryParseFileName(component.name);
            if (parsed.isPresent()
                && parsed.get().indexName == null
                && parsed.get().component == IndexComponentType.GROUP_COMPLETION_MARKER
                && !parsed.get().buildId.version().isApache())
                return true;
        }
        return false;
    }

    /**
     * Removes the components written by Apache Cassandra for the provided dropped index from every live SSTable of the
     * table: unregisters them from the TOC right away, so that no index adopts them, and deletes them under the
     * SSTable's stream/rebuild reservation (see {@link #deleteFilesWhenNotStreamed}). DROP INDEX only cleans up the
     * SSTables in the index's view, which misses them while the index is rebuilt or after a failed build.
     */
    static void removeApacheIndexComponents(ColumnFamilyStore cfs, String indexName)
    {
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            Set<Component> components = apacheIndexComponents(sstable, indexName);
            if (components.isEmpty())
                continue;
            Ref<? extends SSTableReader> ref = sstable.tryRef();
            if (ref == null)
                continue;
            try
            {
                sstable.unregisterComponents(components, cfs.getTracker());
                logger.debug("[{}.{}.{}] Unregistered the Apache Cassandra SAI components of the dropped index from {}",
                             cfs.getKeyspaceName(), cfs.name, indexName, sstable.descriptor);
                deleteFilesWhenNotStreamed(sstable, components);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                logger.warn("[{}.{}.{}] Failed to remove the Apache Cassandra SAI components of the dropped index from {}",
                            cfs.getKeyspaceName(), cfs.name, indexName, sstable.descriptor, t);
            }
            finally
            {
                ref.release();
            }
        }
    }

    /**
     * Discards the components written by Apache Cassandra for the provided index name from every live SSTable of the
     * table, for an index of that name added to it: found from the TOC and the files on disk, unregistered and deleted
     * right away, even if the TOC can't be rewritten, as {@link StorageAttachedIndex#onAddedToExistingTable} does for
     * our components.
     *
     * @param sstables the live SSTables of the table
     * @param filesOnDisk the names of the SAI component files on disk of those SSTables
     * @return false if the completion marker of such components is still on disk on an SSTable
     */
    static boolean discardApacheIndexComponents(ColumnFamilyStore cfs, String indexName,
                                                Collection<SSTableReader> sstables,
                                                Map<SSTableReader, Set<String>> filesOnDisk)
    {
        String prefix = Version.APACHE_COMPONENT_PREFIX + indexName + Version.SAI_SEPARATOR;
        boolean discarded = true;
        for (SSTableReader sstable : sstables)
        {
            Set<Component> listed = apacheIndexComponents(sstable, indexName);
            Set<Component> components = new HashSet<>(listed);
            for (String file : filesOnDisk.getOrDefault(sstable, Collections.emptySet()))
                if (file.startsWith(prefix))
                    components.add(new Component(SSTableFormat.Components.Types.CUSTOM, file));
            if (components.isEmpty())
                continue;
            Ref<? extends SSTableReader> ref = sstable.tryRef();
            if (ref == null)
                continue;
            try
            {
                try
                {
                    if (!listed.isEmpty())
                        sstable.unregisterComponents(listed, cfs.getTracker());
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    logger.warn("[{}.{}.{}] Failed to unregister the Apache Cassandra SAI components of an index " +
                                "of the same name from {}. Deleting their files.",
                                cfs.getKeyspaceName(), cfs.name, indexName, sstable.descriptor, t);
                }
                deleteFiles(sstable, components);
                String markerName = ApacheOnDiskFormat.fileNameFormat(IndexComponentType.COLUMN_COMPLETION_MARKER,
                                                                      indexName, 0);
                File marker = sstable.descriptor.fileFor(new Component(SSTableFormat.Components.Types.CUSTOM,
                                                                       markerName));
                if (marker.exists())
                {
                    discarded = false;
                    logger.error("[{}.{}.{}] Could not delete {}, the completion marker of the Apache Cassandra SAI " +
                                 "components of an index of the same name",
                                 cfs.getKeyspaceName(), cfs.name, indexName, marker);
                }
                else
                {
                    logger.info("[{}.{}.{}] Discarded the Apache Cassandra SAI components of an index of the same name on {}",
                                cfs.getKeyspaceName(), cfs.name, indexName, sstable.descriptor);
                }
            }
            finally
            {
                ref.release();
            }
        }
        return discarded;
    }

    private static Set<Component> apacheIndexComponents(SSTableReader sstable, String indexName)
    {
        String prefix = Version.APACHE_COMPONENT_PREFIX + indexName + Version.SAI_SEPARATOR;
        return sstable.components()
                      .stream()
                      .filter(c -> c.name.startsWith(prefix))
                      .collect(Collectors.toCollection(HashSet::new));
    }

    /**
     * Deletes the files of the provided (unregistered) components of the provided SSTable under its stream/rebuild
     * reservation, now if possible, or once no stream or rebuild holds it.
     */
    static void deleteFilesWhenNotStreamed(SSTableReader sstable, Set<Component> components)
    {
        if (sstable.streamRebuildState().tryBeginRebuild())
        {
            try
            {
                deleteFiles(sstable, components);
            }
            finally
            {
                sstable.streamRebuildState().endRebuild();
            }
        }
        else
        {
            sstable.streamRebuildState().runWhenNormal(() -> ScheduledExecutors.optionalTasks.execute(
                () -> deleteFilesWhenNotStreamed(sstable, components)));
        }
    }

    private static void deleteFiles(SSTableReader sstable, Set<Component> components)
    {
        for (Component component : components)
        {
            try
            {
                sstable.descriptor.fileFor(component).deleteIfExists();
            }
            catch (FSError e)
            {
                logger.warn("Could not delete the Apache Cassandra SAI component {} of {}", component.name, sstable.descriptor, e);
            }
        }
    }

    private static Set<Component> apacheComponents(SSTableReader sstable)
    {
        return sstable.components()
                      .stream()
                      .filter(c -> Version.isApacheComponentFileName(c.name))
                      .collect(Collectors.toCollection(HashSet::new));
    }

    private String logMessage(String message)
    {
        return String.format("[%s.%s.*] %s", baseCfs.getKeyspaceName(), baseCfs.name, message);
    }

    public void validateComponents(SSTableReader sstable, boolean validateChecksum) throws CorruptIndexException
    {
        IndexDescriptor indexDescriptor = descriptorFor(sstable);
        if (!indexDescriptor.perSSTableComponents().isValid(validateChecksum))
            throw new CorruptIndexException("Failed validation of per-SSTable components", sstable.getFilename());

        for (StorageAttachedIndex index : indices)
        {
            IndexContext ctx = index.getIndexContext();
            if (!indexDescriptor.perIndexComponents(ctx).isValid(validateChecksum))
                throw new CorruptIndexException("Failed validation of per-column components for " + ctx, sstable.getFilename());
        }
    }

    /**
     * A vector index doesn't support splitting a flush into several SSTables: the memtable index builds its on-disk
     * graph once, for the whole memtable (CASSANDRA-19661).
     */
    @Override
    public boolean supportsL0Shards()
    {
        for (StorageAttachedIndex index : indices)
        {
            if (index.getIndexContext().isVector())
                return false;
        }
        return true;
    }

    @Override
    public boolean validateSSTableAttachedIndexes(Collection<SSTableReader> sstables, boolean throwOnIncomplete, boolean validateChecksum)
    {
        boolean complete = true;

        for (SSTableReader sstable : sstables)
        {
            logger.debug("Validating {} SAI indices for {}", indices.size(), sstable.getFilename());
            if (indices.isEmpty())
            {
                logger.debug("No SAI indices to validate for {}", sstable.getFilename());
                return true;
            }
            
            // For validation, we need to load a fresh descriptor to ensure we see the current state of files
            IndexDescriptor indexDescriptor = IndexDescriptor.load(sstable, contexts());
            IndexComponents.ForRead perSSTableComponents = indexDescriptor.perSSTableComponents();

            logger.debug("Per-SSTable components complete: {}", indexDescriptor.perSSTableComponents().isComplete());
            if (indexDescriptor.perSSTableComponents().isComplete())
            {
                perSSTableComponents.validateComponents(sstable, baseCfs.getTracker(), validateChecksum, true);

                for (StorageAttachedIndex index : indices)
                {
                    IndexComponents.ForRead perIndexComponents = indexDescriptor.perIndexComponents(index.getIndexContext());

                    logger.debug("Per-index components complete for {}: {}", index.getIndexContext().getIndexName(), perIndexComponents.isComplete());
                    // Complete components with an obsolete encoding can't be read, so they must be rebuilt like
                    // incomplete ones (see IndexDescriptor#hasObsoleteEncoding).
                    if (perIndexComponents.isComplete() && !IndexDescriptor.hasObsoleteEncoding(perIndexComponents))
                        perIndexComponents.validateComponents(sstable, baseCfs.getTracker(), validateChecksum, true);
                    else if (throwOnIncomplete)
                        throw new IllegalStateException(
                            indexDescriptor.logMessage("Incomplete or obsolete per-column index build for SSTable " +
                                                       sstable.descriptor));
                    else
                        complete = false;
                }
            }
            else if (throwOnIncomplete)
            {
                throw new IllegalStateException(indexDescriptor.logMessage("Incomplete per-SSTable index build" + sstable.descriptor));
            }
            else
            {
                complete = false;
            }
        }

        return complete;
    }

    /**
     * open index files by checking number of {@link SSTableContext} and {@link SSTableIndex},
     * so transient open files during validation and files that are still open for in-flight requests will not be tracked.
     *
     * @return total number of open files for all {@link StorageAttachedIndex}es.
     */
    public int openIndexFiles()
    {
        return contextManager.openFiles() + indices.stream().mapToInt(index -> index.getIndexContext().openPerIndexFiles()).sum();
    }

    /**
     * @return total disk usage of all per-sstable index files
     */
    public long diskUsage()
    {
        return contextManager.diskUsage();
    }

    /**
     * @return count of indexes building
     */
    public int totalIndexBuildsInProgress()
    {
        return (int) indices.stream().filter(i -> baseCfs.indexManager.isIndexBuilding(i.getIndexMetadata().name)).count();
    }

    /**
     * @return count of queryable indexes
     */
    public int totalQueryableIndexCount()
    {
        return (int) indices.stream().filter(i -> baseCfs.indexManager.isIndexQueryable(i)).count();
    }

    /**
     * @return count of indexes
     */
    public int totalIndexCount()
    {
        return indices.size();
    }

    /**
     * @return total disk usage of all per-sstable index files and per-column index files
     */
    public long totalDiskUsage()
    {
        // Note that this only account the "active" files. That is, if we have old versions/generations or incomplete
        // build still on disk, those won't be counted. Counting only "live" data here is consistent with the fact
        // that `TableStateMetrics.diskUsagePercentageOfBaseTable` compare the number obtain from this to the base
        // table "live" disk space use. But there is certainly a small risk for being misleading, and where base
        // tables expose both a "liveDiskSpaceUsed" and "totalDiskSpaceUsed", SAI only exposes "diskUsageBytes", which
        // has we just mentioned is the "live" usage. Might be worth improving at some point.
        return diskUsage() + indices.stream().flatMap(i -> i.getIndexContext().getView().getIndexes().stream())
                                    .mapToLong(SSTableIndex::sizeOfPerColumnComponents).sum();
    }

    public TableMetadata metadata()
    {
        return baseCfs.metadata();
    }

    // Needed by CNDB
    public TableQueryMetrics queryMetrics()
    {
        return queryMetrics;
    }

    // Needed by CNDB
    public Optional<TableStateMetrics> stateMetrics()
    {
        return stateMetrics;
    }

    public ColumnFamilyStore table()
    {
        return baseCfs;
    }

    @VisibleForTesting
    public SSTableContextManager sstableContextManager()
    {
        return contextManager;
    }

    /**
     * Returns the {@link IndexDescriptor} for the given {@link SSTableReader} (which must belong to the base table
     * of this group).
     * Note that this always return a non-null value, since all sstables must be indexed, but that descriptor could
     * be "empty" if the sstable has never had an index built yet.
     */
    public IndexDescriptor descriptorFor(SSTableReader sstable)
    {
        return contextManager.getOrLoadIndexDescriptor(sstable, indices);
    }

    /** @return the descriptor cached for the SSTable, if any, without loading one */
    @Nullable
    IndexDescriptor cachedDescriptorFor(SSTableReader sstable)
    {
        return contextManager.cachedIndexDescriptor(sstable);
    }

    /** Forgets the per-index components of that name of the descriptors cached for the SSTables of the table. */
    void forgetCachedComponentsOfName(String indexName)
    {
        contextManager.forgetPerIndexComponentsOfName(indexName);
    }

    /**
     * simulate index loading on restart with index file validation
     */
    @VisibleForTesting
    public void unsafeReload()
    {
        contextManager.clear();
        onSSTableChanged(baseCfs.getLiveSSTables(), Collections.emptySet(), indices, false);
        onSSTableChanged(Collections.emptySet(), baseCfs.getLiveSSTables(), indices, true);
    }

    /**
     * Simulate the index going through a restart of node
     */
    @VisibleForTesting
    public void reset()
    {
        contextManager.clear();
        indices.forEach(index -> index.makeIndexNonQueryable());
        onSSTableChanged(baseCfs.getLiveSSTables(), Collections.emptySet(), indices, false);
    }

    private Set<IndexContext> contexts()
    {
        Set<IndexContext> contexts = Sets.newHashSetWithExpectedSize(indices.size());
        for (StorageAttachedIndex index : indices)
            contexts.add(index.getIndexContext());
        return contexts;
    }

    /**
     * @return the minimum version that new per-index components of this group would be written in on its sstables
     * (that of their per-sstable components, see {@link IndexDescriptor#versionForNewComponents()}), or the current
     * version if there are no sstable indexes.
     */
    public Version getMinVersion()
    {
        StorageAttachedIndexGroup indexGroup = StorageAttachedIndexGroup.getIndexGroup(baseCfs);
        assert indexGroup != null;
        Version minVersion = null;
        // The canonical set: the sources of a running compaction, which have their components, rather than its
        // results opened early, which may not have them yet.
        for (SSTableReader sstable : baseCfs.getSSTables(SSTableSet.CANONICAL))
        {
            IndexDescriptor indexDescriptor = indexGroup.descriptorFor(sstable);
            assert indexDescriptor != null;

            // The version new indexes would be written in, which isn't that of the per-sstable components if those
            // were written by Apache Cassandra (whose format we don't write).
            Version version = indexDescriptor.versionForNewComponents();
            if (minVersion == null || version.compareTo(minVersion) < 0)
                minVersion = version;
        }
        return minVersion == null ? Version.current(baseCfs.metadata.keyspace) : minVersion;
    }
}
