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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.CassandraWriteContext;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.TableOperation;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexBuildDecider;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.FeatureNeedsIndexRebuildException;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.analyzer.AnalyzerEqOperatorSupport;
import org.apache.cassandra.index.sai.analyzer.LuceneAnalyzer;
import org.apache.cassandra.index.sai.analyzer.NonTokenizingOptions;
import org.apache.cassandra.index.sai.disk.PerIndexFilesOwner;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.ComponentsBuildId;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDefinitionDigest;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.SSTableIndexWriter;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableWatcher;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.config.CassandraRelevantProperties.RANGE_READ_ENDPOINT_GROUPING_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VALIDATE_TERMS_AT_COORDINATOR;
import static org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig.MAX_TOP_K;

public class StorageAttachedIndex implements Index
{
    public static final String VECTOR_NON_FLOAT_ERROR = "SAI ANN indexes are only allowed on vector columns with float elements";
    public static final String QUERY_VALUE_TOO_LARGE_ERROR_MESSAGE =
    "%s of column %s is too large to be sent to the replicas: %d bytes, the maximum is %d bytes.";
    public static final String MAP_ENTRY_TOO_LARGE_ERROR_MESSAGE =
    "%s of column %s is too large: %d bytes, the maximum is %d bytes.";
    public static final String MAP_KEY_TOO_LARGE_FOR_ENTRIES_INDEX_ERROR_MESSAGE =
    "%s of column %s is too large for its entries index: %d bytes, the maximum is %d bytes.";
    /**
     * The largest first component of a {@link CompositeType} value, such as the map key of an entries index term: a
     * component length is an unsigned short, and 0xFFFF is reserved for the first one (the static prefix).
     */
    public static final int MAX_COMPOSITE_FIRST_COMPONENT_SIZE = FBUtilities.MAX_UNSIGNED_SHORT - 1;
    public static final String EMPTY_CLUSTERING_FIELD_ERROR =
    "Cannot write a row whose clustering has an empty value, such as blobAsInt(0x), in a tuple or UDT field of a " +
    "type that orders it like a null value, to a table with storage-attached index %s: use null instead, or a range " +
    "deletion to delete such a row";
    public static final String EMPTY_CLUSTERING_FIELD_ROW_ERROR =
    "Cannot index the row %s: its clustering has an empty value, such as blobAsInt(0x), in a tuple or UDT field of " +
    "a type that orders it like a null value, and storage-attached indexes cannot order it with the rows that have " +
    "a null value there";
    public static final String NGRAM_WITHOUT_QUERY_ANALYZER_WARNING =
    "Using an ngram analyzer without defining a query_analyzer. " +
    "This means that the same ngram analyzer will be applied to both indexed and queried column values. " +
    "Applying ngram analysis to the queried values usually produces too many search tokens to be useful. " +
    "The large number of tokens can also have a negative impact in performance. " +
    "In most cases it's better to use a simpler query_analyzer such as the standard one.";

    private static final Logger logger = LoggerFactory.getLogger(StorageAttachedIndex.class);

    private static final boolean VALIDATE_TERMS_AT_COORDINATOR = SAI_VALIDATE_TERMS_AT_COORDINATOR.getBoolean();
    private static final boolean ENDPOINT_GROUPING_ENABLED = RANGE_READ_ENDPOINT_GROUPING_ENABLED.getBoolean();

    private static class StorageAttachedIndexBuildingSupport implements IndexBuildingSupport
    {
        public NavigableMap<SSTableReader, Set<StorageAttachedIndex>> prepareSSTablesToBuild(StorageAttachedIndexGroup group,
                                                       Set<Index> indexes,
                                                       Collection<SSTableReader> sstablesToRebuild,
                                                       boolean isFullRebuild)
        {
            NavigableMap<SSTableReader, Set<StorageAttachedIndex>> sstables = new TreeMap<>(SSTableReader.idComparator);

            indexes.stream()
                   .filter((i) -> i instanceof StorageAttachedIndex)
                   .forEach((i) ->
                            {
                                StorageAttachedIndex sai = (StorageAttachedIndex) i;
                                IndexContext indexContext = ((StorageAttachedIndex) i).getIndexContext();

                                // If this is not a full manual index rebuild we can skip SSTables that already have an
                                // attached index. Otherwise, we override any pre-existent index.
                                // A compaction result opened early is still being written, with its index
                                // components, by the compaction, which adds it when it completes: never build it.
                                Collection<SSTableReader> ss = sstablesToRebuild.stream()
                                                                                .filter(s -> s.openReason != SSTableReader.OpenReason.EARLY)
                                                                                .collect(Collectors.toList());
                                if (!isFullRebuild)
                                {
                                    ss = ss.stream()
                                           .filter(s -> !IndexDescriptor.isIndexBuildCompleteOnDisk(s, indexContext))
                                           .collect(Collectors.toList());
                                }

                                group.prepareIndexSSTablesForRebuild(ss, sai);

                                ss.forEach((sstable) ->
                                           {
                                               Set<StorageAttachedIndex> toBuild = sstables.get(sstable);
                                               if (toBuild == null) sstables.put(sstable, (toBuild = new HashSet<>()));
                                               toBuild.add(sai);
                                           });
                            });

            return sstables;
        }

        @Override
        public SecondaryIndexBuilder getIndexBuildTask(ColumnFamilyStore cfs, Set<Index> indexes, Collection<SSTableReader> sstablesToRebuild, boolean isFullRebuild)
        {
            StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
            NavigableMap<SSTableReader, Set<StorageAttachedIndex>> sstables = prepareSSTablesToBuild(group, indexes, sstablesToRebuild, isFullRebuild);
            return new StorageAttachedIndexBuilder(group, sstables, isFullRebuild, false);
        }

        @Override
        public List<SecondaryIndexBuilder> getParallelIndexBuildTasks(ColumnFamilyStore cfs, Set<Index> indexes, Collection<SSTableReader> sstablesToRebuild, boolean isFullRebuild)
        {
            StorageAttachedIndexGroup indexGroup = StorageAttachedIndexGroup.getIndexGroup(cfs);
            NavigableMap<SSTableReader, Set<StorageAttachedIndex>> sstables = prepareSSTablesToBuild(indexGroup, indexes, sstablesToRebuild, isFullRebuild);

            List<List<SSTableReader>> groups = groupBySize(new ArrayList<>(sstables.keySet()), DatabaseDescriptor.getConcurrentCompactors());
            List<SecondaryIndexBuilder> builders = new ArrayList<>();

            for (List<SSTableReader> group : groups)
            {
                SortedMap<SSTableReader, Set<StorageAttachedIndex>> current = new TreeMap<>(Comparator.comparing(sstable -> sstable.descriptor.id));
                group.forEach(sstable -> current.put(sstable, sstables.get(sstable)));

                builders.add(new StorageAttachedIndexBuilder(indexGroup, current, isFullRebuild, false));
            }

            logger.info("Creating {} parallel index builds over {} total sstables for {}...", builders.size(), sstables.size(), cfs.metadata());

            return builders;
        }
    }

    // Used to build indexes on newly added SSTables:
    private static final StorageAttachedIndexBuildingSupport INDEX_BUILDER_SUPPORT = new StorageAttachedIndexBuildingSupport();

    /**
     * Serializes the discarding of the components of an index name ({@link #onAddedToExistingTable}) with the deletion
     * of the files of a dropped index once its last reader is released ({@link SSTableIndex#release}), with the
     * completion of the components an index builds ({@link SSTableIndexWriter#complete}), and with the builds and
     * writers that own the files of an index on an SSTable ({@link PerIndexFilesOwner}): all go by file names, which an
     * index created again with the same name reuses.
     */
    public static final Object SAME_NAME_FILES_LOCK = new Object();

    /** How many times the initial build of an index builds the sstables it missed, see buildSSTablesMissedByInitialBuild. */
    @VisibleForTesting
    static final int MAX_BUILDS_OF_MISSED_SSTABLES = 10;

    private static final Set<String> VALID_OPTIONS = ImmutableSet.of(NonTokenizingOptions.CASE_SENSITIVE,
                                                                     NonTokenizingOptions.NORMALIZE,
                                                                     NonTokenizingOptions.ASCII,
                                                                     // For now, we leave this for backward compatibility even though it's not used
                                                                     IndexContext.ENABLE_SEGMENT_COMPACTION_OPTION_NAME,
                                                                     IndexTarget.TARGET_OPTION_NAME,
                                                                     IndexTarget.CUSTOM_INDEX_OPTION_NAME,
                                                                     IndexWriterConfig.POSTING_LIST_LVL_MIN_LEAVES,
                                                                     IndexWriterConfig.POSTING_LIST_LVL_SKIP_OPTION,
                                                                     IndexWriterConfig.MAXIMUM_NODE_CONNECTIONS,
                                                                     IndexWriterConfig.CONSTRUCTION_BEAM_WIDTH,
                                                                     IndexWriterConfig.NEIGHBORHOOD_OVERFLOW,
                                                                     IndexWriterConfig.ALPHA,
                                                                     IndexWriterConfig.ENABLE_HIERARCHY,
                                                                     IndexWriterConfig.SIMILARITY_FUNCTION,
                                                                     IndexWriterConfig.SOURCE_MODEL,
                                                                     IndexWriterConfig.OPTIMIZE_FOR,
                                                                     LuceneAnalyzer.INDEX_ANALYZER,
                                                                     LuceneAnalyzer.QUERY_ANALYZER,
                                                                     AnalyzerEqOperatorSupport.OPTION);

    // this does not include vectors because each Vector declaration is a separate type instance
    public static final Set<CQL3Type> SUPPORTED_TYPES = ImmutableSet.of(CQL3Type.Native.ASCII, CQL3Type.Native.BIGINT, CQL3Type.Native.DATE,
                                                                        CQL3Type.Native.DOUBLE, CQL3Type.Native.FLOAT, CQL3Type.Native.INT,
                                                                        CQL3Type.Native.SMALLINT, CQL3Type.Native.TEXT, CQL3Type.Native.TIME,
                                                                        CQL3Type.Native.TIMESTAMP, CQL3Type.Native.TIMEUUID, CQL3Type.Native.TINYINT,
                                                                        CQL3Type.Native.UUID, CQL3Type.Native.VARCHAR, CQL3Type.Native.INET,
                                                                        CQL3Type.Native.VARINT, CQL3Type.Native.DECIMAL, CQL3Type.Native.BOOLEAN);

    private static final Set<Class<? extends IPartitioner>> ILLEGAL_PARTITIONERS =
            ImmutableSet.of(OrderPreservingPartitioner.class, LocalPartitioner.class, ByteOrderedPartitioner.class, RandomPartitioner.class);

    private final ColumnFamilyStore baseCfs;
    private final IndexMetadata config;
    private final IndexContext indexContext;

    // Tracks whether or not we've started the index build on initialization.
    private volatile boolean canFlushFromMemtableIndex = false;

    // Tracks whether the index has been dropped due to removal, a table drop, etc or index schema is unloaded after schema unassignment
    private volatile boolean dropped = false;
    private volatile boolean unloaded = false;

    // Why this index must not be built and made queryable, if it must not: components of its name that it must not use
    // may be left on disk (see onAddedToExistingTable). Only the initialization task, which SecondaryIndexManager runs
    // once per instance, and the truncate task read it. It is never cleared: a rebuild (nodetool rebuild_index) doesn't
    // read it, and builds every SSTable from the data, deleting the files of the index's names first.
    private volatile String cannotBuildReason = null;

    /**
     * Called via reflection from SecondaryIndexManager
     */
    public StorageAttachedIndex(ColumnFamilyStore baseCfs, IndexMetadata config)
    {
        this.baseCfs = baseCfs;
        this.config = config;
        TableMetadata tableMetadata = baseCfs.metadata();
        Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(tableMetadata, config);
        this.indexContext = new IndexContext(tableMetadata.keyspace,
                                             tableMetadata.name,
                                             tableMetadata.id,
                                             tableMetadata.partitionKeyType,
                                             tableMetadata.comparator,
                                             target.left,
                                             target.right,
                                             config,
                                             baseCfs);
    }

    /**
     * Rejects the creation of an index on vectors whose elements aren't floats ({@link #VECTOR_NON_FLOAT_ERROR}). Such
     * columns only exist with {@code -Dcassandra.float_only_vectors=false}. Only checked when the index is created, so
     * that an index created on such a column by an earlier version stays in the schema and doesn't block schema
     * changes.
     */
    public static void validateVectorElementType(TableMetadata table, IndexMetadata index)
    {
        // This runs before validateOptions (see CreateIndexStatement), which reports invalid targets
        String targetColumn = index.options.get(IndexTarget.TARGET_OPTION_NAME);
        Pair<ColumnMetadata, IndexTarget.Type> target = targetColumn == null ? null : TargetParser.parse(table, targetColumn);
        if (target == null)
            return;

        AbstractType<?> type = TypeUtil.cellValueType(target.left, target.right);
        if (type.isVector() && !(((VectorType<?>) type).elementType instanceof FloatType))
            throw new InvalidRequestException(VECTOR_NON_FLOAT_ERROR);
    }

    /**
     * Used via reflection in {@link IndexMetadata}
     */
    @SuppressWarnings({ "unused" })
    public static Map<String, String> validateOptions(Map<String, String> options, TableMetadata metadata)
    {
        Map<String, String> unknown = new HashMap<>(2);

        for (Map.Entry<String, String> option : options.entrySet())
        {
            if (!VALID_OPTIONS.contains(option.getKey()))
            {
                unknown.put(option.getKey(), option.getValue());
            }
        }

        if (!unknown.isEmpty())
        {
            return unknown;
        }

        if (ILLEGAL_PARTITIONERS.contains(metadata.partitioner.getClass()))
        {
            throw new InvalidRequestException("Storage-attached index does not support the following IPartitioner implementations: " + ILLEGAL_PARTITIONERS);
        }

        String targetColumn = options.get(IndexTarget.TARGET_OPTION_NAME);

        if (targetColumn == null)
        {
            throw new InvalidRequestException("Missing target column");
        }

        if (targetColumn.split(",").length > 1)
        {
            throw new InvalidRequestException("A storage-attached index cannot be created over multiple columns: " + targetColumn);
        }

        Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(metadata, targetColumn);

        if (target == null)
        {
            throw new InvalidRequestException("Failed to retrieve target column for: " + targetColumn);
        }

        // Check for duplicate indexes considering both target and analyzer configuration
        boolean isAnalyzed = AbstractAnalyzer.isAnalyzed(options);
        long duplicateCount = metadata.indexes.stream()
                                              .filter(index -> index.getIndexClassName().equals(StorageAttachedIndex.class.getName()))
                                              .filter(index -> {
                                                  // Indexes on the same column with different target (KEYS, VALUES, ENTRIES)
                                                  // are allowed on non-frozen Maps
                                                  var existingTarget = TargetParser.parse(metadata, index.options.get(IndexTarget.TARGET_OPTION_NAME));
                                                  if (existingTarget == null || !existingTarget.equals(target))
                                                      return false;
                                                  // Also allow different indexes if one is analyzed and the other isn't
                                                  return isAnalyzed == AbstractAnalyzer.isAnalyzed(index.options);
                                              })
                                              .count();
        // >1 because "metadata.indexes" already includes current index
        if (duplicateCount > 1)
            throw new InvalidRequestException(String.format("Cannot create duplicate storage-attached index on column: %s", target.left));

        // Analyzer is not supported against PK columns
        if (isAnalyzed)
        {
            for (ColumnMetadata column : metadata.primaryKeyColumns())
            {
                if (column.name.equals(target.left.name))
                    logger.warn("Schema contains an invalid index analyzer on primary key column, allowed for backwards compatibility: " + target.left);
            }
        }

        AbstractType<?> type = TypeUtil.cellValueType(target.left, target.right);

        // Validate analyzers by building them
        try (AbstractAnalyzer.AnalyzerFactory analyzerFactory = AbstractAnalyzer.fromOptions(targetColumn, type, options))
        {
            if (AbstractAnalyzer.hasQueryAnalyzer(options))
                AbstractAnalyzer.fromOptionsQueryAnalyzer(type, options).close();
            else if (analyzerFactory.isNGram())
                ClientWarn.instance.warn(NGRAM_WITHOUT_QUERY_ANALYZER_WARNING);
        }

        var config = IndexWriterConfig.fromOptions(null, type, options);

        // If we are indexing map entries we need to validate the sub-types
        if (TypeUtil.isComposite(type))
        {
            for (AbstractType<?> subType : type.subTypes())
            {
                if (!SUPPORTED_TYPES.contains(subType.asCQL3Type()) && !TypeUtil.isFrozen(subType))
                    throw new InvalidRequestException("Unsupported composite type for SAI: " + subType.asCQL3Type());
            }
        }
        else if (type.isVector())
        {
            if (!Version.current(metadata.keyspace).onOrAfter(Version.JVECTOR_EARLIEST))
            {
                throw new InvalidRequestException(vectorUnsupportedByCurrentVersionError(Version.current(metadata.keyspace)));
            }

            // Also, any pre-existing indexes must be in a version compatible with vectors.
            ColumnFamilyStore baseCfs = Schema.instance.getColumnFamilyStoreInstance(metadata.id);
            if (baseCfs != null)
            {
                StorageAttachedIndexGroup indexGroup = StorageAttachedIndexGroup.getIndexGroup(baseCfs);
                if (indexGroup != null)
                {
                    Version version = indexGroup.getMinVersion();
                    if (!version.onOrAfter(Version.JVECTOR_EARLIEST))
                        throw new InvalidRequestException(vectorUnsupportedByExistingVersionError(version));
                }
            }

            if (type.valueLengthIfFixed() == 4 && config.getSimilarityFunction() == VectorSimilarityFunction.COSINE)
                throw new InvalidRequestException("Cosine similarity is not supported for single-dimension vectors");

            // vectors of fixed length types are fixed length too, so we can reject the index creation
            // if that fixed length is over the max term size for vectors
            if (type.isValueLengthFixed() && IndexContext.MAX_VECTOR_TERM_SIZE < type.valueLengthIfFixed())
            {
                AbstractType<?> elementType = ((VectorType<?>) type).elementType;
                var error = String.format("Vector index created with %s will produce terms of %s, " +
                                          "exceeding the max vector term size of %s. " +
                                          "That sets an implicit limit of %d dimensions for %s vectors.",
                                          type.asCQL3Type(),
                                          FBUtilities.prettyPrintMemory(type.valueLengthIfFixed()),
                                          FBUtilities.prettyPrintMemory(IndexContext.MAX_VECTOR_TERM_SIZE),
                                          IndexContext.MAX_VECTOR_TERM_SIZE / elementType.valueLengthIfFixed(),
                                          elementType.asCQL3Type());
                // VSTODO until we can safely differentiate client and system requests, we can only log here
                // Ticket for this: https://github.com/riptano/VECTOR-SEARCH/issues/85
                logger.warn(error);
            }
        }
        else if (!SUPPORTED_TYPES.contains(type.asCQL3Type()) && !TypeUtil.isFrozen(type))
        {
            throw new InvalidRequestException("Unsupported type for SAI: " + type.asCQL3Type());
        }

        return Collections.emptyMap();
    }

    @Override
    public void register(IndexRegistry registry)
    {
        // index will be available for writes
        registry.registerIndex(this, StorageAttachedIndexGroup.GROUP_KEY, () -> new StorageAttachedIndexGroup(baseCfs));
    }

    @Override
    public void unregister(IndexRegistry registry)
    {
        registry.unregisterIndex(this, StorageAttachedIndexGroup.GROUP_KEY);
    }

    @Override
    public IndexMetadata getIndexMetadata()
    {
        return config;
    }

    @Override
    public boolean shouldSkipInitialization()
    {
        // SAI performs partial initialization so it must always execute it; the actual index build is then still skipped
        // if IndexBuildDecider.instance.onInitialBuild().skipped() is true.
        return false;
    }

    /**
     * Discards the per-index components named after this index that are already on disk for the table's SSTables and
     * weren't built for its definition, so that it is built from the data: in particular, components written before
     * their completion marker carried the definition of their index (see {@link IndexDefinitionDigest}) don't say which
     * index they were written for (e.g. a dropped index of the same name). They are found from the TOC and from the
     * files on disk, and deleted right away, even while a stream or a rebuild holds the SSTable, as the build of this
     * index would append to them. If a completion marker of them is still on disk afterwards, the initial build of this
     * index fails rather than use them.
     */
    @Override
    public void onAddedToExistingTable()
    {
        synchronized (SAME_NAME_FILES_LOCK)
        {
            // The live SSTables, and the sources a running compaction has written out of the live set: it restores them
            // if it is stopped. The instances of the canonical set first, as those are the ones it restores.
            Set<SSTableReader> sstables = Sets.newHashSet(baseCfs.getSSTables(SSTableSet.CANONICAL));
            sstables.addAll(baseCfs.getLiveSSTables());
            Map<SSTableReader, Set<String>> filesOnDisk;
            try
            {
                filesOnDisk = Objects.requireNonNull(SSTableWatcher.instance.listSAIComponentFiles(sstables),
                                                     "SSTableWatcher#listSAIComponentFiles returned null");
            }
            catch (Throwable t)
            {
                // Files of the name that the TOC doesn't list may be left: don't build over them
                JVMStabilityInspector.inspectThrowable(t);
                logger.error(indexContext.logMessage("Could not list the SAI files of the table's SSTables, " +
                                                     "to find the components of an index of the same name. " +
                                                     "This index won't be built on this node until it is rebuilt: " +
                                                     "run nodetool rebuild_index before restarting this node."), t);
                cannotBuildReason = "could not list the SAI files of the table's SSTables, " +
                                    "to find the components of an index of the same name";
                filesOnDisk = Collections.emptyMap();
            }
            for (SSTableReader sstable : sstables)
            {
                if (!discardComponentsOfName(sstable, filesOnDisk.getOrDefault(sstable, Collections.emptySet())))
                    cannotBuildReason = "could not delete the components of an index of the same name on " +
                                        sstable.descriptor;
            }
        }
    }

    /**
     * Unregisters from the TOC, and deletes, the per-index components named after this index on the provided SSTable
     * that it doesn't use: those the TOC lists, and those of the provided files on disk.
     *
     * @param filesOnDisk names of SAI component files of the SSTable on disk
     *                    (see {@link SSTableWatcher#listSAIComponentFiles})
     * @return false if a completion marker of those components is still on disk, so that they could still be used
     */
    private boolean discardComponentsOfName(SSTableReader sstable, Set<String> filesOnDisk)
    {
        String name = indexContext.getIndexName();
        Map<Component, ComponentsBuildId> listed = new HashMap<>();
        Set<ComponentsBuildId> builds = new HashSet<>();
        for (Component component : sstable.components())
        {
            Optional<Version.ParsedFileName> parsed = tryParseFileName(component.name, indexContext);
            if (parsed.isEmpty() || !name.equals(parsed.get().indexName))
                continue;
            listed.put(component, parsed.get().buildId);
            builds.add(parsed.get().buildId);
        }
        for (String file : filesOnDisk)
        {
            Optional<Version.ParsedFileName> parsed = tryParseFileName(file, indexContext);
            if (parsed.isPresent() && name.equals(parsed.get().indexName))
                builds.add(parsed.get().buildId);
        }

        // Components built for this definition are what a build of this index would write. Those the TOC lists are
        // kept, and so is the build the SSTable's descriptor holds for an index of this definition: DROP INDEX unlisted
        // it while a reader kept its files, and this index loads it. Others, e.g. a generation an earlier rebuild
        // replaced, are discarded, and so is everything while a build or writer of a dropped index of this name may
        // still write or delete these files.
        IndexDescriptor descriptor = IndexDescriptor.empty(sstable.descriptor, sstable.metadata().comparator);
        Set<ComponentsBuildId> listedBuilds = new HashSet<>(listed.values());
        ComponentsBuildId loaded = listedBuilds.isEmpty() ? loadedBuild(sstable) : null;
        Set<ComponentsBuildId> kept = new HashSet<>();
        if (!PerIndexFilesOwner.hasOwnerOfDroppedIndex(sstable.descriptor, name))
        {
            for (ComponentsBuildId buildId : builds)
            {
                boolean used = listedBuilds.contains(buildId) || buildId.equals(loaded);
                if (used && isBuiltForDefinition(descriptor, buildId))
                    kept.add(buildId);
            }
        }
        builds.removeAll(kept);
        if (loaded != null && kept.contains(loaded))
            listKeptBuild(sstable, descriptor, loaded);
        if (builds.isEmpty())
            return true;
        Set<Component> stale = new HashSet<>();
        listed.forEach((component, buildId) -> {
            if (builds.contains(buildId))
                stale.add(component);
        });

        Ref<? extends SSTableReader> ref = sstable.tryRef();
        if (ref == null)
            return true; // gone, and its files with it
        try
        {
            try
            {
                if (!stale.isEmpty())
                    sstable.unregisterComponents(stale, baseCfs.getTracker());
            }
            catch (Throwable t)
            {
                // Deleting the files, completion markers included, is then what keeps them from being used.
                JVMStabilityInspector.inspectThrowable(t);
                logger.warn(indexContext.logMessage("Failed to unregister the components of an index of the same " +
                                                    "name from {}. Deleting their files."),
                            sstable.descriptor, t);
            }

            boolean discarded = true;
            for (ComponentsBuildId buildId : builds)
            {
                // All types, including any the TOC doesn't list: the build of this index would append to them.
                EnumSet<IndexComponentType> types = EnumSet.allOf(IndexComponentType.class);
                IndexComponents.ForWrite components =
                    descriptor.perIndexComponentsOfBuild(indexContext, buildId, types);
                try
                {
                    SSTableWatcher.instance.onIndexDropped(sstable.metadata(), components);
                }
                catch (Throwable t)
                {
                    // The local check below doesn't see storage that isn't local: a failure counts as such
                    JVMStabilityInspector.inspectThrowable(t);
                    logger.warn(indexContext.logMessage("Failed to delete the components of an index of the same " +
                                                        "name from {}"),
                                sstable.descriptor, t);
                    discarded = false;
                }
                discarded &= completionMarkerDeleted(descriptor, buildId);
            }
            if (discarded)
                logger.info(indexContext.logMessage("Discarded the components of an index of the same name on {}"),
                            sstable.descriptor);
            else
                logger.error(indexContext.logMessage("Could not delete the completion marker of the components " +
                                                     "of an index of the same name on {}. " +
                                                     "This index won't be built on this node until they are deleted " +
                                                     "and the index is rebuilt."),
                             sstable.descriptor);
            return discarded;
        }
        finally
        {
            ref.release();
        }
    }

    /**
     * @return the parsed SAI file name, or empty if it isn't one, or is of a version this node doesn't know, which
     * index discovery doesn't read either
     */
    static Optional<Version.ParsedFileName> tryParseFileName(String name, IndexContext context)
    {
        try
        {
            return Version.tryParseFileName(name);
        }
        catch (IllegalArgumentException e)
        {
            logger.warn(context.logMessage("Ignoring SAI file {}: {}"), name, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * @return the build of this index's name that the cached descriptor of the provided SSTable holds, complete, for an
     * index of this definition (that of a dropped index of the same definition), or null
     */
    private @Nullable ComponentsBuildId loadedBuild(SSTableReader sstable)
    {
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(baseCfs);
        if (group == null)
            return null;
        IndexDescriptor cached = group.descriptorFor(sstable);
        if (!cached.includedIndexes().contains(indexContext))
            return null;
        IndexComponents.ForRead components = cached.perIndexComponents(indexContext);
        return components.isComplete() ? components.buildId() : null;
    }

    /**
     * Registers in the TOC the components of the provided build of this index's name on disk.
     */
    private void listKeptBuild(SSTableReader sstable, IndexDescriptor descriptor, ComponentsBuildId buildId)
    {
        EnumSet<IndexComponentType> types = EnumSet.allOf(IndexComponentType.class);
        IndexComponents.ForWrite components = descriptor.perIndexComponentsOfBuild(indexContext, buildId, types);
        Set<Component> kept = components.allAsCustomComponents();
        if (kept.isEmpty())
            return;

        Ref<? extends SSTableReader> ref = sstable.tryRef();
        if (ref == null)
            return;
        try
        {
            Tracker tracker = baseCfs.getTracker();
            sstable.registerComponents(kept, tracker.getView().liveSSTables().contains(sstable) ? tracker : null);
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.warn(indexContext.logMessage("Failed to register the components of this index's definition on {}"),
                        sstable.descriptor, t);
        }
        finally
        {
            ref.release();
        }
    }

    /**
     * Whether the completion marker of the provided build of this index's name holds the digest of this index's
     * definition (see {@link IndexDefinitionDigest}).
     */
    private boolean isBuiltForDefinition(IndexDescriptor descriptor, ComponentsBuildId buildId)
    {
        EnumSet<IndexComponentType> types = EnumSet.of(IndexComponentType.COLUMN_COMPLETION_MARKER);
        IndexComponents.ForWrite marker = descriptor.perIndexComponentsOfBuild(indexContext, buildId, types);
        if (!marker.has(IndexComponentType.COLUMN_COMPLETION_MARKER))
            return false;
        try
        {
            File file = marker.get(IndexComponentType.COLUMN_COMPLETION_MARKER).file();
            return Arrays.equals(IndexDefinitionDigest.readMarker(file), IndexDefinitionDigest.markerContent(config));
        }
        catch (IOException e)
        {
            return false;
        }
    }

    /**
     * Makes sure the completion marker of the provided build of this index's name is not on disk: deletes it if it is.
     *
     * @return whether the completion marker is not on disk
     */
    private boolean completionMarkerDeleted(IndexDescriptor descriptor, ComponentsBuildId buildId)
    {
        EnumSet<IndexComponentType> types = EnumSet.of(IndexComponentType.COLUMN_COMPLETION_MARKER);
        IndexComponents.ForWrite marker = descriptor.perIndexComponentsOfBuild(indexContext, buildId, types);
        if (!marker.has(IndexComponentType.COLUMN_COMPLETION_MARKER))
            return true;
        File file = marker.get(IndexComponentType.COLUMN_COMPLETION_MARKER).file();
        try
        {
            file.delete();
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.warn(indexContext.logMessage("Failed to delete {}"), file, t);
        }
        return !file.exists();
    }

    @Override
    public Callable<?> getInitializationTask()
    {
        IndexBuildDecider.Decision decision = IndexBuildDecider.instance.onInitialBuild();
        // New storage-attached indexes will be available for queries after on disk index data are built.
        // Memtable data will be indexed via flushing triggered by schema change
        // We only want to validate the index files if we are starting up
        boolean isStarting = StorageService.instance.isStarting();

        // If we are starting up and the index is already built for every live SSTable, mark it queryable right away,
        // before gossip starts, so peers never see this node UP with a non-queryable index (CASSANDRA-20732).
        // Not if the index must not be built, as its initial build then fails (see checkCanBuild).
        if (isStarting && !decision.skipped() && cannotBuildReason == null)
        {
            try
            {
                if (tryMarkQueryableWithoutInitialBuild())
                    return () -> null;
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                // This runs synchronously while the table is opened, so don't fail there. The initial build will
                // validate the index again and fail it if it has to.
                logger.warn(indexContext.logMessage("Failed to check whether the index can skip its initial build. " +
                                                    "Falling back to the initial build."), t);
            }
        }

        return () -> {
            checkCanBuild();
            Object result = startInitialBuild(baseCfs, isStarting, decision.skipped()).get();
            if (!decision.skipped())
                buildSSTablesMissedByInitialBuild();
            return result;
        };
    }

    /**
     * Marks this index queryable without an initial build if every live SSTable already has a valid index, and
     * nothing else stands in the way.
     * <p>
     * This is only safe when there is no memtable data: rows written before the index was created are not in the
     * memtable index, and the initial build flushes them so they get indexed. That holds on startup, where this is
     * called before the commit log is replayed. It also checks the memtables, in case something leaves the node in
     * STARTING mode for longer (unit tests, for example).
     *
     * @return {@code true} if the index was marked queryable, {@code false} if the initial build must run as usual
     */
    private boolean tryMarkQueryableWithoutInitialBuild()
    {
        for (Memtable memtable : baseCfs.getTracker().getView().getAllMemtables())
        {
            if (!memtable.isClean())
                return false;
        }

        StorageAttachedIndexGroup indexGroup = StorageAttachedIndexGroup.getIndexGroup(baseCfs);
        assert indexGroup != null : "Index group does not exist for table " + baseCfs.keyspace + '.' + baseCfs.name;

        // Leave version incompatibilities to the initial build, which fails the index with a clear error.
        if (vectorVersionIncompatibility(indexGroup) != null)
            return false;

        if (!isIndexLoadedForAllSSTables(indexGroup))
            return false;

        // CNDB may want the index to stay non-queryable after the initial build.
        if (!IndexBuildDecider.instance.isIndexQueryableAfterInitialBuild(baseCfs))
            return false;

        // From now on, all memtables will have an attached memtable index, so it is safe to flush indexes directly
        // from flushing memtables.
        canFlushFromMemtableIndex = true;
        baseCfs.indexManager.makeIndexQueryable(this, Status.BUILD_SUCCEEDED);
        logger.debug(indexContext.logMessage("Skipping initial build, as the index is already built for all SSTables..."));
        return true;
    }

    /**
     * Loads the index of every live SSTable into the view, validating its components.
     *
     * @return {@code true} if every component was valid and every live SSTable is now in the view, {@code false}
     * otherwise, in which case the initial build must take care of it
     */
    private synchronized boolean isIndexLoadedForAllSSTables(StorageAttachedIndexGroup indexGroup)
    {
        Set<SSTableReader> sstables = baseCfs.getLiveSSTables();
        if (!indexGroup.onSSTableChanged(Collections.emptyList(), sstables, Collections.singleton(this), true).isEmpty())
            return false;

        // Unlike findNonIndexedSSTables, don't trust completion markers on disk: an SSTable that isn't in the view
        // can't be queried, whatever the reason, so the index isn't ready.
        View view = indexContext.getView();
        for (SSTableReader sstable : sstables)
        {
            if (!sstable.isMarkedCompacted() && !view.containsSSTableIndex(sstable.descriptor))
                return false;
        }
        return true;
    }

    /**
     * @return the error message if this is a vector index and either the current on-disk format version or the
     * version of other indexes on the existing SSTables doesn't support vectors, or {@code null} otherwise
     */
    private String vectorVersionIncompatibility(StorageAttachedIndexGroup indexGroup)
    {
        if (!indexContext.isVector())
            return null;

        // The current version must support vectors
        Version currentVersion = indexContext.version();
        if (currentVersion.compareTo(Version.JVECTOR_EARLIEST) < 0)
            return vectorUnsupportedByCurrentVersionError(currentVersion);

        // Also, any pre-existing indexes must be in a version compatible with vectors
        Version minVersion = indexGroup.getMinVersion();
        if (minVersion.compareTo(Version.JVECTOR_EARLIEST) < 0)
            return vectorUnsupportedByExistingVersionError(minVersion);

        return null;
    }

    private void checkCanBuild()
    {
        String reason = cannotBuildReason;
        if (reason != null)
            throw new IllegalStateException(indexContext.logMessage("Cannot build the index: " + reason + ". " +
                                                                    "Rebuild the index once the cause is fixed."));
    }

    /**
     * Builds this index on the sstables that are still without it once its initial build is over, until there are
     * none. The builders skip an sstable they can't reference any more: that's right when a compaction has replaced it
     * (its result has the index, as its writers were created after the initial build stopped the compactions of the
     * table), but not when that compaction was then stopped, which restores its sources as other readers.
     *
     * @throws IllegalStateException if some sstables are still without this index after several attempts
     */
    private void buildSSTablesMissedByInitialBuild() throws Exception
    {
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(baseCfs);
        if (group == null || !group.containsIndex(this))
            return;

        for (int builds = 0; ; builds++)
        {
            // What the index lost so far (e.g. components found invalid as the build loaded them) is missing from its
            // view, and built below: only what it loses from now on fails the initial build.
            baseCfs.indexManager.forgetRunningBuildInvalidations(this);
            // Usually, every sstable is in the view: don't reload the index on all of them to find that out.
            View view = indexContext.getView();
            boolean anyMissing = false;
            for (SSTableReader sstable : baseCfs.getSSTables(SSTableSet.CANONICAL))
                anyMissing |= !view.containsSSTableIndex(sstable.descriptor) && !sstable.isMarkedCompacted();
            if (!anyMissing)
                return;
            if (builds == MAX_BUILDS_OF_MISSED_SSTABLES)
                throw new IllegalStateException(indexContext.logMessage("SSTables are still without this index after its initial build"));

            List<SSTableReader> nonIndexed = findNonIndexedSSTables(baseCfs, group, false);
            if (nonIndexed.isEmpty() || !group.containsIndex(this))
                return;

            logger.info(indexContext.logMessage("Building {} SSTables that are still without this index after its initial build: {}"),
                        nonIndexed.size(), nonIndexed.stream().map(sstable -> sstable.descriptor).collect(Collectors.toList()));
            buildSSTables(group, nonIndexed);
        }
    }

    private void buildSSTables(StorageAttachedIndexGroup group, List<SSTableReader> sstables) throws Exception
    {
        List<ListenableFuture<?>> futures = new ArrayList<>();
        for (List<SSTableReader> sstablesGroup : groupBySize(sstables, DatabaseDescriptor.getConcurrentCompactors()))
        {
            SortedMap<SSTableReader, Set<StorageAttachedIndex>> current = new TreeMap<>(SSTableReader.idComparator);
            sstablesGroup.forEach(sstable -> current.put(sstable, Collections.singleton(this)));
            futures.add(CompactionManager.instance.submitIndexBuild(new StorageAttachedIndexBuilder(group, current, false, true)));
        }
        Futures.allAsList(futures).get();
    }

    private Future<?> startInitialBuild(ColumnFamilyStore baseCfs, boolean validate, boolean skipIndexBuild)
    {
        if (skipIndexBuild)
        {
            logger.info("Skipping initialization task for {}.{} after flushing memtable", baseCfs.metadata(), indexContext.getIndexName());
            // Force another flush to make sure on disk index is generated for memtable data before marking it queryable.
            // In case of offline scrub, there is no live memtables.
            if (!baseCfs.getTracker().getView().liveMemtables.isEmpty())
                baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.INDEX_BUILD_STARTED);

            // even though we're skipping the index build, we still want to add any initial sstables that have indexes into SAI.
            // Index will be queryable if all existing sstables have index files; otherwise non-queryable
            Set<SSTableReader> sstables = baseCfs.getLiveSSTables();
            StorageAttachedIndexGroup indexGroup = StorageAttachedIndexGroup.getIndexGroup(baseCfs);
            indexGroup.onSSTableChanged(Collections.emptyList(), sstables, Collections.singleton(this), validate);

            // From now on, all memtable will have attached memtable index. It is now safe to flush indexes directly from flushing Memtables.
            canFlushFromMemtableIndex = true;
            return CompletableFuture.completedFuture(null);
        }

        if (baseCfs.indexManager.isIndexQueryable(this))
        {
            logger.debug(indexContext.logMessage("Skipping validation and building in initialization task, as pre-join has already made the storage attached index queryable..."));
            canFlushFromMemtableIndex = true;
            return CompletableFuture.completedFuture(null);
        }

        StorageAttachedIndexGroup indexGroup = StorageAttachedIndexGroup.getIndexGroup(baseCfs);
        assert indexGroup != null;

        // verify the compatibility of the on-disk format version with the vector index
        String vectorVersionError = vectorVersionIncompatibility(indexGroup);
        if (vectorVersionError != null)
            throw new FeatureNeedsIndexRebuildException(vectorVersionError);

        // stop in-progress compaction tasks to prevent compacted sstables not being indexed.
        logger.debug(indexContext.logMessage("Stopping active compactions to make sure all sstables are indexed after initial build."));
        CompactionManager.instance.interruptCompactionFor(Collections.singleton(baseCfs.metadata()),
                                                          OperationType.REWRITES_SSTABLES,
                                                          Predicates.alwaysTrue(),
                                                          true,
                                                          TableOperation.StopTrigger.INDEX_BUILD);

        // Force another flush to make sure on disk index is generated for memtable data before marking it queryable.
        // In case of offline scrub, there is no live memtables.
        if (!baseCfs.getTracker().getView().liveMemtables.isEmpty())
        {
            baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.INDEX_BUILD_STARTED);
        }

        // From now on, all memtable will have attached memtable index. It is now safe to flush indexes directly from flushing Memtables.
        canFlushFromMemtableIndex = true;

        List<SSTableReader> nonIndexed = findNonIndexedSSTables(baseCfs, indexGroup, validate);

        if (nonIndexed.isEmpty())
        {
            return CompletableFuture.completedFuture(null);
        }

        // split sorted sstables into groups with similar size and build each group in separate compaction thread
        List<List<SSTableReader>> groups = groupBySize(nonIndexed, DatabaseDescriptor.getConcurrentCompactors());
        List<ListenableFuture<?>> futures = new ArrayList<>();

        for (List<SSTableReader> group : groups)
        {
            SortedMap<SSTableReader, Set<StorageAttachedIndex>> current = new TreeMap<>(SSTableReader.idComparator);
            group.forEach(sstable -> current.put(sstable, Collections.singleton(this)));

            futures.add(CompactionManager.instance.submitIndexBuild(new StorageAttachedIndexBuilder(indexGroup, current, false, true)));
        }

        logger.info(indexContext.logMessage("Submitting {} parallel initial index builds over {} total sstables..."), futures.size(), nonIndexed.size());
        return Futures.allAsList(futures);
    }

    @VisibleForTesting
    public static String vectorUnsupportedByCurrentVersionError(Version currentVersion)
    {
        return String.format("The current configured on-disk format version %s does not support vector indexes. " +
                             "The minimum version that supports vectors is %s. " +
                             "The on-disk format version can be set via the -D%s system property.",
                             currentVersion,
                             Version.JVECTOR_EARLIEST,
                             CassandraRelevantProperties.SAI_CURRENT_VERSION.name());
    }

    @VisibleForTesting
    public static String vectorUnsupportedByExistingVersionError(Version existingVersion)
    {
        return String.format("The current sstables have other indexes using the on-disk format version %s, " +
                             "which is not compatible with vector indexes. " +
                             "The minimum version that supports vectors is %s. " +
                             "The on-disk format version can be set via the -D%s system property, " +
                             "and the indexes on the sstables can be upgraded via nodetool upgradesstables." +
                             "Then it will be needed to rebuild the index after the upgrade.",
                             existingVersion,
                             Version.JVECTOR_EARLIEST,
                             CassandraRelevantProperties.SAI_CURRENT_VERSION.name());
    }

    /**
     * Splits SSTables into groups of similar overall size.
     *
     * @param toRebuild a list of SSTables to split (Note that this list will be sorted in place!)
     * @param parallelism an upper bound on the number of groups
     *
     * @return a {@link List} of SSTable groups, each represented as a {@link List} of {@link SSTableReader}
     */
    @VisibleForTesting
    public static List<List<SSTableReader>> groupBySize(List<SSTableReader> toRebuild, int parallelism)
    {
        List<List<SSTableReader>> groups = new ArrayList<>();

        toRebuild.sort(Comparator.comparingLong(SSTableReader::onDiskLength).reversed());
        Iterator<SSTableReader> sortedSSTables = toRebuild.iterator();
        double dataPerCompactor = toRebuild.stream().mapToLong(SSTableReader::onDiskLength).sum() * 1.0 / parallelism;

        while (sortedSSTables.hasNext())
        {
            long sum = 0;
            List<SSTableReader> current = new ArrayList<>();

            while (sortedSSTables.hasNext() && sum < dataPerCompactor)
            {
                SSTableReader sstable = sortedSSTables.next();
                sum += sstable.onDiskLength();
                current.add(sstable);
            }

            assert !current.isEmpty();
            groups.add(current);
        }

        return groups;
    }

    @Override
    public Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata)
    {
        return null;
    }

    @Override
    public Callable<?> getBlockingFlushTask()
    {
        return null; // storage-attached indexes are flushed alongside memtable
    }

    @Override
    public Callable<?> getInvalidateTask()
    {
        return () ->
        {
            // mark index as invalid, in-progress SSTableIndexWriters will abort
            dropped = true;

            // in case of dropping table, SSTable indexes should already been removed by SSTableListChangedNotification.
            for (SSTableIndex sstableIndex : indexContext.getView().getIndexes())
            {
                var components = sstableIndex.usedPerIndexComponents();
                sstableIndex.getSSTable().unregisterComponents(components.allAsCustomComponents(), baseCfs.getTracker());
            }

            indexContext.invalidate(true);
            return null;
        };
    }

    @Override
    public Callable<?> getUnloadTask()
    {
        return () ->
        {
            // mark index as invalid, in-progress SSTableIndexWriters will abort
            unloaded = true;

            indexContext.invalidate(false);
            return null;
        };
    }

    @Override
    public Callable<?> getPreJoinTask(boolean hadBootstrap)
    {
        /*
         * During bootstrap, streamed SSTable are already built for existing indexes via {@link StorageAttachedIndexBuildingSupport}
         * from {@link org.apache.cassandra.streaming.StreamReceiveTask.OnCompletionRunnable}.
         *
         * For indexes created during bootstrapping, we don't have to block bootstrap for them.
         */

        return this::startPreJoinTask;
    }

    @VisibleForTesting
    public boolean canFlushFromMemtableIndex()
    {
        return canFlushFromMemtableIndex;
    }

    public BooleanSupplier isDropped()
    {
        return () -> dropped;
    }

    public BooleanSupplier isUnloaded()
    {
        return () -> unloaded;
    }

    private Future<?> startPreJoinTask()
    {
        try
        {
            if (baseCfs.indexManager.isIndexQueryable(this))
            {
                logger.debug(indexContext.logMessage("Skipping validation in pre-join task, as the initialization task has already made the index queryable..."));
                baseCfs.indexManager.makeIndexQueryable(this, Status.BUILD_SUCCEEDED);
                return null;
            }

            StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(baseCfs);
            Collection<SSTableReader> nonIndexed = findNonIndexedSSTables(baseCfs, group, true);

            if (nonIndexed.isEmpty() && cannotBuildReason == null)
            {
                // If the index is complete, mark it queryable before the node starts accepting requests:
                baseCfs.indexManager.makeIndexQueryable(this, Status.BUILD_SUCCEEDED);
            }
        }
        catch (Throwable t)
        {
            logger.error(indexContext.logMessage("Failed in pre-join task!"), t);
        }

        return null;
    }

    @Override
    public Callable<?> getTruncateTask(long truncatedAt)
    {
        /*
         * index files will be removed as part of base sstable lifecycle in {@link LogTransaction#delete(java.io.File)}
         * asynchronously, but we need to mark the index queryable because if the truncation is during the initial
         * build of the index it won't get marked queryable by the build. An SSTable kept by the truncation that the
         * index doesn't cover leaves the index as it is.
         */
        return () -> {
            // The index may be dropped while the table is truncated
            StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(baseCfs);
            if (group == null || !group.containsIndex(this))
                return null;

            // Until the initialization task flushes them, the memtables can have rows written before the index was
            // created, which its memtable index doesn't have. That task makes the index queryable once they're flushed.
            if (!canFlushFromMemtableIndex)
                return null;

            // The SSTables may have the components of a dropped index of the same name, which mustn't be loaded
            String reason = cannotBuildReason;
            if (reason != null)
            {
                logger.warn(indexContext.logMessage("Not making index queryable during table truncation: {}"), reason);
                return null;
            }

            List<SSTableReader> nonIndexed;
            try
            {
                synchronized (this)
                {
                    // Checked again once the selection can start (findNonIndexedSSTables is synchronized on this index
                    // too): the SSTables mustn't be loaded for an index that was dropped, or found unbuildable, while
                    // this waited for it
                    if (dropped || !group.containsIndex(this) || cannotBuildReason != null)
                        return null;
                    nonIndexed = findNonIndexedSSTables(baseCfs, group, false);
                }
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                // Failing here would fail the truncation after its SSTables are discarded
                logger.warn(indexContext.logMessage("Not making index queryable during table truncation"), t);
                return null;
            }

            if (!nonIndexed.isEmpty())
            {
                logger.info(indexContext.logMessage("Not making index queryable during table truncation, as the kept SSTables {} are not indexed"),
                            nonIndexed.stream().map(sstable -> sstable.descriptor).collect(Collectors.toList()));
                return null;
            }

            if (dropped || !group.containsIndex(this) || cannotBuildReason != null)
                return null;

            logger.info(indexContext.logMessage("Making index queryable during table truncation"));
            baseCfs.indexManager.makeIndexQueryable(this, Status.BUILD_SUCCEEDED);
            return null;
        };
    }

    @Override
    public boolean shouldBuildBlocking()
    {
        return true;
    }

    @Override
    public Optional<ColumnFamilyStore> getBackingTable()
    {
        return Optional.empty();
    }

    @Override
    public boolean dependsOn(ColumnMetadata column)
    {
        return indexContext.getDefinition().compareTo(column) == 0;
    }

    @Override
    public boolean supportsExpression(ColumnMetadata column, Operator operator)
    {
        return dependsOn(column) && indexContext.supports(operator);
    }

    @Override
    public AbstractType<?> customExpressionValueType()
    {
        return null;
    }

    @Override
    public boolean isAnalyzed()
    {
        return indexContext.isAnalyzed();
    }

    @Override
    public Optional<Analyzer> getAnalyzer(ByteBuffer queriedValue)
    {
        if (!indexContext.isAnalyzed())
            return Optional.empty();

        // memoize the analyzed queried value, so we don't have to re-analyze it for every evaluated column value
        List<ByteBuffer> queriedTokens = analyze(indexContext.getQueryAnalyzerFactory(), queriedValue);

        return Optional.of(new Analyzer() {
            @Override
            public List<ByteBuffer> indexedTokens(ByteBuffer value)
            {
                return analyze(indexContext.getAnalyzerFactory(), value);
            }

            @Override
            public List<ByteBuffer> queriedTokens()
            {
                return queriedTokens;
            }
        });
    }

    private static List<ByteBuffer> analyze(AbstractAnalyzer.AnalyzerFactory factory, ByteBuffer value)
    {
        if (value == null)
            return null;

        List<ByteBuffer> tokens = new ArrayList<>();
        AbstractAnalyzer analyzer = factory.create();
        try
        {
            analyzer.reset(value.duplicate());
            while (analyzer.hasNext())
                tokens.add(analyzer.next());
        }
        finally
        {
            analyzer.end();
        }
        return tokens;
    }

    @Override
    public RowFilter getPostIndexQueryFilter(RowFilter filter)
    {
        // it should be executed from the SAI query plan, this is only used by the singleton index query plan
        throw new UnsupportedOperationException();
    }

    @Override
    public void validate(ReadCommand command) throws InvalidRequestException
    {
        // With range read endpoint grouping (the default), an SAI range read is always sent through messaging, even to
        // this node, and so is a top-k one. A single-partition read, or without endpoint grouping a range read, served
        // by this node's replica alone isn't serialized, and works with larger values.
        boolean serialized = command instanceof PartitionRangeReadCommand
                             && (ENDPOINT_GROUPING_ENABLED || command.isTopK());
        if (serialized)
            validateSerializedValueSizes(command.rowFilter().root);
        validateMapEntrySizes(command.rowFilter().root);

        var indexQueryPlan = command.indexQueryPlan();
        if (indexQueryPlan == null)
            return;

        if (!indexQueryPlan.isTopK())
            return;

        // to avoid overflow HNSW internal data structure and avoid OOM when filtering top-k
        if (command.limits().isUnlimited() || command.limits().count() > MAX_TOP_K)
            throw new InvalidRequestException(String.format("SAI based ORDER BY clause requires a LIMIT that is not greater than %s. LIMIT was %s",
                                                            MAX_TOP_K, command.limits().isUnlimited() ? "NO LIMIT" : command.limits().count()));

        indexContext.validate(command.rowFilter());
    }

    /**
     * Rejects the query values of a range read sent through messaging, on any column, that are larger than
     * {@link FBUtilities#MAX_UNSIGNED_SHORT} bytes, the most the read command can serialize. The whole value of an IN
     * restriction (the packed list) is serialized as one value, and a map entry's key and value separately.
     * <p>
     * Values larger than the index's maximum term size aren't rejected: that limit is a write-time guardrail, which
     * may have been different when the indexed values were written, and such a value can only match nothing.
     */
    private static void validateSerializedValueSizes(RowFilter.FilterElement element)
    {
        for (RowFilter.Expression expression : element.expressions())
        {
            if (expression instanceof RowFilter.SimpleExpression)
            {
                validateValueSize(QUERY_VALUE_TOO_LARGE_ERROR_MESSAGE,
                                  expression.operator() == Operator.IN ? "IN list" : "Query value",
                                  expression.column(), expression.getIndexValue(), FBUtilities.MAX_UNSIGNED_SHORT);
            }
            else if (expression instanceof RowFilter.MapComparisonExpression)
            {
                RowFilter.MapComparisonExpression entry = (RowFilter.MapComparisonExpression) expression;
                validateValueSize(QUERY_VALUE_TOO_LARGE_ERROR_MESSAGE, "Map key", expression.column(),
                                  entry.getMapKey(), FBUtilities.MAX_UNSIGNED_SHORT);
                validateValueSize(QUERY_VALUE_TOO_LARGE_ERROR_MESSAGE, "Map value", expression.column(),
                                  entry.getMapValue(), FBUtilities.MAX_UNSIGNED_SHORT);
            }
        }

        for (RowFilter.FilterElement child : element.children())
            validateSerializedValueSizes(child);
    }

    /**
     * Rejects, in any read that SAI plans, the map entry comparisons whose key or value can't be encoded in a
     * {@link CompositeType} value, which SAI builds for each of them, on any map column, indexed or not: a component
     * length is an unsigned short, so the key and value can have at most 65535 bytes. On an entries index the key is
     * limited to 65534 bytes, as {@code 0xFFFF} is reserved for the length of the first component of a composite.
     */
    private void validateMapEntrySizes(RowFilter.FilterElement element)
    {
        boolean entriesIndex = TypeUtil.isComposite(indexContext.getValidator());
        for (RowFilter.Expression expression : element.expressions())
        {
            if (expression instanceof RowFilter.MapComparisonExpression)
            {
                RowFilter.MapComparisonExpression entry = (RowFilter.MapComparisonExpression) expression;
                validateValueSize(MAP_ENTRY_TOO_LARGE_ERROR_MESSAGE, "Map key", expression.column(),
                                  entry.getMapKey(), FBUtilities.MAX_UNSIGNED_SHORT);
                validateValueSize(MAP_ENTRY_TOO_LARGE_ERROR_MESSAGE, "Map value", expression.column(),
                                  entry.getMapValue(), FBUtilities.MAX_UNSIGNED_SHORT);

                if (entriesIndex && expression.column().equals(indexContext.getDefinition()))
                    validateValueSize(MAP_KEY_TOO_LARGE_FOR_ENTRIES_INDEX_ERROR_MESSAGE, "Map key", expression.column(),
                                      entry.getMapKey(), MAX_COMPOSITE_FIRST_COMPONENT_SIZE);
            }
        }

        for (RowFilter.FilterElement child : element.children())
            validateMapEntrySizes(child);
    }

    private static void validateValueSize(String message, String what, ColumnMetadata column, ByteBuffer value, int max)
    {
        if (value != null && value.remaining() > max)
            throw new InvalidRequestException(String.format(message, what, column.name, value.remaining(), max));
    }

    @Override
    public long getEstimatedResultRows()
    {
        // This is temporary (until proper QueryPlan is integrated into Cassandra)
        // and allows us to prioritize storage-attached indexes if any in the query since they
        // are going to be more efficient, to query and intersect, than built-in indexes.
        // See CNDB-14764 for details.
        // Please remember to update StorageAttachedIndexQueryPlan#getEstimatedResultRows() when changing this.
        return Long.MIN_VALUE;
    }

    @Override
    public boolean isQueryable(Status status)
    {
        return !CassandraRelevantProperties.SAI_INDEX_READS_DISABLED.getBoolean()
            && (status == Status.BUILD_SUCCEEDED || status == Status.UNKNOWN);
    }

    @Override
    public void validate(PartitionUpdate update) throws InvalidRequestException
    {
        if (update.metadata().comparator.size() > 0)
        {
            for (Row row : update.rows())
            {
                if (TypeUtil.hasEmptyFieldEncodedAsNull(update.metadata().comparator, row.clustering()))
                    throw new InvalidRequestException(String.format(EMPTY_CLUSTERING_FIELD_ERROR, indexContext.getIndexName()));
            }
        }

        if (!VALIDATE_TERMS_AT_COORDINATOR)
            return;

        DecoratedKey key = update.partitionKey();
        for (Row row : update.rows())
            indexContext.validate(key, row);
    }


    @Override
    public int getFlushPeriodInMs()
    {
        return indexContext.isVector()
               ? CassandraRelevantProperties.SAI_VECTOR_FLUSH_PERIOD_IN_MILLIS.getInt()
               : CassandraRelevantProperties.SAI_NON_VECTOR_FLUSH_PERIOD_IN_MILLIS.getInt();
    }

    /**
     * This method is called by the startup tasks to find SSTables that don't have indexes. The method is
     * synchronized so that the view is unchanged between validation and the selection of non-indexed SSTables.
     *
     * @return a list SSTables without attached indexes
     * @throws IllegalStateException if the index of some SSTables failed to load although their components are
     * complete, and neither missing nor corrupt: they aren't rebuilt, and the index can't be made queryable
     */
    private synchronized List<SSTableReader> findNonIndexedSSTables(ColumnFamilyStore baseCfs, StorageAttachedIndexGroup group, boolean validate)
    {
        // The canonical set: the sources of a running compaction as they were, rather than the readers whose starts it
        // has moved (which can't be built, as their first keys aren't readable) or its results opened early.
        Set<SSTableReader> sstables = Sets.newHashSet(baseCfs.getSSTables(SSTableSet.CANONICAL));

        // Initialize the SSTable indexes w/ valid existing components (some of them may already be in the view)...
        assert group != null : "Missing index group on " + baseCfs.name;
        if (group.onSSTableChanged(Collections.emptyList(), sstablesNotInView(sstables), Collections.singleton(this), validate).contains(this))
        {
            // Some components were invalid, and have been invalidated. An invalid per-SSTable component fails the
            // whole batch, so the valid SSTables may not have been added to the view either. Load the SSTables that
            // aren't in the view again, now that the invalid components are out of the way: the valid ones are added,
            // and the ones whose components were invalidated are reported as not indexed below, and rebuilt.
            group.onSSTableChanged(Collections.emptyList(), sstablesNotInView(sstables), Collections.singleton(this), validate);
        }

        // ...then identify and rebuild the SSTable indexes that are missing.
        List<SSTableReader> nonIndexed = new ArrayList<>();
        List<SSTableReader> failedToLoad = new ArrayList<>();
        for (SSTableReader sstable : sstablesNotInView(sstables))
        {
            if (sstable.isMarkedCompacted())
                continue;

            // The view doesn't contain the SSTable after the load above. If its components are still complete on
            // disk, they are neither missing nor corrupt (corrupt components are invalidated when they fail to
            // validate or to load), so loading them failed for another reason (e.g. too many open files, out of
            // direct memory, or the mmap limit). Rebuilding wouldn't help, and would replace healthy components.
            if (IndexDescriptor.isIndexBuildCompleteOnDisk(sstable, indexContext))
                failedToLoad.add(sstable);
            else
                nonIndexed.add(sstable);
        }

        if (!failedToLoad.isEmpty())
        {
            String message = String.format("Failed to load the index of %d SSTable(s) whose index components are " +
                                           "complete: %s. They are not rebuilt, as the failure isn't corruption " +
                                           "(see the errors above), and the index stays non-queryable. Once the " +
                                           "cause is fixed, restart the node, or rebuild the index.",
                                           failedToLoad.size(),
                                           failedToLoad.stream().map(s -> s.descriptor.toString())
                                                       .collect(Collectors.joining(", ")));
            throw new IllegalStateException(indexContext.logMessage(message));
        }

        return nonIndexed;
    }

    private List<SSTableReader> sstablesNotInView(Collection<SSTableReader> sstables)
    {
        View view = indexContext.getView();
        List<SSTableReader> notInView = new ArrayList<>();
        for (SSTableReader sstable : sstables)
        {
            if (!view.containsSSTableIndex(sstable.descriptor))
                notInView.add(sstable);
        }
        return notInView;
    }

    private class UpdateIndexer extends IndexerAdapter
    {
        private final DecoratedKey key;
        private final Memtable mt;
        private final WriteContext writeContext;

        UpdateIndexer(DecoratedKey key, Memtable mt, WriteContext writeContext)
        {
            this.key = key;
            this.mt = mt;
            this.writeContext = writeContext;
        }

        @Override
        public void insertRow(Row row)
        {
            indexContext.index(key, row, mt, CassandraWriteContext.fromContext(writeContext).getGroup());
        }

        @Override
        public void updateRow(Row oldRow, Row newRow)
        {
            indexContext.update(key, oldRow, newRow, mt, CassandraWriteContext.fromContext(writeContext).getGroup());
        }

        @Override
        public void partitionDelete(DeletionTime deletionTime)
        {
            // Initialize the memtable index to ensure proper SAI views of the data
            indexContext.initializeMemtableIndex(mt);
        }

        @Override
        public void rangeTombstone(RangeTombstone tombstone)
        {
            // Initialize the memtable index to ensure proper SAI views of the data
            indexContext.initializeMemtableIndex(mt);
        }

        @Override
        public void removeRow(Row row)
        {
            // Initialize the memtable index to ensure proper SAI views of the data
            indexContext.initializeMemtableIndex(mt);
        }
    }

    protected static abstract class IndexerAdapter implements Indexer
    {
        @Override
        public void begin() { }

        @Override
        public void finish() { }
    }

    @Override
    public Searcher searcherFor(ReadCommand command) throws InvalidRequestException
    {
        // searchers should be created from the query plan, this is only used by the singleton index query plan
        throw new UnsupportedOperationException();
    }

    @Override
    public SSTableFlushObserver getFlushObserver(Descriptor descriptor, LifecycleNewTracker tracker)
    {
        throw new UnsupportedOperationException("Storage-attached index flush observers should never be created directly.");
    }

    @Override
    public boolean notifyIndexerAboutRowsInFullyExpiredSSTables()
    {
        // The index components of a fully expired SSTable are deleted together with it, and the index doesn't need to
        // hear about its rows (indexerFor doesn't even return an indexer for compaction), so don't make compaction
        // read the whole SSTable just to drop it.
        return false;
    }

    @Override
    public Indexer indexerFor(DecoratedKey key,
                              RegularAndStaticColumns columns,
                              int nowInSec,
                              WriteContext writeContext,
                              IndexTransaction.Type transactionType,
                              Memtable memtable)
    {
        if (transactionType == IndexTransaction.Type.UPDATE)
        {
            return new UpdateIndexer(key, memtable, writeContext);
        }

        // we are only interested in the data from Memtable
        // everything else is going to be handled by SSTableWriter observers
        return null;
    }

    @Override
    public IndexBuildingSupport getBuildTaskSupport()
    {
        return INDEX_BUILDER_SUPPORT;
    }

    public IndexContext getIndexContext()
    {
        return indexContext;
    }

    @Override
    public String toString()
    {
        return String.format("%s.%s.%s", baseCfs.keyspace.getName(), baseCfs.name, config == null ? "?" : config.name);
    }

    /**
     * Removes this index from the {@link SecondaryIndexManager}'s set of queryable indexes.
     *
     * This usually happens in response to an index writing failure from {@link StorageAttachedIndexWriter}.
     */
    public void makeIndexNonQueryable()
    {
        // a build of the index that is running may have built data the index no longer serves
        baseCfs.indexManager.invalidateRunningBuilds(this);
        baseCfs.indexManager.makeIndexNonQueryable(this, Status.BUILD_FAILED);
        logger.warn(indexContext.logMessage("Storage-attached index is no longer queryable. Please restart this node to repair it."));
    }

    public static boolean isStorageAttachedIndexClass(String className)
    {
        // Subclasses are storage-attached indexes too. A class that can't be found isn't taken for one.
        try
        {
            return StorageAttachedIndex.class.isAssignableFrom(Class.forName(className, false, StorageAttachedIndex.class.getClassLoader()));
        }
        catch (ClassNotFoundException | LinkageError e)
        {
            return false;
        }
    }
}
