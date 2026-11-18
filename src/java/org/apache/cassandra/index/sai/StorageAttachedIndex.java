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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
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
import org.apache.cassandra.gms.DataStaxFeatureSupport;
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
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableStreamRebuildState;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.nodes.IPeerInfo;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import static org.apache.cassandra.config.CassandraRelevantProperties.RANGE_READ_ENDPOINT_GROUPING_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VALIDATE_TERMS_AT_COORDINATOR;
import static org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig.MAX_TOP_K;

public class StorageAttachedIndex implements Index
{
    public static final String NAME = "sai";
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
    public static final String VECTOR_NON_FLOAT_ERROR = "SAI ANN indexes are only allowed on vector columns with float elements";
    public static final String BLOB_INDEX_UNSUPPORTED_BY_NODES_MESSAGE =
    "Cannot create a storage-attached index on the blob values of column %s while there are nodes on a messaging " +
    "version below DS 21 (5.0.10): nodes %s don't support it";
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
        /** Maximum number of blocked sstable names to log when a rebuild is rejected, to keep the message bounded. */
        private static final int MAX_LOGGED_SSTABLES = 16;

        /** Reserves the stream rebuild status of every returned sstable; the builders created by the caller release it. */
        public NavigableMap<SSTableReader, Set<StorageAttachedIndex>> prepareSSTablesToBuild(StorageAttachedIndexGroup group,
                                                       Set<Index> indexes,
                                                       Collection<SSTableReader> sstablesToRebuild,
                                                       boolean isFullRebuild)
        {
            NavigableMap<SSTableReader, Set<StorageAttachedIndex>> sstables = new TreeMap<>(SSTableReader.idComparator);

            // First resolve, without mutating anything, which SSTables each index will rebuild.
            Map<StorageAttachedIndex, Collection<SSTableReader>> targets = new LinkedHashMap<>();
            indexes.stream()
                   .filter((i) -> i instanceof StorageAttachedIndex)
                   .forEach((i) ->
                            {
                                StorageAttachedIndex sai = (StorageAttachedIndex) i;
                                IndexContext indexContext = ((StorageAttachedIndex) i).getIndexContext();

                                // If this is not a full manual index rebuild we can skip SSTables that already have an
                                // attached index. Otherwise, we override any pre-existent index.
                                Collection<SSTableReader> ss = sstablesToRebuild;
                                if (!isFullRebuild)
                                {
                                    ss = sstablesToRebuild.stream()
                                                          .filter(s -> !IndexDescriptor.isIndexBuildCompleteOnDisk(s, indexContext))
                                                          .collect(Collectors.toList());
                                }

                                targets.put(sai, ss);
                            });

            // Reserve the per-sstable rebuild status for every unique target sstable BEFORE preparing (and possibly
            // deleting) any index components. A zero-copy stream reserves the same status when its outgoing file is
            // constructed, so this ensures a rebuild cannot delete/rewrite SAI components underneath an in-flight
            // stream. The returned builder(s) own these reservations and release them when they finish.
            Set<SSTableReader> reserved = new LinkedHashSet<>();
            for (Collection<SSTableReader> ss : targets.values())
            {
                for (SSTableReader sstable : ss)
                {
                    if (reserved.contains(sstable))
                        continue;
                    if (sstable.streamRebuildState().tryBeginRebuild())
                    {
                        reserved.add(sstable);
                    }
                    else
                    {
                        reserved.forEach(s -> s.streamRebuildState().endRebuild());

                        // For debugging, list the SSTables that are blocking this rebuild because they are not in a
                        // NORMAL state (a zero-copy stream is in flight). Cap the list so the message stays bounded.
                        String blockedSSTables = targets.values().stream()
                                                        .flatMap(Collection::stream)
                                                        .distinct()
                                                        .filter(s -> s.streamRebuildState().state() != SSTableStreamRebuildState.State.NORMAL)
                                                        .map(s -> s.descriptor.toString())
                                                        .limit(MAX_LOGGED_SSTABLES)
                                                        .collect(Collectors.joining(", "));
                        throw new RuntimeException(String.format(
                            "Cannot build SAI index while entire SSTable streaming is in progress. " +
                            "Blocked sstable(s) (up to %d shown): %s", MAX_LOGGED_SSTABLES, blockedSSTables));
                    }
                }
            }

            // Now it is safe to prepare the index views/components and assemble the build map.
            try
            {
                targets.forEach((sai, ss) ->
                                {
                                    group.prepareIndexSSTablesForRebuild(ss, sai);

                                    ss.forEach((sstable) ->
                                               {
                                                   Set<StorageAttachedIndex> toBuild = sstables.get(sstable);
                                                   if (toBuild == null) sstables.put(sstable, (toBuild = new HashSet<>()));
                                                   toBuild.add(sai);
                                               });
                                });
            }
            catch (RuntimeException | Error e)
            {
                reserved.forEach(s -> s.streamRebuildState().endRebuild());
                throw e;
            }

            return sstables;
        }

        @Override
        public SecondaryIndexBuilder getIndexBuildTask(ColumnFamilyStore cfs, Set<Index> indexes, Collection<SSTableReader> sstablesToRebuild, boolean isFullRebuild)
        {
            StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
            NavigableMap<SSTableReader, Set<StorageAttachedIndex>> sstables = prepareSSTablesToBuild(group, indexes, sstablesToRebuild, isFullRebuild);
            // the builder owns (and releases) the stream rebuild status reserved by prepareSSTablesToBuild
            return new StorageAttachedIndexBuilder(group, sstables, isFullRebuild, false, true);
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

                // each builder owns (and releases) the stream rebuild status reserved by prepareSSTablesToBuild for
                // its own subset of the sstables; the groups partition the reserved sstables
                builders.add(new StorageAttachedIndexBuilder(indexGroup, current, isFullRebuild, false, true));
            }

            logger.info("Creating {} parallel index builds over {} total sstables for {}...", builders.size(), sstables.size(), cfs.metadata());

            return builders;
        }
    }

    // Used to build indexes on newly added SSTables:
    private static final StorageAttachedIndexBuildingSupport INDEX_BUILDER_SUPPORT = new StorageAttachedIndexBuildingSupport();

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
                                                                        CQL3Type.Native.VARINT, CQL3Type.Native.DECIMAL, CQL3Type.Native.BOOLEAN,
                                                                        CQL3Type.Native.BLOB);

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
     * that an index created on such a column by an earlier version keeps working and doesn't block schema changes.
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
     * Rejects the creation of an index on blob values (CASSANDRA-20012), on a blob column or on the elements, keys,
     * values or entries of a non-frozen collection of blobs, while some node of the cluster may not be able to build
     * it: only DataStax nodes on messaging version DS 21 or later support them (see {@link DataStaxFeatureSupport}).
     * Frozen ({@code FULL}) indexes of blobs aren't affected. Only checked when the index is created, so that an
     * existing index doesn't block other schema changes.
     */
    public static void validateSupportedByAllNodes(TableMetadata table, IndexMetadata index)
    {
        Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(table, index);
        if (target == null || !indexesBlobTerms(TypeUtil.cellValueType(target.left, target.right)))
            return;

        if (DatabaseDescriptor.getNewFeaturePeerVersionCheck() == Config.NewFeaturePeerVersionCheck.none)
            return;

        // The schema reaches every node: the endpoints with a host ID, which include the ring members, and the other
        // peers known to Nodes, such as joining nodes. A node that is down gets it when it comes back, so a node whose
        // version isn't known counts even when it's down.
        Iterable<InetAddressAndPort> nodes = Iterables.concat(StorageService.instance.getTokenMetadataForKeyspace(table.keyspace).getAllEndpoints(),
                                                              Nodes.peers().get()
                                                                   .filter(peer -> !peer.isRemoved())
                                                                   .map(IPeerInfo::getPeerAddressAndPort)
                                                                   .collect(Collectors.toList()));
        Set<InetAddressAndPort> unsupported = DataStaxFeatureSupport.nodesNotSupporting(nodes, true);
        if (!unsupported.isEmpty())
            throw new InvalidRequestException(String.format(BLOB_INDEX_UNSUPPORTED_BY_NODES_MESSAGE, target.left.name, unsupported));
    }

    /**
     * Whether an index on values of the given type has blob terms, i.e. the type is blob or a composite (map entries)
     * with a blob component.
     */
    private static boolean indexesBlobTerms(AbstractType<?> type)
    {
        if (TypeUtil.isBytes(type))
            return true;
        if (TypeUtil.isComposite(type))
            return type.subTypes().stream().anyMatch(TypeUtil::isBytes);
        return false;
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

        // Blob values can't be analyzed, alone or in map entries. Unlike the other non-string types, whose
        // index_analyzer is ignored with a warning, reject any analyzer option, query_analyzer included: blob indexes
        // are new (CASSANDRA-20012), so no existing index has one.
        if (indexesBlobTerms(type) && (AbstractAnalyzer.isAnalyzed(options) || options.containsKey(LuceneAnalyzer.QUERY_ANALYZER)))
            throw new InvalidRequestException("CQL type " + CQL3Type.Native.BLOB + " cannot be analyzed.");

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
            long maxVectorTermSize = IndexContext.maxVectorTermSize();
            if (type.isValueLengthFixed() && maxVectorTermSize < type.valueLengthIfFixed())
            {
                AbstractType<?> elementType = ((VectorType<?>) type).elementType;
                var error = String.format("Vector index created with %s will produce terms of %s, " +
                                          "exceeding the max vector term size of %s. " +
                                          "That sets an implicit limit of %d dimensions for %s vectors.",
                                          type.asCQL3Type(),
                                          FBUtilities.prettyPrintMemory(type.valueLengthIfFixed()),
                                          FBUtilities.prettyPrintMemory(maxVectorTermSize),
                                          maxVectorTermSize / elementType.valueLengthIfFixed(),
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
        if (isStarting && !decision.skipped())
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

        return () -> startInitialBuild(baseCfs, isStarting, decision.skipped()).get();
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

        // If SSTables are being added without this index's components (streamed or imported), leave it to the initial
        // build, which selects them once added
        Boolean loaded =
            baseCfs.indexManager.trySelectSSTablesForInitialBuild(() -> isIndexLoadedForAllSSTables(indexGroup));
        if (!Boolean.TRUE.equals(loaded))
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
            return ImmediateFuture.success(null);
        }

        if (baseCfs.indexManager.isIndexQueryable(this))
        {
            logger.debug(indexContext.logMessage("Skipping validation and building in initialization task, as pre-join has already made the storage attached index queryable..."));
            canFlushFromMemtableIndex = true;
            return ImmediateFuture.success(null);
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

        // Wait for SSTables being added without this index's components (streamed or imported), so they are selected
        List<SSTableReader> nonIndexed = baseCfs.indexManager.selectSSTablesForInitialBuild(() -> findNonIndexedSSTables(baseCfs, indexGroup, validate));

        if (nonIndexed.isEmpty())
        {
            return ImmediateFuture.success(null);
        }

        // split sorted sstables into groups with similar size and build each group in separate compaction thread
        List<List<SSTableReader>> groups = groupBySize(nonIndexed, DatabaseDescriptor.getConcurrentCompactors());
        List<Future<?>> futures = new ArrayList<>();

        for (List<SSTableReader> group : groups)
        {
            SortedMap<SSTableReader, Set<StorageAttachedIndex>> current = new TreeMap<>(SSTableReader.idComparator);
            group.forEach(sstable -> current.put(sstable, Collections.singleton(this)));

            futures.add(CompactionManager.instance.submitIndexBuild(new StorageAttachedIndexBuilder(indexGroup, current, false, true)));
        }

        logger.info(indexContext.logMessage("Submitting {} parallel initial index builds over {} total sstables..."), futures.size(), nonIndexed.size());
        return FutureCombiner.allOf(futures);
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

    @SuppressWarnings("SameReturnValue")
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

            // If SSTables are being added without this index's components (streamed or imported), leave it to the
            // initial build, which selects them once added, to make the index queryable
            Collection<SSTableReader> nonIndexed = baseCfs.indexManager.trySelectSSTablesForInitialBuild(() -> findNonIndexedSSTables(baseCfs, group, true));

            if (nonIndexed != null && nonIndexed.isEmpty())
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
         * build of the index it won't get marked queryable by the build.
         */
        return () -> {
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
    public boolean isSSTableAttached()
    {
        return true;
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
    public boolean filtersMultipleContains()
    {
        return false;
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
            validateSerializedValueSizes(command.rowFilter().root());
        validateMapEntrySizes(command.rowFilter().root());

        var indexQueryPlan = command.indexQueryPlan();
        if (indexQueryPlan == null || !indexQueryPlan.isTopK())
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
    public void validate(PartitionUpdate update, ClientState state) throws InvalidRequestException
    {
        if (!VALIDATE_TERMS_AT_COORDINATOR)
            return;

        DecoratedKey key = update.partitionKey();
        if (indexContext.getDefinition().isStatic())
        {
            Row staticRow = update.staticRow();
            if (!staticRow.isEmpty())
                indexContext.validate(key, staticRow, state);
        }
        else
        {
            for (Row row : update.rows())
                indexContext.validate(key, row, state);
        }
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
        Set<SSTableReader> sstables = baseCfs.getLiveSSTables();

        // Initialize the SSTable indexes w/ valid existing components (the startup fast path may already have loaded
        // some of them)...
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
    public Indexer indexerFor(DecoratedKey key,
                              RegularAndStaticColumns columns,
                              long nowInSec,
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
        baseCfs.indexManager.makeIndexNonQueryable(this, Status.BUILD_FAILED);
        logger.warn(indexContext.logMessage("Storage-attached index is no longer queryable. Please restart this node to repair it."));
    }
}
