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
package org.apache.cassandra.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.LongPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import io.netty.util.concurrent.FastThreadLocal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.statements.SelectOptions;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.db.transform.BasePartitions;
import org.apache.cassandra.db.transform.BaseRows;
import org.apache.cassandra.db.guardrails.Threshold;
import org.apache.cassandra.exceptions.QueryCancelledException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.IndexHints;
import org.apache.cassandra.db.filter.LocalReadSizeTooLargeException;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.db.marshal.Redaction;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PurgeFunction;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.BTreeComplexColumn;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.transform.RTBoundCloser;
import org.apache.cassandra.db.transform.RTBoundValidator;
import org.apache.cassandra.db.transform.RTBoundValidator.Stage;
import org.apache.cassandra.db.transform.StoppingTransformation;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.exceptions.UnknownIndexException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexNotAvailableException;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.SchemaProvider;
import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.read.TrackingRowIterator;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CassandraUInt;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.btree.BTree;

import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.filter;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.db.partitions.UnfilteredPartitionIterators.MergeListener.NOOP;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

/**
 * General interface for storage-engine read commands (common to both range and
 * single partition commands).
 * <p>
 * This contains all the informations needed to do a local read.
 */
public abstract class ReadCommand extends AbstractReadQuery
{
    private static final int TEST_ITERATION_DELAY_MILLIS = CassandraRelevantProperties.TEST_READ_ITERATION_DELAY_MS.getInt();

    protected static final Logger logger = LoggerFactory.getLogger(ReadCommand.class);
    public static final IVersionedSerializer<ReadCommand> serializer = new Serializer();

    /**
     * The key of the custom message parameter that coordinators add to reads whose row filter isn't strict, to tell the
     * replicas that they filter the reconciled rows, so that the replicas can relax their filtering (see
     * {@link ReadCommandVerbHandler#coordinatorAsksForNonStrictFiltering}). Nodes that don't know it ignore it.
     */
    public static final String NON_STRICT_FILTERING_PARAM = "ds.sai.nonstrict";
    private static final byte[] NON_STRICT_FILTERING_PARAM_VALUE = { 1 };

    // Expose the active command running so transitive calls can lookup this command.
    // This is useful for a few reasons, but mainly because the CQL query is here.
    private static final FastThreadLocal<ReadCommand> COMMAND = new FastThreadLocal<>();

    private final Kind kind;

    private final boolean isDigestQuery;
    private final boolean acceptsTransient;
    // if a digest query, the version for which the digest is expected. Ignored if not a digest.
    private int digestVersion;

    private boolean trackWarnings;

    /**
     * Whether this replica must apply the row filter strictly even when {@link RowFilter#isStrict()} is false, because
     * the coordinator might not filter the reconciled rows, or doesn't let the replicas relax the filter. This isn't
     * serialized: commands received from other nodes have it until {@link ReadCommandVerbHandler} finds that their
     * coordinator asks for non-strict filtering.
     */
    private boolean strictFilteringRequired;

    /**
     * Whether this node is missing an index that this command was built with: the index the command names, an index its
     * hints include, or an index for an expression of its row filter that only an index can evaluate (see
     * {@link RowFilter#hasIndexOnlyOperator()}).
     * This isn't serialized: {@link Serializer#deserialize} sets it.
     */
    private boolean missingIndex;

    protected final DataRange dataRange;

    @Nullable
    protected final Index.QueryPlan indexQueryPlan;

    private Supplier<ExecutionInfo> executionInfoSupplier = ExecutionInfo.EMPTY_SUPPLIER;

    protected static abstract class SelectionDeserializer
    {
        public abstract ReadCommand deserialize(DataInputPlus in,
                                                int version,
                                                boolean isDigest,
                                                int digestVersion,
                                                boolean acceptsTransient,
                                                TableMetadata metadata,
                                                long nowInSec,
                                                ColumnFilter columnFilter,
                                                RowFilter rowFilter,
                                                DataLimits limits,
                                                Index.QueryPlan indexQueryPlan) throws IOException;
    }

    protected enum Kind
    {
        SINGLE_PARTITION (SinglePartitionReadCommand.selectionDeserializer),
        PARTITION_RANGE  (PartitionRangeReadCommand.selectionDeserializer),
        MULTI_RANGE      (MultiRangeReadCommand.selectionDeserializer);

        private final SelectionDeserializer selectionDeserializer;

        Kind(SelectionDeserializer selectionDeserializer)
        {
            this.selectionDeserializer = selectionDeserializer;
        }
    }

    protected ReadCommand(Kind kind,
                          boolean isDigestQuery,
                          int digestVersion,
                          boolean acceptsTransient,
                          TableMetadata metadata,
                          long nowInSec,
                          ColumnFilter columnFilter,
                          RowFilter rowFilter,
                          DataLimits limits,
                          Index.QueryPlan indexQueryPlan,
                          boolean trackWarnings,
                          DataRange dataRange)
    {
        super(metadata, nowInSec, columnFilter, rowFilter, limits);
        if (acceptsTransient && isDigestQuery)
            throw new IllegalArgumentException("Attempted to issue a digest response to transient replica");

        this.kind = kind;
        this.isDigestQuery = isDigestQuery;
        this.digestVersion = digestVersion;
        this.acceptsTransient = acceptsTransient;
        this.indexQueryPlan = indexQueryPlan;
        this.trackWarnings = trackWarnings;
        this.dataRange = dataRange;
    }

    public static ReadCommand getCommand()
    {
        return COMMAND.get();
    }

    protected abstract void serializeSelection(DataOutputPlus out, int version) throws IOException;
    protected abstract long selectionSerializedSize(int version);

    /**
     * Throws if this command can't be sent to nodes on the given messaging version. Checked before the command writes
     * anything, as a large message may be flushed to the network while it's being serialized.
     */
    protected void checkSerializable(int version)
    {
    }

    public abstract boolean isLimitedToOnePartition();

    /**
     * Whether this command is a single partition read
     *
     * @return true if the command is a single partition read, false otherwise
     */
    public boolean isSinglePartitionRead()
    {
        return kind == Kind.SINGLE_PARTITION;
    }

    public abstract boolean isRangeRequest();

    /**
     * Creates a new <code>ReadCommand</code> instance with new limits.
     *
     * @param newLimits the new limits
     * @return a new <code>ReadCommand</code> with the updated limits
     */
    public abstract ReadCommand withUpdatedLimit(DataLimits newLimits);

    /**
     * The configured timeout for this command.
     *
     * @return the configured timeout for this command.
     */
    public abstract long getTimeout(TimeUnit unit);

    /**
     * Whether this query is a digest one or not.
     *
     * @return Whether this query is a digest query.
     */
    public boolean isDigestQuery()
    {
        return isDigestQuery;
    }

    /**
     * If the query is a digest one, the requested digest version.
     *
     * @return the requested digest version if the query is a digest. Otherwise, this can return
     * anything.
     */
    public int digestVersion()
    {
        return digestVersion;
    }

    /**
     * Sets the digest version, for when digest for that command is requested.
     * <p>
     * Note that we allow setting this independently of setting the command as a digest query as
     * this allows us to use the command as a carrier of the digest version even if we only call
     * setIsDigestQuery on some copy of it.
     *
     * @param digestVersion the version for the digest is this command is used for digest query..
     * @return this read command.
     */
    public ReadCommand setDigestVersion(int digestVersion)
    {
        this.digestVersion = digestVersion;
        return this;
    }

    /**
     * @return Whether this query expects only a transient data response, or a full response
     */
    public boolean acceptsTransient()
    {
        return acceptsTransient;
    }

    @Override
    public void trackWarnings()
    {
        trackWarnings = true;
    }

    public boolean isTrackingWarnings()
    {
        return trackWarnings;
    }

    /**
     * Requires this replica to apply the row filter strictly, because the coordinator that sent the command might not
     * filter the rows after reconciling them (CASSANDRA-19018).
     */
    public void requireStrictFiltering()
    {
        strictFilteringRequired = true;
    }

    /**
     * Lets this replica relax the row filter if it isn't strict, because the coordinator that sent the command asks
     * for it, and filters the rows after reconciling them (CASSANDRA-19018).
     */
    public void allowNonStrictFiltering()
    {
        strictFilteringRequired = false;
    }

    /**
     * @return {@code true} if this replica must apply the row filter strictly, even if {@link RowFilter#isStrict()}
     * says it doesn't need to
     */
    public boolean isStrictFilteringRequired()
    {
        return strictFilteringRequired;
    }

    /**
     * Index query plan chosen for this query. Can be null.
     *
     * @return index query plan chosen for this query
     */
    @Override
    @Nullable
    public Index.QueryPlan indexQueryPlan()
    {
        return indexQueryPlan;
    }

    /**
     * Returns whether the replicas may return rows that don't match the row filter of this command, because the filter
     * isn't strict (see {@link RowFilter#isStrict()}) and the index query plan relaxes it (CASSANDRA-19018). The
     * coordinator must then filter the rows after reconciling the responses of all the replicas it reads, so it can't
     * return the rows of a single replica when the digests of the responses match.
     *
     * @return {@code true} if the replicas may return rows that don't match the row filter, {@code false} otherwise
     */
    public boolean returnsCandidateRows()
    {
        return indexQueryPlan != null
               && !rowFilter().isStrict()
               && indexQueryPlan.returnsCandidatesForNonStrictFilters();
    }

    /**
     * @return {@code true} if this command uses index-based filtering, {@code false} otherwise
     */
    public boolean usesIndexFiltering()
    {
        return indexQueryPlan != null && indexQueryPlan.usesIndexFiltering();
    }

    @Override
    public boolean isTopK()
    {
        return indexQueryPlan != null && indexQueryPlan.isTopK();
    }

    /**
     * The column filter of this command as it is sent to a node on the given messaging version. Nodes below
     * {@link MessagingService#VERSION_DS_10}, such as Apache Cassandra 5.0 nodes, can't deserialize synthetic columns,
     * so an ANN command is sent to them without them. The coordinator then computes the scores of their rows from the
     * vector column, which the filter has for the ANN restriction (see {@code TopKProcessor}). A BM25 score can't be
     * computed on the coordinator, and those nodes don't know BM25 anyway, so a BM25 command keeps its score.
     */
    public ColumnFilter columnFilter(int version)
    {
        return version < MessagingService.VERSION_DS_10 && !isBM25() ? columnFilter().withoutSyntheticColumns() : columnFilter();
    }

    /**
     * @return {@code true} if this command is a BM25 request, {@code false} otherwise
     */
    public boolean isBM25()
    {
        return indexQueryPlan != null && indexQueryPlan.isBM25();
    }

    /**
     * @return {@code true} if this command only queries a single partition, {@code false} otherwise.
     */
    public abstract boolean isSinglePartition();

    @VisibleForTesting
    public Index.Searcher indexSearcher()
    {
        return indexQueryPlan == null ? null : indexQueryPlan.searcherFor(this);
    }

    @Override
    public ExecutionInfo executionInfo()
    {
        return executionInfoSupplier.get();
    }

    /**
     * The clustering index filter this command to use for the provided key.
     * <p>
     * Note that that method should only be called on a key actually queried by this command
     * and in practice, this will almost always return the same filter, but for the sake of
     * paging, the filter on the first key of a range command might be slightly different.
     *
     * @param key a partition key queried by this command.
     *
     * @return the {@code ClusteringIndexFilter} to use for the partition of key {@code key}.
     */
    public abstract ClusteringIndexFilter clusteringIndexFilter(DecoratedKey key);

    @Override
    public DataRange dataRange()
    {
        return dataRange;
    }

    /**
     * Returns a copy of this command.
     *
     * @return a copy of this command.
     */
    public abstract ReadCommand copy();

    /**
     * Returns a copy of this command with acceptsTransient set to true.
     */
    public ReadCommand copyAsTransientQuery(Replica replica)
    {
        Preconditions.checkArgument(replica.isTransient(),
                                    "Can't make a transient request on a full replica: " + replica);
        return copyAsTransientQuery();
    }

    /**
     * Returns a copy of this command with acceptsTransient set to true.
     */
    public ReadCommand copyAsTransientQuery(Iterable<Replica> replicas)
    {
        if (any(replicas, Replica::isFull))
            throw new IllegalArgumentException("Can't make a transient request on full replicas: " + Iterables.toString(filter(replicas, Replica::isFull)));
        return copyAsTransientQuery();
    }

    protected abstract ReadCommand copyAsTransientQuery();

    /**
     * Returns a copy of this command with isDigestQuery set to true.
     */
    public ReadCommand copyAsDigestQuery(Replica replica)
    {
        Preconditions.checkArgument(replica.isFull(),
                                    "Can't make a digest request on a transient replica " + replica);
        return copyAsDigestQuery();
    }

    /**
     * Returns a copy of this command with isDigestQuery set to true.
     */
    public ReadCommand copyAsDigestQuery(Iterable<Replica> replicas)
    {
        if (any(replicas, Replica::isTransient))
            throw new IllegalArgumentException("Can't make a digest request on a transient replica " + Iterables.toString(filter(replicas, Replica::isTransient)));

        return copyAsDigestQuery();
    }

    protected abstract ReadCommand copyAsDigestQuery();

    @VisibleForTesting
    public abstract UnfilteredPartitionIterator queryStorage(ColumnFamilyStore cfs, ReadExecutionController executionController);

    /**
     * Lowers the oldest unrepaired tombstone of the controller to the min local deletion time of the memtables and
     * unrepaired sstables that may hold the data this command reads.
     */
    protected abstract void updateMinOldestUnrepairedTombstone(ColumnFamilyStore cfs, ReadExecutionController executionController);

    /**
     * Whether the underlying {@code ClusteringIndexFilter} is reversed or not.
     *
     * @return whether the underlying {@code ClusteringIndexFilter} is reversed or not.
     */
    public abstract boolean isReversed();

    public ReadResponse createResponse(UnfilteredPartitionIterator iterator, RepairedDataInfo rdi)
    {
        // validate that the sequence of RT markers is correct: open is followed by close, deletion times for both
        // ends equal, and there are no dangling RT bound in any partition.
        iterator = RTBoundValidator.validate(iterator, Stage.PROCESSED, true);

        return isDigestQuery()
               ? ReadResponse.createDigestResponse(iterator, this)
               : ReadResponse.createDataResponse(iterator, this, rdi);
    }

    public ReadResponse createEmptyResponse()
    {
        UnfilteredPartitionIterator iterator = EmptyIterators.unfilteredPartition(metadata());

        return isDigestQuery()
                ? ReadResponse.createDigestResponse(iterator, this)
                : ReadResponse.createDataResponse(iterator, this, RepairedDataInfo.NO_OP_REPAIRED_DATA_INFO);
    }
    
    public DataLimits.Counter createLimitedCounter(boolean assumeLiveData)
    {
        return limits().newCounter(nowInSec(), assumeLiveData, selectsFullPartition(), metadata().enforceStrictLiveness()).onlyCount();
    }

    public DataLimits.Counter createUnlimitedCounter(boolean assumeLiveData)
    {
        return DataLimits.NONE.newCounter(nowInSec(), assumeLiveData, selectsFullPartition(), metadata().enforceStrictLiveness());
    }

    long indexSerializedSize(int version)
    {
        return null != indexQueryPlan
             ? IndexMetadata.serializer.serializedSize(indexQueryPlan.getFirst().getIndexMetadata(), version)
             : 0;
    }

    public Index getIndex(ColumnFamilyStore cfs)
    {
        return null != indexQueryPlan
             ? indexQueryPlan.getFirst()
             : null;
    }

    static Index.QueryPlan findIndexQueryPlan(TableMetadata table, RowFilter rowFilter)
    {
        if (table.indexes.isEmpty() || rowFilter.isEmpty())
            return null;

        ColumnFamilyStore cfs = Keyspace.openAndGetStore(table);

        return cfs.indexManager.getBestIndexQueryPlanFor(rowFilter);
    }

    @Override
    public void maybeValidateIndexes()
    {
        if (null != indexQueryPlan)
        {
            indexQueryPlan.validate(this);
        }
    }

    @Override
    public void validateSelectOptions(SelectOptions selectOptions, ClientState state)
    {
        selectOptions.validate(state, metadata(), limits().count(), IndexRegistry.obtain(metadata()), indexQueryPlan);
    }

    /**
     * Executes this command on the local host.
     *
     * @param executionController the execution controller spanning this command
     *
     * @return an iterator over the result of executing this command locally.
     */
                                  // iterators created inside the try as long as we do close the original resultIterator), or by closing the result.
    public UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController)
    {
        long startTimeNanos = nanoTime();

        COMMAND.set(this);
        try
        {
            ColumnFamilyStore cfs = Keyspace.openAndGetStore(metadata());
            Index.Searcher searcher = null;

            if (indexQueryPlan != null)
            {
                cfs.indexManager.checkQueryability(indexQueryPlan);

                searcher = indexQueryPlan.searcherFor(this);
                Tracing.trace("Executing read on {}.{} using index{} {}",
                              cfs.metadata.keyspace,
                              cfs.metadata.name,
                              indexQueryPlan.getIndexes().size() == 1 ? "" : "es",
                              indexQueryPlan.getIndexes()
                                            .stream()
                                            .map(i -> i.getIndexMetadata().name)
                                            .collect(Collectors.joining(",")));
            }
            // a read must use all its included indexes, and other indexes may evaluate it differently
            else if (rowFilter().indexHints.includesUnknownIndex
                     || (missingIndex ? rowFilter().hasIndexOnlyOperator() : rowFilter().requiresIndex()))
            {
                throw new IndexNotAvailableException(metadata());
            }

            Context context = Context.from(this);

            // Prepare the monitorable execution info, which will be null if it's deferred to the index
            ReadCommandExecutionInfo executionInfo = setupExecutionInfo(searcher);

            // Searchers lower the controller's value as they read the base table through it. It starts from the sources
            // of the queried partition or range, for searchers that read the base table another way
            if (null != searcher && cfs.onlyPurgeRepairedTombstones())
                updateMinOldestUnrepairedTombstone(cfs, executionController);

            var storageTarget = (null == searcher) ? queryStorage(cfs, executionController)
                                                   : searchStorage(searcher, executionController);

            // The commands of a SinglePartitionReadQuery.Group share the controller.
            boolean mayNeedSkippedCells = executionController.takeMayNeedSkippedCells();

            // Skippable cells are not returned, so they are not counted as read bytes.
            UnfilteredPartitionIterator iterator = Transformation.apply(withoutSkippableCells(storageTarget, cfs, executionController, mayNeedSkippedCells),
                                                                        new TrackingRowIterator(context));
            iterator = RTBoundValidator.validate(iterator, Stage.MERGED, false);

            try
            {
                iterator = withQuerySizeTracking(iterator);
                iterator = maybeSlowDownForTesting(iterator);
                iterator = withQueryCancellation(iterator);
                iterator = withReadObserver(iterator);
                iterator = RTBoundValidator.validate(withoutPurgeableTombstones(iterator, cfs, executionController), Stage.PURGED, false);
                iterator = withMetricsRecording(iterator, cfs.metric, startTimeNanos, executionController);
                if (executionInfo != null)
                    iterator = executionInfo.countFetched(iterator, nowInSec());

                // If we've used a 2ndary index, we know the result already satisfy the primary expression used, so
                // no point in checking it again.
                RowFilter filter = (null == searcher) ? rowFilter() : indexQueryPlan.postIndexQueryFilter();

                /*
                 * TODO: We'll currently do filtering by the rowFilter here because it's convenient. However,
                 * we'll probably want to optimize by pushing it down the layer (like for dropped columns) as it
                 * would be more efficient (the sooner we discard stuff we know we don't care, the less useless
                 * processing we do on it).
                 */
                iterator = filter.filter(iterator, nowInSec());

                // GROUP BY limits can read a row that they don't return, RTBoundCloser needs to know about it
                RTBoundCloser.LastReadRow lastReadRow = null;
                if (limits().isGroupByLimit())
                {
                    lastReadRow = new RTBoundCloser.LastReadRow();
                    iterator = Transformation.apply(iterator, lastReadRow);
                }

                // apply the limits/row counter; this transformation is stopping and would close the iterator as soon
                // as the count is observed; if that happens in the middle of an open RT, its end bound will not be included.
                // If tracking repaired data, the counter is needed for overreading repaired data, otherwise we can
                // optimise the case where this.limit = DataLimits.NONE which skips an unnecessary transform
                if (executionController.isTrackingRepairedStatus())
                {
                    DataLimits.Counter limit =
                    limits().newCounter(nowInSec(), false, selectsFullPartition(), metadata().enforceStrictLiveness());
                    iterator = limit.applyTo(iterator);
                    // ensure that a consistent amount of repaired data is read on each replica. This causes silent
                    // overreading from the repaired data set, up to limits(). The extra data is not visible to
                    // the caller, only iterated to produce the repaired data digest.
                    iterator = executionController.getRepairedDataInfo().extend(iterator, limit);
                }
                else
                {
                    iterator = limits().filter(iterator, nowInSec(), selectsFullPartition());
                }

                // because of the above, we need to append an artifical end bound if the source iterator was stopped short by a counter.
                iterator = RTBoundCloser.close(iterator, lastReadRow);

                if (executionInfo != null)
                    iterator = executionInfo.countReturned(iterator, nowInSec());

                return iterator;
            }
            catch (RuntimeException | Error e)
            {
                iterator.close();
                throw e;
            }
        }
        finally
        {
            COMMAND.set(null);
        }
    }

    public UnfilteredPartitionIterator withReadObserver(UnfilteredPartitionIterator partitions)
    {
        ReadObserver observer = ReadObserverFactory.instance.create(this.metadata());

        // skip if observer is disabled
        if (observer == ReadObserver.NO_OP)
            return partitions;

        class ReadObserverTransformation extends Transformation<UnfilteredRowIterator>
        {
            @Override
            protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
            {
                observer.onPartition(partition.partitionKey(), partition.partitionLevelDeletion());
                return Transformation.apply(partition, this);
            }

            @Override
            protected Row applyToStatic(Row row)
            {
                if (!row.isEmpty())
                    observer.onStaticRow(row);
                return row;
            }

            @Override
            protected Row applyToRow(Row row)
            {
                observer.onUnfiltered(row);
                return row;
            }

            @Override
            protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
            {
                observer.onUnfiltered(marker);
                return marker;
            }

            @Override
            protected void onClose()
            {
                observer.onComplete();
            }
        }

        return Transformation.apply(partitions, new ReadObserverTransformation());
    }

    public UnfilteredPartitionIterator searchStorage(Index.Searcher searcher, ReadExecutionController executionController)
    {
        return searcher.search(executionController);
    }

    protected abstract void recordReadRequest(TableMetrics metric);
    protected abstract void recordReadLatency(TableMetrics metric, long latencyNanos);

    public ReadExecutionController executionController(boolean trackRepairedStatus)
    {
        return ReadExecutionController.forCommand(this, trackRepairedStatus);
    }

    /**
     * Allow to post-process the result of the query after it has been reconciled on the coordinator
     * but before it is passed to the CQL layer to return the ResultSet.
     *
     * See CASSANDRA-8717 for why this exists.
     */
    public PartitionIterator postReconciliationProcessing(PartitionIterator result)
    {
        return indexQueryPlan == null ? result : indexQueryPlan.postProcessor(this).apply(result);
    }

    @Override
    public PartitionIterator executeInternal(ReadExecutionController controller)
    {
        return postReconciliationProcessing(UnfilteredPartitionIterators.filter(executeLocally(controller), nowInSec()));
    }

    public ReadExecutionController executionController()
    {
        return ReadExecutionController.forCommand(this, false);
    }

    /**
     * Whether tombstone guardrail ({@link Guardrails#scannedTombstones} should be respected for this query.
     *
     * @return {@code true} if the tombstone thresholds should be respected for the query. If {@code false}, no
     * tombstone warning will ever be logged, and the query will never fail due to tombstones.
     */
    protected boolean shouldRespectTombstoneThresholds()
    {
        return !SchemaConstants.isLocalSystemKeyspace(ReadCommand.this.metadata().keyspace);
    }

    /**
     * The number of tombstones that the read counts for a row, static or not, or a range tombstone marker
     * ({@link #withMetricsRecording}): one for each cell of a row that isn't live, or, for a row without such cells and
     * without live data, one for its deletion or expired primary key liveness; one for a marker.
     */
    public static int countedTombstones(Unfiltered unfiltered, long nowInSec, boolean enforceStrictLiveness)
    {
        if (!unfiltered.isRow())
            return 1;

        Row row = (Row) unfiltered;
        return countedRowTombstones(row, nowInSec, row.hasLiveData(nowInSec, enforceStrictLiveness));
    }

    private static int countedRowTombstones(Row row, long nowInSec, boolean hasLiveData)
    {
        int tombstones = 0;
        for (Cell<?> cell : row.cells())
            if (!cell.isLive(nowInSec))
                tombstones++;

        // a row without tombstone cells counts one for its primary key deletion
        if (tombstones == 0
            && !hasLiveData
            && !row.primaryKeyLivenessInfo().isLive(nowInSec)
            && row.hasDeletion(nowInSec))
            tombstones = 1;

        return tombstones;
    }

    /**
     * Wraps the provided iterator so that metrics on what is scanned by the command are recorded.
     * This also log warning/trow TombstoneOverwhelmingException if appropriate.
     * <p>
     * It also counts the tombstones that an index searcher scans and doesn't return
     * ({@link ReadExecutionController#scannedTombstones()}), so that the thresholds and metrics see one total.
     */
    private UnfilteredPartitionIterator withMetricsRecording(UnfilteredPartitionIterator iter,
                                                             final TableMetrics metric,
                                                             final long startTimeNanos,
                                                             ReadExecutionController executionController)
    {
        class MetricRecording extends Transformation<UnfilteredRowIterator> implements ReadExecutionController.ScannedTombstones
        {
            private final boolean enforceStrictLiveness = metadata().enforceStrictLiveness();

            private int liveRows = 0;
            private int lastReportedLiveRows = 0;
            private final Threshold.GuardedCounter tombstones = createTombstoneCounter();
            private long lastReportedTombstones = 0;

            private DecoratedKey currentKey;
            private boolean inPartition;

                // tombstones dropped by the searcher for a partition the read hasn't seen yet: reported with that
                // partition if the searcher returns it, otherwise on their own
            private DecoratedKey droppedKey;
            private long droppedTombstones;

            private Threshold.GuardedCounter createTombstoneCounter()
            {
                Threshold guardrail = shouldRespectTombstoneThresholds()
                                                ? Guardrails.scannedTombstones
                                                : Threshold.NEVER_TRIGGERED;
                return guardrail.newCounter(() -> ReadCommand.this.toCQLString(Redaction.REDACT), false, null);
            }

            private MetricRecording()
            {
                recordReadRequest(metric);
                executionController.setScannedTombstones(this);
            }

            @Override
            public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator iter)
            {
                if (droppedKey != null)
                {
                    if (droppedKey.equals(iter.partitionKey()))
                    {
                        // dropped before the searcher returned the partition: reported with it
                        droppedKey = null;
                        droppedTombstones = 0;
                    }
                    else
                    {
                        reportDropped();
                    }
                }
                currentKey = iter.partitionKey();
                inPartition = true;
                return Transformation.apply(iter, this);
            }

            @Override
            public Row applyToStatic(Row row)
            {
                return applyToRow(row);
            }

            @Override
            public Row applyToRow(Row row)
            {
                boolean hasLiveData = row.hasLiveData(ReadCommand.this.nowInSec(), enforceStrictLiveness);
                int rowTombstones = countedRowTombstones(row, ReadCommand.this.nowInSec(), hasLiveData);
                for (int i = 0; i < rowTombstones; i++)
                    countTombstone(currentKey, row.clustering());

                if (hasLiveData)
                    ++liveRows;

                return row;
            }

            @Override
            public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
            {
                countTombstone(currentKey, marker.clustering());
                return marker;
            }

            @Override
            public void countDropped(DecoratedKey partitionKey, Unfiltered dropped)
            {
                int droppedNow = countedTombstones(dropped, ReadCommand.this.nowInSec(), enforceStrictLiveness);
                if (droppedNow == 0)
                    return;

                if (!inPartition || !partitionKey.equals(currentKey))
                {
                    if (droppedKey != null && !droppedKey.equals(partitionKey))
                        reportDropped();
                    droppedKey = partitionKey;
                    droppedTombstones += droppedNow;
                }
                for (int i = 0; i < droppedNow; i++)
                    countTombstone(partitionKey, dropped.clustering());
            }

            @Override
            public void checkHeld(DecoratedKey partitionKey, Unfiltered held, long heldTombstones)
            {
                try
                {
                    tombstones.checkIncrement(heldTombstones);
                }
                catch (InvalidRequestException e)
                {
                    throw failOnTombstones(partitionKey, held.clustering());
                }
            }

            private void countTombstone(DecoratedKey partitionKey, ClusteringPrefix<?> clustering)
            {
                try
                {
                    tombstones.add(1);
                }
                catch (InvalidRequestException e)
                {
                    throw failOnTombstones(partitionKey, clustering);
                }
            }

            private TombstoneOverwhelmingException failOnTombstones(DecoratedKey partitionKey, ClusteringPrefix<?> clustering)
            {
                metric.tombstoneFailures.inc();
                return new TombstoneOverwhelmingException(tombstones.get(),
                                                          ReadCommand.this.toCQLString(Redaction.REDACT),
                                                          ReadCommand.this.metadata(),
                                                          partitionKey,
                                                          clustering);
            }

            /**
             * Reports the tombstones that the searcher dropped of a partition that it didn't return.
             */
            private void reportDropped()
            {
                if (droppedTombstones > 0)
                    metric.topReadPartitionTombstoneCount.addSample(droppedKey.getKey(), droppedTombstones);
                lastReportedTombstones += droppedTombstones;
                droppedKey = null;
                droppedTombstones = 0;
            }

            @Override
            protected void onPartitionClose()
            {
                int lr = liveRows - lastReportedLiveRows;
                // what was dropped of another partition meanwhile is that partition's
                long ts = tombstones.get() - droppedTombstones - lastReportedTombstones;

                if (lr > 0)
                    metric.topReadPartitionRowCount.addSample(currentKey.getKey(), lr);

                if (ts > 0)
                    metric.topReadPartitionTombstoneCount.addSample(currentKey.getKey(), ts);

                lastReportedLiveRows = liveRows;
                lastReportedTombstones = tombstones.get() - droppedTombstones;
                inPartition = false;
            }

            @Override
            public void onClose()
            {
                if (droppedKey != null)
                    reportDropped();

                recordReadLatency(metric, nanoTime() - startTimeNanos);

                metric.incLiveRows(liveRows);
                metric.incTombstones(tombstones.get(), tombstones.checkAndTriggerWarning());

                Tracing.trace("Read {} live rows and {} tombstone ones", liveRows, tombstones.get());
            }
        }

        return Transformation.apply(iter, new MetricRecording());
    }

    private boolean shouldTrackSize(DataStorageSpec.LongBytesBound warnThresholdBytes, DataStorageSpec.LongBytesBound abortThresholdBytes)
    {
        return trackWarnings
               && !SchemaConstants.isSystemKeyspace(metadata().keyspace)
               && !(warnThresholdBytes == null && abortThresholdBytes == null);
    }

    private UnfilteredPartitionIterator withQuerySizeTracking(UnfilteredPartitionIterator iterator)
    {
        DataStorageSpec.LongBytesBound warnThreshold = DatabaseDescriptor.getLocalReadSizeWarnThreshold();
        DataStorageSpec.LongBytesBound failThreshold = DatabaseDescriptor.getLocalReadSizeFailThreshold();
        if (!shouldTrackSize(warnThreshold, failThreshold))
            return iterator;
        final long warnBytes = warnThreshold == null ? -1 : warnThreshold.toBytes();
        final long failBytes = failThreshold == null ? -1 : failThreshold.toBytes();
        class QuerySizeTracking extends Transformation<UnfilteredRowIterator>
        {
            private long sizeInBytes = 0;

            @Override
            public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator iter)
            {
                sizeInBytes += ObjectSizes.sizeOnHeapOf(iter.partitionKey().getKey());
                return Transformation.apply(iter, this);
            }

            @Override
            protected Row applyToStatic(Row row)
            {
                if (row == Rows.EMPTY_STATIC_ROW)
                    return row;

                return applyToRow(row);
            }

            @Override
            protected Row applyToRow(Row row)
            {
                addSize(row.unsharedHeapSize());
                return row;
            }

            @Override
            protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
            {
                addSize(marker.unsharedHeapSize());
                return marker;
            }

            @Override
            protected DeletionTime applyToDeletion(DeletionTime deletionTime)
            {
                addSize(deletionTime.unsharedHeapSize());
                return deletionTime;
            }

            private void addSize(long size)
            {
                this.sizeInBytes += size;
                if (failBytes != -1 && this.sizeInBytes >= failBytes)
                {
                    String msg = String.format("Query %s attempted to read %d bytes but max allowed is %s; query aborted  (see local_read_size_fail_threshold)",
                                               ReadCommand.this.toCQLString(Redaction.REDACT), this.sizeInBytes, failThreshold);
                    Tracing.trace(msg);
                    MessageParams.remove(ParamType.LOCAL_READ_SIZE_WARN);
                    MessageParams.add(ParamType.LOCAL_READ_SIZE_FAIL, this.sizeInBytes);
                    throw new LocalReadSizeTooLargeException(msg);
                }
                else if (warnBytes != -1 && this.sizeInBytes >= warnBytes)
                {
                    MessageParams.add(ParamType.LOCAL_READ_SIZE_WARN, this.sizeInBytes);
                }
            }

            @Override
            protected void onClose()
            {
                ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(metadata().id);
                if (cfs != null)
                    cfs.metric.localReadSize.update(sizeInBytes);
            }
        }

        iterator = Transformation.apply(iterator, new QuerySizeTracking());
        return iterator;
    }

    private class QueryCancellationChecker extends StoppingTransformation<UnfilteredRowIterator>
    {
        long lastCheckedAt = 0;

        @Override
        protected void attachTo(BasePartitions partitions)
        {
            Preconditions.checkArgument(this.partitions == null || this.partitions == partitions,
                                        "Attempted to attach 2nd different BasePartitions in StoppingTransformation; this is a bug.");
            this.partitions = partitions;
        }

        @Override
        protected void attachTo(BaseRows rows)
        {
            Preconditions.checkArgument(this.rows == null || this.rows == rows,
                                        "Attempted to attach 2nd different BaseRows in StoppingTransformation; this is a bug.");
            this.rows = rows;
        }

        @Override
        protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            maybeCancel();
            return Transformation.apply(partition, this);
        }

        @Override
        protected Row applyToRow(Row row)
        {
            maybeCancel();
            return row;
        }

        private void maybeCancel()
        {
            /*
             * The value returned by approxTime.now() is updated only every
             * {@link org.apache.cassandra.utils.MonotonicClock.SampledClock.CHECK_INTERVAL_MS}, by default 2 millis.
             * Since MonitorableImpl relies on approxTime, we don't need to check unless the approximate time has elapsed.
             */
            if (lastCheckedAt == approxTime.now())
                return;
            lastCheckedAt = approxTime.now();

            if (isAborted())
            {
                stop();
                throw new QueryCancelledException(ReadCommand.this);
            }
        }
    }

    private UnfilteredPartitionIterator withQueryCancellation(UnfilteredPartitionIterator iter)
    {
        return Transformation.apply(iter, new QueryCancellationChecker());
    }

    /**
     *  A transformation used for simulating slow queries by tests.
     */
    @VisibleForTesting
    private static class DelayInjector extends Transformation<UnfilteredRowIterator>
    {
        @Override
        protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            FBUtilities.sleepQuietly(TEST_ITERATION_DELAY_MILLIS);
            return Transformation.apply(partition, this);
        }

        @Override
        protected Row applyToRow(Row row)
        {
            FBUtilities.sleepQuietly(TEST_ITERATION_DELAY_MILLIS);
            return row;
        }
    }

    private UnfilteredPartitionIterator maybeSlowDownForTesting(UnfilteredPartitionIterator iter)
    {
        if (TEST_ITERATION_DELAY_MILLIS > 0 && !SchemaConstants.isSystemKeyspace(metadata().keyspace))
            return Transformation.apply(iter, new DelayInjector());
        else
            return iter;
    }

    /**
     * Creates a message for this command.
     */
    public Message<ReadCommand> createMessage(boolean trackRepairedData, Dispatcher.RequestTime requestTime)
    {
        List<MessageFlag> flags = new ArrayList<>(3);
        flags.add(MessageFlag.CALL_BACK_ON_FAILURE);
        if (trackWarnings)
            flags.add(MessageFlag.TRACK_WARNINGS);
        if (trackRepairedData)
            flags.add(MessageFlag.TRACK_REPAIRED_DATA);

        Message<ReadCommand> message = Message.outWithFlags(verb(),
                                                            this,
                                                            requestTime,
                                                            flags);
        return rowFilter().isStrict() ? message : withNonStrictFilteringParam(message);
    }

    /**
     * Adds {@link #NON_STRICT_FILTERING_PARAM} to the custom parameters of the given message, to tell the replicas
     * that this coordinator filters the reconciled rows of its non-strict reads (see
     * {@link ReadCommandVerbHandler#coordinatorAsksForNonStrictFiltering}).
     */
    @VisibleForTesting
    static Message<ReadCommand> withNonStrictFilteringParam(Message<ReadCommand> message)
    {
        Map<String, byte[]> existing = message.header.customParams();
        Map<String, byte[]> customParams = existing == null ? new HashMap<>() : new HashMap<>(existing);
        customParams.put(NON_STRICT_FILTERING_PARAM, NON_STRICT_FILTERING_PARAM_VALUE);
        return message.withParam(ParamType.CUSTOM_MAP, customParams);
    }

    protected abstract boolean intersects(SSTableReader sstable);

    protected boolean hasRequiredStatics(SSTableReader sstable) {
        // If some static columns are queried, we should always include the sstable: the clustering values stats of the sstable
        // don't tell us if the sstable contains static values in particular.
        return !columnFilter().fetchedColumns().statics.isEmpty() && sstable.header.hasStatic();
    }

    protected boolean hasPartitionLevelDeletions(SSTableReader sstable)
    {
        return sstable.getSSTableMetadata().hasPartitionLevelDeletions;
    }

    public abstract Verb verb();

    /**
     * The filter of the sstables of a read. When none of the sstables can hold a primary key liveness info that is not
     * live (no TTL, or nothing expired or deleted at {@code nowInSec}), the primary key liveness info of each row
     * merged from them is live, or is shadowed by a deletion that also shadows the cells older than it.
     * {@link #withoutSkippableCells} then removes from the merged rows the fetched but not queried simple cells older
     * than the primary key liveness info of their row in any source, so the sstables can skip them.
     * <p>
     * Memtable data can bring a liveness info that is not live, so the sstables of a read with memtable data only
     * skip those cells if the caller reads again each merged row that has one, as {@link #withoutSkippableCells}
     * does for {@link #queryStorage}. They don't when a memtable holds expired or deleted data, as many rows could
     * then be read again.
     *
     * @param memtables the memtables whose data the read merges with the sstables
     */
    ColumnFilter sstableColumnFilter(List<SSTableReader> sstables, Iterable<Memtable> memtables, ReadExecutionController controller)
    {
        ColumnFilter filter = columnFilter();
        if (filter.allFetchedColumnsAreQueried() || controller.isTrackingRepairedStatus())
            return filter;

        for (SSTableReader sstable : sstables)
        {
            int maxTTL = sstable.getMaxTTL();
            // a view's expired liveness info is never live, whatever its local expiration time
            if (maxTTL == LivenessInfo.EXPIRED_LIVENESS_TTL
                || (maxTTL != LivenessInfo.NO_TTL && sstable.getMinLocalDeletionTime() <= nowInSec()))
                return filter;
        }

        boolean withMemtableData = false;
        for (Memtable memtable : memtables)
        {
            if (memtable.getMinLocalDeletionTime() <= nowInSec())
                return filter;
            withMemtableData = true;
        }

        if (withMemtableData)
            controller.setMayNeedSkippedCells();
        return filter.withNotQueriedCellsOlderThanRowDropped();
    }

    /**
     * Removes the cells of fetched but not queried columns (see {@link ColumnFilter}) that are older than the primary
     * key liveness info of their row: live cells only if that info is live (CASSANDRA-10657), other cells in any case.
     * A complex column that isn't queried at all only loses its cells that are not live. It runs on the merged rows,
     * as an expired liveness info of one source must not hide older live cells of another.
     * <p>
     * A merged row whose primary key liveness info is not live is first read again from all its sources when the
     * sstables of the read skipped cells that it may need (see {@link #sstableColumnFilter}). That read updates the
     * table's single partition read metrics like any other.
     */
    @VisibleForTesting
    UnfilteredPartitionIterator withoutSkippableCells(UnfilteredPartitionIterator iterator)
    {
        return withoutSkippableCells(iterator, null, null, false);
    }

    private UnfilteredPartitionIterator withoutSkippableCells(UnfilteredPartitionIterator iterator,
                                                              @Nullable ColumnFamilyStore cfs,
                                                              @Nullable ReadExecutionController executionController,
                                                              boolean mayNeedSkippedCells)
    {
        ColumnFilter columnFilter = columnFilter();
        if (columnFilter.allFetchedColumnsAreQueried())
            return iterator;

        long nowInSec = nowInSec();
        boolean enforceStrictLiveness = metadata().enforceStrictLiveness();
        boolean rereadsRows = mayNeedSkippedCells && !enforceStrictLiveness;

        class WithoutSkippableCells extends Transformation<UnfilteredRowIterator>
        {
            DecoratedKey partitionKey;

            @Override
            protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
            {
                partitionKey = partition.partitionKey();
                return Transformation.apply(partition, this);
            }

            @Override
            protected Row applyToRow(Row row)
            {
                LivenessInfo liveness = row.primaryKeyLivenessInfo();
                if (liveness.isEmpty())
                    return row;

                if (rereadsRows && !liveness.isLive(nowInSec))
                {
                    row = reread(row);
                    liveness = row.primaryKeyLivenessInfo();
                    if (liveness.isEmpty())
                        return row;
                }

                long timestamp = liveness.timestamp();
                boolean isBTreeRow = row instanceof BTreeRow;
                // Only the cells older than the primary key liveness info are removed.
                if (isBTreeRow && !hasCellOlderThan(row, timestamp))
                    return row;

                boolean isLive = enforceStrictLiveness || liveness.isLive(nowInSec);
                // Row#transformAndFilter gives the cell function of a TrieBackedRow CellData without column or path,
                // so other rows are converted to a BTreeRow, only when they have a cell to remove.
                if (!isBTreeRow && !hasSkippableCell(row, timestamp, isLive))
                    return row;

                BTreeRow btreeRow = BTreeRow.asBTreeRow(row);
                Row filtered = btreeRow.transformAndFilterColumns(btreeRow.primaryKeyLivenessInfo(), btreeRow.deletion(), cd -> {
                    ColumnMetadata column = cd.column();
                    if (column.isComplex())
                    {
                        BTreeComplexColumn complex = (BTreeComplexColumn) cd;
                        if (!columnFilter.fetchedColumnIsQueried(column))
                            return complex.transformAndFilter(complex.complexDeletion(), cell -> isSkippable(cell, timestamp, false) ? null : cell);

                        ColumnFilter.Tester tester = columnFilter.newTester(column);
                        if (tester == null)
                            return cd;

                        return complex.transformAndFilter(complex.complexDeletion(),
                                                          cell -> isSkippable(cell, timestamp, isLive) && !tester.fetchedCellIsQueried(cell.path())
                                                                  ? null : cell);
                    }

                    return isSkippable((Cell<?>) cd, timestamp, isLive) && !columnFilter.fetchedColumnIsQueried(column) ? null : cd;
                });
                return filtered == btreeRow ? row : filtered;
            }

            // The row read again from all its sources, by name, so that the sstables keep the cells this row may need.
            private Row reread(Row row)
            {
                ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(FBUtilities.singleton(row.clustering(), metadata().comparator), false);
                SinglePartitionReadCommand command = SinglePartitionReadCommand.create(metadata(), nowInSec, partitionKey, columnFilter, filter);
                try (UnfilteredRowIterator partition = command.queryMemtableAndDisk(cfs, executionController))
                {
                    while (partition.hasNext())
                    {
                        Unfiltered unfiltered = partition.next();
                        if (unfiltered.isRow())
                            return (Row) unfiltered;
                    }
                }
                return row;
            }

            private boolean hasSkippableCell(Row row, long rowTimestamp, boolean isRowLivenessLive)
            {
                for (Cell<?> cell : row.cells())
                {
                    ColumnMetadata column = cell.column();
                    boolean isQueried = columnFilter.fetchedColumnIsQueried(column);
                    if (column.isComplex() && !isQueried)
                    {
                        if (isSkippable(cell, rowTimestamp, false))
                            return true;
                    }
                    else if (isSkippable(cell, rowTimestamp, isRowLivenessLive)
                             && !(column.isComplex() ? columnFilter.fetchedCellIsQueried(column, cell.path()) : isQueried))
                    {
                        return true;
                    }
                }
                return false;
            }

            private boolean isSkippable(Cell<?> cell, long rowTimestamp, boolean isRowLivenessLive)
            {
                return cell.timestamp() < rowTimestamp && (isRowLivenessLive || !cell.isLive(nowInSec));
            }
        }
        return Transformation.apply(iterator, new WithoutSkippableCells());
    }

    private static boolean hasCellOlderThan(Row row, long timestamp)
    {
        return row.accumulate(ReadCommand::stopAtColumnDataOlderThan, timestamp) == BTree.STOP_SENTINEL_VALUE;
    }

    private static long stopAtColumnDataOlderThan(ColumnData data, long timestamp)
    {
        if (data.column().isSimple())
            return stopAtCellOlderThan((Cell<?>) data, timestamp);
        return ((ComplexColumnData) data).accumulate(ReadCommand::stopAtCellOlderThan, timestamp);
    }

    private static long stopAtCellOlderThan(Cell<?> cell, long timestamp)
    {
        return cell.timestamp() < timestamp ? BTree.STOP_SENTINEL_VALUE : timestamp;
    }

    // Skip purgeable tombstones. We do this because it's safe to do (post-merge of the memtable and sstable at least), it
    // can save us some bandwith, and avoid making us throw a TombstoneOverwhelmingException for purgeable tombstones (which
    // are to some extend an artefact of compaction lagging behind and hence counting them is somewhat unintuitive).
    protected UnfilteredPartitionIterator withoutPurgeableTombstones(UnfilteredPartitionIterator iterator,
                                                                     ColumnFamilyStore cfs,
                                                                     ReadExecutionController controller)
    {
        return Transformation.apply(iterator, new WithoutPurgeableTombstones(cfs, controller));
    }

    /**
     * The purge of {@link #withoutPurgeableTombstones} for one partition at a time, for an index searcher that holds,
     * drops or counts deletions before the read's own purge: a tombstone the read would purge isn't one it would count.
     * It must be created before the read starts. The function returns {@code null}, after closing the partition, if
     * nothing is left of it.
     */
    public UnaryOperator<UnfilteredRowIterator> partitionPurger(ColumnFamilyStore cfs, ReadExecutionController controller)
    {
        WithoutPurgeableTombstones purge = new WithoutPurgeableTombstones(cfs, controller);
        return purge::applyToPartition;
    }

    private class WithoutPurgeableTombstones extends PurgeFunction
    {
        private final ReadExecutionController controller;

        private WithoutPurgeableTombstones(ColumnFamilyStore cfs, ReadExecutionController controller)
        {
            super(nowInSec(), cfs.gcBefore(nowInSec()), controller.oldestUnrepairedTombstone(),
                  cfs.onlyPurgeRepairedTombstones(),
                  metadata().enforceStrictLiveness());
            this.controller = controller;
        }

        // Index searches, and writes to the memtables the read opened, lower the controller's value after this is created
        @Override
        protected long oldestUnrepairedTombstone()
        {
            return controller.oldestUnrepairedTombstone();
        }

        protected LongPredicate getPurgeEvaluator()
        {
            return time -> true;
        }

        @Override
        protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            return super.applyToPartition(partition);
        }
    }

    /**
     * Return the queried token(s) for logging
     */
    public abstract String loggableTokens();

    InputCollector<UnfilteredRowIterator> iteratorsForPartition(ColumnFamilyStore.ViewFragment view, ReadExecutionController controller)
    {
        final BiFunction<List<UnfilteredRowIterator>, RepairedDataInfo, UnfilteredRowIterator> merge =
            (unfilteredRowIterators, repairedDataInfo) -> {
                UnfilteredRowIterator repaired = UnfilteredRowIterators.merge(unfilteredRowIterators);
                return repairedDataInfo.withRepairedDataInfo(repaired);
            };

        // For single partition reads, after reading up to the command's DataLimit nothing extra is required.
        // The merged & repaired row iterator will be consumed until it's exhausted or the RepairedDataInfo's
        // internal counter is satisfied
        final Function<UnfilteredRowIterator, UnfilteredPartitionIterator> postLimitPartitions =
            (rows) -> EmptyIterators.unfilteredPartition(metadata());
        return new InputCollector<>(view, controller, merge, postLimitPartitions);
    }

    InputCollector<UnfilteredPartitionIterator> iteratorsForRange(ColumnFamilyStore.ViewFragment view, ReadExecutionController controller)
    {
        final BiFunction<List<UnfilteredPartitionIterator>, RepairedDataInfo, UnfilteredPartitionIterator> merge =
            (unfilteredPartitionIterators, repairedDataInfo) -> {
                UnfilteredPartitionIterator repaired = UnfilteredPartitionIterators.merge(unfilteredPartitionIterators,
                                                                                          NOOP);
                return repairedDataInfo.withRepairedDataInfo(repaired);
            };

        // Uses identity function to provide additional partitions to be consumed after the command's
        // DataLimits are satisfied. The input to the function will be the iterator of merged, repaired partitions
        // which we'll keep reading until the RepairedDataInfo's internal counter is satisfied.
        return new InputCollector<>(view, controller, merge, Function.identity());
    }

    /**
     * Handles the collation of unfiltered row or partition iterators that comprise the
     * input for a query. Separates them according to repaired status and of repaired
     * status is being tracked, handles the merge and wrapping in a digest generator of
     * the repaired iterators.
     *
     * Intentionally not AutoCloseable so we don't mistakenly use this in ARM blocks
     * as this prematurely closes the underlying iterators
     */
    static class InputCollector<T extends AutoCloseable>
    {
        final RepairedDataInfo repairedDataInfo;
        private final boolean isTrackingRepairedStatus;
        Set<SSTableReader> repairedSSTables;
        BiFunction<List<T>, RepairedDataInfo, T> repairedMerger;
        Function<T, UnfilteredPartitionIterator> postLimitAdditionalPartitions;
        List<T> repairedIters;
        List<T> unrepairedIters;

        InputCollector(ColumnFamilyStore.ViewFragment view,
                       ReadExecutionController controller,
                       BiFunction<List<T>, RepairedDataInfo, T> repairedMerger,
                       Function<T, UnfilteredPartitionIterator> postLimitAdditionalPartitions)
        {
            this.repairedDataInfo = controller.getRepairedDataInfo();
            this.isTrackingRepairedStatus = controller.isTrackingRepairedStatus();

            if (isTrackingRepairedStatus)
            {
                for (SSTableReader sstable : view.sstables)
                {
                    if (considerRepairedForTracking(sstable))
                    {
                        if (repairedSSTables == null)
                            repairedSSTables = Sets.newHashSetWithExpectedSize(view.sstables.size());
                        repairedSSTables.add(sstable);
                    }
                }
            }
            if (repairedSSTables == null)
            {
                repairedIters = Collections.emptyList();
                unrepairedIters = new ArrayList<>(view.sstables.size());
            }
            else
            {
                repairedIters = new ArrayList<>(repairedSSTables.size());
                // when we're done collating, we'll merge the repaired iters and add the
                // result to the unrepaired list, so size that list accordingly
                unrepairedIters = new ArrayList<>((view.sstables.size() - repairedSSTables.size()) + Iterables.size(view.memtables) + 1);
            }
            this.repairedMerger = repairedMerger;
            this.postLimitAdditionalPartitions = postLimitAdditionalPartitions;
        }

        void addMemtableIterator(T iter)
        {
            unrepairedIters.add(iter);
        }

        void addSSTableIterator(SSTableReader sstable, T iter)
        {
            if (repairedSSTables != null && repairedSSTables.contains(sstable))
                repairedIters.add(iter);
            else
                unrepairedIters.add(iter);
        }

        List<T> finalizeIterators(ColumnFamilyStore cfs, long nowInSec, long oldestUnrepairedTombstone)
        {
            if (repairedIters.isEmpty())
                return unrepairedIters;

            // merge the repaired data before returning, wrapping in a digest generator
            repairedDataInfo.prepare(cfs, nowInSec, oldestUnrepairedTombstone);
            T repairedIter = repairedMerger.apply(repairedIters, repairedDataInfo);
            repairedDataInfo.finalize(postLimitAdditionalPartitions.apply(repairedIter));
            unrepairedIters.add(repairedIter);
            return unrepairedIters;
        }

        boolean isEmpty()
        {
            return repairedIters.isEmpty() && unrepairedIters.isEmpty();
        }

        // For tracking purposes we consider data repaired if the sstable is either:
        // * marked repaired
        // * marked pending, but the local session has been committed. This reduces the window
        //   whereby the tracking is affected by compaction backlog causing repaired sstables to
        //   remain in the pending state
        // If an sstable is involved in a pending repair which is not yet committed, we mark the
        // repaired data info inconclusive, as the same data on other replicas may be in a
        // slightly different state.
        private boolean considerRepairedForTracking(SSTableReader sstable)
        {
            if (!isTrackingRepairedStatus)
                return false;

            TimeUUID pendingRepair = sstable.getPendingRepair();
            if (pendingRepair != ActiveRepairService.NO_PENDING_REPAIR)
            {
                if (ActiveRepairService.instance().consistent.local.isSessionFinalized(pendingRepair))
                    return true;

                // In the edge case where compaction is backed up long enough for the session to
                // timeout and be purged by LocalSessions::cleanup, consider the sstable unrepaired
                // as it will be marked unrepaired when compaction catches up
                if (!ActiveRepairService.instance().consistent.local.sessionExists(pendingRepair))
                    return false;

                repairedDataInfo.markInconclusive();
            }

            return sstable.isRepaired();
        }

        void markInconclusive()
        {
            repairedDataInfo.markInconclusive();
        }

        public void close() throws Exception
        {
            FBUtilities.closeAll(unrepairedIters);
            FBUtilities.closeAll(repairedIters);
        }
    }

    @VisibleForTesting
    public static class Serializer implements IVersionedSerializer<ReadCommand>
    {
        private static final int IS_DIGEST = 0x01;
        private static final int IS_FOR_THRIFT = 0x02;
        private static final int HAS_INDEX = 0x04;
        private static final int ACCEPTS_TRANSIENT = 0x08;
        private static final int NEEDS_RECONCILIATION = 0x10;

        private final SchemaProvider schema;

        public Serializer()
        {
            this(Schema.instance);
        }

        @VisibleForTesting
        public Serializer(SchemaProvider schema)
        {
            this.schema = Objects.requireNonNull(schema, "schema");
        }

        private static int digestFlag(boolean isDigest)
        {
            return isDigest ? IS_DIGEST : 0;
        }

        private static boolean isDigest(int flags)
        {
            return (flags & IS_DIGEST) != 0;
        }

        private static boolean acceptsTransient(int flags)
        {
            return (flags & ACCEPTS_TRANSIENT) != 0;
        }

        private static int acceptsTransientFlag(boolean acceptsTransient)
        {
            return acceptsTransient ? ACCEPTS_TRANSIENT : 0;
        }

        // We don't set this flag anymore, but still look if we receive a
        // command with it set in case someone is using thrift a mixed 3.0/4.0+
        // cluster (which is unsupported). This is also a reminder for not
        // re-using this flag until we drop 3.0/3.X compatibility (since it's
        // used by these release for thrift and would thus confuse things)
        private static boolean isForThrift(int flags)
        {
            return (flags & IS_FOR_THRIFT) != 0;
        }

        private static int indexFlag(boolean hasIndex)
        {
            return hasIndex ? HAS_INDEX : 0;
        }

        private static boolean hasIndex(int flags)
        {
            return (flags & HAS_INDEX) != 0;
        }

        private static int needsReconciliationFlag(boolean needsReconciliation)
        {
            return needsReconciliation ? NEEDS_RECONCILIATION : 0;
        }
        
        private static boolean needsReconciliation(int flags)
        {
            return (flags & NEEDS_RECONCILIATION) != 0;
        }

        public void serialize(ReadCommand command, DataOutputPlus out, int version) throws IOException
        {
            command.checkSerializable(version);

            out.writeByte(command.kind.ordinal());
            out.writeByte(
                    digestFlag(command.isDigestQuery())
                    | indexFlag(null != command.indexQueryPlan())
                    | acceptsTransientFlag(command.acceptsTransient())
                    | needsReconciliationFlag(command.rowFilter().needsReconciliation())
            );
            if (command.isDigestQuery())
                out.writeUnsignedVInt32(command.digestVersion());
            command.metadata().id.serialize(out);
            out.writeInt(MessagingService.Version.supportsExtendedDeletionTime(version) ? CassandraUInt.fromLong(command.nowInSec()) : (int) command.nowInSec());
            ColumnFilter.serializer.serialize(command.columnFilter(version), out, version);
            RowFilter.serializer.serialize(command.rowFilter(), out, version);
            DataLimits.serializer.serialize(command.limits(), out, version, command.metadata().comparator);
            // Using the name of one of the indexes in the plan to identify the index group because we want
            // to keep compatibility with legacy nodes. Each replica can create its own different index query plan
            // from the index name.
            if (null != command.indexQueryPlan)
                IndexMetadata.serializer.serialize(command.indexQueryPlan.getFirst().getIndexMetadata(), out, version);

            command.serializeSelection(out, version);
        }

        public ReadCommand deserialize(DataInputPlus in, int version) throws IOException
        {
            Kind kind = Kind.values()[in.readByte()];
            int flags = in.readByte();
            boolean isDigest = isDigest(flags);
            boolean acceptsTransient = acceptsTransient(flags);
            // Shouldn't happen or it's a user error (see comment above) but
            // better complain loudly than doing the wrong thing.
            if (isForThrift(flags))
                throw new IllegalStateException("Received a command with the thrift flag set. "
                                               + "This means thrift is in use in a mixed 3.0/3.X and 4.0+ cluster, "
                                               + "which is unsupported. Make sure to stop using thrift before "
                                               + "upgrading to 4.0");

            boolean hasIndex = hasIndex(flags);
            int digestVersion = isDigest ? in.readUnsignedVInt32() : 0;
            boolean needsReconciliation = needsReconciliation(flags);

            TableMetadata metadata = schema.getExistingTableMetadata(TableId.deserialize(in));
            long nowInSec = MessagingService.Version.supportsExtendedDeletionTime(version) ? CassandraUInt.toLong(in.readInt()) : in.readInt();
            ColumnFilter columnFilter = ColumnFilter.serializer.deserialize(in, version, metadata);

            // add synthetic columns to the tablemetadata so we can serialize them in our response
            var tmb = metadata.unbuild();
            for (var it = columnFilter.fetchedColumns().regulars.simpleColumns(); it.hasNext(); )
            {
                var c = it.next();
                // synthetic columns sort first, so when we hit the first non-synthetic, we're done
                if (!c.isSynthetic())
                    break;
                assert c.sythenticSourceColumn != null;
                tmb.addColumn(c);
            }
            metadata = tmb.build();

            RowFilter rowFilter = RowFilter.serializer.deserialize(in, version, metadata, needsReconciliation);
            DataLimits limits = DataLimits.serializer.deserialize(in, version,  metadata);

            Index.QueryPlan indexQueryPlan = null;
            IndexMetadata index = hasIndex ? deserializeIndexMetadata(in, version, metadata) : null;
            boolean missingIndex = rowFilter.indexHints.includesUnknownIndex;
            if (hasIndex && !missingIndex)
            {
                SecondaryIndexManager indexManager = Keyspace.openAndGetStore(metadata).indexManager;
                Index.Group indexGroup = index == null ? null : indexManager.getIndexGroup(index);
                // a plan without the index of an expression that only an index can evaluate would drop the expression,
                // or fail to order by it
                if (indexGroup == null || !hasIndexesForIndexOnlyExpressions(rowFilter, indexManager, indexGroup))
                {
                    missingIndex = true;
                }
                else
                {
                    indexQueryPlan = indexGroup.queryPlanFor(rowFilter);
                    if (indexQueryPlan != null && !evaluatesIndexOnlyExpressions(indexQueryPlan, rowFilter, indexGroup))
                    {
                        indexQueryPlan = null;
                        missingIndex = true;
                    }
                }
            }

            ReadCommand command = kind.selectionDeserializer.deserialize(in, version, isDigest, digestVersion,
                                                                         acceptsTransient, metadata, nowInSec,
                                                                         columnFilter, rowFilter, limits,
                                                                         indexQueryPlan);
            // the filter can't say whether its coordinator lets replicas relax it, see ReadCommandVerbHandler
            command.requireStrictFiltering();
            command.missingIndex = missingIndex;
            return command;
        }

        private static boolean hasIndexesForIndexOnlyExpressions(RowFilter rowFilter, SecondaryIndexManager indexManager, Index.Group indexGroup)
        {
            for (RowFilter.Expression expression : rowFilter.expressions())
            {
                if (!expression.hasIndexOnlyOperator())
                    continue;

                // the plan of the named index's group orders the results, so that group must have an index for it
                boolean hasIndex = expression.isOrderingExpression()
                                   ? hasIndexInGroup(indexGroup, expression, rowFilter.indexHints)
                                   : indexManager.getBestIndexFor(expression, rowFilter.indexHints, Index.class).isPresent();
                if (!hasIndex)
                    return false;
            }
            return true;
        }

        /**
         * Whether the plan can evaluate each ":" expression: one it leaves to the row filter if the expression it leaves
         * has an analyzer, and any other with an index of the plan's group.
         */
        private static boolean evaluatesIndexOnlyExpressions(Index.QueryPlan plan, RowFilter rowFilter, Index.Group indexGroup)
        {
            List<RowFilter.Expression> postIndexExpressions = plan.postIndexQueryFilter().expressions();
            for (RowFilter.Expression expression : rowFilter.expressions())
            {
                if (!expression.hasIndexOnlyOperator() || expression.isOrderingExpression())
                    continue;

                int residual = postIndexExpressions.indexOf(expression);
                boolean evaluated = residual >= 0
                                    ? !postIndexExpressions.get(residual).requiresIndex()
                                    : hasIndexInGroup(indexGroup, expression, rowFilter.indexHints);
                if (!evaluated)
                    return false;
            }
            return true;
        }

        private static boolean hasIndexInGroup(Index.Group indexGroup, RowFilter.Expression expression, IndexHints hints)
        {
            for (Index index : indexGroup.getIndexes())
            {
                if (!hints.excludes(index) && index.supportsExpression(expression))
                    return true;
            }
            return false;
        }

        /**
         * @return the index the command was built with, or {@code null} if this node does not know that index yet
         */
        @Nullable
        private IndexMetadata deserializeIndexMetadata(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            try
            {
                return IndexMetadata.serializer.deserialize(in, version, metadata);
            }
            catch (UnknownIndexException e)
            {
                logger.info("Couldn't find a defined index on {}.{} with the id {}. " +
                            "If an index was just created, this is likely due to the schema not " +
                            "being fully propagated. Local read will proceed without using the " +
                            "index, or fail as index not available if one of its expressions needs " +
                            "the index. Please wait for schema agreement after index creation.",
                            metadata.keyspace, metadata.name, e.indexId);
                return null;
            }
        }

        public long serializedSize(ReadCommand command, int version)
        {
            return 2 // kind + flags
                   + (command.isDigestQuery() ? TypeSizes.sizeofUnsignedVInt(command.digestVersion()) : 0)
                   + command.metadata().id.serializedSize()
                   + TypeSizes.INT_SIZE // command.nowInSec() is serialized as uint
                   + ColumnFilter.serializer.serializedSize(command.columnFilter(version), version)
                   + RowFilter.serializer.serializedSize(command.rowFilter(), version)
                   + DataLimits.serializer.serializedSize(command.limits(), version, command.metadata().comparator)
                   + command.selectionSerializedSize(version)
                   + command.indexSerializedSize(version);
        }
    }

    @Nullable
    private ReadCommandExecutionInfo setupExecutionInfo(Index.Searcher searcher)
    {
        // if we have a searcher, it may use its own custom execution info instead of the generic one
        if (searcher != null)
        {
            Supplier<ExecutionInfo> searcherExecutionInfoSupplier = searcher.monitorableExecutionInfo();
            if (searcherExecutionInfoSupplier != null)
            {
                executionInfoSupplier = searcherExecutionInfoSupplier;
                return null;
            }
        }

        // if execution info is disabled, return null so we will keep using the default empty supplier
        if (!CassandraRelevantProperties.MONITORING_EXECUTION_INFO_ENABLED.getBoolean())
            return null;

        // otherwise, create and use the generic execution info
        ReadCommandExecutionInfo commandExecutionInfo = new ReadCommandExecutionInfo();
        executionInfoSupplier = () -> commandExecutionInfo;
        return commandExecutionInfo;
    }
}