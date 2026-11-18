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

package org.apache.cassandra.service.reads;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clusterable;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Redaction;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryInfoTracker;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.reads.repair.NoopReadRepair;
import org.apache.cassandra.service.reads.repair.PartitionIteratorMergeListener;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.btree.BTreeSet;

/**
 * Helper in charge of collecting additional queries to be done on the coordinator to protect against invalid results
 * being included due to replica-side filtering (secondary indexes or {@code ALLOW * FILTERING}).
 * <p>
 * When using replica-side filtering with CL > ONE, a replica can send a stale result satisfying the filter, while 
 * updated replicas won't send a corresponding tombstone to discard that result during reconciliation. This helper 
 * identifies the rows in a replica response that don't have a corresponding row in other replica responses (or don't
 * have corresponding cell values), and requests them by primary key on the "silent" replicas in a second fetch round.
 * 
 * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-8272">CASSANDRA-8272</a>
 * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-8273">CASSANDRA-8273</a>
 * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-15907">CASSANDRA-15907</a>
 * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-19018">CASSANDRA-19018</a>
 */
@NotThreadSafe
public class ReplicaFilteringProtection<E extends Endpoints<E>>
{
    private static final Logger logger = LoggerFactory.getLogger(ReplicaFilteringProtection.class);
    private static final NoSpamLogger oneMinuteLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    private static final Function<UnfilteredRowIterator, EncodingStats> NULL_TO_NO_STATS =
        rowIterator -> rowIterator == null ? EncodingStats.NO_STATS : rowIterator.stats();

    private static final String CACHED_ROWS_WARN_MESSAGE = "Replica filtering protection has cached up to %d rows during query %s, " +
                                                           "which is over the warning threshold of %d rows defined by " +
                                                           "'cached_replica_rows_warn_threshold' in cassandra.yaml.";
    private static final String CACHED_ROWS_FAIL_MESSAGE = "Replica filtering protection has cached %d rows during query %s, " +
                                                           "which is over the failure threshold of %d rows defined by " +
                                                           "'cached_replica_rows_fail_threshold' in cassandra.yaml.";

    // The operators whose result on a static row the coordinator computes exactly: = also on an analyzed column, as
    // RowFilter evaluates it with the analyzer of the index.
    private static final Set<Operator> EXACT_OPERATORS = EnumSet.of(Operator.EQ, Operator.NEQ, Operator.LT, Operator.LTE,
                                                                    Operator.GT, Operator.GTE, Operator.IN, Operator.NOT_IN,
                                                                    Operator.CONTAINS, Operator.CONTAINS_KEY,
                                                                    Operator.NOT_CONTAINS, Operator.NOT_CONTAINS_KEY);

    private final Keyspace keyspace;
    private final ReadCommand command;
    private final ConsistencyLevel consistency;
    private final Dispatcher.RequestTime requestTime;
    private final E sources;
    private final TableMetrics tableMetrics;

    /** The filter that the coordinator applies to the merged, completed rows. */
    private final UnaryOperator<PartitionIterator> coordinatorFilter;

    private final QueryMergeListener mergeListener;

    private final int cachedRowsWarnThreshold;
    private final int cachedRowsFailThreshold;

    /** Tracks whether or not we've already hit the failure threshold while evaluating a partition. */
    private boolean hitFailureThreshold = false;

    private int currentRowsCached = 0; // tracks the current number of cached rows
    private int maxRowsCached = 0; // tracks the high watermark for the number of cached rows

    /**
     * Per-source list of the pending partitions seen by the merge listener, to be merged with the extra fetched rows.
     */
    private final List<Queue<PartitionBuilder>> originalPartitions;

    /** Whether to consume entire partitions or not in {@link #queryProtectedPartitions}. */
    private final boolean consumeEntirePartitions;

    /** Tracks the current partitions when not consuming entire partitions in {@link #queryProtectedPartitions}. */
    private RowIterator currentRowIterator = null;

    /** Counts the first-phase rows that the second phase returns, if they can be counted for the query's limits. */
    @Nullable
    private final CompleteRows completeRows;

    private static boolean isEvaluatedExactly(RowFilter.Expression expression)
    {
        return (expression instanceof RowFilter.SimpleExpression || expression instanceof RowFilter.MapComparisonExpression)
               && EXACT_OPERATORS.contains(expression.operator());
    }

    /**
     * The given filter tree with its static expressions replaced by the given results: {@code TRUE}, {@code FALSE},
     * or what remains of it, as the remaining expression or subtree, or as whether it is a disjunction and the list of
     * those that remain. As the tree only has conjunctions and disjunctions, a tree that remains the same with fewer
     * static expressions satisfied selects the same rows.
     */
    private static Object residual(RowFilter.FilterElement element, Predicate<RowFilter.Expression> staticResult)
    {
        Boolean decisive = element.isDisjunction();
        List<Object> remaining = new ArrayList<>();
        for (RowFilter.Expression expression : element.expressions())
        {
            Object operand = expression.column().isStatic() ? (Object) staticResult.test(expression) : expression;
            if (decisive.equals(operand))
                return decisive;
            if (!(operand instanceof Boolean))
                remaining.add(operand);
        }
        for (RowFilter.FilterElement child : element.children())
        {
            Object operand = residual(child, staticResult);
            if (decisive.equals(operand))
                return decisive;
            if (!(operand instanceof Boolean))
                remaining.add(operand);
        }
        if (remaining.isEmpty())
            return !decisive;
        return remaining.size() == 1 ? remaining.get(0) : Arrays.asList(decisive, remaining);
    }

    /**
     * Returns whether replica filtering protection reads whole partitions of the given command from every replica
     * before it filters them (see {@link #queryProtectedPartitions}).
     * <p>
     * Per-partition and GROUP BY limits need entire partitions: a partition handed over in several pages restarts the
     * second phase's per-partition count, and GroupMaker expects each partition key once.
     */
    static boolean consumesEntirePartitions(ReadCommand command)
    {
        return command.limits().isUnlimited()
               || !command.isLimitedToOnePartition()
               || command.rowFilter().hasStaticExpression()
               || command.limits().perPartitionCount() != DataLimits.NO_LIMIT
               || command.limits().isGroupByLimit();
    }

    ReplicaFilteringProtection(Keyspace keyspace,
                               ReadCommand command,
                               ConsistencyLevel consistency,
                               Dispatcher.RequestTime requestTime,
                               E sources,
                               UnaryOperator<PartitionIterator> coordinatorFilter,
                               int cachedRowsWarnThreshold,
                               int cachedRowsFailThreshold,
                               DataLimits.Counter firstPhaseCounter)
    {
        this.keyspace = keyspace;
        this.command = command;
        this.consumeEntirePartitions = consumesEntirePartitions(command);
        this.consistency = consistency;
        this.requestTime = requestTime;
        this.sources = sources;
        this.coordinatorFilter = coordinatorFilter;
        this.originalPartitions = new ArrayList<>(sources.size());

        for (int i = 0; i < sources.size(); i++)
        {
            originalPartitions.add(new ArrayDeque<>());
        }

        tableMetrics = ColumnFamilyStore.metricsFor(command.metadata().id);

        this.cachedRowsWarnThreshold = cachedRowsWarnThreshold;
        this.cachedRowsFailThreshold = cachedRowsFailThreshold;

        mergeListener = new QueryMergeListener();

        // Per-partition limits would need the rows counted by partition. Without a row or group limit (no limit, or
        // only a limit in bytes), the rows would be held and never counted. A filter on a static column can stop
        // matching the rows of a partition when its static row is completed.
        DataLimits limits = command.limits();
        completeRows = limits.count() == DataLimits.NO_LIMIT
                       || limits.perPartitionCount() != DataLimits.NO_LIMIT
                       || command.rowFilter().hasStaticExpression()
                       ? null
                       : new CompleteRows(firstPhaseCounter);
    }

    private ReadResponse executeReadCommand(ReadCommand cmd, Replica source, ReplicaPlan.Shared<EndpointsForToken, ReplicaPlan.ForTokenRead> replicaPlan)
    {
        @SuppressWarnings("unchecked")
        DataResolver<EndpointsForToken, ReplicaPlan.ForTokenRead> resolver =
            new DataResolver<>(cmd,
                               replicaPlan,
                               (NoopReadRepair<EndpointsForToken, ReplicaPlan.ForTokenRead>) NoopReadRepair.instance,
                               requestTime,
                               QueryInfoTracker.ReadTracker.NOOP);

        ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> handler = new ReadCallback<>(resolver, cmd, replicaPlan, requestTime);

        if (source.isSelf())
        {
            Stage.READ.maybeExecuteImmediately(new StorageProxy.LocalReadRunnable(cmd, handler, requestTime));
        }
        else
        {
            if (source.isTransient())
                cmd = cmd.copyAsTransientQuery(source);
            MessagingService.instance().sendWithCallback(cmd.createMessage(false, requestTime), source.endpoint(), handler);
        }

        // We don't call handler.get() because we want to preserve tombstones since we're still in the middle of merging node results.
        handler.awaitResults();
        assert resolver.getMessages().size() == 1;
        return resolver.getMessages().get(0).payload;
    }

    private class PartitionMergeListerner implements UnfilteredRowIterators.MergeListener
    {
        final DecoratedKey key;
        final List<PartitionBuilder> builders = new ArrayList<>(sources.size());
        final RegularAndStaticColumns columns;
        final EncodingStats stats;
        final boolean[] silentRowAt;
        final boolean[] silentColumnAt;
        Clustering<?> lastClustering;
        boolean populated;

        PartitionMergeListerner(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
        {
            key = partitionKey;
            columns = PartitionIteratorMergeListener.columns(versions);
            stats = EncodingStats.merge(versions, NULL_TO_NO_STATS);

            for (int i = 0; i < sources.size(); i++)
                builders.add(i, new PartitionBuilder(partitionKey, sources.get(i), columns, stats, versions.get(i) != null));

            silentRowAt = new boolean[builders.size()];
            silentColumnAt = new boolean[builders.size()];
        }

        @Override
        public void onMergedPartitionLevelDeletion(DeletionTime mergedDeletion, DeletionTime[] versions)
        {
            // cache the deletion time versions to be able to regenerate the original row iterator
            for (int i = 0; i < versions.length; i++)
                builders.get(i).setDeletionTime(versions[i]);
        }

        @Override
        public void onMergedRows(Row merged, Row[] versions)
        {
            if (!merged.isStatic())
                lastClustering = merged.clustering();

            // Cache the row versions to be able to regenerate the original row iterator:
            for (int i = 0; i < versions.length; i++)
                builders.get(i).addRow(versions[i]);

            // If all versions are empty, there's no divergence to resolve:
            if (merged.isEmpty())
                return;

            Arrays.fill(silentRowAt, false);

            // Mark replicas silent if they provide no data for the row:
            for (int i = 0; i < versions.length; i++)
                if (versions[i] == null || (merged.isStatic() && versions[i].isEmpty()))
                    silentRowAt[i] = true;

            // Even if there are no completely missing rows, replicas may still be silent about individual
            // columns, so we need to check for divergence at the column level:
            for (ColumnMetadata column : merged.isStatic() ? columns.statics : columns.regulars)
            {
                Arrays.fill(silentColumnAt, false);
                boolean allSilent = true;

                for (int i = 0; i < versions.length; i++)
                {
                    // If the version at this replica is null, we've already marked it as silent:
                    if (versions[i] != null && versions[i].getColumnData(column) == null)
                        silentColumnAt[i] = true;
                    else
                        allSilent = false;
                }

                for (int i = 0; i < versions.length; i++)
                    // Mark the replica silent if it is silent about this column and there is actually 
                    // divergence between the replicas. (i.e. If all replicas are silent for this 
                    // column, there is nothing to fetch to complete the row anyway.)
                    silentRowAt[i] |= silentColumnAt[i] && !allSilent;
            }

            boolean complete = true;
            for (int i = 0; i < silentRowAt.length; i++)
            {
                if (silentRowAt[i])
                {
                    builders.get(i).addToFetch(merged);
                    complete = false;
                }
            }

            if (complete && completeRows != null && !merged.isStatic())
                completeRows.add(key, merged);
        }

        @Override
        public void onMergedRangeTombstoneMarkers(RangeTombstoneMarker merged, RangeTombstoneMarker[] versions)
        {
            // cache the marker versions to be able to regenerate the original row iterator
            for (int i = 0; i < versions.length; i++)
                builders.get(i).addRangeTombstoneMarker(versions[i]);
        }

        @Override
        public void close() {}

        public void populate()
        {
            populated = true;
            for (int i = 0; i < sources.size(); i++)
                builders.get(i).closeOpenMarker(lastClustering);

            if (command.rowFilter().hasStaticExpression())
            {
                fetchWhereStaticRowIsStricter();
                completeFetchedPartitions();
            }

            for (int i = 0; i < sources.size(); i++)
                originalPartitions.get(i).add(builders.get(i));
        }

        /**
         * Fetches the entire partition from each replica whose static row is stricter than the static row of the
         * partition (see isStricter): it can have left out rows that the static row of the partition selects. The
         * static row of the partition merges the static rows and partition deletions of all the replicas.
         */
        private void fetchWhereStaticRowIsStricter()
        {
            List<UnfilteredRowIterator> known = new ArrayList<>(builders.size());
            for (PartitionBuilder builder : builders)
                known.add(builder.knownPartition());

            Row staticRow;
            try (UnfilteredRowIterator merged = UnfilteredRowIterators.merge(known))
            {
                staticRow = merged.staticRow();
            }

            for (PartitionBuilder builder : builders)
                if (!builder.unresolvedStatic && isStricter(builder.knownStaticRow(), staticRow))
                    builder.fetchEntirePartition();
        }

        /**
         * Whether the replica's static row fails a static expression that the static row of the partition satisfies,
         * in a way that changes what the filter selects. An expression that the coordinator can't evaluate exactly
         * counts as failed unless both rows have the same data for its column.
         */
        private boolean isStricter(Row replicaStaticRow, Row staticRow)
        {
            Predicate<RowFilter.Expression> partition =
                expression -> expression.isSatisfiedBy(command.metadata(), key, staticRow, command.nowInSec());
            Predicate<RowFilter.Expression> replica =
                expression -> expression.isSatisfiedBy(command.metadata(), key, replicaStaticRow, command.nowInSec());
            boolean fails = false;
            for (RowFilter.Expression expression : command.rowFilter().expressions())
            {
                if (!expression.column().isStatic())
                    continue;

                if (!isEvaluatedExactly(expression))
                {
                    if (!Objects.equals(replicaStaticRow.getColumnData(expression.column()), staticRow.getColumnData(expression.column())))
                        return true;
                }
                else if (partition.test(expression) && !replica.test(expression))
                {
                    fails = true;
                }
            }

            return fails && !residual(command.rowFilter().root(), partition).equals(residual(command.rowFilter().root(), partition.and(replica)));
        }

        /**
         * The entire partitions fetched bring rows that no replica returned, and the other replicas left out of what
         * they returned every row that doesn't satisfy the filter on their own data, deleted rows included. So the
         * rows that the coordinator would select from what is known of the partition (the fetched partitions, and
         * what the other replicas returned) are fetched from each other replica that didn't return them.
         */
        private void completeFetchedPartitions()
        {
            if (!Iterables.any(builders, builder -> builder.unresolvedStatic))
                return;

            List<UnfilteredRowIterator> known = new ArrayList<>(builders.size());
            for (PartitionBuilder builder : builders)
                known.add(builder.knownPartition());

            List<Clustering<?>> selected = new ArrayList<>();
            UnfilteredPartitionIterator merged = new SingletonUnfilteredPartitionIterator(UnfilteredRowIterators.merge(known));
            try (PartitionIterator partitions = coordinatorFilter.apply(UnfilteredPartitionIterators.filter(merged, command.nowInSec())))
            {
                while (partitions.hasNext())
                {
                    // an index filter can return null for a partition that it rejects entirely
                    try (RowIterator rows = partitions.next())
                    {
                        while (rows != null && rows.hasNext())
                            selected.add(rows.next().clustering());
                    }
                }
            }

            for (PartitionBuilder builder : builders)
                if (!builder.unresolvedStatic)
                    builder.addToFetchUnlessReturned(selected);
        }
    }

    private class QueryMergeListener implements UnfilteredPartitionIterators.MergeListener
    {
        private PartitionMergeListerner currentListener;

        @Override
        public void close()
        {
            // If we hit the failure threshold before consuming a single partition, record the current rows cached.
            maxRowsCached = Math.max(currentRowsCached, maxRowsCached);
            tableMetrics.rfpRowsCachedPerQuery.update(maxRowsCached);

            // Check the cached rows warning threshold at the end of the query, so we can report the maximum number
            // of cached rows we have had during the query.
            if (!hitFailureThreshold && maxRowsCached > cachedRowsWarnThreshold)
            {
                String unredactedMessage = cachedRowsWarnMessage(Redaction.NONE);
                String redactedMessage = cachedRowsWarnMessage(Redaction.REDACT);
                ClientWarn.instance.warn(unredactedMessage);
                oneMinuteLogger.warn(redactedMessage);
                Tracing.trace(unredactedMessage);
            }
        }

        private String cachedRowsWarnMessage(Redaction redaction)
        {
            return String.format(CACHED_ROWS_WARN_MESSAGE,
                                 maxRowsCached,
                                 command.toCQLString(redaction),
                                 cachedRowsWarnThreshold);
        }

        @Override
        public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
        {
            if (currentListener == null || !currentListener.key.equals(partitionKey))
            {
                populateDiscarded();
                currentListener = new PartitionMergeListerner(partitionKey, versions);
                if (completeRows != null)
                    completeRows.startPartition(partitionKey);
            }

            return currentListener;
        }

        public void populate()
        {
            if (currentListener != null)
                currentListener.populate();
        }

        /**
         * Populates the current partition if the first phase discarded it before populate was called: with an
         * expression on a static column, a replica that didn't return it can have rows that its static row selects.
         */
        public void populateDiscarded()
        {
            if (currentListener != null && !currentListener.populated && command.rowFilter().hasStaticExpression())
                currentListener.populate();
        }
    }

    /**
     * This listener tracks both the accepted data and the primary keys of the rows that may be incomplete.
     * That way, once the query results are merged using this listener, subsequent calls to
     * {@link #queryProtectedPartitions(PartitionIterator, int)} will use the collected data to return a copy of the
     * data originally collected from the specified replica, completed with the potentially outdated rows.
     */
    UnfilteredPartitionIterators.MergeListener mergeController()
    {
        return mergeListener;
    }

    private void incrementCachedRows()
    {
        currentRowsCached++;

        // Check the cached rows failure threshold every time the cached row count is incremented,
        // so we can detect a violation and abort as soon as the threshold is crossed.
        if (currentRowsCached == cachedRowsFailThreshold + 1)
        {
            hitFailureThreshold = true;
            String unredactedMessage = cachedRowsFailMessage(Redaction.NONE);
            String redactedMessage = cachedRowsFailMessage(Redaction.REDACT);

            logger.error(redactedMessage);
            Tracing.trace(unredactedMessage);
            throw new OverloadedException(redactedMessage);
        }
    }

    private String cachedRowsFailMessage(Redaction redaction)
    {
        return String.format(CACHED_ROWS_FAIL_MESSAGE,
                             currentRowsCached,
                             command.toCQLString(redaction),
                             cachedRowsFailThreshold);
    }

    private void releaseCachedRows(int count)
    {
        maxRowsCached = Math.max(currentRowsCached, maxRowsCached);
        currentRowsCached -= count;
    }

    /**
     * Returns the protected results for the specified replica. These are generated fetching the extra rows and merging
     * them with the cached original filtered results for that replica.
     *
     * @param merged the first iteration partitions, that should have been read used with the {@link #mergeController()}
     * @param source the source
     * @return the protected results for the specified replica
     */
    UnfilteredPartitionIterator queryProtectedPartitions(PartitionIterator merged, int source)
    {
        return new UnfilteredPartitionIterator()
        {
            final Queue<PartitionBuilder> partitions = originalPartitions.get(source);

            @Override
            public TableMetadata metadata()
            {
                return command.metadata();
            }

            @Override
            public void close()
            {
                if (currentRowIterator != null)
                    currentRowIterator.close();
            }

            @Override
            public boolean hasNext()
            {
                // If there are no cached partition builders for this source, advance the first phase iterator, which
                // will force the RFP merge listener to load rows from the next protected partition.
                if (partitions.isEmpty())
                {
                    if (consumeEntirePartitions)
                    {
                        if (merged.hasNext())
                        {
                            try (RowIterator partition = merged.next())
                            {
                                while (partition.hasNext())
                                    partition.next();

                                mergeListener.populate();
                            }
                        }
                        else
                        {
                            mergeListener.populateDiscarded();
                        }
                    }
                    else
                    {
                        if (currentRowIterator == null || !currentRowIterator.hasNext())
                        {
                            // If there is an iterator, it's done, so just close it.
                            if (currentRowIterator != null)
                            {
                                currentRowIterator.close();
                                currentRowIterator = null;
                            }

                            // Take the next filtered partition from the merged partition iterator.
                            if (merged.hasNext())
                                currentRowIterator = merged.next();
                        }

                        if (currentRowIterator != null)
                        {
                            int i = 0;

                            // Consume LIMIT filtered rows from the current partition, unless there are fewer results.
                            // The underlying iterator is short-read protected, and limiting the number of rows we
                            // consume avoids needless SRP reads when there are many more than LIMIT results.
                            while (i < command.limits().count() && currentRowIterator.hasNext())
                            {
                                currentRowIterator.next();
                                i++;
                            }

                            // If we actually consumed a row, checkpoint to populate the builders.
                            if (i > 0)
                                mergeListener.populate();
                        }
                    }
                }

                return !partitions.isEmpty();
            }

            @Override
            public UnfilteredRowIterator next()
            {
                PartitionBuilder builder = partitions.poll();
                assert builder != null;
                return builder.protectedPartition();
            }
        };
    }

    /**
     * Counts, in the first phase's merged result counter, the rows that no replica is silent about and that match the
     * row filter: the second phase returns them as they are, so short read protection stops asking for more rows once
     * they fill the limit. The rows are filtered and counted in batches big enough to fill the rest of the limit, or
     * one at a time for GROUP BY.
     * <p>
     * A GROUP BY counter closes a group when the next one starts and expects each partition once, so it is given the
     * start of each merged partition and each row as they come. A group whose last row is the last row of a replica's
     * response tells short read protection that the replica isn't needed (see DataLimits#isCompleteWithLastRowOf). The
     * rows it doesn't count can only make more groups.
     */
    private class CompleteRows
    {
        private final DataLimits.Counter counter;
        private final boolean groupBy;
        // whether the coordinator filter filters rows (see DataResolver#preCountFilterForReplicaFilteringProtection)
        private final boolean filtersRows;
        private final List<DecoratedKey> keys = new ArrayList<>();
        private final List<List<Row>> rows = new ArrayList<>();
        private int pending = 0;

        private CompleteRows(DataLimits.Counter counter)
        {
            this.counter = counter;
            this.groupBy = command.limits().isGroupByLimit();
            this.filtersRows = command.rowFilter().hasNonKeyExpression();
        }

        private void startPartition(DecoratedKey key)
        {
            if (groupBy && !counter.isDone())
                counter.countPartitionStart(key);
        }

        private void add(DecoratedKey key, Row merged)
        {
            if (counter.isDone())
                return;

            // The second phase purges the merged rows before filtering them
            Row row = merged.purge(DeletionPurger.PURGE_ALL, command.nowInSec(), command.metadata().enforceStrictLiveness());
            if (row == null)
                return;

            if (groupBy)
            {
                // The filter has no static expression, so this is what the coordinator filter does with the row
                if (!filtersRows || command.rowFilter().root().isSatisfiedBy(command.metadata(), key, row, Rows.EMPTY_STATIC_ROW, command.nowInSec()))
                    counter.countRow(row);
                return;
            }

            if (keys.isEmpty() || !keys.get(keys.size() - 1).equals(key))
            {
                keys.add(key);
                rows.add(new ArrayList<>());
            }
            rows.get(rows.size() - 1).add(row);

            if (++pending >= command.limits().count() - counter.rowsCounted())
                count();
        }

        private void count()
        {
            try (PartitionIterator partitions = counter.applyTo(coordinatorFilter.apply(pendingPartitions())))
            {
                PartitionIterators.consume(partitions);
            }

            keys.clear();
            rows.clear();
            pending = 0;
        }

        private PartitionIterator pendingPartitions()
        {
            Iterator<DecoratedKey> keyIterator = keys.iterator();
            Iterator<List<Row>> rowsIterator = rows.iterator();
            return new PartitionIterator()
            {
                @Override
                public boolean hasNext()
                {
                    return keyIterator.hasNext();
                }

                @Override
                public RowIterator next()
                {
                    return partition(keyIterator.next(), rowsIterator.next().iterator());
                }

                @Override
                public void close()
                {
                }
            };
        }

        private RowIterator partition(DecoratedKey key, Iterator<Row> partitionRows)
        {
            return new RowIterator()
            {
                @Override
                public TableMetadata metadata()
                {
                    return command.metadata();
                }

                @Override
                public boolean isReverseOrder()
                {
                    return command.isReversed();
                }

                @Override
                public RegularAndStaticColumns columns()
                {
                    return command.metadata().regularAndStaticColumns();
                }

                @Override
                public DecoratedKey partitionKey()
                {
                    return key;
                }

                @Override
                public Row staticRow()
                {
                    return Rows.EMPTY_STATIC_ROW;
                }

                @Override
                public boolean hasNext()
                {
                    return partitionRows.hasNext();
                }

                @Override
                public Row next()
                {
                    return partitionRows.next();
                }

                @Override
                public void close()
                {
                }
            };
        }
    }

    private class PartitionBuilder
    {
        private final DecoratedKey key;
        private final Replica source;
        private final RegularAndStaticColumns columns;
        private final EncodingStats stats;

        private DeletionTime deletionTime;
        private Row staticRow = Rows.EMPTY_STATIC_ROW;
        private final Queue<Unfiltered> contents = new ArrayDeque<>();
        private DeletionTime openDeletion;
        private RangeTombstoneMarker reopenMarker;
        private BTreeSet.Builder<Clustering<?>> toFetch;
        private int partitionRowsCached;

        private boolean unresolvedStatic = false;
        private ReadResponse fetched;
        private final boolean returnedPartition;

        private PartitionBuilder(DecoratedKey key, Replica source, RegularAndStaticColumns columns, EncodingStats stats, boolean returned)
        {
            this.key = key;
            this.source = source;
            this.columns = columns;
            this.stats = stats;
            this.returnedPartition = returned;
        }

        private void setDeletionTime(DeletionTime deletionTime)
        {
            this.deletionTime = deletionTime;
        }

        private void addRow(Row row)
        {
            partitionRowsCached++;

            incrementCachedRows();

            // Note that even null rows are counted against the row caching limit. The assumption is that
            // a subsequent protection query will later fetch the row onto the heap anyway.
            if (row == null)
                return;

            if (row.isStatic())
            {
                staticRow = row;
            }
            else
            {
                addReopenMarker();
                contents.add(row);
            }
        }

        private void addRangeTombstoneMarker(RangeTombstoneMarker marker)
        {
            if (null == marker)
                return;

            boolean reversed = command.isReversed();
            addReopenMarker();
            openDeletion = marker.isOpen(reversed) ? marker.openDeletionTime(reversed) : null;
            contents.add(marker);
        }

        /**
         * Each page's protected partition must close the ranges it opens, as the next page is merged separately.
         * Close an open range after the last row of the page, and reopen it before this replica's next item.
         */
        private void closeOpenMarker(Clustering<?> lastClustering)
        {
            if (null == openDeletion)
                return;

            assert null != lastClustering : "open deletion with no merged row to anchor the close bound at";

            // A replica with nothing since the previous page still needs the range, to shadow other replicas' rows.
            addReopenMarker();
            boolean reversed = command.isReversed();
            contents.add(RangeTombstoneBoundMarker.inclusiveClose(reversed, lastClustering, openDeletion));
            reopenMarker = RangeTombstoneBoundMarker.exclusiveOpen(reversed, lastClustering, openDeletion);
        }

        private void addReopenMarker()
        {
            if (null == reopenMarker)
                return;

            contents.add(reopenMarker);
            reopenMarker = null;
        }

        /**
         * Adds the given clusterings, in the order of the query, to the rows to fetch, except those that this replica
         * returned: what it returned is already its version of the row.
         */
        private void addToFetchUnlessReturned(List<Clustering<?>> clusterings)
        {
            Comparator<Clusterable> comparator = command.isReversed() ? command.metadata().comparator.reversed()
                                                                      : command.metadata().comparator;
            Iterator<Unfiltered> returned = contents.iterator();
            Unfiltered next = returned.hasNext() ? returned.next() : null;
            for (Clustering<?> clustering : clusterings)
            {
                while (next != null && (!next.isRow() || comparator.compare(next, clustering) < 0))
                    next = returned.hasNext() ? returned.next() : null;

                if (next == null || comparator.compare(next, clustering) != 0)
                {
                    if (toFetch == null)
                        toFetch = BTreeSet.builder(command.metadata().comparator);
                    toFetch.add(clustering);
                    fetched = null;
                }
            }
        }

        /**
         * What is known of this replica's partition before the rows that the coordinator selects are fetched by name:
         * what it returned, merged with the response of the fetch from it if its entire partition is fetched or if it
         * didn't return the partition (the response has its static row and deletion). Unlike
         * {@link #protectedPartition()}, this doesn't consume what the replica returned.
         */
        private UnfilteredRowIterator knownPartition()
        {
            Iterator<Unfiltered> returned = contents.iterator();
            UnfilteredRowIterator original = new AbstractUnfilteredRowIterator(command.metadata(), key, deletionTime, columns,
                                                                               staticRow, command.isReversed(), stats)
            {
                @Override
                protected Unfiltered computeNext()
                {
                    return returned.hasNext() ? returned.next() : endOfData();
                }
            };

            if (!unresolvedStatic && returnedPartition)
                return original;

            if (toFetch == null)
                toFetch = BTreeSet.builder(command.metadata().comparator);
            return mergeFetched(original);
        }

        /** The static row that this replica filtered its rows with. */
        private Row knownStaticRow()
        {
            try (UnfilteredRowIterator known = knownPartition())
            {
                return known.staticRow();
            }
        }

        private void fetchEntirePartition()
        {
            if (toFetch == null)
                toFetch = BTreeSet.builder(command.metadata().comparator);
            unresolvedStatic = true;
            fetched = null;
        }

        private void addToFetch(Row row)
        {
            if (toFetch == null)
                toFetch = BTreeSet.builder(command.metadata().comparator);

            if (row.isStatic())
                // If there is an expression on a static column, the static row must be marked unresolved and the 
                // partition fetched, as completing the static row could produce matches across the entire partition.
                // The static row itself will still be retrieved and completed if there is any unresolved non-static 
                // row, however, ensuring the latest static values are returned from the query.
                unresolvedStatic = command.rowFilter().hasStaticExpression();
            else
                toFetch.add(row.clustering());

            fetched = null;
        }

        private UnfilteredRowIterator originalPartition()
        {
            return new UnfilteredRowIterator()
            {
                @Override
                public DeletionTime partitionLevelDeletion()
                {
                    return deletionTime;
                }

                @Override
                public EncodingStats stats()
                {
                    return stats;
                }

                @Override
                public TableMetadata metadata()
                {
                    return command.metadata();
                }

                @Override
                public boolean isReverseOrder()
                {
                    return command.isReversed();
                }

                @Override
                public RegularAndStaticColumns columns()
                {
                    return columns;
                }

                @Override
                public DecoratedKey partitionKey()
                {
                    return key;
                }

                @Override
                public Row staticRow()
                {
                    return staticRow;
                }

                @Override
                public void close()
                {
                    releaseCachedRows(partitionRowsCached);
                    toFetch = null;
                    fetched = null;
                    // TODO: the counters might not be accurate for the static row at this point?
                }

                @Override
                public boolean hasNext()
                {
                    return !contents.isEmpty();
                }

                @Override
                public Unfiltered next()
                {
                    return contents.poll();
                }
            };
        }

        private UnfilteredRowIterator protectedPartition()
        {
            UnfilteredRowIterator original = originalPartition();
            return toFetch == null ? original : mergeFetched(original);
        }

        /**
         * Merges the given rows of this replica with the rows to fetch from it, which are only fetched again if rows
         * were added to them: a partition read with a limit is populated at several checkpoints, and each of them can
         * add rows to fetch.
         */
        private UnfilteredRowIterator mergeFetched(UnfilteredRowIterator original)
        {
            if (fetched == null)
                fetched = fetchFromSource();
            ReadResponse response = fetched;

            try (UnfilteredPartitionIterator partitions = response.makeIterator(command))
            {
                if (partitions.hasNext())
                {
                    try (UnfilteredRowIterator fetchedRows = partitions.next())
                    {
                        return UnfilteredRowIterators.merge(Arrays.asList(original, fetchedRows));
                    }
                }
            }

            return original;
        }

        private ReadResponse fetchFromSource()
        {
            assert toFetch != null;

            NavigableSet<Clustering<?>> clusterings = toFetch.build();
            tableMetrics.replicaFilteringProtectionRequests.mark();

            if (logger.isTraceEnabled())
                logger.trace("Requesting rows {} in partition {} from {} for replica filtering protection",
                             clusterings, key, source);

            Tracing.trace("Requesting {} rows in partition {} from {} for replica filtering protection",
                          clusterings.size(), key, source);

            // If there is an unresolved static column, we must fetch the entire partition, as static column predicates
            // may produce row matches across the entire partition. If there are only non-static rows to complete, we
            // query the partition specifically for the corresponding cluterings by name. In either case, we do not
            // provide a limit. (In the unresolved static case, we have no way of knowing how many stale rows we might
            // read on a silent replica before finding a live one.)
            ClusteringIndexFilter filter = unresolvedStatic ? command.clusteringIndexFilter(key)
                                                            : new ClusteringIndexNamesFilter(clusterings, command.isReversed());

            SinglePartitionReadCommand cmd = SinglePartitionReadCommand.create(command.metadata(),
                                                                               command.nowInSec(),
                                                                               command.columnFilter(),
                                                                               RowFilter.none(),
                                                                               DataLimits.NONE,
                                                                               key,
                                                                               filter);

            ReplicaPlan.ForTokenRead replicaPlan = ReplicaPlans.forSingleReplicaRead(keyspace, key.getToken(), source);
            ReplicaPlan.SharedForTokenRead sharedReplicaPlan = ReplicaPlan.shared(replicaPlan);

            try
            {
                return executeReadCommand(cmd, source, sharedReplicaPlan);
            }
            catch (ReadTimeoutException e)
            {
                int blockFor = consistency.blockFor(replicaPlan.replicationStrategy());
                throw new ReadTimeoutException(consistency, blockFor - 1, blockFor, true);
            }
            catch (UnavailableException e)
            {
                int blockFor = consistency.blockFor(replicaPlan.replicationStrategy());
                throw UnavailableException.create(consistency, blockFor, blockFor - 1);
            }
        }
    }
}
