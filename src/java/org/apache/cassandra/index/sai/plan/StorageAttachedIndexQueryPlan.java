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
package org.apache.cassandra.index.sai.plan;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.LikePatterns;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.IndexHints;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;

public class StorageAttachedIndexQueryPlan implements Index.QueryPlan
{
    public static final String UNSUPPORTED_NON_STRICT_OPERATOR =
    "Operator %s is only supported in intersections for reads that do not require replica reconciliation.";

    private final ColumnFamilyStore cfs;
    private final TableQueryMetrics queryMetrics;

    /**
     * postIndexFilter comprised by those expressions in the read command row filter that can't be handled by
     * {@link FilterTree#isSatisfiedBy(DecoratedKey, Unfiltered, Row)}. That includes expressions targeted
     * at {@link RowFilter.UserExpression}s like those used by RLAC.
     */
    private final RowFilter postIndexFilter;
    private final Set<Index> indexes;
    private final IndexFeatureSet indexFeatureSet;
    private final Orderer orderer;
    private final boolean usesIndexFiltering;

    private StorageAttachedIndexQueryPlan(ColumnFamilyStore cfs,
                                          TableQueryMetrics queryMetrics,
                                          RowFilter filter,
                                          ImmutableSet<Index> indexes,
                                          IndexFeatureSet indexFeatureSet)
    {
        this.cfs = cfs;
        this.queryMetrics = queryMetrics;
        this.postIndexFilter = filter.restrict(RowFilter.Expression::isUserDefined);
        this.indexes = indexes;
        this.indexFeatureSet = indexFeatureSet;
        this.orderer = Orderer.from(cfs.getIndexManager(), filter);
        this.usesIndexFiltering = hasIndexFilters(filter, indexes);
    }

    @Nullable
    public static StorageAttachedIndexQueryPlan create(ColumnFamilyStore cfs,
                                                       TableQueryMetrics queryMetrics,
                                                       Set<StorageAttachedIndex> allIndexes,
                                                       RowFilter rowFilter)
    {
        // collect the indexes that can be used with the provided row filter
        Set<StorageAttachedIndex> selectedIndexes = new HashSet<>();
        if (!selectedIndexes(rowFilter.root, allIndexes, selectedIndexes, rowFilter.indexHints))
            return null;

        // collect the features of the selected indexes
        Version version = Version.current(cfs.keyspace.getName());
        IndexFeatureSet.Accumulator accumulator = new IndexFeatureSet.Accumulator(version);
        for (StorageAttachedIndex index : selectedIndexes)
            accumulator.accumulate(index.getIndexContext().indexFeatureSet());

        return new StorageAttachedIndexQueryPlan(cfs,
                                                 queryMetrics,
                                                 rowFilter,
                                                 ImmutableSet.copyOf(selectedIndexes),
                                                 accumulator.complete());
    }

    /**
     * Collects the indexes that can be used with the specified filtering tree without doing a full index scan.
     * </p>
     * The selected indexes are those that can satisfy at least one of the expressions of the filter, and that
     * aren't part of an OR operation that contains not indexed expressions, unless that OR operation is nested inside
     * an AND operation that has at least one indexed operation.
     * </p>
     * For example, for {@code x AND y} we can use any index in {@code x}, {@code y}, or both.
     * </p>
     * For {@code x OR y} we can't use a single index on {@code x} or {@code y} because we would need to do a full index
     * scan because of the unidexed expression. However, if both columns were indexed, we could use those two indexes.
     * </p>
     * For {@code (x OR y) AND z}, where {@code x} and {@code z} are indexed, we can use the index on {@code z}, even
     * though we will ignore the index on {@code x}.
     *
     * @param element a row filter tree node
     * @param allIndexes all the indexes in the index group
     * @param selectedIndexes the set of indexes where we'll add those indexes can be used with the specified expression
     * @param hints the user-provided index hints for the query, used to exclude indexes
     * @return {@code true} if this has collected any indexes, {@code false} otherwise
     */
    private static boolean selectedIndexes(RowFilter.FilterElement element,
                                           Set<StorageAttachedIndex> allIndexes,
                                           Set<StorageAttachedIndex> selectedIndexes,
                                           IndexHints hints)
    {
        if (element.isDisjunction()) // OR, all restrictions should have an index
        {
            Set<StorageAttachedIndex> orIndexes = new HashSet<>();
            for (RowFilter.Expression expression : element.expressions())
            {
                if (!selectedIndexes(expression, allIndexes, orIndexes, hints))
                    return false;
            }
            for (RowFilter.FilterElement child : element.children())
            {
                if (!selectedIndexes(child, allIndexes, orIndexes, hints))
                    return false;
            }
            selectedIndexes.addAll(orIndexes);
            return !orIndexes.isEmpty();
        }
        else // AND, only one restriction needs to have an index
        {
            boolean hasIndex = false;
            for (RowFilter.Expression expression : element.expressions())
            {
                hasIndex |= selectedIndexes(expression, allIndexes, selectedIndexes, hints);
            }
            for (RowFilter.FilterElement child : element.children())
            {
                hasIndex |= selectedIndexes(child, allIndexes, selectedIndexes, hints);
            }
            return hasIndex;
        }
    }

    /**
     * Collects the indexes that can be used with the specified expression.
     *
     * @param expression a row filter expression
     * @param allIndexes all the indexes in the index group
     * @param selectedIndexes the set of indexes where we'll add those indexes can be used with the specified expression
     * @param hints the user-provided index hints for the query, used to exclude indexes
     * @return {@code true} if this has collected any indexes, {@code false} otherwise
     */
    private static boolean selectedIndexes(RowFilter.Expression expression,
                                           Set<StorageAttachedIndex> allIndexes,
                                           Set<StorageAttachedIndex> selectedIndexes,
                                           IndexHints hints)
    {
        // we ignore user-defined expressions here because we don't have a way to translate their #isSatifiedBy
        // method, they will be included in the filter returned by QueryPlan#postIndexQueryFilter()
        if (expression.isUserDefined())
            return false;

        // collect the indexes that support the specified expression
        Set<StorageAttachedIndex> candidates = new HashSet<>();
        for (StorageAttachedIndex index : allIndexes)
        {
            if (index.supportsExpression(expression))
            {
                candidates.add(index);
            }
        }

        // let the hints choose the best index from those supporting the expression
        Optional<StorageAttachedIndex> preferred = hints.getBestIndexFor(candidates, p -> true, expression.operator().isAnyContains());
        preferred.ifPresent(selectedIndexes::add);

        return preferred.isPresent();
    }

    @Override
    public Set<Index> getIndexes()
    {
        return indexes;
    }

    @Override
    public long getEstimatedResultRows()
    {
        return DatabaseDescriptor.getPrioritizeSAIOverLegacyIndex() ? Long.MIN_VALUE : Long.MAX_VALUE;
    }

    @Override
    public boolean shouldEstimateInitialConcurrency()
    {
        return false;
    }

    @Override
    public StorageAttachedIndexSearcher searcherFor(ReadCommand command)
    {
        return new StorageAttachedIndexSearcher(cfs,
                                                queryMetrics,
                                                command,
                                                orderer,
                                                indexFeatureSet);
    }

    /**
     * Called on coordinator after merging replica responses before returning to client
     */
    @Override
    public Function<PartitionIterator, PartitionIterator> postProcessor(ReadCommand command)
    {
        if (!isTopK())
            return partitions -> partitions;

        // in case of top-k query, filter out rows that are not actually global top-K
        return partitions -> new TopKProcessor(command).reorder(partitions);
    }

    /**
     * @return a filter with all the expressions that are user-defined
     */
    @Override
    public RowFilter postIndexQueryFilter()
    {
        return postIndexFilter;
    }

    @Override
    public boolean supportsMultiRangeReadCommand()
    {
        return true;
    }

    /**
     * Replicas relax the filtering of reads whose row filter isn't strict, unless they are top-K queries, which always
     * filter strictly (see {@code QueryController#usesStrictFiltering}).
     */
    @Override
    public boolean returnsCandidatesForNonStrictFilters()
    {
        return !isTopK();
    }

    @Override
    public boolean isTopK()
    {
        return orderer != null;
    }

    /**
     * Validates the command against the indexes, and rejects ordering by an index on a table with clustering columns
     * if any of the indexes used by the query isn't row-aware, as the replicas would do (see
     * {@link QueryController#isOrderingSupported}). This only sees the indexes of this node, so replicas check it too.
     * <p>
     * If an index isn't queryable (for example a vector index that couldn't be built with the current version), the
     * replicas report that instead, so we don't check it here.
     */
    @Override
    public void validate(ReadCommand command) throws InvalidRequestException
    {
        Index.QueryPlan.super.validate(command);
        validatePatterns(command.rowFilter().root(), command.rowFilter().indexHints);

        if (orderer != null
            && !QueryController.isOrderingSupported(indexFeatureSet, cfs.metadata())
            && indexes.stream().allMatch(cfs.indexManager::isIndexQueryable))
        {
            throw new InvalidRequestException(QueryController.orderingNotSupportedMessage(orderer, cfs.metadata()));
        }
    }

    /**
     * Rejects a LIKE suffix or contains literal that the replicas search with an automaton, if the automaton can't be
     * compiled. Filtering compares the bytes of such a literal (see Expression#isSatisfiedBy), so the coordinator only
     * checks the LIKE patterns with inner wildcards (see LikeRestriction#addToRowFilter).
     */
    private void validatePatterns(RowFilter.FilterElement element, IndexHints hints)
    {
        if (element.isDisjunction() && !searchesIndexes(element, hints))
            return;

        for (RowFilter.Expression expression : element.expressions())
        {
            if (expression.operator() != Operator.LIKE_SUFFIX && expression.operator() != Operator.LIKE_CONTAINS)
                continue;

            Optional<StorageAttachedIndex> index = cfs.indexManager.getBestIndexFor(expression, hints, StorageAttachedIndex.class);
            if (index.isEmpty())
                continue;

            ByteBuffer literal = Operation.analyzeLiteral(index.get().getIndexContext().getQueryAnalyzerFactory(), expression.getIndexValue());
            if (literal != null)
                LikePatterns.forPatternOperator(expression.operator(), literal, expression.column().name);
        }
        for (RowFilter.FilterElement child : element.children())
            validatePatterns(child, hints);
    }

    /**
     * Whether the replicas search the indexes of an element, as they decide it on the tree they build from it (see
     * Operation.Node#usesIndexes): a conjunction with an expression an index serves, or a disjunction whose expressions
     * an index all serve and whose nested operations all search the indexes. Unlike {@link #selectedIndexes}, an IN is
     * judged as the equalities to its elements, which is how the replicas search it.
     */
    private boolean searchesIndexes(RowFilter.FilterElement element, IndexHints hints)
    {
        if (element.isDisjunction())
        {
            if (element.expressions().isEmpty() && element.children().isEmpty())
                return false;
            for (RowFilter.Expression expression : element.expressions())
                if (!searchesIndex(expression, hints))
                    return false;
            for (RowFilter.FilterElement child : element.children())
                if (!searchesIndexes(child, hints))
                    return false;
            return true;
        }

        for (RowFilter.Expression expression : element.expressions())
            if (searchesIndex(expression, hints))
                return true;
        for (RowFilter.FilterElement child : element.children())
            if (searchesIndexes(child, hints))
                return true;
        return false;
    }

    private boolean searchesIndex(RowFilter.Expression expression, IndexHints hints)
    {
        if (expression.isUserDefined())
            return false;

        if (expression.operator() == Operator.IN)
        {
            // an IN without elements matches nothing, which needs no index
            expression = Operation.Node.firstInElement(expression);
            if (expression == null)
                return true;
        }
        return cfs.indexManager.getBestIndexFor(expression, hints, StorageAttachedIndex.class).isPresent();
    }

    @Override
    public boolean isBM25()
    {
        return orderer != null && orderer.isBM25();
    }

    @Override
    public boolean usesIndexFiltering()
    {
        return usesIndexFiltering;
    }

    public static boolean hasIndexFilters(RowFilter filter, Set<Index> indexes)
    {
        for (RowFilter.Expression e : filter.expressions())
        {
            for (Index index : indexes)
            {
                if (index.supportsExpression(e) && !Orderer.isFilterExpressionOrderer(e))
                    return true;
            }
        }
        return false;
    }
}
