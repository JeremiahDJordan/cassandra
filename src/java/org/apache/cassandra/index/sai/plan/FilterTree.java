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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import com.google.common.collect.ListMultimap;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.ColumnMetadata.Kind;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.index.sai.plan.Operation.OperationType;

/**
 * Tree-like structure to filter base table data using indexed expressions and non-user-defined filters.
 *
 * This is needed because:
 * 1. SAI doesn't index tombstones, base data may have been shadowed.
 * 2. SAI indexes partition offset, not all rows in partition match index condition.
 * 3. Replica filter protecting may fetch data that doesn't match index expressions.
 * <p>
 * If the query can't be strictly filtered on local data (see {@link QueryController#usesStrictFiltering()}) and
 * has index matches on unrepaired data, the intersections are relaxed into unions. If all the matches are repaired,
 * the indexed expressions are still intersected, but the expressions of several unindexed mutable columns, or of
 * several elements of an unindexed non-frozen collection, are unioned (CASSANDRA-20189).
 */
public class FilterTree
{
    protected final OperationType op;
    protected final ListMultimap<ColumnMetadata, Expression> expressions;
    protected final List<FilterTree> children = new ArrayList<>();
    private final boolean isStrict;
    private final QueryContext context;

    /** The mutable columns of this node with expressions that aren't served by index scans */
    private final Set<ColumnMetadata> unindexedColumns;

    /** Lazily computed result of {@link #unindexedMutableTerms()}, as the children are added after construction */
    private int unindexedMutableTerms = -1;

    /**
     * @param operation the operation combining the expressions and the children of this node
     * @param expressions the expressions of this node, by column
     * @param isStrict whether the intersections can be strictly evaluated on local data
     * @param context the context of the query, which tells whether there are index matches on unrepaired data;
     *                it can only be {@code null} if {@code isStrict} is {@code true}
     * @param unindexedColumns the mutable (non-primary key) columns in {@code expressions} with expressions that
     *                         aren't served by index scans
     */
    FilterTree(OperationType operation,
               ListMultimap<ColumnMetadata, Expression> expressions,
               boolean isStrict,
               QueryContext context,
               Set<ColumnMetadata> unindexedColumns)
    {
        assert isStrict || context != null : "Non-strict filtering requires a query context";
        this.op = operation;
        this.expressions = expressions;
        this.isStrict = isStrict;
        this.context = context;
        this.unindexedColumns = unindexedColumns;
    }

    void addChild(FilterTree child)
    {
        children.add(child);
    }

    public boolean isSatisfiedBy(DecoratedKey key, Unfiltered currentCluster, Row staticRow)
    {
        return isSatisfiedBy(key, currentCluster, staticRow, false);
    }

    /**
     * Whether the static row of a partition without other matching rows satisfies this tree, as the only row of the
     * partition: an expression on a regular or clustering column is never satisfied, since that row has no value for
     * them, as in {@link org.apache.cassandra.db.filter.RowFilter.FilterElement#isSatisfiedByStaticRow}. Evaluated on
     * the static row, a negation on such a column, like {@code v != 2}, would be satisfied by the missing value, and the
     * partition would be returned for its static row although none of its rows matches.
     */
    public boolean isSatisfiedByStaticRow(DecoratedKey key, Row staticRow)
    {
        return isSatisfiedBy(key, staticRow, staticRow, true);
    }

    private boolean isSatisfiedBy(DecoratedKey key, Unfiltered currentCluster, Row staticRow, boolean staticRowOnly)
    {
        OperationType localOperator = localOperator();

        if (localOperator == OperationType.AND && !isStrict && unindexedMutableTerms() > 1)
            return isSatisfiedByIsolatingUnindexed(key, currentCluster, staticRow, staticRowOnly);

        boolean result = localSatisfiedBy(key, currentCluster, staticRow, localOperator, staticRowOnly);

        if (shouldReturnNow(localOperator, result))
            return result;

        for (FilterTree child : children)
        {
            result = localOperator.apply(result, child.isSatisfiedBy(key, currentCluster, staticRow, staticRowOnly));
            if (shouldReturnNow(localOperator, result))
                return result;
        }

        return result;
    }

    /**
     * @return the operation to evaluate this node with on local data, which is relaxed from AND to OR unless the
     * coordinator says that strict filtering is safe, or all the index matches are on repaired data
     */
    private OperationType localOperator()
    {
        return isStrict || !context.hasUnrepairedMatches() ? op : OperationType.OR;
    }

    private boolean localSatisfiedBy(DecoratedKey key, Unfiltered currentCluster, Row staticRow, OperationType localOperator, boolean staticRowOnly)
    {
        if (currentCluster == null || !currentCluster.isRow())
            return false;

        final long now = FBUtilities.nowInSeconds();
        boolean result = localOperator == OperationType.AND;

        Iterator<ColumnMetadata> columnIterator = expressions.keySet().iterator();
        while(columnIterator.hasNext())
        {
            ColumnMetadata column = columnIterator.next();
            Row row = column.kind == Kind.STATIC ? staticRow : (Row)currentCluster;

            // If there is a column with multiple expressions that can mean an OR or (in the case of map
            // collections) it can mean different map indexes.
            List<Expression> filters = expressions.get(column);

            // We do a reverse iteration over the filters because NOT_EQ operations will be at the end
            // of the filter list and we want to check them first.
            ListIterator<Expression> filterIterator = filters.listIterator(filters.size());
            while(filterIterator.hasPrevious())
            {
                Expression filter = filterIterator.previous();
                result = localOperator.apply(result, !neverSatisfied(column, staticRowOnly) && isSatisfiedBy(key, row, column, filter, now));

                if (shouldReturnNow(localOperator, result))
                    return result;
            }
        }
        return result;
    }

    /**
     * Evaluates an intersection that can't be strictly evaluated, when all the index matches are on repaired data.
     * <p>
     * Repaired data is consistent across replicas, so the indexed expressions are still intersected. However, the
     * unindexed mutable columns might have been updated on different replicas, so the expressions on those columns,
     * and the nested operations restricting those columns, are unioned (CASSANDRA-20189).
     */
    private boolean isSatisfiedByIsolatingUnindexed(DecoratedKey key, Unfiltered currentCluster, Row staticRow, boolean staticRowOnly)
    {
        if (currentCluster == null || !currentCluster.isRow())
            return false;

        final long now = FBUtilities.nowInSeconds();
        boolean unindexedResult = false;

        for (ColumnMetadata column : expressions.keySet())
        {
            Row row = column.kind == Kind.STATIC ? staticRow : (Row) currentCluster;
            List<Expression> filters = expressions.get(column);
            boolean isUnindexed = unindexedColumns.contains(column);

            // Once an unindexed column matches, the union of the unindexed columns does too
            if (isUnindexed && unindexedResult)
                continue;

            // Reverse iteration to check the NOT_EQ expressions first, as in #localSatisfiedBy
            ListIterator<Expression> filterIterator = filters.listIterator(filters.size());
            while (filterIterator.hasPrevious())
            {
                Expression filter = filterIterator.previous();
                boolean matches = !neverSatisfied(column, staticRowOnly) && isSatisfiedBy(key, row, column, filter, now);
                if (isUnindexed)
                {
                    unindexedResult = matches;
                    if (matches)
                        break;
                }
                else if (!matches)
                {
                    return false;
                }
            }
        }

        for (FilterTree child : children)
        {
            if (!child.restrictsUnindexedMutableColumns())
            {
                if (!child.isSatisfiedBy(key, currentCluster, staticRow, staticRowOnly))
                    return false;
            }
            else if (!unindexedResult)
            {
                unindexedResult = child.isSatisfiedBy(key, currentCluster, staticRow, staticRowOnly);
            }
        }

        return unindexedResult;
    }

    /**
     * @return the number of terms of this node, if it is an intersection, that restrict mutable columns that aren't
     * served by index scans: the unindexed columns of the node, each expression on an unindexed non-frozen collection
     * (its elements can be updated on different replicas), and its children restricting any such column
     */
    private int unindexedMutableTerms()
    {
        if (unindexedMutableTerms < 0)
        {
            int terms = 0;
            for (ColumnMetadata column : unindexedColumns)
                terms += TypeUtil.isNonFrozenCollection(column.type) ? expressions.get(column).size() : 1;
            for (FilterTree child : children)
            {
                if (child.restrictsUnindexedMutableColumns())
                    terms++;
            }
            unindexedMutableTerms = terms;
        }
        return unindexedMutableTerms;
    }

    /**
     * @return {@code true} if this node or any of its descendants restricts a mutable column that isn't served by
     * index scans, {@code false} otherwise
     */
    private boolean restrictsUnindexedMutableColumns()
    {
        return unindexedMutableTerms() > 0;
    }

    private static boolean neverSatisfied(ColumnMetadata column, boolean staticRowOnly)
    {
        return staticRowOnly && (column.kind == Kind.REGULAR || column.kind == Kind.CLUSTERING);
    }

    private static boolean isSatisfiedBy(DecoratedKey key, Row row, ColumnMetadata column, Expression filter, long now)
    {
        if (filter.unindexed != null)
        {
            return row != null && filter.unindexed.isSatisfiedBy(filter.context.columnFamilyStore().metadata(), key, row, now);
        }
        else if (TypeUtil.isNonFrozenCollection(column.type))
        {
            Iterator<ByteBuffer> valueIterator = filter.context.getValuesOf(row, now);
            return filter.isSatisfiedBy(valueIterator);
        }
        else if (filter.context.isFrozenCollectionElements())
        {
            return filter.isSatisfiedBy(filter.context.getFrozenCollectionElementsOf(key, row, now));
        }
        else
        {
            ByteBuffer value = filter.context.getValueOf(key, row, now);
            return filter.isSatisfiedBy(value);
        }
    }

    /**
     * When evaluating an AND expression, if the current result is false, we can return immediately.
     * When evaluating an OR expression, if the current result is true, we can return immediately.
     * @param operator the operation being evaluated
     * @param result the current result
     * @return true if it is valid to return the current result
     */
    private static boolean shouldReturnNow(OperationType operator, boolean result)
    {
        return (operator == OperationType.AND && !result) || (operator == OperationType.OR && result);
    }

    /**
     * Returns the highest number of SSTable indexes that a single indexed expression of this filter tree searches: those
     * whose key range overlaps the given key range, which {@link QueryView.Builder} puts in the query view of the column
     * and {@link org.apache.cassandra.index.sai.iterators.KeyRangeTermIterator} searches. This includes the expressions
     * that order the results (ANN, BM25, ORDER BY), which search every index in the key range.
     * <p>
     * This is what the {@code sai_sstable_indexes_per_query} guardrail limits: how many SSTable indexes a single
     * column search touches, not how many indexed columns the query has.
     *
     * @param keyRange the key range of the query
     * @return the highest number of SSTable indexes searched for an expression of this tree, or 0 if there are none
     */
    public int maxSSTableIndexesPerExpression(AbstractBounds<PartitionPosition> keyRange)
    {
        int max = 0;
        for (Expression expression : expressions.values())
        {
            if (!expression.context.isIndexed())
                continue;

            int count = 0;
            for (SSTableIndex index : expression.context.getView().getIndexes())
            {
                if (QueryView.Builder.indexInRange(index, keyRange))
                    count++;
            }
            max = Math.max(max, count);
        }
        for (FilterTree child : children)
            max = Math.max(max, child.maxSSTableIndexesPerExpression(keyRange));
        return max;
    }
}
