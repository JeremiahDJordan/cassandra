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
package org.apache.cassandra.index.sai.disk.v1;

import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingListRangeIterator;
import org.apache.cassandra.index.sai.disk.v1.segment.Segment;
import org.apache.cassandra.index.sai.disk.v1.vector.RowIdToPrimaryKeyWithScoreIterator;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.inject.ActionBuilder.newActionBuilder;
import static org.apache.cassandra.inject.Expression.expr;
import static org.apache.cassandra.inject.Expression.quote;
import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A query whose search fails in a segment, an SSTable index or an expression closes the iterators of the searches
 * done before it, for filtering and for ANN ordering.
 */
public class IndexSearchFailureTest extends SAITester
{
    private static final AtomicInteger searches = new AtomicInteger();

    public static boolean isSecondSearch()
    {
        return searches.incrementAndGet() == 2;
    }

    @Test
    public void testFailedSegmentSearchClosesEarlierSegments() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        disableCompaction();
        for (int k = 0; k < 100; k++)
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", k, k);
        flush();
        setSegmentWriteBufferSpace(0);
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        assertFailedSearchClosesIterators(Segment.class, "search", PostingListRangeIterator.class, () -> execute("SELECT k FROM %s WHERE v >= 0"));
    }

    @Test
    public void testFailedSSTableIndexSearchClosesEarlierSSTableIndexes() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        disableCompaction();
        execute("INSERT INTO %s (k, v) VALUES (1, 1)");
        flush();
        execute("INSERT INTO %s (k, v) VALUES (2, 2)");
        flush();

        assertFailedSearchClosesIterators(V1SSTableIndex.class, "search", PostingListRangeIterator.class, () -> execute("SELECT k FROM %s WHERE v >= 0"));
    }

    @Test
    public void testFailedExpressionSearchClosesEarlierRepairedResults() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int)");
        createIndex("CREATE INDEX ON %s(v1) USING 'sai'");
        createIndex("CREATE INDEX ON %s(v2) USING 'sai'");
        disableCompaction();
        execute("INSERT INTO %s (k, v1, v2) VALUES (1, 1, 1)");
        flush();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, 1, null, false);
            sstable.reloadSSTableMetadata();
        }

        // A filter needing reconciliation on two mutable columns isn't strict, so each expression is searched in the
        // repaired SSTables separately
        TableMetadata metadata = cfs.metadata();
        RowFilter filter = RowFilter.create(true);
        filter.add(metadata.getColumn(ColumnIdentifier.getInterned("v1", false)), Operator.GTE, Int32Type.instance.decompose(0));
        filter.add(metadata.getColumn(ColumnIdentifier.getInterned("v2", false)), Operator.GTE, Int32Type.instance.decompose(0));
        ReadCommand command = PartitionRangeReadCommand.create(metadata,
                                                               FBUtilities.nowInSeconds(),
                                                               ColumnFilter.all(metadata),
                                                               filter,
                                                               DataLimits.NONE,
                                                               DataRange.allData(metadata.partitioner));

        assertFailedSearchClosesIterators(V1SSTableIndex.class, "search", PostingListRangeIterator.class, () -> {
            try (ReadExecutionController controller = command.executionController();
                 UnfilteredPartitionIterator partitions = command.executeLocally(controller))
            {
                while (partitions.hasNext())
                    partitions.next().close();
            }
        });
    }

    @Test
    public void testFailedSegmentOrderByClosesEarlierSegments() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 2>)");
        disableCompaction();
        for (int k = 0; k < 100; k++)
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", k, vector((float) k, (float) k + 1));
        flush();
        setSegmentWriteBufferSpace(0);
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        assertFailedSearchClosesIterators(Segment.class, "orderBy", RowIdToPrimaryKeyWithScoreIterator.class,
                                          () -> execute("SELECT k FROM %s ORDER BY v ANN OF [1.0, 2.0] LIMIT 3"));
    }

    @Test
    public void testFailedSegmentOrderResultsByClosesEarlierSegments() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, x int, v vector<float, 2>)");
        disableCompaction();
        for (int k = 0; k < 100; k++)
            execute("INSERT INTO %s (k, x, v) VALUES (?, 1, ?)", k, vector((float) k, (float) k + 1));
        flush();
        setSegmentWriteBufferSpace(0);
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        createIndex("CREATE INDEX ON %s(x) USING 'sai'");

        assertFailedSearchClosesIterators(Segment.class, "orderResultsBy", RowIdToPrimaryKeyWithScoreIterator.class,
                                          () -> execute("SELECT k FROM %s WHERE x = 1 ORDER BY v ANN OF [1.0, 2.0] LIMIT 3"));
    }

    private void assertFailedSearchClosesIterators(Class<?> searched, String method, Class<?> iterator, ThrowingCallable query) throws Throwable
    {
        Injections.Counter opened = Injections.newCounter("IteratorOpened")
                                              .add(newInvokePoint().onClass(iterator).onMethod("<init>").atExit())
                                              .build();
        Injections.Counter closed = Injections.newCounter("IteratorClosed")
                                              .add(newInvokePoint().onClass(iterator).onMethod("close"))
                                              .build();
        Injection failSecondSearch = Injections.newCustom("FailSecondSearch")
                                               .add(newInvokePoint().onClass(searched).onMethod(method))
                                               .add(newActionBuilder().conditions()
                                                                      .when(expr(IndexSearchFailureTest.class.getName() + ".isSecondSearch()"))
                                                                      .toActionBuilder()
                                                                      .actions()
                                                                      .doThrow(RuntimeException.class, quote("Injected failure")))
                                               .build();
        searches.set(0);
        Injections.inject(opened, closed, failSecondSearch);

        assertThatThrownBy(query).hasStackTraceContaining("Injected failure");

        assertThat(searches.get()).isGreaterThanOrEqualTo(2);
        assertThat(opened.get()).isGreaterThan(0);
        assertThat(closed.get()).isEqualTo(opened.get());
    }
}
