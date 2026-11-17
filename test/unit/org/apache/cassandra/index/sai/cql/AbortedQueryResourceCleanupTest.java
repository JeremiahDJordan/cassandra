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

package org.apache.cassandra.index.sai.cql;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.PostingListKeyRangeIterator;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.util.RandomAccessReader;

import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that a query aborted by a timeout closes the iterators it opened (CASSANDRA-19177).
 */
public class AbortedQueryResourceCleanupTest extends SAITester
{
    private static final String QUERY = "SELECT * FROM %s WHERE v >= 0";

    // set by an injection, see abortAtNextCheckpointAfter
    public static final AtomicBoolean ABORT_AT_NEXT_CHECKPOINT = new AtomicBoolean();

    private final Injections.Counter readersOpened = Injections.newCounter("RandomAccessReadersOpened")
                                                               .add(newInvokePoint().onClass(RandomAccessReader.class)
                                                                                    .onMethod("<init>"))
                                                               .build();

    // RandomAccessReader.close() is idempotent: count only the first close, which closes the reader in the rebufferer
    private final Injections.Counter readersClosed = Injections.newCounter("RandomAccessReadersClosed")
                                                               .add(newInvokePoint().onClass(RandomAccessReader.class)
                                                                                    .onMethod("close")
                                                                                    .at("INVOKE Rebufferer.closeReader"))
                                                               .build();

    private final Injections.Counter partitionsRead =
        Injections.newCounter("PartitionsRead")
                  .add(newInvokePoint().onClass(QueryController.class)
                                       .onMethod("getPartition", List.class, ReadExecutionController.class)
                                       .atExit())
                  .build();

    // every partition iterator of queryMemtableAndDisk updates the "SSTables per read" metric when closed
    private final Injections.Counter partitionsClosed =
        Injections.newCounter("PartitionsClosed")
                  .add(newInvokePoint().onClass(SinglePartitionReadCommand.class.getName() + "$1UpdateSstablesIterated")
                                       .onMethod("onPartitionClose"))
                  .build();

    @Before
    public void setup() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        disableCompaction();

        for (int sstable = 0; sstable < 3; sstable++)
        {
            for (int i = 0; i < 10; i++)
                execute("INSERT INTO %s (k, v) VALUES (?, ?)", sstable * 10 + i, i);
            flush();
        }

        // load everything that is opened once and kept, so it isn't counted below
        assertThat(execute(QUERY)).hasSize(30);

        Injections.inject(readersOpened, readersClosed, partitionsRead, partitionsClosed);
        resetCounters();
    }

    @After
    public void removeInjections()
    {
        Injections.deleteAll();
        resetCounters();
    }

    private void resetCounters()
    {
        ABORT_AT_NEXT_CHECKPOINT.set(false);
        readersOpened.reset();
        readersClosed.reset();
        partitionsRead.reset();
        partitionsClosed.reset();
    }

    @Test
    public void testCompletedQueryClosesIterators() throws Throwable
    {
        assertThat(execute(QUERY)).hasSize(30);
        assertReadersClosed();
        assertThat(partitionsRead.get()).isEqualTo(30);
        assertPartitionsClosed();
    }

    /**
     * The query times out after a partition was read, but before it is returned to the searcher.
     */
    @Test
    public void testAbortAfterPartitionReadClosesPartition() throws Throwable
    {
        abortAtNextCheckpointAfter(newInvokePoint().onClass(StorageAttachedIndexSearcher.class.getName() + "$ResultRetriever")
                                                   .onMethod("apply")
                                                   .after("INVOKE getPartition"));

        assertThatThrownBy(() -> execute(QUERY)).isNotNull();
        assertThat(partitionsRead.get()).isEqualTo(1);
        assertPartitionsClosed();
        assertReadersClosed();
    }

    /**
     * The query times out when reading the first key of the index of an SSTable, before the iterator over its keys is
     * added to the union of the SSTables.
     */
    @Test
    public void testAbortOnFirstIndexKeyClosesIndexIterator() throws Throwable
    {
        abortAtNextCheckpointAfter(newInvokePoint().onClass(PostingListKeyRangeIterator.class)
                                                   .onMethod("computeNext")
                                                   .atEntry());

        assertThatThrownBy(() -> execute(QUERY)).isNotNull();
        assertThat(partitionsRead.get()).isZero();
        assertReadersClosed();
    }

    /**
     * Makes the first {@code QueryContext.checkpoint()} after the given point throw, as a timeout does: Byteman throws
     * an exception injected at a call site from outside the try blocks of the calling method.
     */
    private static void abortAtNextCheckpointAfter(InvokePointBuilder point) throws Throwable
    {
        String flag = AbortedQueryResourceCleanupTest.class.getName() + ".ABORT_AT_NEXT_CHECKPOINT";
        Injection arm = Injections.newCustom("arm_abort")
                                  .add(point)
                                  .add(ActionBuilder.newActionBuilder()
                                                    .actions()
                                                    .doAction(flag + ".set(true)"))
                                  .build();
        Injection abort = Injections.newCustom("abort_at_checkpoint")
                                    .add(newInvokePoint().onClass(QueryContext.class)
                                                         .onMethod("checkpoint")
                                                         .atEntry())
                                    .add(ActionBuilder.newActionBuilder()
                                                      .conditions()
                                                      .when(flag + ".compareAndSet(true, false)")
                                                      .actions()
                                                      .doAction("throw new org.apache.cassandra.index.sai.utils.AbortedOperationException();"))
                                    .build();
        Injections.inject(arm, abort);
    }

    private void assertReadersClosed()
    {
        assertThat(readersOpened.get()).as("readers opened").isPositive();
        assertThat(readersClosed.get()).as("readers closed").isEqualTo(readersOpened.get());
    }

    private void assertPartitionsClosed()
    {
        assertThat(partitionsClosed.get()).as("partition iterators closed").isEqualTo(partitionsRead.get());
    }
}
