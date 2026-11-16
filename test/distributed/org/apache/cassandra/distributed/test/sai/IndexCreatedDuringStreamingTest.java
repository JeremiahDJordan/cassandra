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

package org.apache.cassandra.distributed.test.sai;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.FieldValue;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.streaming.CassandraStreamReceiver;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * An SSTable-attached index created while a stream is being received must cover the streamed SSTables: their writers
 * were created before the index existed, so they were written without its components, and the initial build of the
 * index has run before the SSTables were added to the table.
 */
public class IndexCreatedDuringStreamingTest extends TestBaseImpl
{
    private static final int ROWS = 100;

    private static Cluster cluster;
    private static IInvokableInstance sender;
    private static IInvokableInstance receiver;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = init(Cluster.build(2)
                              .withConfig(c -> c.with(NETWORK, GOSSIP))
                              .withInstanceInitializer((cl, num) -> {
                                  if (num == 2)
                                      BBHelper.install(cl);
                              })
                              .start());
        cluster.disableAutoCompaction(KEYSPACE);
        sender = cluster.get(1);
        receiver = cluster.get(2);
    }

    @AfterClass
    public static void closeCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testIndexCreatedDuringStream()
    {
        String table = "no_existing_index";
        createTable(table, false);
        streamWhileCreatingIndex(table, false).asserts().success();
        assertRowsThroughIndex(table, "v", ROWS);
    }

    @Test
    public void testIndexCreatedDuringStreamWithExistingIndex()
    {
        String table = "existing_index";
        createTable(table, true);
        streamWhileCreatingIndex(table, false).asserts().success();
        assertRowsThroughIndex(table, "w", ROWS);
        assertRowsThroughIndex(table, "v", ROWS);
    }

    @Test
    public void testIndexCreatedDuringEntireSSTableStream()
    {
        String table = "entire_sstable";
        createTable(table, false);

        // The streamed SSTables are validated against all the indexes of the table, so the stream fails, and the
        // received SSTables are removed
        NodeToolResult result = streamWhileCreatingIndex(table, true);
        result.asserts().failure();
        assertThat(result.getError()).hasStackTraceContaining("Incomplete per-SSTable index build");
        assertThat(receiver.executeInternal(withKeyspace("SELECT * FROM %s." + table))).isEmpty();
        assertThat(receiver.callOnInstance(() -> dataFiles(table))).isZero();

        // A new stream carries the components of the new index
        repair(table).asserts().success();
        assertRowsThroughIndex(table, "v", ROWS);
    }

    @Test
    public void testIndexCreatedDuringReceiverIndexBuild()
    {
        String table = "during_receiver_build";
        createTable(table, false);

        // Hold the receiver after it has built the index on v, and create an index on w meanwhile
        receiver.runOnInstance(() -> BBHelper.holdAfterBuild(table));
        CompletableFuture<NodeToolResult> repair = streamWhileCreatingIndexAsync(table, false, () -> {
            await().atMost(1, TimeUnit.MINUTES).until(() -> receiver.callOnInstance(BBHelper::isHeldAfterBuild));
            cluster.schemaChange(withKeyspace("CREATE INDEX " + table + "_w_idx ON %s." + table + "(w) USING 'sai'"));
            SAIUtil.waitForIndexQueryableOnFirstNode(cluster, KEYSPACE, table + "_w_idx");
        });

        repair.join().asserts().success();
        assertRowsThroughIndex(table, "v", ROWS);
        assertRowsThroughIndex(table, "w", ROWS);
    }

    @Test
    public void testFailedReceiverIndexBuild()
    {
        String table = "failed_receiver_build";
        createTable(table, false);

        // The build of the new index fails, so does the stream, and the received SSTables are removed
        receiver.runOnInstance(() -> BBHelper.failBuild(table));
        NodeToolResult result = streamWhileCreatingIndexAsync(table, false, () -> {}).join();
        receiver.runOnInstance(() -> BBHelper.failBuild(null));
        result.asserts().failure();
        assertThat(receiver.executeInternal(withKeyspace("SELECT * FROM %s." + table))).isEmpty();
        assertThat(receiver.callOnInstance(() -> dataFiles(table))).isZero();

        // A new stream succeeds
        repair(table).asserts().success();
        assertRowsThroughIndex(table, "v", ROWS);
    }

    private static int dataFiles(String table)
    {
        LifecycleTransaction.waitForDeletions();
        int dataFiles = 0;
        for (File directory : Keyspace.open(KEYSPACE).getColumnFamilyStore(table).getDirectories().getCFDirectories())
            for (File file : directory.tryList())
                if (file.name().endsWith("-Data.db"))
                    dataFiles++;
        return dataFiles;
    }

    private static void createTable(String table, boolean existingIndex)
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (pk int PRIMARY KEY, v int, w int)"));
        if (existingIndex)
        {
            cluster.schemaChange(withKeyspace("CREATE INDEX " + table + "_w_idx ON %s." + table + "(w) USING 'sai'"));
            SAIUtil.waitForIndexQueryableOnFirstNode(cluster, KEYSPACE, table + "_w_idx");
        }

        for (int i = 0; i < ROWS; i++)
            sender.executeInternal(withKeyspace("INSERT INTO %s." + table + " (pk, v, w) VALUES (?, ?, ?)"), i, i, i);
        sender.flush(KEYSPACE);
    }

    private static NodeToolResult repair(String table)
    {
        return receiver.nodetoolResult("repair", "--full", KEYSPACE, table);
    }

    /**
     * Repairs the table, so the receiver streams it from the sender, holding the receiver once it has written all the
     * SSTables of the table and before it adds them to the table, and creates an index on {@code v} while it holds.
     */
    private static NodeToolResult streamWhileCreatingIndex(String table, boolean entireSSTables)
    {
        return streamWhileCreatingIndexAsync(table, entireSSTables, () -> {}).join();
    }

    /**
     * As {@link #streamWhileCreatingIndex}, running {@code afterRelease} once the receiver is released.
     */
    private static CompletableFuture<NodeToolResult> streamWhileCreatingIndexAsync(String table, boolean entireSSTables, Runnable afterRelease)
    {
        cluster.forEach(i -> i.runOnInstance(() -> DatabaseDescriptor.setStreamEntireSSTables(entireSSTables)));
        receiver.runOnInstance(() -> BBHelper.hold(table));

        CompletableFuture<NodeToolResult> repair = CompletableFuture.supplyAsync(() -> repair(table));
        try
        {
            try
            {
                await().atMost(1, TimeUnit.MINUTES).until(() -> repair.isDone() || receiver.callOnInstance(BBHelper::isHeld));
                assertThat(receiver.callOnInstance(BBHelper::isHeld)).describedAs("The receiver should be held").isTrue();

                String index = table + "_v_idx";
                cluster.schemaChange(withKeyspace("CREATE INDEX " + index + " ON %s." + table + "(v) USING 'sai'"));
                SAIUtil.waitForIndexQueryableOnFirstNode(cluster, KEYSPACE, index);
            }
            finally
            {
                receiver.runOnInstance(BBHelper::release);
            }
            afterRelease.run();
        }
        finally
        {
            receiver.runOnInstance(BBHelper::releaseAfterBuild);
        }

        return repair;
    }

    private static void assertRowsThroughIndex(String table, String column, int expected)
    {
        int found = 0;
        for (int i = 0; i < ROWS; i++)
            found += receiver.executeInternal(withKeyspace("SELECT pk FROM %s." + table + " WHERE " + column + " = ?"), i).length;
        assertThat(found).describedAs("Rows found through the index on " + column).isEqualTo(expected);
    }

    public static class BBHelper
    {
        private static volatile String heldTable;
        private static volatile CountDownLatch held = CountDownLatch.newCountDownLatch(1);
        private static volatile CountDownLatch release = CountDownLatch.newCountDownLatch(1);

        private static volatile String heldAfterBuildTable;
        private static volatile CountDownLatch heldAfterBuild = CountDownLatch.newCountDownLatch(1);
        private static volatile CountDownLatch releaseAfterBuild = CountDownLatch.newCountDownLatch(1);
        private static volatile String failBuildTable;

        static void install(ClassLoader classLoader)
        {
            new ByteBuddy().rebase(CassandraStreamReceiver.class)
                           .method(named("finished"))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            new ByteBuddy().rebase(SecondaryIndexManager.class)
                           .method(named("buildSSTableAttachedIndexesBlocking"))
                           .intercept(MethodDelegation.to(BuildInterceptor.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        static void holdAfterBuild(String table)
        {
            heldAfterBuild = CountDownLatch.newCountDownLatch(1);
            releaseAfterBuild = CountDownLatch.newCountDownLatch(1);
            heldAfterBuildTable = table;
        }

        static boolean isHeldAfterBuild()
        {
            return heldAfterBuild.count() == 0;
        }

        static void releaseAfterBuild()
        {
            heldAfterBuildTable = null;
            releaseAfterBuild.decrement();
        }

        static void failBuild(String table)
        {
            failBuildTable = table;
        }

        static void hold(String table)
        {
            held = CountDownLatch.newCountDownLatch(1);
            release = CountDownLatch.newCountDownLatch(1);
            heldTable = table;
        }

        static boolean isHeld()
        {
            return held.count() == 0;
        }

        static void release()
        {
            heldTable = null;
            release.decrement();
        }

        @SuppressWarnings("unused")
        public static void finished(@FieldValue("cfs") ColumnFamilyStore cfs, @SuperCall Callable<Void> zuper) throws Exception
        {
            if (cfs.name.equals(heldTable))
            {
                held.decrement();
                release.awaitUninterruptibly(1, TimeUnit.MINUTES);
            }
            zuper.call();
        }

        public static class BuildInterceptor
        {
            @SuppressWarnings("unused")
            public static void buildSSTableAttachedIndexesBlocking(@FieldValue("baseCfs") ColumnFamilyStore cfs, @SuperCall Callable<Void> zuper) throws Exception
            {
                if (cfs.name.equals(failBuildTable))
                    throw new RuntimeException("Injected index build failure");

                zuper.call();

                if (cfs.name.equals(heldAfterBuildTable))
                {
                    heldAfterBuildTable = null;
                    heldAfterBuild.decrement();
                    releaseAfterBuild.awaitUninterruptibly(1, TimeUnit.MINUTES);
                }
            }
        }
    }
}
