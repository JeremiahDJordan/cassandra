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
package org.apache.cassandra.distributed.test;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.ActiveOperations;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.TableOperation;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.LogAction;
import org.apache.cassandra.utils.NonThrowingCloseable;

import static net.bytebuddy.matcher.ElementMatchers.named;

public class UpgradeSSTablesTest extends TestBaseImpl
{
    /**
     * TRUNCATE must interrupt a running upgradesstables, including the per-sstable tasks that upgradesstables creates
     * after TRUNCATE's first cancellation pass, instead of waiting for the whole upgrade to finish.
     */
    @Test
    public void truncateWhileUpgrading() throws Throwable
    {
        try (ICluster<IInvokableInstance> cluster = init(builder().withNodes(1).withInstanceInitializer(UpgradeSStablesLatchByteman::install).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
            cluster.get(1).acceptsOnInstance((String ks) -> {
                ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore("tbl");
                cfs.disableAutoCompaction();
                CompactionManager.instance.setCoreCompactorThreads(1);
                CompactionManager.instance.setMaximumCompactorThreads(1);
            }).accept(KEYSPACE);

            String blob = "blob";
            for (int i = 0; i < 10; i++)
                blob += blob;

            for (int i = 0; i < 500; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?,?,?)",
                                               ConsistencyLevel.QUORUM, i, i, blob);
                if (i > 0 && i % 100 == 0)
                    cluster.get(1).nodetool("flush", KEYSPACE, "tbl");
            }

            LogAction logAction = cluster.get(1).logs();
            logAction.mark();

            Future<?> upgrade = CompletableFuture.runAsync(() -> {
                cluster.get(1).nodetool("upgradesstables", "-a", KEYSPACE, "tbl");
            });

            Assert.assertTrue(cluster.get(1).callOnInstance(() -> Uninterruptibles.awaitUninterruptibly(UpgradeSStablesLatchByteman.starting, 1, TimeUnit.MINUTES)));
            cluster.get(1).runOnInstance(() -> UpgradeSStablesLatchByteman.start.countDown());
            cluster.schemaChange("TRUNCATE " + KEYSPACE + ".tbl");
            upgrade.get();
            Assert.assertFalse(logAction.grep("Compaction interrupted").getResult().isEmpty());
        }
    }

    public static class UpgradeSStablesLatchByteman
    {
        // Will be initialized in the context of the instance class loader
        static CountDownLatch starting = new CountDownLatch(1);
        static CountDownLatch start = new CountDownLatch(1);

        public static void install(ClassLoader classLoader, Integer num)
        {
            new ByteBuddy().rebase(ActiveOperations.class)
                           .method(named("onOperationStart"))
                           .intercept(MethodDelegation.to(UpgradeSStablesLatchByteman.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static NonThrowingCloseable onOperationStart(TableOperation op, @SuperCall Callable<NonThrowingCloseable> zuperCall)
        {
            try
            {
                NonThrowingCloseable result = zuperCall.call();
                if (op.getProgress().operationType() == OperationType.UPGRADE_SSTABLES)
                {
                    starting.countDown();
                    Assert.assertTrue(Uninterruptibles.awaitUninterruptibly(start, 1, TimeUnit.MINUTES));
                }
                return result;
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
