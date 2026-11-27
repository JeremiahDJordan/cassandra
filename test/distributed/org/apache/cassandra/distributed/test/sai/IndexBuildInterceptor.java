/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.test.sai;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.index.sai.StorageAttachedIndexBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

/**
 * Fails or holds the build of an SSTable-attached index on the SSTables of a table: a ByteBuddy rebase of
 * {@link StorageAttachedIndexBuilder} delegates {@code indexSSTable} to {@link IndexSSTableInterceptor}. The state is
 * in the class loader of each instance, so tests set it on the instance.
 */
public class IndexBuildInterceptor
{
    public static final String INJECTED_FAILURE = "Injected index build failure";

    public static final Hold BUILD = new Hold();

    private static volatile String failBuildAfterSSTablesTable;
    // the builds of an SSTable-attached index on several SSTables can run in parallel
    private static final AtomicInteger sstablesBeforeFailure = new AtomicInteger();

    public static void ignoreInjectedFailures(Cluster cluster)
    {
        cluster.setUncaughtExceptionsFilter(t -> Throwables.anyCauseMatches(t, c -> INJECTED_FAILURE.equals(c.getMessage())));
    }

    public static void failBuildAfterSSTables(String table, int sstables)
    {
        sstablesBeforeFailure.set(sstables);
        failBuildAfterSSTablesTable = table;
    }

    public static class IndexSSTableInterceptor
    {
        @SuppressWarnings("unused")
        public static boolean indexSSTable(SSTableReader sstable, @SuperCall Callable<Boolean> zuper) throws Exception
        {
            if (sstable.descriptor.cfname.equals(failBuildAfterSSTablesTable) && sstablesBeforeFailure.getAndDecrement() <= 0)
                throw new RuntimeException(INJECTED_FAILURE);

            BUILD.reach(sstable.descriptor.cfname);
            return zuper.call();
        }
    }

    /**
     * Holds the threads that reach it for a table until it is released, for one minute at most.
     */
    public static class Hold
    {
        private volatile String table;
        private volatile CountDownLatch reached = CountDownLatch.newCountDownLatch(1);
        private volatile CountDownLatch released = CountDownLatch.newCountDownLatch(1);

        public void hold(String table)
        {
            reached = CountDownLatch.newCountDownLatch(1);
            released = CountDownLatch.newCountDownLatch(1);
            this.table = table;
        }

        public boolean isHeld()
        {
            return reached.count() == 0;
        }

        public void release()
        {
            table = null;
            released.decrement();
        }

        /** Waits for the release if the hold is for {@code table}; returns whether it was. */
        public boolean reach(String table)
        {
            return reach(table, false);
        }

        /** As {@link #reach}, but only the first thread to reach the hold waits. */
        public boolean reachOnce(String table)
        {
            return reach(table, true);
        }

        private boolean reach(String table, boolean once)
        {
            if (!table.equals(this.table))
                return false;

            if (once)
                this.table = null;
            reached.decrement();
            released.awaitUninterruptibly(1, TimeUnit.MINUTES);
            return true;
        }
    }
}
