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

package org.apache.cassandra.db.commitlog;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.view.ViewManager;
import org.apache.cassandra.exceptions.RetryOnDifferentSystemException;

import static org.apache.cassandra.db.ReadCommand.PotentialTxnConflicts.ALLOW;
import static org.apache.cassandra.db.ReadCommand.PotentialTxnConflicts.DISALLOW;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CommitLogReplayConsensusMigrationTest extends CQLTester
{
    @Test
    public void testReplayWritesToTableMigratingToAccord() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("ALTER TABLE %s WITH transactional_mode = 'full'");

        // Writes that are allowed to conflict with transactions, such as Paxos commits, are still applied
        // to ranges that are migrating to Accord
        for (int k = 0; k < 10; k++)
            new Mutation(new RowUpdateBuilder(getCurrentColumnFamilyStore().metadata(), 0, k).add("v", k).buildUpdate(), ALLOW).apply();

        // a write that must go through Accord is rejected, and isn't applied on replay either
        Mutation rejected = new Mutation(new RowUpdateBuilder(getCurrentColumnFamilyStore().metadata(), 0, 100).add("v", 100).buildUpdate(), DISALLOW);
        assertThatThrownBy(rejected::apply).isInstanceOf(RetryOnDifferentSystemException.class);

        getCurrentColumnFamilyStore().clearUnsafe();
        CommitLog.instance.resetUnsafe(false);

        assertRows(execute("SELECT k, v FROM %s WHERE k = 3"), row(3, 3));
        assertRows(execute("SELECT count(*) FROM %s"), row(10L));
    }

    @Test
    public void testWriteWaitingForViewLockDuringMigration() throws Throwable
    {
        DatabaseDescriptor.setMaterializedViewsEnabled(true);
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        createViewAsync("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)");
        ColumnFamilyStore cfs = getColumnFamilyStore(KEYSPACE, table);
        Mutation mutation = new Mutation(new RowUpdateBuilder(cfs.metadata(), 0, 1).add("v", 1).buildUpdate(), DISALLOW);

        // the write waits for the view lock while the table starts migrating to Accord
        Lock lock = ViewManager.acquireLockFor(Objects.hash(mutation.key().getKey(), cfs.metadata.id));
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread writer = new Thread(() -> {
            try
            {
                mutation.apply();
            }
            catch (Throwable t)
            {
                failure.set(t);
            }
        });
        writer.start();
        try
        {
            Util.spinAssertEquals(Thread.State.TIMED_WAITING, writer::getState, 10);
            execute(String.format("ALTER TABLE %s.%s WITH transactional_mode = 'full'", KEYSPACE, table));
        }
        finally
        {
            lock.unlock();
            writer.join();
        }

        assertThat(failure.get()).isInstanceOf(RetryOnDifferentSystemException.class);
        cfs.clearUnsafe();
        CommitLog.instance.resetUnsafe(false);
        assertEmpty(execute(String.format("SELECT * FROM %s.%s WHERE k = 1", KEYSPACE, table)));
    }
}
