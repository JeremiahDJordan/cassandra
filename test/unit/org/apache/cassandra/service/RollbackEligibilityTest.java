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

package org.apache.cassandra.service;

import java.util.Collections;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.batchlog.Batch;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.hints.Hint;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.FileUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.StorageCompatibilityMode;
import org.apache.cassandra.utils.TimeUUID;

import static org.assertj.core.api.Assertions.assertThat;

public class RollbackEligibilityTest extends SAITester
{
    @BeforeClass
    public static void setUpMode()
    {
        // the default test mode stores Paxos state and batches at messaging version 12, which Apache reads
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.UPGRADING);
    }

    @Test
    public void testBlockers() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (pk, v) VALUES (0, 0)");
        flush();
        assertThat(RollbackEligibility.blockers()).isEmpty();

        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        assertThat(RollbackEligibility.blockers())
        .containsExactly("SAI indexes without complete aa groups on " + KEYSPACE + '.' + currentTable() + "; rewrite them with nodetool upgradesstables -a");

        execute("INSERT INTO %s (pk, v) VALUES (1, 1) IF NOT EXISTS");
        assertThat(RollbackEligibility.blockers()).contains("Paxos state of DataStax versions; it is purged as paxos_state_purging sets (after the table's gc_grace_seconds, at least 3 hours with legacy, or after Paxos repair with repaired); check that it has cleared");

        Mutation mutation = new RowUpdateBuilder(getCurrentColumnFamilyStore().metadata(), FBUtilities.timestampMicros(), 2).add("v", 2).build();
        BatchlogManager.store(Batch.createLocal(TimeUUID.Generator.nextTimeUUID(), FBUtilities.timestampMicros(), Collections.singleton(mutation)));
        assertThat(RollbackEligibility.blockers()).contains("batches of DataStax versions; let them replay, or run nodetool replaybatchlog");

        UUID hostId = UUID.randomUUID();
        HintsService.instance.write(hostId, Hint.create(mutation, System.currentTimeMillis()));
        HintsService.instance.flushAndFsyncBlockingly(Collections.singleton(hostId));
        assertThat(RollbackEligibility.blockers()).hasSize(4).contains("hints of DataStax versions; let them be delivered");
    }

    @Test
    public void testApacheGroupsComplete() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, st int static, s text, n bigint, PRIMARY KEY (pk, ck))");
        for (String column : new String[]{ "s", "n", "st" })
            createIndex("CREATE INDEX wide_" + column + " ON %s(" + column + ") USING 'sai'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        FileUtils.copySSTablesAndIndexes(cfs.getDirectories().getDirectoryForNewSSTables().toJavaIOFile(), "apache-aa/wide");
        cfs.loadNewSSTables();
        assertThat(cfs.getLiveSSTables()).hasSize(2);
        assertThat(RollbackEligibility.blockers()).noneMatch(blocker -> blocker.contains(KEYSPACE + '.' + currentTable()));
    }
}
