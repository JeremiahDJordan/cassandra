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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.db.SystemKeyspace.BATCHES;
import static org.apache.cassandra.db.SystemKeyspace.PAXOS;

/**
 * The data of a node that Apache Cassandra 5.0 can't read, which keeps the node from going back to it: SAI indexes
 * without complete {@link Version#APACHE_AA} groups, and hints, batches and Paxos state of DataStax versions. Commit log
 * segments of DataStax versions don't count, as they are replayed before the node runs in
 * storage_compatibility_mode CASSANDRA_5.
 */
public final class RollbackEligibility
{
    private static final Logger logger = LoggerFactory.getLogger(RollbackEligibility.class);

    private RollbackEligibility()
    {
    }

    /**
     * Warns, on a node in storage_compatibility_mode CASSANDRA_5, of the data that keeps it from going back to Apache
     * Cassandra 5.0.
     */
    public static void warnInCassandra5Mode()
    {
        if (DatabaseDescriptor.getStorageCompatibilityMode() != StorageCompatibilityMode.CASSANDRA_5)
            return;

        List<String> blockers = blockers();
        if (!blockers.isEmpty())
            logger.warn("This node holds data Apache Cassandra 5.0 can't read, so it can't go back to Apache Cassandra 5.0 " +
                        "while the data exists: {}", String.join("; ", blockers));
    }

    @VisibleForTesting
    static List<String> blockers()
    {
        List<String> blockers = new ArrayList<>();

        Set<String> tables = new TreeSet<>();
        for (Keyspace keyspace : Keyspace.all())
        {
            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
            {
                if (!hasCompleteAaGroups(cfs))
                    tables.add(cfs.getKeyspaceName() + '.' + cfs.name);
            }
        }
        if (!tables.isEmpty())
            blockers.add(String.format("SAI indexes without complete aa groups on %s; rewrite them with nodetool upgradesstables -a",
                                       String.join(", ", tables)));

        if (HintsService.hasDataStaxVersionFiles(DatabaseDescriptor.getHintsDirectory()))
            blockers.add("hints of DataStax versions; let them be delivered");
        if (hasDataStaxVersion(BATCHES, "version"))
            blockers.add("batches of DataStax versions; let them replay, or run nodetool replaybatchlog");
        if (hasDataStaxVersion(PAXOS, "most_recent_commit_version") || hasDataStaxVersion(PAXOS, "proposal_version"))
            blockers.add("Paxos state of DataStax versions; it is purged as paxos_state_purging sets (after the table's " +
                         "gc_grace_seconds, at least 3 hours with legacy, or after Paxos repair with repaired); check that it has cleared");

        return blockers;
    }

    private static boolean hasCompleteAaGroups(ColumnFamilyStore cfs)
    {
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        if (group == null || group.getIndexes().isEmpty())
            return true;

        Set<IndexContext> contexts = group.getIndexes().stream().map(StorageAttachedIndex::getIndexContext).collect(Collectors.toSet());
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IndexDescriptor descriptor = IndexDescriptor.load(sstable, contexts);
            if (!isCompleteAa(descriptor.perSSTableComponents()))
                return false;
            for (IndexContext context : contexts)
            {
                if (!isCompleteAa(descriptor.perIndexComponents(context)))
                    return false;
            }
        }
        return true;
    }

    private static boolean isCompleteAa(IndexComponents.ForRead components)
    {
        return components.isComplete() && components.version().equals(Version.APACHE_AA);
    }

    private static boolean hasDataStaxVersion(String table, String column)
    {
        String query = String.format("SELECT %s FROM %s.%s WHERE %s > %d LIMIT 1 ALLOW FILTERING",
                                     column, SchemaConstants.SYSTEM_KEYSPACE_NAME, table, column, MessagingService.VERSION_50);
        return !QueryProcessor.executeInternal(query).isEmpty();
    }
}
