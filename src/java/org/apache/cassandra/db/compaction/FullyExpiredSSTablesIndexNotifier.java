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

package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Tells secondary indexes about the rows in fully expired SSTables that compaction drops without reading them
 * (CASSANDRA-20829).
 * <p>
 * Only indexes whose {@link Index#notifyIndexerAboutRowsInFullyExpiredSSTables()} returns {@code true} are notified,
 * and the SSTables are only read if at least one such index exists. SSTable-attached indexes, whose components are
 * deleted together with the SSTable, opt out.
 * <p>
 * Used both by {@link CompactionTask}, which drops fully expired SSTables as part of a compaction, and by
 * {@link ExpirationTask}, which UCS uses to drop them on their own.
 */
public final class FullyExpiredSSTablesIndexNotifier
{
    private static final Logger logger = LoggerFactory.getLogger(FullyExpiredSSTablesIndexNotifier.class);

    private FullyExpiredSSTablesIndexNotifier()
    {
    }

    /**
     * Calls {@link Index.Indexer#removeRow(Row)} for every row (including static rows) of the given fully expired
     * SSTables within the given token range, for every index of the realm that asks for it.
     * <p>
     * This is best-effort: a failure to read an SSTable or a failure in an indexer is logged, and doesn't prevent the
     * SSTable from being dropped.
     *
     * @param realm the realm the SSTables belong to
     * @param fullyExpiredSSTables the fully expired SSTables that are about to be dropped
     * @param range the token range of the operation, or null for the whole SSTables
     * @param operationId the id of the operation that drops the SSTables, for logging
     */
    public static void notifyIndexes(CompactionRealm realm,
                                     Collection<? extends CompactionSSTable> fullyExpiredSSTables,
                                     @Nullable Range<Token> range,
                                     TimeUUID operationId)
    {
        if (fullyExpiredSSTables.isEmpty())
            return;

        List<Index> indexes = new ArrayList<>();
        try
        {
            // A realm may have no index manager (for example, in tools), and then has no indexes to notify
            SecondaryIndexManager indexManager = realm.getIndexManager();
            if (indexManager == null)
                return;

            for (Index index : indexManager.listIndexes())
            {
                if (index.notifyIndexerAboutRowsInFullyExpiredSSTables())
                    indexes.add(index);
            }
        }
        catch (Throwable t)
        {
            // Best-effort, as below: a realm that can't list its indexes mustn't keep the SSTables from being dropped.
            JVMStabilityInspector.inspectThrowable(t);
            logger.warn("Failed to list the secondary indexes to notify about rows in fully expired SSTables {} during " +
                        "operation {}; the SSTables will still be dropped, but index cleanup for them may be incomplete.",
                        fullyExpiredSSTables, operationId, t);
            return;
        }

        if (indexes.isEmpty())
            return;

        for (CompactionSSTable expiredSSTable : fullyExpiredSSTables)
        {
            if (expiredSSTable instanceof SSTableReader)
            {
                // Notifying indexers is best-effort: the SSTable is fully expired and is going to be obsoleted regardless.
                // A read error (e.g. a corrupt SSTable that is fully expired) or a failure in a custom indexer must not
                // silently leave the SSTable undropped, or every subsequent compaction would hit the same failure,
                // leaving the expired SSTables permanently stuck.
                try (ISSTableScanner scanner = ((SSTableReader) expiredSSTable).getScanner(range))
                {
                    while (scanner.hasNext())
                    {
                        try (UnfilteredRowIterator partition = scanner.next();
                             WriteContext ctx = Keyspace.open(realm.metadata().keyspace).getWriteHandler().createContextForIndexing())
                        {
                            notifyIndexes(indexes, partition, ctx);
                        }
                    }
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    logger.warn("Failed to notify secondary indexes about rows in fully expired SSTable {} during operation {}; " +
                                "the SSTable will still be dropped, but index cleanup for it may be incomplete.",
                                expiredSSTable, operationId, t);
                }
            }
        }
    }

    private static void notifyIndexes(List<Index> indexes, UnfilteredRowIterator partition, WriteContext ctx)
    {
        List<Index.Indexer> indexers = new ArrayList<>();
        for (int i = 0; i < indexes.size(); i++)
        {
            Index.Indexer indexer = indexes.get(i).indexerFor(partition.partitionKey(),
                                                              partition.columns(),
                                                              FBUtilities.nowInSeconds(),
                                                              ctx,
                                                              IndexTransaction.Type.COMPACTION,
                                                              null);

            if (indexer != null)
                indexers.add(indexer);
        }

        if (indexers.isEmpty())
            return;

        for (Index.Indexer indexer : indexers)
            indexer.begin();

        try
        {
            Row staticRow = partition.staticRow();
            if (!staticRow.isEmpty())
            {
                for (Index.Indexer indexer : indexers)
                    indexer.removeRow(staticRow);
            }

            while (partition.hasNext())
            {
                Unfiltered unfiltered = partition.next();
                if (unfiltered instanceof Row)
                {
                    for (Index.Indexer indexer : indexers)
                        indexer.removeRow((Row) unfiltered);
                }
            }
        }
        finally
        {
            for (Index.Indexer indexer : indexers)
                indexer.finish();
        }
    }
}
