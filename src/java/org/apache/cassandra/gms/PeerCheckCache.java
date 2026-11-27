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
package org.apache.cassandra.gms;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;

/**
 * Keeps the result of a check of the nodes that the reads of a keyspace can contact, for every node or for those of
 * this node's datacenter, until something the check reads changes: the endpoints of the keyspace's token metadata, the
 * peers known to {@link org.apache.cassandra.nodes.Nodes} and their datacenter, the messaging version of a node, its
 * gossip state and liveness, or the keyspace. Each of those changes calls {@link #peersChanged()} once it's made. A
 * read that finds a result older than the last change checks the nodes again, one read at a time for a keyspace and
 * scope; the reads that come meanwhile wait for that result.
 *
 * @param <T> the result of the check
 */
public final class PeerCheckCache<T>
{
    /** How many times a read checks the nodes, at most, before it returns: again if they changed during the check. */
    private static final int MAX_CHECKS_PER_READ = 2;

    private static final AtomicLong peerChanges = new AtomicLong();

    private static final List<PeerCheckCache<?>> caches = new CopyOnWriteArrayList<>();
    private static final AtomicBoolean listeningToSchema = new AtomicBoolean();

    private final BiFunction<String, String, T> check;
    private final ConcurrentMap<String, Scope<T>> everyNode = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Scope<T>> localDatacenter = new ConcurrentHashMap<>();

    private static final class Scope<T> extends AtomicReference<Result<T>>
    {
    }

    private static final class Result<T>
    {
        final long peerChanges;
        final T value;

        Result(long peerChanges, T value)
        {
            this.peerChanges = peerChanges;
            this.value = value;
        }
    }

    /**
     * @param check checks the nodes of a keyspace, given its name and the datacenter of the nodes, {@code null} for
     * every node
     */
    public PeerCheckCache(BiFunction<String, String, T> check)
    {
        this.check = check;
        caches.add(this);
    }

    /**
     * Makes the reads check the nodes again, after a change that may change the result of a check.
     */
    public static void peersChanged()
    {
        peerChanges.incrementAndGet();
    }

    /**
     * Returns the result of the check of the nodes that a read of the given keyspace at the given consistency level can
     * contact: those of this node's datacenter at LOCAL_ONE, LOCAL_QUORUM and LOCAL_SERIAL, every node otherwise.
     */
    public T get(String keyspace, ConsistencyLevel consistency)
    {
        long changes = peerChanges.get();
        // LOCAL_SERIAL isn't datacenter-local in ConsistencyLevel, but its reads only contact this datacenter
        boolean local = consistency.isDatacenterLocal() || consistency == ConsistencyLevel.LOCAL_SERIAL;
        Scope<T> scope = (local ? localDatacenter : everyNode).get(keyspace);
        if (scope != null)
        {
            Result<T> result = scope.get();
            if (result != null && result.peerChanges == changes)
                return result.value;
        }
        return check(keyspace, local, changes);
    }

    private T check(String keyspace, boolean local, long seenChanges)
    {
        if (!listeningToSchema.get() && listeningToSchema.compareAndSet(false, true))
            Schema.instance.registerListener(new KeyspaceListener());

        Scope<T> scope = (local ? localDatacenter : everyNode).computeIfAbsent(keyspace, k -> new Scope<>());
        synchronized (scope)
        {
            // a result kept with at least the count of changes this read saw comes from a check that read the nodes
            // after every change this read saw, such as the check of a read it waited for
            Result<T> result = scope.get();
            if (result != null && result.peerChanges >= seenChanges)
                return result.value;

            // A result is kept with the count of changes read before its check, so a change during the check makes the
            // next read check again. This read checks again too, a bounded number of times.
            T value = null;
            for (int i = 0; i < MAX_CHECKS_PER_READ; i++)
            {
                long changes = peerChanges.get();
                String datacenter = local ? DatabaseDescriptor.getLocalDataCenter() : null;
                value = check.apply(keyspace, datacenter);
                scope.set(new Result<>(changes, value));
                if (peerChanges.get() == changes)
                    return value;
            }
            return value;
        }
    }

    private static final class KeyspaceListener implements SchemaChangeListener
    {
        @Override
        public void onCreateKeyspace(KeyspaceMetadata keyspace)
        {
            // a check of a dropped keyspace of the same name may have stored a result after the drop
            peersChanged();
        }

        @Override
        public void onAlterKeyspace(KeyspaceMetadata before, KeyspaceMetadata after)
        {
            if (!before.params.replication.equals(after.params.replication))
                peersChanged();
        }

        @Override
        public void onDropKeyspace(KeyspaceMetadata keyspace, boolean dropData)
        {
            for (PeerCheckCache<?> cache : caches)
            {
                cache.everyNode.remove(keyspace.name);
                cache.localDatacenter.remove(keyspace.name);
            }
            // a check of the dropped keyspace that is running may store its result again
            peersChanged();
        }
    }
}
