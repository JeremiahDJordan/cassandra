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

package org.apache.cassandra.index.sai.disk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import javax.annotation.Nullable;

import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.io.sstable.Descriptor;

/**
 * An owner of the per-index files of an index on an SSTable, while it may write or delete them by name: a writer of the
 * index, for a flush, a compaction or a build. An index created again with the same name uses the same file names
 * without immutable components, so the files that a writer of a dropped index owns may be those of the new index.
 * Guarded by {@link StorageAttachedIndex#SAME_NAME_FILES_LOCK}.
 */
public final class PerIndexFilesOwner implements AutoCloseable
{
    private static final Map<Descriptor, List<PerIndexFilesOwner>> owners = new HashMap<>();

    private final Descriptor descriptor;
    private final String indexName;
    private final BooleanSupplier isIndexDropped;
    private boolean released;

    private PerIndexFilesOwner(Descriptor descriptor, String indexName, BooleanSupplier isIndexDropped)
    {
        this.descriptor = descriptor;
        this.indexName = indexName;
        this.isIndexDropped = isIndexDropped;
    }

    /**
     * Takes the ownership of the per-index files of the provided index on the provided SSTable, unless the index is
     * dropped.
     *
     * @return the owner, to close once nothing writes or deletes the files any more, or null if the index is dropped
     */
    public static @Nullable PerIndexFilesOwner tryOwn(Descriptor descriptor,
                                                      String indexName,
                                                      BooleanSupplier isIndexDropped)
    {
        synchronized (StorageAttachedIndex.SAME_NAME_FILES_LOCK)
        {
            if (isIndexDropped.getAsBoolean())
                return null;
            PerIndexFilesOwner owner = new PerIndexFilesOwner(descriptor, indexName, isIndexDropped);
            owners.computeIfAbsent(descriptor, d -> new ArrayList<>()).add(owner);
            return owner;
        }
    }

    /**
     * @return whether a writer of a dropped index of the provided name owns the files of the provided SSTable
     */
    public static boolean hasOwnerOfDroppedIndex(Descriptor descriptor, String indexName)
    {
        synchronized (StorageAttachedIndex.SAME_NAME_FILES_LOCK)
        {
            for (PerIndexFilesOwner owner : owners.getOrDefault(descriptor, List.of()))
            {
                if (owner.indexName.equals(indexName) && owner.isIndexDropped.getAsBoolean())
                    return true;
            }
            return false;
        }
    }

    @Override
    public void close()
    {
        synchronized (StorageAttachedIndex.SAME_NAME_FILES_LOCK)
        {
            if (released)
                return;
            released = true;
            List<PerIndexFilesOwner> ofSSTable = owners.get(descriptor);
            ofSSTable.remove(this);
            if (ofSSTable.isEmpty())
                owners.remove(descriptor);
            StorageAttachedIndex.SAME_NAME_FILES_LOCK.notifyAll();
        }
    }
}
