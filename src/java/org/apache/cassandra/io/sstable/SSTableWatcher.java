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

package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_SSTABLE_WATCHER;

/**
 * Watcher used when opening sstables to discover extra components, eg. archive component
 */
public interface SSTableWatcher
{
    SSTableWatcher instance = !CUSTOM_SSTABLE_WATCHER.isPresent()
                               ? new SSTableWatcher() {}
                               : FBUtilities.construct(CUSTOM_SSTABLE_WATCHER.getString(), "sstable watcher");

    /** What the file names of SAI components have after the sstable's base name. */
    String SAI_FILE_NAME_PART = "-SAI";

    /**
     * Discover extra components before reading TOC file
     *
     * @param descriptor sstable descriptor for current sstable
     */
    default void discoverComponents(Descriptor descriptor)
    {
    }

    /**
     * Discover extra components before opening sstable
     *
     * @param descriptor sstable descriptor for current sstable
     * @param existing existing sstable components
     * @return all discovered sstable components
     */
    default Set<Component> discoverComponents(Descriptor descriptor, Set<Component> existing)
    {
        return existing;
    }

    /**
     * Called before executing index build on existing sstable
     */
    default void onIndexBuild(SSTableReader sstable, Set<? extends Index> indexes)
    {
    }

    /**
     * Called when an index is dropped on index components affected by that drop.
     * <p>
     * By default, this method simply deletes the components locally, but it can overriden if different/additional
     * behavior is needed.
     *
     * @param metadata table metadata of the table the index was dropped from.
     * @param components index components that are no longer in used due to an index drop. Note that this can
     *                   be either per-index components (for the components of the exact index being dropped),
     *                   or per-sstable components if the index dropped was the only index for the table and the
     *                   per-sstable components are no longer needed. More precisely, if the last index of a table
     *                   is dropped, then this method will usually be called twice per sstable, once for the index
     *                   components, and once for the per-sstable components.
     */
    default void onIndexDropped(TableMetadata metadata, IndexComponents.ForWrite components)
    {
        components.forceDeleteAllComponents();
    }

    /**
     * Lists the files of the SAI components of the provided sstables, as they are in storage, including those their TOC
     * doesn't list (components of dropped indexes, DSE 6.8's, leftovers of an incomplete build). CREATE INDEX on an
     * existing table deletes the files of its index name with {@link #onIndexDropped}, so an implementation must list
     * where index discovery finds files and {@link #onIndexDropped} deletes them. It runs under a JVM-wide lock while a
     * schema change is applied, which index builds and compactions also take to write their completion markers: a
     * remote listing must be bounded in time, and may throw, and neither it nor {@link #onIndexDropped} may wait for
     * index writes in flight; an {@link #onIndexDropped} that fails to delete must throw. The default lists the
     * directory of the sstables.
     *
     * @return the SAI component names of each sstable that has SAI files (e.g. {@code SAI+ec+my_index+TermsData.db})
     * @throws IOException if the files of any of the sstables can't be listed; the index is then not built
     */
    default Map<SSTableReader, Set<String>> listSAIComponentFiles(Collection<SSTableReader> sstables) throws IOException
    {
        Map<File, Map<String, SSTableReader>> byDirectory = new HashMap<>();
        for (SSTableReader sstable : sstables)
            byDirectory.computeIfAbsent(sstable.descriptor.directory, d -> new HashMap<>())
                       .put(sstable.descriptor.filenamePart() + Descriptor.FILENAME_SEPARATOR, sstable);

        Map<SSTableReader, Set<String>> files = new HashMap<>();
        for (Map.Entry<File, Map<String, SSTableReader>> directory : byDirectory.entrySet())
        {
            String[] names = directory.getKey().tryListNames((d, name) -> name.contains(SAI_FILE_NAME_PART));
            if (names == null)
                throw new IOException("Could not list " + directory.getKey());
            for (String file : names)
            {
                int sai = file.indexOf(SAI_FILE_NAME_PART);
                SSTableReader sstable = directory.getValue().get(file.substring(0, sai + 1));
                if (sstable != null)
                    files.computeIfAbsent(sstable, s -> new HashSet<>()).add(file.substring(sai + 1));
            }
        }
        return files;
    }
}
