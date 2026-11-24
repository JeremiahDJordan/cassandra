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

package org.apache.cassandra.io.sstable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.SchemaCompatibility;

/**
 * What of an sstable Apache Cassandra 5.0 can't read: a bti version other than da, a checksum type other than CRC32,
 * a compressor it doesn't have, and SAI components it didn't write.
 */
public final class ApacheReadability
{
    /** The only bti version Apache Cassandra 5.0 reads (cassandra-5.0.9 BtiFormat). */
    private static final String BTI_VERSION = "da";

    private ApacheReadability()
    {
    }

    /**
     * The reasons Apache Cassandra 5.0 can't read the files of an sstable of {@code version} with {@code components},
     * compressed with {@code compression} unless it is null; empty if it can.
     */
    public static List<String> unreadable(org.apache.cassandra.io.sstable.format.Version version,
                                          Collection<Component> components,
                                          @Nullable CompressionParams compression)
    {
        List<String> reasons = new ArrayList<>();
        if (BtiFormat.is(version.format) && !version.version.equals(BTI_VERSION))
            reasons.add("bti version " + version.version + ", while Apache Cassandra 5.0 reads only " + BTI_VERSION);
        if (components.contains(Components.DIGEST_CRC32C))
            reasons.add("checksum type CRC32C");
        else if (components.contains(Components.DIGEST_CRC64NVME))
            reasons.add("checksum type CRC64NVME");
        else if (!components.contains(Components.DIGEST))
            reasons.add("no digest that names its checksum type");
        if (compression != null)
            reasons.addAll(SchemaCompatibility.compressionIncompatibilities(SchemaCompatibility.Target.APACHE_5_0, "compression", compression));
        return reasons;
    }

    /**
     * The SAI components among {@code components} that Apache Cassandra 5.0 didn't write.
     */
    public static List<String> dataStaxSAIComponents(Collection<Component> components)
    {
        List<String> names = new ArrayList<>();
        for (Component component : components)
        {
            if (!Version.isApacheComponentFileName(component.name) && Version.tryParseFileName(component.name).isPresent())
                names.add(component.name);
        }
        return names;
    }

    /**
     * The reasons Apache Cassandra 5.0 can't read the files of {@code sstable}; empty if it can.
     */
    public static List<String> unreadable(SSTableReader sstable)
    {
        return unreadable(sstable.descriptor.version, sstable.components(), sstable.compression ? sstable.getCompressionMetadata().parameters : null);
    }

    /**
     * Whether {@code sstable} has a complete group of components written by Apache Cassandra 5.0 for every SAI index of
     * its table, which an Apache Cassandra 5.0 node needs to take the sstable whole.
     */
    public static boolean hasCompleteAaGroups(SSTableReader sstable)
    {
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(sstable.metadata().id);
        StorageAttachedIndexGroup group = cfs == null ? null : StorageAttachedIndexGroup.getIndexGroup(cfs);
        if (group == null || group.getIndexes().isEmpty())
            return true;

        Set<IndexContext> contexts = group.getIndexes().stream().map(StorageAttachedIndex::getIndexContext).collect(Collectors.toSet());
        IndexDescriptor descriptor = IndexDescriptor.load(sstable, contexts);
        if (!isCompleteAa(descriptor.perSSTableComponents()))
            return false;
        for (IndexContext context : contexts)
        {
            if (!isCompleteAa(descriptor.perIndexComponents(context)))
                return false;
        }
        return true;
    }

    private static boolean isCompleteAa(IndexComponents.ForRead components)
    {
        return components.isComplete() && components.version().equals(Version.APACHE_AA);
    }
}
