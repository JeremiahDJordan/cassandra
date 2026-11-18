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
package org.apache.cassandra.index.sai.cql;

import org.junit.BeforeClass;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.utils.memory.NativePool;

import static org.junit.Assert.assertTrue;

/**
 * {@link OlderDeletionKeepsIndexedValuesTest} with the memtables allocating off heap ({@code offheap_objects}, the
 * default of the shipped cassandra.yaml), where the cell the vector index looks up in the memtable comes from native
 * memory. The test configuration uses heap_buffers.
 */
public class OlderDeletionKeepsIndexedValuesOffheapObjectsTest extends OlderDeletionKeepsIndexedValuesTest
{
    // Hides SAITester.setUpClass, so that the allocation type is set before the server and its memtable pool start.
    @BeforeClass
    public static void setUpClass()
    {
        ServerTestUtils.daemonInitialization();
        DatabaseDescriptor.getRawConfig().memtable_allocation_type = Config.MemtableAllocationType.offheap_objects;
        SAITester.setUpClass();
        assertTrue(AbstractAllocatorMemtable.MEMORY_POOL instanceof NativePool);
    }
}
