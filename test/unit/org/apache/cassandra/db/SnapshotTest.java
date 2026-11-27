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

package org.apache.cassandra.db;

import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SnapshotTest extends CQLTester
{
    @Test
    public void testEmptyTOC() throws Throwable
    {
        createTable("create table %s (id int primary key, k int)");
        execute("insert into %s (id, k) values (1,1)");
        getCurrentColumnFamilyStore().forceBlockingFlush(UNIT_TESTS);
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
        {
            File toc = sstable.descriptor.fileFor(Component.TOC);
            Files.write(toc.toPath(), new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
        }
        getCurrentColumnFamilyStore().snapshot("hello");
    }

    @Test
    public void testClearSnapshotOutsideSnapshotsDirectory() throws Throwable
    {
        createTable("create table %s (id int primary key, k int)");
        execute("insert into %s (id, k) values (1,1)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.forceBlockingFlush(UNIT_TESTS);
        cfs.snapshot("hello");
        for (ColumnFamilyStore systemCfs : Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStores())
            systemCfs.forceBlockingFlush(UNIT_TESTS);
        File tableDir = cfs.getDirectories().getCFDirectories().get(0).toAbsolute();

        for (String tag : new String[]{ "..", "../..", "a/../..", "a/..", ".", tableDir.path(), "/a", "foodot/.", "a/../b" })
        {
            assertThatThrownBy(() -> StorageService.instance.clearSnapshot(tag, "no_such_keyspace"))
                .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> StorageService.instance.clearSnapshot(tag, KEYSPACE, SchemaConstants.SYSTEM_KEYSPACE_NAME))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not name a directory inside snapshots");
            assertThatThrownBy(() -> cfs.clearSnapshot(tag)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> Nodes.nodes().clearSnapshot(tag)).isInstanceOf(IllegalArgumentException.class);
        }

        assertTrue(tableDir.exists());
        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertTrue(sstable.descriptor.fileFor(Component.DATA).exists());
        assertTrue(cfs.snapshotExists("hello"));

        StorageService.instance.clearSnapshot("hello", KEYSPACE, SchemaConstants.SYSTEM_KEYSPACE_NAME);
        assertFalse(cfs.snapshotExists("hello"));

        cfs.snapshot("hello");
        StorageService.instance.clearSnapshot("", KEYSPACE);
        assertFalse(cfs.snapshotExists("hello"));
    }
}
