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

package org.apache.cassandra.config;

import org.junit.Test;

import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.junit.Assert.assertEquals;

/**
 * A client whose configuration sets storage_compatibility_mode CASSANDRA_5, such as CQLSSTableWriter, writes for it.
 */
public class Cassandra5ModeClientTest
{
    @Test
    public void testClientWritesForMode()
    {
        Config config = new Config();
        config.storage_compatibility_mode = StorageCompatibilityMode.CASSANDRA_5;
        DatabaseDescriptor.clientInitialization(true, () -> config);

        assertEquals(StorageCompatibilityMode.CASSANDRA_5, DatabaseDescriptor.getStorageCompatibilityModeForWrites());
        assertEquals(MessagingService.VERSION_DS_21, MessagingService.current_version);
    }
}
