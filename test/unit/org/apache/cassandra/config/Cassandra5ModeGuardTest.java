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

import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * storage_compatibility_mode CASSANDRA_5 from -Dcassandra.test.storage_compatibility_mode, with none in the
 * configuration, also needs the test property that makes the mode available.
 */
public class Cassandra5ModeGuardTest
{
    @Test
    public void testModeFromTestProperty()
    {
        // the mode -Dcassandra.test.storage_compatibility_mode gives a node whose configuration sets none
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.CASSANDRA_5);
        Config config = DatabaseDescriptor.loadConfig();
        config.storage_compatibility_mode = null;
        assertThatThrownBy(() -> DatabaseDescriptor.daemonInitialization(() -> config))
        .hasMessageContaining("storage_compatibility_mode CASSANDRA_5 isn't supported");
    }
}
