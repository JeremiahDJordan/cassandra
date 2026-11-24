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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.config.CassandraRelevantProperties.APACHE_UPGRADE_PATH;

/**
 * The upgrade of a cluster from Apache Cassandra 5.0, during which nodes keep the schema in the form Apache Cassandra
 * 5.0 nodes agree with: schema versions and function and aggregate rows over Apache's schema columns, Apache's
 * default compaction for tables created without one, and schema changes Apache accepts.
 */
public final class ApacheUpgradePath
{
    private static final String ACTIVE = "active";

    private ApacheUpgradePath()
    {
    }

    /**
     * Whether the path is active on this node: it runs in storage_compatibility_mode CASSANDRA_5, or the path is set
     * with {@code -Dcassandra.apache_upgrade_path=active}.
     */
    public static boolean isActive()
    {
        return DatabaseDescriptor.getStorageCompatibilityMode() == StorageCompatibilityMode.CASSANDRA_5
               || ACTIVE.equals(APACHE_UPGRADE_PATH.getString());
    }
}
