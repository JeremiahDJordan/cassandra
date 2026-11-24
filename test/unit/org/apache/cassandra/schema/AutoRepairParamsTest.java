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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.schema;

import java.util.Locale;

import com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.Util.setAutoRepairEnabled;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AutoRepairParamsTest extends CQLTester
{
    private static StorageCompatibilityMode originalMode;

    @BeforeClass
    public static void setupClass() throws Exception
    {
        // auto_repair is rejected with storage_compatibility_mode HCD_1, the test default
        originalMode = DatabaseDescriptor.getStorageCompatibilityMode();
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
        setAutoRepairEnabled(true);
    }

    @AfterClass
    public static void restoreStorageCompatibilityMode()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(originalMode);
    }

    @Test
    public void testCreateTableWithKeysNotInLowerCase()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH auto_repair = {'FULL_ENABLED': 'false', 'Priority': '7'}");

        AutoRepairParams params = autoRepairParams();
        assertFalse(params.repairEnabled(RepairType.FULL));
        assertTrue(params.repairEnabled(RepairType.INCREMENTAL));
        assertEquals(7, params.priority());
        assertEquals(ImmutableMap.of("full_enabled", "false",
                                     "incremental_enabled", "true",
                                     "preview_repaired_enabled", "true",
                                     "priority", "7"),
                     params.options());
    }

    @Test
    public void testAlterTableWithKeysNotInLowerCase()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertTrue(autoRepairParams().repairEnabled(RepairType.INCREMENTAL));

        alterTable("ALTER TABLE %s WITH auto_repair = {'INCREMENTAL_ENABLED': 'false'}");
        assertFalse(autoRepairParams().repairEnabled(RepairType.INCREMENTAL));
        assertEquals(AutoRepairParams.DEFAULT_OPTIONS.keySet(), autoRepairParams().options().keySet());
    }

    @Test
    public void testInvalidValueForKeyNotInLowerCase() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertInvalidMessage("Invalid value maybe for 'full_enabled' repair sub-option - must be a boolean",
                             "ALTER TABLE %s WITH auto_repair = {'Full_Enabled': 'maybe'}");
        assertInvalidMessage("Invalid value high for 'priority' repair sub-option - must be an integer",
                             "ALTER TABLE %s WITH auto_repair = {'PRIORITY': 'high'}");
    }

    @Test
    public void testKeyInSeveralCasesWithoutLowerCase()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        alterTable("ALTER TABLE %s WITH auto_repair = {'Priority': '9', 'PRIORITY': '1'}");
        assertEquals(9, autoRepairParams().priority());
        alterTable("ALTER TABLE %s WITH auto_repair = {'PRIORITY': '1', 'Priority': '8', 'full_enabled': 'true', 'incremental_enabled': 'true'}");
        assertEquals(8, autoRepairParams().priority());

        // the defaults stored with a Turkish default locale
        AutoRepairParams params = AutoRepairParams.fromMap(ImmutableMap.of("PRIORITY", "9",
                                                                           "prıorıty", "0",
                                                                           "full_enabled", "true",
                                                                           "ıncremental_enabled", "false",
                                                                           "prevıew_repaıred_enabled", "true"));
        assertEquals(0, params.priority());
        assertFalse(params.repairEnabled(RepairType.INCREMENTAL));
        assertEquals(AutoRepairParams.DEFAULT_OPTIONS.keySet(), params.options().keySet());
    }

    @Test
    public void testKeyInLowerCaseTakesPrecedence()
    {
        AutoRepairParams params = AutoRepairParams.fromMap(ImmutableMap.of("FULL_ENABLED", "false",
                                                                           "full_enabled", "true",
                                                                           "Priority", "3"));
        assertTrue(params.repairEnabled(RepairType.FULL));
        assertEquals(3, params.priority());
        assertEquals(AutoRepairParams.DEFAULT_OPTIONS.keySet(), params.options().keySet());

        params = AutoRepairParams.fromMap(ImmutableMap.of("full_enabled", "true",
                                                          "FULL_ENABLED", "false"));
        assertTrue(params.repairEnabled(RepairType.FULL));
    }

    @Test
    public void testKeysWithTurkishDefaultLocale()
    {
        Locale defaultLocale = Locale.getDefault();
        Locale.setDefault(new Locale("tr", "TR"));
        try
        {
            AutoRepairParams params = AutoRepairParams.fromMap(ImmutableMap.of("PRIORITY", "3",
                                                                               "priority", "5",
                                                                               "INCREMENTAL_ENABLED", "false"));
            assertEquals(5, params.priority());
            assertFalse(params.repairEnabled(RepairType.INCREMENTAL));
            assertTrue(params.repairEnabled(RepairType.PREVIEW_REPAIRED));
            assertEquals(AutoRepairParams.DEFAULT_OPTIONS.keySet(), params.options().keySet());
        }
        finally
        {
            Locale.setDefault(defaultLocale);
        }
    }

    private AutoRepairParams autoRepairParams()
    {
        return getCurrentColumnFamilyStore().metadata().params.autoRepair;
    }
}
