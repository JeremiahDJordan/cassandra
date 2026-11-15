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
package org.apache.cassandra.gms;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_FAILURE_DETECTOR_PROPERTY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Without the {@code cassandra.custom_failure_detector_class} system property, {@link IFailureDetector#instance} is the
 * failure detector configured with {@code failure_detector} in cassandra.yaml (the simulator relies on this).
 */
public class ConfiguredFailureDetectorTest
{
    @BeforeClass
    public static void configure()
    {
        assertFalse(CUSTOM_FAILURE_DETECTOR_PROPERTY.isPresent());
        DatabaseDescriptor.daemonInitialization(() -> {
            Config config = DatabaseDescriptor.loadConfig();
            config.failure_detector = CustomFailureDetectorTest.TestFailureDetector.class.getName();
            return config;
        });
    }

    @Test
    public void testConfiguredFailureDetector()
    {
        assertTrue(IFailureDetector.instance.getClass().getName(),
                   IFailureDetector.instance instanceof CustomFailureDetectorTest.TestFailureDetector);
    }
}
