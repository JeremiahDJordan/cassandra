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
package org.apache.cassandra.config;

import org.junit.Test;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_ANN_MIN_RERANK_FRACTION;
import static org.junit.Assert.*;

public class StorageAttachedIndexOptionsTest
{
    @Test
    public void rerankFractionIsValidatedAndCapturedDuringStartup()
    {
        var options = new StorageAttachedIndexOptions();
        try (var properties = new WithProperties())
        {
            properties.set(SAI_ANN_MIN_RERANK_FRACTION, "0.30");
            options.validate();
            assertEquals(0.30, options.getAnnMinRerankFraction(), 0);
            for (String value : new String[] { "1.5", "-0.1", "NaN", "Infinity", "-Infinity", "garbage", "" })
            {
                properties.set(SAI_ANN_MIN_RERANK_FRACTION, value);
                try
                {
                    options.validate();
                    fail("Accepted " + value);
                }
                catch (ConfigurationException expected)
                {
                    assertTrue(expected.getMessage().contains(SAI_ANN_MIN_RERANK_FRACTION.getKey()));
                }
                // Invalid input cannot poison a class or change the valid snapshot.
                assertEquals(0.30, options.getAnnMinRerankFraction(), 0);
            }
            for (String value : new String[] { "0", "0.3", "1" })
            {
                properties.set(SAI_ANN_MIN_RERANK_FRACTION, value);
                options.validate();
                assertEquals(Double.parseDouble(value), options.getAnnMinRerankFraction(), 0);
            }
            properties.set(SAI_ANN_MIN_RERANK_FRACTION, "0.1");
            assertEquals(1, options.getAnnMinRerankFraction(), 0);
        }
    }
}
