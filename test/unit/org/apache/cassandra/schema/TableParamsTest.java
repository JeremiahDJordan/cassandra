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

package org.apache.cassandra.schema;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.utils.Gen;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializers;
import org.apache.cassandra.utils.CassandraGenerators.TableParamsBuilder;
import org.apache.cassandra.utils.Generators;

import static accord.utils.Property.qt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;


public class TableParamsTest
{
    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void serdeLatest()
    {
        DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(tableParams()).check(params -> {
            AsymmetricMetadataSerializers.testSerde(output, TableParams.serializer, params, NodeVersion.CURRENT_METADATA_VERSION);
        });
    }

    @Test
    public void serdeUnknownMemtableConfiguration() throws IOException
    {
        TableParams params = TableParams.builder().memtable(MemtableParams.getWithFallback("unknown")).build();
        DataOutputBuffer output = new DataOutputBuffer();
        TableParams.serializer.serialize(params, output, NodeVersion.CURRENT_METADATA_VERSION);
        try (DataInputBuffer input = new DataInputBuffer(output.buffer(), false))
        {
            MemtableParams memtable = TableParams.serializer.deserialize(input, NodeVersion.CURRENT_METADATA_VERSION).memtable;
            assertEquals("unknown", memtable.configurationKey());
            assertSame(MemtableParams.DEFAULT.factory(), memtable.factory());
        }
    }

    private static Gen<TableParams> tableParams()
    {
        return Generators.toGen(new TableParamsBuilder()
                                .withKnownMemtables()
                                .withTransactionalMode()
                                .withFastPathStrategy()
                                .build());
    }
}