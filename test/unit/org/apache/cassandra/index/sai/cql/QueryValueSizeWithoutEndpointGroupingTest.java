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
package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.RANGE_READ_ENDPOINT_GROUPING_ENABLED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Without range read endpoint grouping, an SAI range query whose range is only served by this node isn't serialized
 * and accepts values larger than 65535 bytes, while a top-k query, always sent through messaging, still rejects them.
 * {@link QueryValueSizeTest} tests the default, with endpoint grouping.
 */
public class QueryValueSizeWithoutEndpointGroupingTest extends SAITester
{
    static
    {
        RANGE_READ_ENDPOINT_GROUPING_ENABLED.setBoolean(false);
    }

    private static final int MAX = FBUtilities.MAX_UNSIGNED_SHORT;

    @Test
    public void testRangeReadServedLocally() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, u text, v text, n int, vec vector<float, 2>, PRIMARY KEY (k, c))");
        createIndex("CREATE INDEX ON %s(n) USING 'sai'");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        createIndex("CREATE INDEX ON %s(vec) USING 'sai'");
        String huge = "a".repeat(MAX + 1);
        execute("INSERT INTO %s (k, c, u, v, n, vec) VALUES (0, 0, ?, 'a', 0, [1, 1])", huge);
        execute("INSERT INTO %s (k, c, u, v, n, vec) VALUES (1, 0, 'b', 'b', 0, [2, 2])");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k FROM %s WHERE n = 0 AND u = ? ALLOW FILTERING", huge), row(0));
            assertRows(execute("SELECT k FROM %s WHERE n = 0 AND u > ? ALLOW FILTERING", huge), row(1));
            assertRows(execute("SELECT k FROM %s WHERE token(k) >= token(1) AND n = 0 AND u != ? ALLOW FILTERING", huge), row(1));

            assertRows(execute("SELECT k FROM %s WHERE v = ?", huge));

            assertThatThrownBy(() -> execute("SELECT k FROM %s WHERE v = ? ORDER BY vec ANN OF [1, 1] LIMIT 1", huge))
                    .isInstanceOf(InvalidRequestException.class)
                    .hasMessage(String.format(StorageAttachedIndex.QUERY_VALUE_TOO_LARGE_ERROR_MESSAGE, "Query value", "v", MAX + 1, MAX));
        });
    }
}
