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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.gms.DataStaxFeatureSupport;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FakePeer;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.index.IndexBuildInProgressException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests that {@code ALLOW FILTERING} is required only if needed.
 */
public class AllowFilteringTest extends SAITester
{
    @Test
    public void testAllowFilteringOnFirstClusteringKeyColumn()
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, c1 int, c2 int, c3 int, v1 int, " +
                    "PRIMARY KEY ((k1, k2), c1, c2, c3))");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c1) USING '%s'", StorageAttachedIndex.class.getName()));

        // with only index restrictions
        test("SELECT * FROM %s WHERE c1=0", false);
        test("SELECT * FROM %s WHERE c1>0", false);
        test("SELECT * FROM %s WHERE c1>0 AND c1<1", false);

        // with additional simple filtering restrictions
        test("SELECT * FROM %s WHERE c1=0 AND k1=0", true);
        test("SELECT * FROM %s WHERE c1=0 AND k2=0", true);
        test("SELECT * FROM %s WHERE c1=0 AND c2=0", true);
        test("SELECT * FROM %s WHERE c1=0 AND c3=0", true);
        test("SELECT * FROM %s WHERE c1=0 AND v1=0", true);

        // with token restrictions
        test("SELECT * FROM %s WHERE c1=0 AND token(k1, k2) = token(0, 0)", false);
        test("SELECT * FROM %s WHERE c1=0 AND token(k1, k2) > token(0, 0)", false);
        test("SELECT * FROM %s WHERE c1=0 AND token(k1, k2) > token(0, 0) AND token(k1, k2) <= token(1, 1)", false);

        // with restriction on partition key
        test("SELECT * FROM %s WHERE c1=0 AND k1=0 AND k2=0", false);
        test("SELECT * FROM %s WHERE c1=0 AND k1=0 AND k2=0 AND v1=0", true);
        test("SELECT * FROM %s WHERE c1=0 AND k1=0 AND k2=0 AND c3=0", true);

        // with restriction on partition key and clustering key prefix
        test("SELECT * FROM %s WHERE c1=0 AND k1=0 AND k2=0 AND c2=0", false);
        test("SELECT * FROM %s WHERE c1=0 AND k1=0 AND k2=0 AND c2=0 AND v1=0", true);
        test("SELECT * FROM %s WHERE c1=0 AND k1=0 AND k2=0 AND c2=0 AND c3>0", false);
        test("SELECT * FROM %s WHERE c1=0 AND k1=0 AND k2=0 AND c2=0 AND c3>0 AND v1=0", true);

        // with restriction on partition key and full clustering key
        test("SELECT * FROM %s WHERE c1=0 AND k1=0 AND k2=0 AND c2=0 AND c3=0", false);
        test("SELECT * FROM %s WHERE c1=0 AND k1=0 AND k2=0 AND c2=0 AND c3=0 AND v1=0", true);

        // with restriction on partition key and full clustering key, multicolumn format
        test("SELECT * FROM %s WHERE k1=0 AND k2=0 AND (c1, c2, c3) = (0, 0, 0)", false);
        test("SELECT * FROM %s WHERE k1=0 AND k2=0 AND (c1, c2, c3) = (0, 0, 0) AND v1=0", true);
    }

    @Test
    public void testAllowFilteringOnNotFirstClusteringKeyColumn()
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, c1 int, c2 int, c3 int, c4 int, v1 int, " +
                    "PRIMARY KEY ((k1, k2), c1, c2, c3, c4))");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c3) USING '%s'", StorageAttachedIndex.class.getName()));

        // with only index restrictions
        test("SELECT * FROM %s WHERE c3=0", false);
        test("SELECT * FROM %s WHERE c3>0", false);
        test("SELECT * FROM %s WHERE c3>0 AND c3<1", false);
        test("SELECT * FROM %s WHERE c3!=0", false);

        // with additional simple filtering restrictions
        test("SELECT * FROM %s WHERE c3=0 AND k1=0", true);
        test("SELECT * FROM %s WHERE c3=0 AND k2=0", true);
        test("SELECT * FROM %s WHERE c3=0 AND c1=0", true);
        test("SELECT * FROM %s WHERE c3=0 AND c2=0", true);
        test("SELECT * FROM %s WHERE c3=0 AND c4=0", true);
        test("SELECT * FROM %s WHERE c3=0 AND v1=0", true);

        // with token restrictions
        test("SELECT * FROM %s WHERE c3=0 AND token(k1, k2) = token(0, 0)", false);
        test("SELECT * FROM %s WHERE c3=0 AND token(k1, k2) > token(0, 0)", false);
        test("SELECT * FROM %s WHERE c3=0 AND token(k1, k2) > token(0, 0) AND token(k1, k2) <= token(1, 1)", false);

        // with restriction on partition key
        test("SELECT * FROM %s WHERE c3=0 AND k1=0 AND k2=0", false);
        test("SELECT * FROM %s WHERE c3=0 AND k1=0 AND k2=0 AND v1=0", true);
        test("SELECT * FROM %s WHERE c3=0 AND k1=0 AND k2=0 AND c2=0", true);
        test("SELECT * FROM %s WHERE c3=0 AND k1=0 AND k2=0 AND c4=0", true);

        // with restriction on partition key and clustering key prefix
        test("SELECT * FROM %s WHERE c3=0 AND k1=0 AND k2=0 AND c1=0", true);
        test("SELECT * FROM %s WHERE c3=0 AND k1=0 AND k2=0 AND c1=0 AND v1=0", true);

        // with restriction on partition key and full clustering key
        test("SELECT * FROM %s WHERE c3=0 AND k1=0 AND k2=0 AND c1=0 AND c2=0 AND c4=0", false);
        test("SELECT * FROM %s WHERE c3=0 AND k1=0 AND k2=0 AND c1=0 AND c2=0 AND c4=0 AND v1=0", true);

        // with restriction on partition key and full clustering key, multicolumn format
        test("SELECT * FROM %s WHERE k1=0 AND k2=0 AND (c1, c2, c3, c4) = (0, 0, 0, 0)", false);
        test("SELECT * FROM %s WHERE k1=0 AND k2=0 AND (c1, c2, c3, c4) = (0, 0, 0, 0) AND v1=0", true);
    }

    @Test
    public void testAllowFilteringOnMultipleClusteringKeyColumns()
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, c1 int, c2 int, c3 int, c4 int, v1 int, " +
                    "PRIMARY KEY ((k1, k2), c1, c2, c3, c4))");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c2) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c4) USING '%s'", StorageAttachedIndex.class.getName()));

        // with only index restrictions
        test("SELECT * FROM %s WHERE c2=0 AND c4=0", false);
        test("SELECT * FROM %s WHERE c2=0 AND c4>0", false);
        test("SELECT * FROM %s WHERE c2=0 AND c4>0 AND c4<1", false);
        test("SELECT * FROM %s WHERE c2>0 AND c4=0", false);
        test("SELECT * FROM %s WHERE c2>0 AND c2<1 AND c4=0", false);
        test("SELECT * FROM %s WHERE c2>0 AND c4>0", false);
        test("SELECT * FROM %s WHERE c2>0 AND c2<1 AND c4>0 AND c4<1", false);
        test("SELECT * FROM %s WHERE c2!=0 AND c4!=1", false);

        // with additional simple filtering restrictions
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND k1=0", true);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND k2=0", true);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND c1=0", true);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND c3=0", true);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0", true);

        // with token restrictions
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND token(k1, k2) = token(0, 0)", false);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND token(k1, k2) > token(0, 0)", false);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND token(k1, k2) > token(0, 0) AND token(k1, k2) <= token(1, 1)", false);

        // with restriction on partition key
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND k1=0 AND k2=0", false);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND k1=0 AND k2=0 AND c3=0", true);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND k1=0 AND k2=0 AND v1=0", true);

        // with restriction on partition key and clustering key prefix
        test("SELECT * FROM %s WHERE k1=2 AND k2=3 AND c1=4 AND c2=0 AND c4=1", true);
        test("SELECT * FROM %s WHERE k1=0 AND k2=0 AND c1=0 AND c2=0 AND c4=0 AND v1=0", true);

        // with restriction on partition key and full clustering key
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND k1=0 AND k2=0 AND c1=0 AND c3=0", false);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND k1=0 AND k2=0 AND c1=0 AND c3=0 AND v1=0", true);

        // with restriction on partition key and full clustering key, multicolumn format
        test("SELECT * FROM %s WHERE k1=0 AND k2=0 AND (c1, c2, c3, c4) = (0, 0, 0, 0)", false);
        test("SELECT * FROM %s WHERE k1=0 AND k2=0 AND (c1, c2, c3, c4) = (0, 0, 0, 0) AND v1=0", true);
    }

    @Test
    public void testAllowFilteringOnSingleRegularColumn()
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, c1 int, c2 int, v1 int, v2 int, PRIMARY KEY ((k1, k2), c1, c2))");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v1) USING '%s'", StorageAttachedIndex.class.getName()));

        // with only index restrictions
        test("SELECT * FROM %s WHERE v1=0", false);
        test("SELECT * FROM %s WHERE v1>0", false);
        test("SELECT * FROM %s WHERE v1>0 AND v1<1", false);
        test("SELECT * FROM %s WHERE v1!=0", false);

        // with additional simple filtering restrictions
        test("SELECT * FROM %s WHERE v1=0 AND k1=0", true);
        test("SELECT * FROM %s WHERE v1=0 AND k2=0", true);
        test("SELECT * FROM %s WHERE v1=0 AND c1=0", true);
        test("SELECT * FROM %s WHERE v1=0 AND c2=0", true);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0", true);

        // with token restrictions
        test("SELECT * FROM %s WHERE v1=0 AND token(k1, k2) = token(0, 0)", false);
        test("SELECT * FROM %s WHERE v1=0 AND token(k1, k2) > token(0, 0)", false);
        test("SELECT * FROM %s WHERE v1=0 AND token(k1, k2) > token(0, 0) AND token(k1, k2) <= token(1, 1)", false);

        // with restriction on partition key
        test("SELECT * FROM %s WHERE v1=0 AND k1=0 AND k2=0", false);
        test("SELECT * FROM %s WHERE v1=0 AND k1=0 AND k2=0 AND c2=0", true);
        test("SELECT * FROM %s WHERE v1=0 AND k1=0 AND k2=0 AND v2=0", true);

        // with restriction on partition key and clustering key prefix
        test("SELECT * FROM %s WHERE v1=0 AND k1=0 AND k2=0 AND c1=0", false);
        test("SELECT * FROM %s WHERE v1=0 AND k1=0 AND k2=0 AND c1=0 AND v2=0", true);

        // with restriction on partition key and full clustering key
        test("SELECT * FROM %s WHERE v1=0 AND k1=0 AND k2=0 AND c1=0 AND c2=0", false);
        test("SELECT * FROM %s WHERE v1=0 AND k1=0 AND k2=0 AND c1=0 AND c2=0 AND v2=0", true);

        // with restriction on partition key and full clustering key, multicolumn format
        test("SELECT * FROM %s WHERE v1=0 AND k1=0 AND k2=0 AND (c1, c2) = (0, 0)", false);
        test("SELECT * FROM %s WHERE v1=0 AND k1=0 AND k2=0 AND (c1, c2) = (0, 0) AND v2=0", true);
    }

    @Test
    public void testAllowFilteringOnMultipleRegularColumns()
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, c1 int, c2 int, v1 int, v2 int, v3 int, " +
                    "PRIMARY KEY ((k1, k2), c1, c2))");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v1) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v2) USING '%s'", StorageAttachedIndex.class.getName()));

        // with only index restrictions
        test("SELECT * FROM %s WHERE v1=0 AND v2=0", false);
        test("SELECT * FROM %s WHERE v1>0 AND v2=0", false);
        test("SELECT * FROM %s WHERE v1>0 AND v1<1 AND v2=0", false);
        test("SELECT * FROM %s WHERE v1=0 AND v2>0", false);
        test("SELECT * FROM %s WHERE v1=0 AND v2>0 AND v2<1", false);
        test("SELECT * FROM %s WHERE v1>0 AND v1<1 AND v2>0 AND v2<1", false);
        test("SELECT * FROM %s WHERE v1!=0 AND v2!=0", false);

        // with additional simple filtering restrictions
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0", true);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k2=0", true);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND c1=0", true);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND c2=0", true);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND v3=0", true);

        // with token restrictions
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND token(k1, k2) = token(0, 0)", false);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND token(k1, k2) > token(0, 0)", false);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND token(k1, k2) > token(0, 0) AND token(k1, k2) <= token(1, 1)", false);

        // with restriction on partition key
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0 AND k2=0", false);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0 AND k2=0 AND c2=0", true);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0 AND k2=0 AND v3=0", true);

        // with restriction on partition key and clustering key prefix
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0 AND k2=0 AND c1=0", false);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0 AND k2=0 AND c1=0 AND v3=0", true);

        // with restriction on partition key and full clustering key
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0 AND k2=0 AND c1=0 AND c2=0", false);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0 AND k2=0 AND c1=0 AND c2=0 AND v3=0", true);

        // with restriction on partition key and full clustering key, multicolumn format
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0 AND k2=0 AND (c1, c2) = (0, 0)", false);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0 AND k2=0 AND (c1, c2) = (0, 0) AND v3=0", true);
    }

    @Test
    public void testAllowFilteringOnClusteringAndRegularColumns()
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, c1 int, c2 int, c3 int, c4 int, v1 int, v2 int, v3 int, " +
                    "PRIMARY KEY ((k1, k2), c1, c2, c3, c4))");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c2) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c4) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v1) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v2) USING '%s'", StorageAttachedIndex.class.getName()));

        // with only index restrictions
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0", false);
        test("SELECT * FROM %s WHERE c2>0 AND c4>0 AND v1>0 AND v2>0", false);
        test("SELECT * FROM %s WHERE c2>0 AND c2<1 AND c4>0 AND c4<1 AND v1>0 AND v1<0 AND v2>0 AND v2<1", false);
        test("SELECT * FROM %s WHERE c2!=0 AND c4!=1 AND v1!=0 AND v2!=0", false);

        // with additional simple filtering restrictions
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND k1=0", true);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND k2=0", true);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND c3=0", true);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND v3=0", true);
        test("SELECT * FROM %s WHERE c2!=0 AND c4!=0 AND v1!=0 AND v2!=0 AND v3=0", true);

        // with token restrictions
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND token(k1, k2) = token(0, 0)", false);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND token(k1, k2) > token(0, 0)", false);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND token(k1, k2) > token(0, 0) AND token(k1, k2) <= token(1, 1)", false);

        // with restriction on partition key
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND k1=0 AND k2=0", false);

        // with restriction on partition key and clustering key prefix
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND k1=0 AND k2=0 AND c1=0", true);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND k1=0 AND k2=0 AND c1=0 AND v3=0", true);

        // with restriction on partition key and full clustering key
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND k1=0 AND k2=0 AND c1=0 AND c3=0", false);
        test("SELECT * FROM %s WHERE c2=0 AND c4=0 AND v1=0 AND v2=0 AND k1=0 AND k2=0 AND c1=0 AND c3=0 AND v3=0", true);

        // with restriction on partition key and full clustering key, multicolumn format
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0 AND k2=0 AND (c1, c2, c3, c4) = (0, 0, 0, 0)", false);
        test("SELECT * FROM %s WHERE v1=0 AND v2=0 AND k1=0 AND k2=0 AND (c1, c2, c3, c4) = (0, 0, 0, 0) AND v3=0", true);
    }

    @Test
    public void testAllowFilteringOnCollectionColumn()
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, c1 int, c2 int, l list<int>, s set<int>, m_k map<int,int>,"
                    + " m_v map<int,int>, m_en map<int, int>, not_indexed list<int>, PRIMARY KEY ((k1, k2), c1, c2))");
        createIndex("CREATE CUSTOM INDEX ON %s(l) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(keys(m_k)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(values(m_v)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(m_en)) USING 'StorageAttachedIndex'");

        // single contains
        test("SELECT * FROM %s WHERE l contains 1", false);
        test("SELECT * FROM %s WHERE s contains 1", false);
        test("SELECT * FROM %s WHERE m_k contains key 1", false);
        test("SELECT * FROM %s WHERE m_v contains 1", false);
        test("SELECT * FROM %s WHERE m_en[1] = 1", false);

        // multiple contains on different indexed columns
        test("SELECT * FROM %s WHERE l contains 1 and s contains 2", false);
        test("SELECT * FROM %s WHERE l contains 1 and m_k contains key 2", false);
        test("SELECT * FROM %s WHERE l contains 1 and m_v contains 2", false);
        test("SELECT * FROM %s WHERE l contains 1 and m_en[2] = 2", false);
        test("SELECT * FROM %s WHERE s contains 1 and s contains 2", false);
        test("SELECT * FROM %s WHERE s contains 1 and m_k contains key 2", false);
        test("SELECT * FROM %s WHERE s contains 1 and m_v contains 2", false);
        test("SELECT * FROM %s WHERE s contains 1 and m_en[2] = 2", false);

        // multiple contains on the same column
        test("SELECT * FROM %s WHERE l contains 1 and l contains 2", false);
        test("SELECT * FROM %s WHERE s contains 1 and s contains 2", false);
        test("SELECT * FROM %s WHERE m_k contains key 1 and m_k contains key 2", false);
        test("SELECT * FROM %s WHERE m_v contains 1 and m_v contains 2", false);
        test("SELECT * FROM %s WHERE m_en[1] = 1 and m_en[2] = 2", false);

        // multiple contains on different columns with not indexed column
        test("SELECT * FROM %s WHERE l contains 1 and not_indexed contains 2", true);
        test("SELECT * FROM %s WHERE s contains 1 and not_indexed contains 2", true);
        test("SELECT * FROM %s WHERE m_k contains key 1 and not_indexed contains 2", true);
        test("SELECT * FROM %s WHERE m_v contains 1 and not_indexed contains 2", true);
        test("SELECT * FROM %s WHERE m_en[1] = 1 and not_indexed contains 2", true);
    }

    private void test(String query, boolean requiresAllowFiltering)
    {
        if (requiresAllowFiltering)
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, query);
        else
            assertNotNull(execute(query));

        assertNotNull(execute(query + " ALLOW FILTERING"));
    }

    @Test
    public void testUnsupportedIndexRestrictions()
    {
        createTable("CREATE TABLE %s (a text, b text, c text, d text, PRIMARY KEY (a, b))");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(b) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(d) USING '%s'", StorageAttachedIndex.class.getName()));

        execute("INSERT INTO %s (a, b, c, d) VALUES ('Test1', 'Test1', 'Test1', 'Test1')");
        execute("INSERT INTO %s (a, b, c, d) VALUES ('Test2', 'Test2', 'Test2', 'Test2')");
        execute("INSERT INTO %s (a, b, c, d) VALUES ('Test3', 'Test3', 'Test3', 'Test3')");

        // Single restriction
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, 'b'), "SELECT * FROM %s WHERE b > 'Test'");
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, 'c'), "SELECT * FROM %s WHERE c > 'Test'");
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, 'd'), "SELECT * FROM %s WHERE d > 'Test'");

        // Supported and unsupported restriction
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, 'b'), "SELECT * FROM %s WHERE b > 'Test' AND c = 'Test1'");
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, 'c'), "SELECT * FROM %s WHERE c > 'Test' AND d = 'Test1'");
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, 'd'), "SELECT * FROM %s WHERE d > 'Test' AND b = 'Test1'");

        // Two unsupported restrictions
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, 'b'), "SELECT * FROM %s WHERE b > 'Test' AND b < 'Test3'");
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_MULTI, "[b, c]"), "SELECT * FROM %s WHERE c > 'Test' AND b < 'Test3'");
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_MULTI, "[b, d]"), "SELECT * FROM %s WHERE d > 'Test' AND b < 'Test3'");

        // The same queries with ALLOW FILTERING should work

        // Single restriction
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE b > 'Test' ALLOW FILTERING"), row("Test1", "Test1", "Test1", "Test1"),
                                                                                                    row("Test2", "Test2", "Test2", "Test2"),
                                                                                                    row("Test3", "Test3", "Test3", "Test3"));
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE c > 'Test' ALLOW FILTERING"), row("Test1", "Test1", "Test1", "Test1"),
                                                                                                    row("Test2", "Test2", "Test2", "Test2"),
                                                                                                    row("Test3", "Test3", "Test3", "Test3"));
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE d > 'Test' ALLOW FILTERING"), row("Test1", "Test1", "Test1", "Test1"),
                                                                                                    row("Test2", "Test2", "Test2", "Test2"),
                                                                                                    row("Test3", "Test3", "Test3", "Test3"));

        // Supported and unsupported restriction
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE b > 'Test' AND c = 'Test1' ALLOW FILTERING"), row("Test1", "Test1", "Test1", "Test1"));
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE c > 'Test' AND d = 'Test1' ALLOW FILTERING"), row("Test1", "Test1", "Test1", "Test1"));
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE d > 'Test' AND b = 'Test1' ALLOW FILTERING"), row("Test1", "Test1", "Test1", "Test1"));

        // Two unsupported restrictions
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE b > 'Test' AND b < 'Test3' ALLOW FILTERING"), row("Test1", "Test1", "Test1", "Test1"),
                                                                                                                    row("Test2", "Test2", "Test2", "Test2"));
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE c > 'Test' AND b < 'Test3' ALLOW FILTERING"), row("Test1", "Test1", "Test1", "Test1"),
                                                                                                                    row("Test2", "Test2", "Test2", "Test2"));
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE d > 'Test' AND b < 'Test3' ALLOW FILTERING"), row("Test1", "Test1", "Test1", "Test1"),
                                                                                                                    row("Test2", "Test2", "Test2", "Test2"));
    }

    @Test
    public void testIndexedColumnSupportsLikeRestriction() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b text, c text, d text, PRIMARY KEY (a, b))");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(b) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(d) USING '%s'", StorageAttachedIndex.class.getName()));

        execute("INSERT INTO %s (a, b, c, d) VALUES ('1', 'Test', 'Test', 'Test')");
        execute("INSERT INTO %s (a, b, c, d) VALUES ('2', 'Test2', 'MyTest', 'Testing')");
        execute("INSERT INTO %s (a, b, c, d) VALUES ('3', 'Other', 'Teest', 'Other')");

        // All LIKE variants are supported by the index and don't require ALLOW FILTERING (the non-prefix
        // variants are served by automaton intersection, see AutomatonQueryTest)
        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT a FROM %s WHERE b LIKE 'Test'"), row("1"));
            assertRowsIgnoringOrder(execute("SELECT a FROM %s WHERE c LIKE 'Test'"), row("1"));
            assertRowsIgnoringOrder(execute("SELECT a FROM %s WHERE d LIKE 'Test'"), row("1"));
            assertRowsIgnoringOrder(execute("SELECT a FROM %s WHERE c LIKE '%%Test'"), row("1"), row("2"));
            assertRowsIgnoringOrder(execute("SELECT a FROM %s WHERE c LIKE '%%Test%%'"), row("1"), row("2"));
            assertRowsIgnoringOrder(execute("SELECT a FROM %s WHERE c LIKE 'Te%%st'"), row("1"), row("3"));
            assertRowsIgnoringOrder(execute("SELECT a FROM %s WHERE b LIKE 'Test%%'"), row("1"), row("2"));
            assertRowsIgnoringOrder(execute("SELECT a FROM %s WHERE c LIKE 'Test%%'"), row("1"));
            assertRowsIgnoringOrder(execute("SELECT a FROM %s WHERE d LIKE 'Test%%'"), row("1"), row("2"));
        });
    }

    @Test
    public void testIndexedColumnDoesNotSupportAnalyzerRestriction()
    {
        createTable("CREATE TABLE %s (a text, b text, c text, d text, PRIMARY KEY (a, b))");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(b) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(d) USING '%s'", StorageAttachedIndex.class.getName()));

        // Analyzer restriction
        assertInvalidMessage(": restriction is only supported on properly indexed columns. a : 'Test' is not valid.", "SELECT * FROM %s WHERE a : 'Test'");
        assertInvalidMessage(String.format(SingleColumnRestriction.AnalyzerMatchesRestriction.CANNOT_BE_RESTRICTED_BY_CLUSTERING_ERROR, 'b'), "SELECT * FROM %s WHERE b : 'Test'");
        assertInvalidMessage(String.format(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_ANALYZER_MATCHES_MESSAGE, 'c'), "SELECT * FROM %s WHERE c : 'Test'");
        assertInvalidMessage(String.format(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_ANALYZER_MATCHES_MESSAGE, 'd'), "SELECT * FROM %s WHERE d : 'Test'");
    }

    @Test
    public void testQueryRequiresFilteringButHasANNRestriction()
    {
        createTable("CREATE TABLE %s (pk text, i int, j int, k int, vec vector<float, 3>, PRIMARY KEY((pk, i), j))");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex'");

        // Should not fail because allow filtering is set but not required
        assertRows(execute("SELECT * FROM %s ORDER BY vec ANN OF [1,1,1] LIMIT 10 ALLOW FILTERING;"));

        // Do not recommend ALLOW FILTERING for non-primary key, non clustering column restrictions
        assertInvalidMessage(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_ALL_RESTRICTED_NON_PARTITION_KEY_COLUMNS_INDEXED_MESSAGE,
                             "SELECT * FROM %s WHERE k > 0 ORDER BY vec ANN OF [2.5, 3.5, 4.5] LIMIT 10;");

        // Do not let ALLOW FILTERING to lead to query execution for non-primary key, non clustering column restrictions
        assertInvalidMessage(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_ALL_RESTRICTED_NON_PARTITION_KEY_COLUMNS_INDEXED_MESSAGE,
                             "SELECT * FROM %s WHERE k > 0 ORDER BY vec ANN OF [2.5, 3.5, 4.5] LIMIT 10 ALLOW FILTERING;");

        // Do not recommend ALLOW FILTERING for clustering column restrictions
        assertInvalidMessage(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_ALL_RESTRICTED_NON_PARTITION_KEY_COLUMNS_INDEXED_MESSAGE,
                             "SELECT * FROM %s WHERE j > 0 ORDER BY vec ANN OF [2.5, 3.5, 4.5] LIMIT 10;");

        // Do not let ALLOW FILTERING lead to query execution for clustering column restrictions
        assertInvalidMessage(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_ALL_RESTRICTED_NON_PARTITION_KEY_COLUMNS_INDEXED_MESSAGE,
                             "SELECT * FROM %s WHERE j > 0 ORDER BY vec ANN OF [2.5, 3.5, 4.5] LIMIT 10 ALLOW FILTERING;");

        // Do not recommend ALLOW FILTERING for partial partition key restrictions
        assertInvalidMessage(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_ALL_RESTRICTED_NON_PARTITION_KEY_COLUMNS_INDEXED_MESSAGE,
                             "SELECT * FROM %s WHERE pk > 'A' AND pk < 'C' ORDER BY vec ANN OF [2.5, 3.5, 4.5] LIMIT 10;");

        // Do not let ALLOW FILTERING lead to query execution for partial partition key restrictions
        assertInvalidMessage(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_ALL_RESTRICTED_NON_PARTITION_KEY_COLUMNS_INDEXED_MESSAGE,
                             "SELECT * FROM %s WHERE pk > 'A' AND pk < 'C' ORDER BY vec ANN OF [2.5, 3.5, 4.5] LIMIT 10 ALLOW FILTERING;");

        // Do not recommend ALLOW FILTERING for complete partition key restrictions
        assertInvalidMessage(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_ALL_RESTRICTED_NON_PARTITION_KEY_COLUMNS_INDEXED_MESSAGE,
                             "SELECT * FROM %s WHERE pk > 'A' AND pk < 'C' AND i > 0 ORDER BY vec ANN OF [2.5, 3.5, 4.5] LIMIT 10;");

        // Do not let ALLOW FILTERING lead to query execution for complete partition key restrictions
        assertInvalidMessage(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_ALL_RESTRICTED_NON_PARTITION_KEY_COLUMNS_INDEXED_MESSAGE,
                             "SELECT * FROM %s WHERE pk > 'A' AND pk < 'C' AND i > 0 ORDER BY vec ANN OF [2.5, 3.5, 4.5] LIMIT 10 ALLOW FILTERING;");
    }

    @Test
    public void testMapRangeQueries()
    {
        createTable("CREATE TABLE %s (partition int primary key, item_cost map<text, int>)");
        createIndex("CREATE CUSTOM INDEX ON %s(keys(item_cost)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(values(item_cost)) USING 'StorageAttachedIndex'");

        // Insert data for later
        execute("INSERT INTO %s (partition, item_cost) VALUES (0, {'apple': 2, 'orange': 1})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (1, {'apple': 1, 'orange': 3})");
        flush();
        execute("INSERT INTO %s (partition, item_cost) VALUES (2, {'apple': 4, 'orange': 2})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (3, {'apple': 3, 'orange': 1})");

        // Gen an ALLOW FILTERING recommendation.
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, "item_cost"),
                             "SELECT partition FROM %s WHERE item_cost['apple'] < 6");

        // Show that filtering works correctly
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] > 1 ALLOW FILTERING"),
                   row(0), row(2), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] >= 1 ALLOW FILTERING"),
                   row(1), row(0), row(2), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] < 3 ALLOW FILTERING"),
                   row(1), row(0));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] <= 3 ALLOW FILTERING"),
                   row(1), row(0), row(3));

        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] < 3 AND item_cost['apple'] > 1 ALLOW FILTERING"), row(0));


        createIndex("CREATE CUSTOM INDEX ON %s(entries(item_cost)) USING 'StorageAttachedIndex'");

        // Show that we're now able to execute the query.
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] < 3 AND item_cost['apple'] > 1"), row(0));
    }

    private final Injections.Barrier blockIndexBuild = Injections.newBarrier("block_index_build", 2, false)
                                                                 .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndex.class)
                                                                 .onMethod("startInitialBuild"))
                                                                 .build();

    @Test
    public void testAllowFilteringDuringIndexBuild() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        Injections.inject(blockIndexBuild);
        String idx = createIndexAsync(String.format("CREATE CUSTOM INDEX ON %%s(v) USING '%s'", StorageAttachedIndex.class.getName()));

        assertThatThrownBy(() -> executeInternal("SELECT * FROM %s WHERE v=0"))
                .hasMessage("The secondary index '" + idx + "' is not yet available as it is building")
                .isInstanceOf(IndexBuildInProgressException.class);

        assertThatThrownBy(() -> executeInternal("SELECT * FROM %s WHERE v=0 ALLOW FILTERING"))
                .hasMessage("The secondary index '" + idx + "' is not yet available as it is building")
                .isInstanceOf(IndexBuildInProgressException.class);

        blockIndexBuild.countDown();
        blockIndexBuild.disable();
        waitForIndexQueryable(idx);
        execute("SELECT * FROM %s WHERE v=0");
        execute("SELECT * FROM %s WHERE v=0 ALLOW FILTERING");
    }

    // Adopted from Apache (CASSANDRA-20025): post-filtering with IN on a column that isn't indexed

    @Test
    public void testAllowFilteringTextWithINClause()
    {
        createTable("CREATE TABLE %s (k1 TEXT, k2 TEXT, k3 TEXT, PRIMARY KEY(k1))");
        createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k1,k2,k3) VALUES ('s1','s11','s111')");
        execute("INSERT INTO %s (k1,k2,k3) VALUES ('s2','s11','s11')");
        execute("INSERT INTO %s (k1,k2,k3) VALUES ('s3','s22','s111')");
        execute("INSERT INTO %s (k1,k2,k3) VALUES ('s4','s22','s111')");
        execute("INSERT INTO %s (k1,k2,k3) VALUES ('s5','s31','s111')");

        assertRowCount(execute("SELECT * FROM %s WHERE k2='s11' AND k3 IN ('s11','s111') ALLOW FILTERING"), 2);
        assertRowCount(execute("SELECT * FROM %s WHERE k2='s22' AND k3 IN ('s111','s111') ALLOW FILTERING"), 2);
        assertRowCount(execute("SELECT * FROM %s WHERE k2='s22' AND k3 IN ('s','s1') ALLOW FILTERING"), 0);
        // To test if an IN clause without an AND condition does not create a query plan and works as expected.
        assertRowCount(execute("SELECT * FROM %s WHERE k2 IN ('s11','s22') ALLOW FILTERING"), 4);
    }

    @Test
    public void testAllowFilteringIntWithINClause()
    {
        createTable("CREATE TABLE %s (k1 text, k2 int, k3 int, PRIMARY KEY(k1))");
        createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k1,k2,k3) VALUES ('s1',11,1)");
        execute("INSERT INTO %s (k1,k2,k3) VALUES ('s2',11,11)");
        execute("INSERT INTO %s (k1,k2,k3) VALUES ('s3',11,111)");
        execute("INSERT INTO %s (k1,k2,k3) VALUES ('s4',22,1)");
        execute("INSERT INTO %s (k1,k2,k3) VALUES ('s5',22,11)");
        execute("INSERT INTO %s (k1,k2,k3) VALUES ('s6',22,111)");

        assertRowCount(execute("SELECT * FROM %s WHERE k2=11 AND k3 IN (1,11,111) ALLOW FILTERING"), 3);
        assertRowCount(execute("SELECT * FROM %s WHERE k2=22 AND k3 IN (1,11,111) ALLOW FILTERING"), 3);
        assertRowCount(execute("SELECT * FROM %s WHERE k2=22 AND k3 IN (101,102) ALLOW FILTERING"), 0);
        // To test if an IN clause without an AND condition does not create a query plan and works as expected.
        assertRowCount(execute("SELECT * FROM %s WHERE k2 IN (11,22) ALLOW FILTERING"), 6);
    }

    @Test
    public void testAllowFilteringBigIntWithINClause()
    {
        createTable("CREATE TABLE %s (k1 text, k2 bigint, k3 bigint, PRIMARY KEY(k1))");
        createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k1, k2, k3) VALUES ('s1', 1001, 100)");
        execute("INSERT INTO %s (k1, k2, k3) VALUES ('s2', 1001, 200)");
        execute("INSERT INTO %s (k1, k2, k3) VALUES ('s3', 1001, 300)");
        execute("INSERT INTO %s (k1, k2, k3) VALUES ('s4', 2002, 100)");
        execute("INSERT INTO %s (k1, k2, k3) VALUES ('s5', 2002, 200)");
        execute("INSERT INTO %s (k1, k2, k3) VALUES ('s6', 2002, 300)");

        assertRowCount(execute("SELECT * FROM %s WHERE k2=1001 AND k3 IN (100, 200, 300) ALLOW FILTERING"), 3);
        assertRowCount(execute("SELECT * FROM %s WHERE k2=2002 AND k3 IN (100, 200, 300) ALLOW FILTERING"), 3);
        assertRowCount(execute("SELECT * FROM %s WHERE k2=2002 AND k3 IN (101, 201, 301) ALLOW FILTERING"), 0);
        // To test if an IN clause without an AND condition does not create a query plan and works as expected.
        assertRowCount(execute("SELECT * FROM %s WHERE k2 IN (1001,2002) ALLOW FILTERING"), 6);
    }

    @Test
    public void testAllowFilteringBigDecimalWithINClause()
    {
        createTable("CREATE TABLE %s (k1 text, k2 decimal, k3 decimal, PRIMARY KEY(k1))");
        createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k1, k2, k3) VALUES ('s1', 1.1, 1.11)");
        execute("INSERT INTO %s (k1, k2, k3) VALUES ('s2', 1.1, 1.12)");
        execute("INSERT INTO %s (k1, k2, k3) VALUES ('s3', 1.1, 1.13)");
        execute("INSERT INTO %s (k1, k2, k3) VALUES ('s4', 2.2, 1.11)");
        execute("INSERT INTO %s (k1, k2, k3) VALUES ('s5', 2.2, 1.12)");
        execute("INSERT INTO %s (k1, k2, k3) VALUES ('s6', 2.2, 1.13)");

        assertRowCount(execute("SELECT * FROM %s WHERE k2=1.1 AND k3 IN (1.11, 1.12, 1.13) ALLOW FILTERING"), 3);
        assertRowCount(execute("SELECT * FROM %s WHERE k2=2.2 AND k3 IN (1.11, 1.12, 1.13) ALLOW FILTERING"), 3);
        assertRowCount(execute("SELECT * FROM %s WHERE k2=2.2 AND k3 IN (1.21, 1.22, 1.13) ALLOW FILTERING"), 1);
        assertRowCount(execute("SELECT * FROM %s WHERE k2=2.2 AND k3 IN (1.21, 1.22, 1.23) ALLOW FILTERING"), 0);
        // To test if an IN clause without an AND condition does not create a query plan and works as expected.
        assertRowCount(execute("SELECT * FROM %s WHERE k2 IN (1.1,2.2) ALLOW FILTERING"), 6);
    }

    @Test
    public void testAllowFilteringWithLikePrefixPostFiltering()
    {
        createTable("CREATE TABLE %S (k1 int, k2 text, k3 int, PRIMARY KEY (k1))");
        createIndex("CREATE INDEX ON %s(k3) USING 'sai'");

        execute("insert into %s (k1, k2, k3) values (1, 'fo', 1)");
        execute("insert into %s (k1, k2, k3) values (2, 'foo', 2)");
        execute("insert into %s (k1, k2, k3) values (3, 'fo', 3)");
        execute("insert into %s (k1, k2, k3) values (4, 'ba', 4)");
        execute("insert into %s (k1, k2, k3) values (5, 'bar', 5)");

        assertRowCount(execute("SELECT * FROM %s WHERE k3 > 0 AND k2 LIKE 'f%%' ALLOW FILTERING"), 3);
        assertRowCount(execute("SELECT * FROM %s WHERE k3 > 0 AND k2 LIKE 'ca%%' ALLOW FILTERING"), 0);
        assertThatThrownBy(() -> execute("SELECT * FROM %s WHERE k3 > 0 AND k2 LIKE 'f%%'"))
                .hasMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE)
                .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void testAllowFilteringWithLikeSuffixPostFiltering()
    {
        createTable("CREATE TABLE %S (k1 int, k2 text, k3 int, PRIMARY KEY (k1))");
        createIndex("CREATE INDEX ON %s(k3) USING 'sai'");

        execute("insert into %s (k1, k2, k3) values (1, 'fo', 1)");
        execute("insert into %s (k1, k2, k3) values (2, 'foo', 2)");
        execute("insert into %s (k1, k2, k3) values (3, 'fo', 3)");
        execute("insert into %s (k1, k2, k3) values (4, 'ba', 4)");
        execute("insert into %s (k1, k2, k3) values (5, 'bar', 5)");

        assertRowCount(execute("SELECT * FROM %s WHERE k3 > 0 AND k2 LIKE '%%o' ALLOW FILTERING"), 3);
        assertRowCount(execute("SELECT * FROM %s WHERE k3 > 0 AND k2 LIKE '%%c' ALLOW FILTERING"), 0);
        assertThatThrownBy(() -> execute("SELECT * FROM %s WHERE k3 > 0 AND k2 LIKE '%%c'"))
                .hasMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE)
                .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void testAllowFilteringWithLikeContainsPostFiltering()
    {
        createTable("CREATE TABLE %S (k1 int, k2 text, k3 int, PRIMARY KEY (k1))");
        createIndex("CREATE INDEX ON %s(k3) USING 'sai'");

        execute("insert into %s (k1, k2, k3) values (1, 'fo', 1)");
        execute("insert into %s (k1, k2, k3) values (2, 'foo', 2)");
        execute("insert into %s (k1, k2, k3) values (3, 'fo', 3)");
        execute("insert into %s (k1, k2, k3) values (4, 'ba', 4)");
        execute("insert into %s (k1, k2, k3) values (5, 'bar', 5)");

        assertRowCount(execute("SELECT * FROM %s WHERE k3 > 0 AND k2 LIKE '%%ar%%' ALLOW FILTERING"), 1);
        assertRowCount(execute("SELECT * FROM %s WHERE k3 > 0 AND k2 LIKE '%%ca%%' ALLOW FILTERING"), 0);
        assertThatThrownBy(() -> execute("SELECT * FROM %s WHERE k3 > 0 AND k2 LIKE '%%ca%%'"))
                .hasMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE)
                .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void testAllowFilteringWithLikeMatchesPostFiltering()
    {
        createTable("CREATE TABLE %S (k1 int, k2 text, k3 int, PRIMARY KEY (k1))");
        createIndex("CREATE INDEX ON %s(k3) USING 'sai'");

        execute("insert into %s (k1, k2, k3) values (1, 'fo', 1)");
        execute("insert into %s (k1, k2, k3) values (2, 'foo', 2)");
        execute("insert into %s (k1, k2, k3) values (3, 'fo', 3)");
        execute("insert into %s (k1, k2, k3) values (4, 'ba', 4)");
        execute("insert into %s (k1, k2, k3) values (5, 'bar', 5)");

        assertRowCount(execute("SELECT * FROM %s WHERE k3 > 0 AND k2 LIKE 'foo' ALLOW FILTERING"), 1);
        assertRowCount(execute("SELECT * FROM %s WHERE k3 > 0 AND k2 LIKE 'baar' ALLOW FILTERING"), 0);
        assertThatThrownBy(() -> execute("SELECT * FROM %s WHERE k3 > 0 AND k2 LIKE 'baar'"))
                .hasMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE)
                .isInstanceOf(InvalidRequestException.class);
    }

    /**
     * LIKE filtering: {@code %} anywhere and {@code _} are wildcards, {@code ESCAPE} makes them literal, and a
     * wildcard-free pattern is an equality, with or without SAI indexes on other columns.
     */
    @Test
    public void testAllowFilteringWithLikePatterns() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text, i int)");
        execute("INSERT INTO %s (k, v, i) VALUES (1, 'abc', 1)");
        execute("INSERT INTO %s (k, v, i) VALUES (2, 'axxc', 1)");
        execute("INSERT INTO %s (k, v, i) VALUES (3, 'a%%c', 1)");
        execute("INSERT INTO %s (k, v, i) VALUES (4, 'a_c', 1)");
        execute("INSERT INTO %s (k, v, i) VALUES (5, 'xabcx', 1)");
        execute("INSERT INTO %s (k, i) VALUES (6, 1)");

        for (boolean withIndex : new boolean[]{ false, true })
        {
            if (withIndex)
                createIndex("CREATE INDEX ON %s(i) USING 'sai'");
            String filter = withIndex ? "i = 1 AND " : "";

            beforeAndAfterFlush(() -> {
                assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE " + filter + "v LIKE 'a%%' ALLOW FILTERING"),
                                        row(1), row(2), row(3), row(4));
                assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE " + filter + "v LIKE '%%c' ALLOW FILTERING"),
                                        row(1), row(2), row(3), row(4));
                assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE " + filter + "v LIKE '%%b%%' ALLOW FILTERING"),
                                        row(1), row(5));
                // a wildcard-free pattern matches equal values only, not values containing it
                assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE " + filter + "v LIKE 'abc' ALLOW FILTERING"),
                                        row(1));
                // an inner % is a wildcard, as is _
                assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE " + filter + "v LIKE 'a%%c' ALLOW FILTERING"),
                                        row(1), row(2), row(3), row(4));
                assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE " + filter + "v LIKE 'a_c' ALLOW FILTERING"),
                                        row(1), row(3), row(4));
                assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE " + filter + "v LIKE '%%a_c%%' ALLOW FILTERING"),
                                        row(1), row(3), row(4), row(5));
                // escaped wildcards are literal
                assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE " + filter + "v LIKE 'a#%%c' ESCAPE '#' ALLOW FILTERING"),
                                        row(3));
                assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE " + filter + "v LIKE 'a#_%%' ESCAPE '#' ALLOW FILTERING"),
                                        row(4));
                // bind markers
                assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE " + filter + "v LIKE ? ALLOW FILTERING", "%x%"),
                                        row(2), row(5));
                // disjunctions
                assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE " + filter + "(v LIKE 'abc' OR v LIKE 'x%%') ALLOW FILTERING"),
                                        row(1), row(5));
                // a pattern that is only wildcards is still rejected
                assertInvalidMessage("LIKE value can't be empty.",
                                     "SELECT k FROM %s WHERE " + filter + "v LIKE '%%' ALLOW FILTERING");
            });

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT k FROM %s WHERE " + filter + "v LIKE 'a%%'");
        }
    }

    /**
     * LIKE filtering on primary key and static columns.
     */
    @Test
    public void testAllowFilteringWithLikeOnPrimaryKeyAndStaticColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 text, k2 text, c1 text, c2 text, s text static, v text, PRIMARY KEY ((k1, k2), c1, c2))");
        execute("INSERT INTO %s (k1, k2, c1, c2, s, v) VALUES ('a1', 'b1', 'c1', 'd1', 's1', 'v1')");
        execute("INSERT INTO %s (k1, k2, c1, c2, s, v) VALUES ('a1', 'b1', 'c2', 'd2', 's1', 'v2')");
        execute("INSERT INTO %s (k1, k2, c1, c2, s, v) VALUES ('a2', 'b2', 'c1', 'd3', 'x2', 'v3')");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE k1 LIKE 'a1%%' ALLOW FILTERING"), row("v1"), row("v2"));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE k2 LIKE '%%2' ALLOW FILTERING"), row("v3"));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE k1 = 'a1' AND k2 LIKE 'b%%' ALLOW FILTERING"), row("v1"), row("v2"));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE c1 LIKE '%%1' ALLOW FILTERING"), row("v1"), row("v3"));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE k1 = 'a1' AND k2 = 'b1' AND c1 LIKE 'c_' ALLOW FILTERING"), row("v1"), row("v2"));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE k1 = 'a1' AND k2 = 'b1' AND c1 = 'c1' AND c2 LIKE 'd%%' ALLOW FILTERING"), row("v1"));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE s LIKE 's%%' ALLOW FILTERING"), row("v1"), row("v2"));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE v LIKE '%%3' ALLOW FILTERING"), row("v3"));
        });

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, "SELECT v FROM %s WHERE k1 LIKE 'a1%%'");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, "SELECT v FROM %s WHERE c1 LIKE 'c%%'");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, "SELECT v FROM %s WHERE v LIKE 'v%%'");

        // a LIKE on a partition key component makes a key range query, also with an index serving it
        createIndex("CREATE INDEX ON %s(k2) USING 'sai'");
        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE k1 = 'a1' AND k2 LIKE 'b%%' ALLOW FILTERING"), row("v1"), row("v2"));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE k2 LIKE 'b%%'"), row("v1"), row("v2"), row("v3"));
        });

        // LIKE still can't be used in updates and deletions
        assertInvalidMessage("LIKE restriction is only supported on properly indexed columns",
                             "DELETE FROM %s WHERE k1 = 'a1' AND k2 = 'b1' AND c1 LIKE 'c%%'");
        assertInvalidMessage("LIKE restriction is only supported on properly indexed columns",
                             "UPDATE %s SET v = 'x' WHERE k1 = 'a1' AND k2 = 'b1' AND c1 = 'c1' AND c2 LIKE 'd%%'");
    }

    /**
     * LIKE filtering is only supported on string columns, and on columns whose index doesn't support LIKE.
     */
    @Test
    public void testAllowFilteringWithLikeOnUnsupportedColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a ascii, t text, i int, b blob, l list<text>, analyzed text)");
        createIndex("CREATE CUSTOM INDEX ON %s(analyzed) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        execute("INSERT INTO %s (k, a, t, i, b, l, analyzed) VALUES (1, 'abc', 'abc', 1, 0x01, ['abc'], 'Quick Fox')");

        assertRows(execute("SELECT k FROM %s WHERE a LIKE 'ab%%' ALLOW FILTERING"), row(1));

        // the analyzed index doesn't support LIKE, so the raw values are filtered, without the analyzer
        assertRows(execute("SELECT k FROM %s WHERE analyzed LIKE 'Quick%%' ALLOW FILTERING"), row(1));
        assertRows(execute("SELECT k FROM %s WHERE analyzed LIKE 'quick%%' ALLOW FILTERING"));
        assertRows(execute("SELECT k FROM %s WHERE analyzed LIKE 'Quick Fox' ALLOW FILTERING"), row(1));
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, "analyzed"),
                             "SELECT k FROM %s WHERE analyzed LIKE 'Quick%%'");

        assertInvalidMessage("LIKE restriction is only supported on properly indexed columns",
                             "SELECT k FROM %s WHERE i LIKE ? ALLOW FILTERING", 1);
        assertInvalidMessage("LIKE restriction is only supported on properly indexed columns",
                             "SELECT k FROM %s WHERE b LIKE ? ALLOW FILTERING", ByteBufferUtil.bytes("abc"));
        assertInvalidMessage("cannot be restricted by a 'LIKE' relation",
                             "SELECT k FROM %s WHERE l LIKE 'a%%' ALLOW FILTERING");
    }

    /**
     * LIKE restrictions, filtered or served by an index, are rejected when a replica may evaluate them with other
     * semantics, as its messaging version says, whatever its release version. The fake peer can't be read from, so the
     * queries are executed internally, but the coordinator builds the same row filter.
     */
    @Test
    public void testLikeRequiresDataStaxReplicas() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text, i text)");
        createIndex("CREATE INDEX ON %s(i) USING 'sai'");
        execute("INSERT INTO %s (k, v, i) VALUES (1, 'abc', 'abc')");
        assertLikeAccepted();

        try (FakePeer peer = new FakePeer("127.0.0.2"))
        {
            // a peer with an unknown messaging version that isn't alive isn't counted...
            assertLikeAccepted();
            // ... but it is while alive: it's about to announce its version
            peer.alive(true);
            assertLikeRejected(peer);
            peer.messagingVersion(MessagingService.VERSION_DS_21);
            assertLikeAccepted();

            // Apache Cassandra, DS Converged Cassandra 4.0, DSE 6.8 and earlier DataStax 5.0 messaging versions, alive
            // or not
            for (int version : new int[]{ MessagingService.VERSION_40, MessagingService.VERSION_50,
                                          MessagingService.VERSION_DS_10, MessagingService.VERSION_DS_12,
                                          MessagingService.VERSION_DS_20, MessagingService.VERSION_DSE_68 })
            {
                for (String release : new String[]{ "5.0.7.0", "5.0.10.0", "5.0.10" })
                {
                    peer.messagingVersion(version).releaseVersion(release);
                    assertLikeRejected(peer);
                    peer.alive(false);
                    assertLikeRejected(peer);
                    peer.alive(true);
                }
            }

            // DS 21 is enough
            peer.messagingVersion(MessagingService.VERSION_DS_21);
            for (String release : new String[]{ "5.0.7.0", "5.0.10", "5.0.10.0", "5.1.0" })
            {
                peer.releaseVersion(release);
                assertLikeAccepted();
            }

            // After a restart of this node with the peer still down, its messaging version, only kept in memory, is
            // unknown: the peer isn't counted until it's alive, when it's counted until it announces its version.
            peer.alive(false).forgetMessagingVersion();
            assertLikeAccepted();
            peer.alive(true);
            assertLikeRejected(peer);
            peer.messagingVersion(MessagingService.VERSION_DS_21);
            assertLikeAccepted();
        }
    }

    /**
     * A peer that Nodes doesn't know (such as a peer of a custom token metadata provider) has never been seen alive in
     * gossip: the failure detector isn't asked about it, whatever its gossip state says, and it's decided by its
     * messaging version alone, if known.
     */
    @Test
    public void testLikeWithPeerUnknownToNodes() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text, i text)");
        createIndex("CREATE INDEX ON %s(i) USING 'sai'");
        execute("INSERT INTO %s (k, v, i) VALUES (1, 'abc', 'abc')");

        try (FakePeer peer = new FakePeer("127.0.0.2", false))
        {
            assertLikeAccepted();
            peer.alive(true);
            assertLikeAccepted();
            peer.messagingVersion(MessagingService.VERSION_DS_21);
            assertLikeAccepted();
            // a version known to be unsupported still counts, alive or not
            peer.messagingVersion(MessagingService.VERSION_DS_20);
            assertLikeRejected(peer);
            peer.alive(false);
            assertLikeRejected(peer);
        }
    }

    /**
     * The nodes a query can contact are checked again as soon as a messaging version or the liveness of a node
     * changes. The changes are made directly, not through FakePeer.
     */
    @Test
    public void testLikeChecksPeersAgain() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text, i text)");
        createIndex("CREATE INDEX ON %s(i) USING 'sai'");
        execute("INSERT INTO %s (k, v, i) VALUES (1, 'abc', 'abc')");

        try (FakePeer peer = new FakePeer("127.0.0.2"))
        {
            peer.alive(true).messagingVersion(MessagingService.VERSION_DS_21);
            EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(peer.address);
            assertLikeAccepted();

            // down, with a messaging version below DS 21
            state.markDead();
            MessagingService.instance().versions.set(peer.address, MessagingService.VERSION_DS_20);
            assertLikeRejected(peer);

            // back on DS 21
            MessagingService.instance().versions.set(peer.address, MessagingService.VERSION_DS_21);
            assertLikeAccepted();

            // its messaging version unknown: not counted while down, counted once alive
            MessagingService.instance().versions.reset(peer.address);
            assertLikeAccepted();
            state.markAlive();
            assertLikeRejected(peer);
            MessagingService.instance().versions.set(peer.address, MessagingService.VERSION_DS_21);
            assertLikeAccepted();
        }
    }

    /**
     * A query whose check of the nodes started before a node refused a LIKE read checks them again, and so do the
     * queries after it (see {@link DataStaxFeatureSupport#refreshReadChecks}).
     */
    @Test
    public void testLikeRefusalDuringCheck() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        Injections.Barrier holdCheck = Injections.newBarrier("hold_read_check", 2, false)
                                                 .add(InvokePointBuilder.newInvokePoint()
                                                                        .onClass(DataStaxFeatureSupport.class)
                                                                        .onMethod("checkRead")
                                                                        .atExit())
                                                 .build();
        try (FakePeer peer = new FakePeer("127.0.0.2"))
        {
            peer.alive(false);
            EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(peer.address);
            assertThat(DataStaxFeatureSupport.nodesNotSupportingRead(KEYSPACE, ConsistencyLevel.ONE)).isEmpty();
            Injections.inject(holdCheck);
            DataStaxFeatureSupport.refreshReadChecks(KEYSPACE);

            CompletableFuture<Set<InetAddressAndPort>> checked =
                CompletableFuture.supplyAsync(() -> DataStaxFeatureSupport.nodesNotSupportingRead(KEYSPACE, ConsistencyLevel.ONE));
            Util.spinAssertEquals(1L, holdCheck::getCount, 60);
            // the peer comes back alive with an unknown version, and refuses a read
            state.markAlive();
            DataStaxFeatureSupport.refreshReadChecks(KEYSPACE);
            holdCheck.countDown();

            assertThat(checked.get(60, TimeUnit.SECONDS)).containsExactly(peer.address);
            assertThat(DataStaxFeatureSupport.nodesNotSupportingRead(KEYSPACE, ConsistencyLevel.ONE)).containsExactly(peer.address);
        }
        finally
        {
            holdCheck.disable();
        }
    }

    /**
     * A node that refuses a LIKE read makes the reads of its keyspace check the nodes again, not those of other
     * keyspaces.
     */
    @Test
    public void testLikeRefusalChecksItsKeyspaceAgain() throws Throwable
    {
        String otherKeyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': '1'}");
        Injections.Counter checks = Injections.newCounter("read_checks")
                                              .add(InvokePointBuilder.newInvokePoint()
                                                                     .onClass(DataStaxFeatureSupport.class)
                                                                     .onMethod("checkRead"))
                                              .build();
        List<ConsistencyLevel> scopes = Arrays.asList(ConsistencyLevel.ONE, ConsistencyLevel.LOCAL_ONE);
        for (ConsistencyLevel consistency : scopes)
        {
            DataStaxFeatureSupport.nodesNotSupportingRead(KEYSPACE, consistency);
            DataStaxFeatureSupport.nodesNotSupportingRead(otherKeyspace, consistency);
        }
        Injections.inject(checks);
        try
        {
            DataStaxFeatureSupport.refreshReadChecks(KEYSPACE);
            for (ConsistencyLevel consistency : scopes)
                DataStaxFeatureSupport.nodesNotSupportingRead(otherKeyspace, consistency);
            assertEquals(0, checks.get());
            // once for every node, once for the local datacenter
            for (int i = 0; i < 2; i++)
                for (ConsistencyLevel consistency : scopes)
                    DataStaxFeatureSupport.nodesNotSupportingRead(KEYSPACE, consistency);
            assertEquals(2, checks.get());
        }
        finally
        {
            checks.disable();
        }
    }

    /**
     * A check of the nodes that is running when a node refuses a LIKE read of its keyspace is done again before the read
     * returns, and its first result isn't kept.
     */
    @Test
    public void testLikeRefusalDuringRunningCheck() throws Throwable
    {
        Injections.Counter checks = Injections.newCounter("read_checks_during_refusal")
                                              .add(InvokePointBuilder.newInvokePoint()
                                                                     .onClass(DataStaxFeatureSupport.class)
                                                                     .onMethod("checkRead"))
                                              .build();
        Injections.Barrier holdCheck = Injections.newBarrier("hold_invalidated_read_check", 2, false)
                                                 .add(InvokePointBuilder.newInvokePoint()
                                                                        .onClass(DataStaxFeatureSupport.class)
                                                                        .onMethod("checkRead")
                                                                        .atExit())
                                                 .build();
        try
        {
            DataStaxFeatureSupport.nodesNotSupportingRead(KEYSPACE, ConsistencyLevel.ONE);
            DataStaxFeatureSupport.refreshReadChecks(KEYSPACE);
            Injections.inject(checks, holdCheck);

            CompletableFuture<Set<InetAddressAndPort>> checked =
                CompletableFuture.supplyAsync(() -> DataStaxFeatureSupport.nodesNotSupportingRead(KEYSPACE, ConsistencyLevel.ONE));
            Util.spinAssertEquals(1L, holdCheck::getCount, 60);
            DataStaxFeatureSupport.refreshReadChecks(KEYSPACE);
            holdCheck.countDown();

            assertThat(checked.get(60, TimeUnit.SECONDS)).isEmpty();
            assertEquals(2, checks.get());
            DataStaxFeatureSupport.nodesNotSupportingRead(KEYSPACE, ConsistencyLevel.ONE);
            assertEquals(2, checks.get());
        }
        finally
        {
            holdCheck.disable();
            checks.disable();
        }
    }

    /**
     * The nodes a LIKE query can contact aren't checked again until they change, however long it takes.
     */
    @Test
    public void testLikeNoCheckWithoutChange() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text, i text)");
        createIndex("CREATE INDEX ON %s(i) USING 'sai'");
        execute("INSERT INTO %s (k, v, i) VALUES (1, 'abc', 'abc')");

        Injections.Counter checks = Injections.newCounter("read_checks")
                                              .add(InvokePointBuilder.newInvokePoint()
                                                                     .onClass(DataStaxFeatureSupport.class)
                                                                     .onMethod("checkRead"))
                                              .build();
        try (FakePeer peer = new FakePeer("127.0.0.2"))
        {
            peer.alive(true).messagingVersion(MessagingService.VERSION_DS_21);
            assertLikeAccepted();
            Injections.inject(checks);

            TimeUnit.MILLISECONDS.sleep(1100);
            assertLikeAccepted();
            assertEquals(0, checks.get());

            peer.messagingVersion(MessagingService.VERSION_DS_20);
            assertLikeRejected(peer);
            assertEquals(1, checks.get());
        }
        finally
        {
            checks.disable();
        }
    }

    /**
     * new_feature_peer_version_check: messaging checks the messaging versions, and none checks nothing.
     */
    @Test
    public void testNewFeaturePeerVersionCheck() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text, i text)");
        createIndex("CREATE INDEX ON %s(i) USING 'sai'");
        execute("INSERT INTO %s (k, v, i) VALUES (1, 'abc', 'abc')");

        Config.NewFeaturePeerVersionCheck check = DatabaseDescriptor.getNewFeaturePeerVersionCheck();
        assertEquals(Config.NewFeaturePeerVersionCheck.messaging, check);
        try (FakePeer peer = new FakePeer("127.0.0.2"))
        {
            // a DataStax 5.0 node before 5.0.10, and an Apache Cassandra node
            for (int version : new int[]{ MessagingService.VERSION_DS_20, MessagingService.VERSION_50 })
            {
                peer.messagingVersion(version);
                StorageService.instance.setNewFeaturePeerVersionCheck("messaging");
                assertLikeRejected(peer);
                StorageService.instance.setNewFeaturePeerVersionCheck("none");
                assertLikeAccepted();
            }

            // a live node with an unknown version
            peer.alive(true).forgetMessagingVersion();
            StorageService.instance.setNewFeaturePeerVersionCheck("messaging");
            assertLikeRejected(peer);
            StorageService.instance.setNewFeaturePeerVersionCheck("none");
            assertLikeAccepted();

            assertThatThrownBy(() -> StorageService.instance.setNewFeaturePeerVersionCheck("full"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid new_feature_peer_version_check full, expected one of [messaging, none]");
            assertEquals("none", StorageService.instance.getNewFeaturePeerVersionCheck());
        }
        finally
        {
            DatabaseDescriptor.setNewFeaturePeerVersionCheck(check);
        }
    }

    private void assertLikeAccepted() throws Throwable
    {
        assertRows(executeInternal("SELECT k FROM %s WHERE i LIKE 'a%%' AND v LIKE 'a%%' ALLOW FILTERING"), row(1));
        assertRows(executeInternal("SELECT k FROM %s WHERE v LIKE 'a%%' ALLOW FILTERING"), row(1));
        assertRows(executeInternal("SELECT k FROM %s WHERE i LIKE 'a%%'"), row(1));
        assertRows(executeInternal("SELECT k FROM %s WHERE i LIKE 'a%%' ALLOW FILTERING"), row(1));
    }

    private void assertLikeRejected(FakePeer peer)
    {
        for (String column : new String[]{ "v", "i" })
        {
            assertThatThrownBy(() -> executeInternal("SELECT k FROM %s WHERE " + column + " LIKE 'a%%' ALLOW FILTERING"))
                    .isInstanceOf(InvalidRequestException.class)
                    .hasMessage(String.format(SingleColumnRestriction.LikeRestriction.LIKE_UNSUPPORTED_BY_NODES_MESSAGE,
                                              column, Collections.singleton(peer.address)));
        }
        // the nodes are checked once per query, whatever its number of LIKE restrictions
        assertThatThrownBy(() -> executeInternal("SELECT k FROM %s WHERE i LIKE 'a%%' AND v LIKE 'a%%' ALLOW FILTERING"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageStartingWith("LIKE restriction on column ")
                .hasMessageEndingWith(String.format(SingleColumnRestriction.LikeRestriction.LIKE_UNSUPPORTED_BY_NODES_MESSAGE,
                                                    "x", Collections.singleton(peer.address)).substring("LIKE restriction on column x".length()));
    }

    /**
     * A LIKE restriction on a clustering column without a supporting index needs ALLOW FILTERING, even within a
     * partition.
     */
    @Test
    public void testLikeOnClusteringColumnsRequiresAllowFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 text, c2 text, v int, PRIMARY KEY (k, c1, c2))");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (1, 'aa', 'xa', 1)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (1, 'aa', 'ya', 2)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (1, 'ba', 'xa', 3)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (2, 'ab', 'xa', 4)");

        String[] queries = { "SELECT v FROM %s WHERE k = 1 AND c1 LIKE 'a%%'",
                             "SELECT v FROM %s WHERE k IN (1, 2) AND c1 LIKE 'a%%'",
                             "SELECT v FROM %s WHERE k = 1 AND c1 = 'aa' AND c2 LIKE 'x%%'",
                             "SELECT v FROM %s WHERE c1 = 'aa' AND c2 LIKE 'x%%'",
                             "SELECT v FROM %s WHERE c1 LIKE 'a%%' AND c2 = 'xa'" };
        Object[][][] expected = { { row(1), row(2) },
                                  { row(1), row(2), row(4) },
                                  { row(1) },
                                  { row(1) },
                                  { row(1), row(4) } };

        beforeAndAfterFlush(() -> {
            for (int i = 0; i < queries.length; i++)
            {
                assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, queries[i]);
                assertRowsIgnoringOrder(execute(queries[i] + " ALLOW FILTERING"), expected[i]);
            }
        });

        // an index serving the LIKE doesn't need filtering
        createIndex("CREATE INDEX ON %s(c1) USING 'sai'");
        assertRowsIgnoringOrder(execute(queries[0]), expected[0]);
    }

    /**
     * Materialized views can't be restricted by LIKE.
     */
    @Test
    public void testLikeInMaterializedViewIsRejected()
    {
        boolean enabled = DatabaseDescriptor.getMaterializedViewsEnabled();
        DatabaseDescriptor.setMaterializedViewsEnabled(true);
        try
        {
            createTable("CREATE TABLE %s (k int, c text, v int, PRIMARY KEY (k, c))");
            assertThatThrownBy(() -> createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                                "WHERE k IS NOT NULL AND c LIKE 'a%%' PRIMARY KEY (c, k)"))
                    .satisfies(e -> assertThat(Throwables.getRootCause(e))
                                    .isInstanceOf(InvalidRequestException.class)
                                    .hasMessage(String.format(StatementRestrictions.RESTRICTION_REQUIRES_INDEX_MESSAGE,
                                                              "LIKE", "c LIKE 'a%'")));
        }
        finally
        {
            DatabaseDescriptor.setMaterializedViewsEnabled(enabled);
        }
    }

    /**
     * A LIKE pattern can't be null or unset.
     */
    @Test
    public void testLikeWithNullOrUnsetPattern()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text, i text)");
        createIndex("CREATE INDEX ON %s(i) USING 'sai'");
        execute("INSERT INTO %s (k, v, i) VALUES (1, 'abc', 'abc')");

        for (Object pattern : new Object[]{ null, unset() })
        {
            assertInvalidMessage("Invalid null or unset value for LIKE on column v",
                                 "SELECT k FROM %s WHERE v LIKE ? ALLOW FILTERING", pattern);
            assertInvalidMessage("Invalid null or unset value for LIKE on column i",
                                 "SELECT k FROM %s WHERE i LIKE ?", pattern);
        }
    }
}
