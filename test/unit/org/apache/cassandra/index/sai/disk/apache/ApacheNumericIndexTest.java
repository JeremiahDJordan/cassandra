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

package org.apache.cassandra.index.sai.disk.apache;

import java.util.List;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reads the numeric (block balanced tree) indexes Apache Cassandra 5.0 wrote (test/data/legacy-sai/apache-aa,
 * written by Apache cassandra-5.0 at fb7f1b7aee).
 */
public class ApacheNumericIndexTest extends ApacheIndexTester
{
    @Test
    public void testSkinny() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, s text, n int, v vector<float, 3>)");
        disableCompaction();
        createIndex("CREATE INDEX skinny_s ON %s(s) USING 'sai'");
        createIndex("CREATE INDEX skinny_n ON %s(n) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("skinny");

        assertThat(execute("SELECT pk FROM %s WHERE n > 89")).hasSize(10);
        assertSameResultsAfterRebuild(List.of("SELECT pk FROM %s WHERE n = 42",
                                              "SELECT pk FROM %s WHERE n = 420",
                                              "SELECT pk FROM %s WHERE n = -1",
                                              "SELECT pk FROM %s WHERE n > 89",
                                              "SELECT pk FROM %s WHERE n >= 89",
                                              "SELECT pk FROM %s WHERE n < 3",
                                              "SELECT pk FROM %s WHERE n <= 3",
                                              "SELECT pk FROM %s WHERE n > 10 AND n < 20",
                                              "SELECT pk FROM %s WHERE n >= 10 AND n <= 20",
                                              "SELECT pk FROM %s WHERE n > 1000",
                                              "SELECT pk FROM %s WHERE n != 5",
                                              "SELECT pk FROM %s WHERE n > 40 AND s = 's3'",
                                              "SELECT pk FROM %s WHERE n < 40 AND s = 's3' AND pk > 10 ALLOW FILTERING",
                                              "SELECT pk FROM %s WHERE (n < 20 OR n > 80) AND s = 's1'",
                                              "SELECT pk, n FROM %s ORDER BY n LIMIT 10",
                                              "SELECT pk, n FROM %s ORDER BY n DESC LIMIT 10",
                                              "SELECT pk, n FROM %s WHERE n > 33 ORDER BY n LIMIT 5",
                                              "SELECT pk, n FROM %s WHERE s = 's7' ORDER BY n DESC LIMIT 3"),
                                      "skinny_s", "skinny_n");
    }

    @Test
    public void testWide() throws Throwable
    {
        createTable(WIDE);
        disableCompaction();
        createIndex("CREATE INDEX wide_s ON %s(s) USING 'sai'");
        createIndex("CREATE INDEX wide_n ON %s(n) USING 'sai'");
        createIndex("CREATE INDEX wide_st ON %s(st) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("wide");

        assertThat(execute("SELECT pk FROM %s WHERE st = 2 PER PARTITION LIMIT 1")).hasSize(20);
        assertSameResultsAfterRebuild(List.of("SELECT pk, ck FROM %s WHERE n = 423",
                                              "SELECT pk, ck FROM %s WHERE n > 950",
                                              "SELECT pk, ck FROM %s WHERE n >= 100 AND n <= 120",
                                              "SELECT pk, ck FROM %s WHERE n < 5",
                                              "SELECT pk, ck FROM %s WHERE n > 100 AND n < 200 AND pk = 15",
                                              "SELECT pk, ck FROM %s WHERE n > 500 AND s = 'w1'",
                                              "SELECT pk, ck FROM %s WHERE n != 423 AND pk = 42",
                                              "SELECT pk, ck, n FROM %s ORDER BY n LIMIT 7",
                                              "SELECT pk, ck, n FROM %s ORDER BY n DESC LIMIT 7",
                                              "SELECT pk, ck, n FROM %s WHERE s = 'w2' ORDER BY n LIMIT 4",
                                              "SELECT pk, st FROM %s WHERE st = 2",
                                              "SELECT pk FROM %s WHERE st = 2 PER PARTITION LIMIT 1",
                                              "SELECT pk, ck FROM %s WHERE st > 3",
                                              "SELECT pk, ck FROM %s WHERE st = 2 AND n > 500",
                                              "SELECT pk, ck FROM %s WHERE st = 4 AND s = 'w0'",
                                              "SELECT pk, ck FROM %s WHERE st < 1 AND n < 100 LIMIT 3"),
                                      "wide_s", "wide_n", "wide_st");
    }
}
