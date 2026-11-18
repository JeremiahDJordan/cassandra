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
package org.apache.cassandra.index.sai.cql;

import java.nio.ByteBuffer;
import java.util.Collections;

import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.gms.FakePeer;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.apache.cassandra.utils.ByteBufferUtil.hexToBytes;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests SAI indexes on blob values, beyond the generic type tests of
 * {@link org.apache.cassandra.index.sai.cql.types.BlobTest} and the collection ones.
 */
public class BlobIndexTest extends SAITester
{
    private static final ByteBuffer B00 = hexToBytes("00");
    private static final ByteBuffer B0001 = hexToBytes("0001");
    private static final ByteBuffer B01 = hexToBytes("01");
    private static final ByteBuffer BFF = hexToBytes("ff");

    @Test
    public void testOperators() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, b blob)");
        createIndex("CREATE INDEX ON %s(b) USING 'sai'");

        execute("INSERT INTO %s (k, b) VALUES (0, ?)", B00);
        execute("INSERT INTO %s (k, b) VALUES (1, ?)", B0001);
        execute("INSERT INTO %s (k, b) VALUES (2, ?)", B01);
        execute("INSERT INTO %s (k, b) VALUES (3, ?)", BFF);
        execute("INSERT INTO %s (k, b) VALUES (4, ?)", EMPTY_BYTE_BUFFER);
        execute("INSERT INTO %s (k) VALUES (5)");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k FROM %s WHERE b = 0x00"), row(0));
            assertRows(execute("SELECT k FROM %s WHERE b = ?", BFF), row(3));
            assertRows(execute("SELECT k FROM %s WHERE b = 0x02"));
            // empty blobs are indexed, as empty strings are
            assertRows(execute("SELECT k FROM %s WHERE b = 0x"), row(4));
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE b IN (0x00, 0x01, 0x02)"), row(0), row(2));
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE b = 0x00 OR b = 0xff"), row(0), row(3));

            // the index serves only equality: other operators are filtered, comparing unsigned bytes
            assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, "b"),
                                 "SELECT k FROM %s WHERE b > 0x00");
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE b > 0x00 ALLOW FILTERING"), row(1), row(2), row(3));
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE b >= 0x01 AND b < 0xff ALLOW FILTERING"), row(2));
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE b != 0x00 ALLOW FILTERING"), row(1), row(2), row(3), row(4));

            // LIKE isn't supported on blobs, with or without filtering
            assertInvalidMessage(String.format(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_LIKE_MESSAGE, "b"),
                                 "SELECT k FROM %s WHERE b LIKE ? ALLOW FILTERING", B00);

            // ORDER BY sorts unsigned bytes
            assertRows(execute("SELECT k FROM %s ORDER BY b ASC LIMIT 10"), row(4), row(0), row(1), row(2), row(3));
            assertRows(execute("SELECT k FROM %s ORDER BY b DESC LIMIT 10"), row(3), row(2), row(1), row(0), row(4));
        });
    }

    @Test
    public void testPrimaryKeyAndStaticColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 blob, c1 blob, c2 blob, s blob static, v int, " +
                    "PRIMARY KEY ((k1, k2), c1, c2)) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)");
        createIndex("CREATE INDEX ON %s(k2) USING 'sai'");
        createIndex("CREATE INDEX ON %s(c1) USING 'sai'");
        createIndex("CREATE INDEX ON %s(c2) USING 'sai'");
        createIndex("CREATE INDEX ON %s(s) USING 'sai'");

        execute("INSERT INTO %s (k1, k2, c1, c2, s, v) VALUES (0, 0x00, 0x00, 0x01, 0x0a, 0)");
        execute("INSERT INTO %s (k1, k2, c1, c2, s, v) VALUES (0, 0x00, 0x01, 0x00, 0x0a, 1)");
        execute("INSERT INTO %s (k1, k2, c1, c2, s, v) VALUES (1, 0xff, 0x00, 0x00, 0x0b, 2)");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE k2 = 0x00"), row(0), row(1));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE c1 = 0x00"), row(0), row(2));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE c2 = 0x00"), row(1), row(2));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE s = 0x0b"), row(2));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE c1 = 0x00 AND c2 = 0x01"), row(0));
        });
    }

    @Test
    public void testCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l list<blob>, s set<blob>, m map<blob, blob>, f frozen<list<blob>>)");
        createIndex("CREATE INDEX ON %s(l) USING 'sai'");
        createIndex("CREATE INDEX ON %s(s) USING 'sai'");
        createIndex("CREATE INDEX ON %s(KEYS(m)) USING 'sai'");
        createIndex("CREATE INDEX ON %s(VALUES(m)) USING 'sai'");
        createIndex("CREATE INDEX ON %s(ENTRIES(m)) USING 'sai'");
        createIndex("CREATE INDEX ON %s(FULL(f)) USING 'sai'");

        execute("INSERT INTO %s (k, l, s, m, f) VALUES (0, [0x00, 0x01], {0x00}, {0x00: 0x01}, [0x00])");
        execute("INSERT INTO %s (k, l, s, m, f) VALUES (1, [0x02], {0x01, 0xff}, {0x01: 0x00}, [0x00, 0x01])");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k FROM %s WHERE l CONTAINS 0x01"), row(0));
            assertRows(execute("SELECT k FROM %s WHERE s CONTAINS 0xff"), row(1));
            assertRows(execute("SELECT k FROM %s WHERE m CONTAINS KEY 0x01"), row(1));
            assertRows(execute("SELECT k FROM %s WHERE m CONTAINS 0x01"), row(0));
            assertRows(execute("SELECT k FROM %s WHERE m[0x00] = 0x01"), row(0));
            assertRows(execute("SELECT k FROM %s WHERE f = [0x00, 0x01]"), row(1));
        });
    }

    @Test
    public void testAnalyzerOptionsRejected()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, b blob, l list<blob>, mk map<blob, text>, mv map<text, blob>)");
        for (String target : new String[]{ "b", "l", "ENTRIES(mk)", "ENTRIES(mv)" })
        {
            for (String option : new String[]{ "'case_sensitive': 'false'", "'normalize': 'true'", "'ascii': 'true'",
                                               "'index_analyzer': 'standard'", "'query_analyzer': 'standard'" })
            {
                assertThatThrownBy(() -> createIndex("CREATE INDEX ON %s(" + target + ") USING 'sai' WITH OPTIONS = {" + option + '}'))
                        .isInstanceOf(InvalidRequestException.class)
                        .hasMessage("CQL type blob cannot be analyzed.");
            }
        }
    }

    /**
     * Indexes on blob values are only created once all the nodes support them, as their messaging versions say,
     * whatever their release versions.
     */
    @Test
    public void testCreationRequiresSupportingNodes() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, b blob, l list<blob>, m map<int, blob>, " +
                    "f frozen<list<blob>>, t text)");

        try (FakePeer peer = new FakePeer("127.0.0.2").alive(true))
        {
            // a live peer whose messaging version isn't known isn't trusted
            assertCreationRejected("b", "b", peer);

            // Apache Cassandra, DS Converged Cassandra 4.0, DSE 6.8 and earlier DataStax 5.0 messaging versions
            for (int version : new int[]{ MessagingService.VERSION_40, MessagingService.VERSION_50,
                                          MessagingService.VERSION_DS_12, MessagingService.VERSION_DS_20,
                                          MessagingService.VERSION_DSE_68 })
            {
                peer.messagingVersion(version).releaseVersion("5.0.10.0");
                assertCreationRejected("b", "b", peer);
                assertCreationRejected("l", "l", peer);
                assertCreationRejected("VALUES(m)", "m", peer);
                assertCreationRejected("ENTRIES(m)", "m", peer);
            }

            // indexes that don't index blob values aren't affected: frozen collections were already supported
            createIndex("CREATE INDEX ON %s(KEYS(m)) USING 'sai'");
            createIndex("CREATE INDEX ON %s(FULL(f)) USING 'sai'");
            createIndex("CREATE INDEX ON %s(t) USING 'sai'");

            // a node that is down gets the schema when it comes back: one whose messaging version isn't known counts
            // too, as after a restart of this node, which forgets the messaging versions
            peer.alive(false).forgetMessagingVersion();
            assertCreationRejected("b", "b", peer);

            peer.messagingVersion(MessagingService.VERSION_DS_21).releaseVersion("5.0.7.0");
            createIndex("CREATE INDEX ON %s(b) USING 'sai'");
            createIndex("CREATE CUSTOM INDEX ON %s(l) USING 'StorageAttachedIndex'");
            createIndex("CREATE INDEX ON %s(ENTRIES(m)) USING 'sai'");
        }
    }

    /**
     * new_feature_peer_version_check applies to the creation of blob indexes too: none skips the check, e.g. for a
     * deployment whose nodes never learn their peers' versions.
     */
    @Test
    public void testCreationWithNewFeaturePeerVersionCheck() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, b blob)");

        Config.NewFeaturePeerVersionCheck check = DatabaseDescriptor.getNewFeaturePeerVersionCheck();
        try (FakePeer peer = new FakePeer("127.0.0.2", false))
        {
            // a ring member that Nodes, gossip and messaging don't know
            assertCheckRejects(peer, "messaging");
            assertCreationAccepted("none");

            // a DataStax 5.0 node before 5.0.10, and an Apache Cassandra node
            for (int version : new int[]{ MessagingService.VERSION_DS_20, MessagingService.VERSION_50 })
            {
                peer.messagingVersion(version).alive(true);
                assertCheckRejects(peer, "messaging");
                assertCreationAccepted("none");
            }
        }
        finally
        {
            DatabaseDescriptor.setNewFeaturePeerVersionCheck(check);
        }
    }

    private void assertCheckRejects(FakePeer peer, String check)
    {
        StorageService.instance.setNewFeaturePeerVersionCheck(check);
        assertCreationRejected("b", "b", peer);
    }

    private void assertCreationAccepted(String check)
    {
        StorageService.instance.setNewFeaturePeerVersionCheck(check);
        createIndex("CREATE INDEX blob_idx ON %s(b) USING 'sai'");
        dropIndex("DROP INDEX %s.blob_idx");
    }

    private void assertCreationRejected(String target, String column, FakePeer peer)
    {
        assertThatThrownBy(() -> execute("CREATE INDEX ON %s(" + target + ") USING 'sai'"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(String.format(StorageAttachedIndex.BLOB_INDEX_UNSUPPORTED_BY_NODES_MESSAGE,
                                          column, Collections.singleton(peer.address)));
    }
}
