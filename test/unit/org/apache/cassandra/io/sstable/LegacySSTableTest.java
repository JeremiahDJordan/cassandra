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
package org.apache.cassandra.io.sstable;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.crypto.IKeyProvider;
import org.apache.cassandra.crypto.IKeyProviderFactory;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.SinglePartitionSliceCommandTest;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.repair.PendingAntiCompaction;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.StatsComponent;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.sstable.keycache.KeyCacheSupport;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.net.EndpointMessagingVersions;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.async.NettyStreamingConnectionFactory;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.StorageCompatibilityMode;
import org.apache.cassandra.utils.TimeUUID;
import org.assertj.core.api.SoftAssertions;

import static java.util.Collections.singleton;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_LEGACY_SSTABLE_ROOT;
import static org.apache.cassandra.io.sstable.format.AbstractTestVersionSupportedFeatures.ALL_VERSIONS;
import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;
import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests backwards compatibility for SSTables
 */
public class LegacySSTableTest
{
    private static final Logger logger = LoggerFactory.getLogger(LegacySSTableTest.class);

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    public static File LEGACY_SSTABLE_ROOT;

    private static final String LEGACY_TABLES_KEYSPACE = "legacy_tables";

    /**
     * When adding a new sstable version, add that one here.
     * See {@link #testGenerateSstables()} to generate sstables.
     * Take care on commit as you need to add the sstable files using {@code git add -f}
     *
     * There are two me sstables, where the sequence number indicates the C* version they come from.
     * For example:
     *     me-3025-big-* sstables are generated from 3.0.25
     *     me-31111-big-* sstables are generated from 3.11.11
     * Both exist because of differences introduced in 3.6 (and 3.11) in how frozen multi-cell headers are serialised
     *  without the sstable format `me` being bumped, ref CASSANDRA-15035
     *
     * Sequence numbers represent the C* version used when creating the SSTable, i.e. with #testGenerateSstables()
     */
    public static String[] legacyVersions = null;
    private static int noSpamLoggerIntervalsSkipped;

    // Get all versions the node reads, which depends on the storage compatibility mode (C18301)
    private static String[] getValidLegacyVersions()
    {
        return getShippedLegacyVersions().stream()
                                         .filter(v -> getLegacyFormat(v).getVersion(v).isCompatible())
                                         .toArray(String[]::new);
    }

    private static List<String> getShippedLegacyVersions()
    {
        return ALL_VERSIONS.stream()
                           .filter(v -> new File(LEGACY_SSTABLE_ROOT, v + "/" + LEGACY_TABLES_KEYSPACE).isDirectory())
                           .collect(Collectors.toList());
    }

    // the shipped versions of a later major version than the releases of storage compatibility mode HCD_1 read in
    // their format: big nb, bti cc
    private static List<String> getNewerLegacyVersions()
    {
        return getShippedLegacyVersions().stream()
                                         .filter(v -> v.charAt(0) > (BigFormat.is(getLegacyFormat(v)) ? BigFormat.STORAGE_COMPATIBILITY_4_VERSION : BtiFormat.STORAGE_COMPATIBILITY_HCD_1_VERSION).charAt(0))
                                         .collect(Collectors.toList());
    }

    // the legacy sstables of versions aa to da are bti sstables, those of versions ma to oa big sstables
    private static SSTableFormat<?, ?> getLegacyFormat(String legacyVersion)
    {
        return legacyVersion.compareTo("m") < 0 ? BtiFormat.getInstance() : BigFormat.getInstance();
    }

    // 1200 chars
    static final String longString = StringUtils.repeat("0123456789", 120);

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        String scp = TEST_LEGACY_SSTABLE_ROOT.getString();
        Assert.assertNotNull("System property " + TEST_LEGACY_SSTABLE_ROOT.getKey() + " not set", scp);

        LEGACY_SSTABLE_ROOT = new File(scp).toAbsolute();
        Assert.assertTrue("System property " + LEGACY_SSTABLE_ROOT + " does not specify a directory", LEGACY_SSTABLE_ROOT.isDirectory());

        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
        Keyspace.setInitialized();
        createKeyspace();

        legacyVersions = getValidLegacyVersions();
        for (String legacyVersion : legacyVersions)
        {
            createTables(legacyVersion);
        }
    }

    @After
    public void tearDown()
    {
        for (String legacyVersion : legacyVersions)
        {
            truncateLegacyTables(legacyVersion);
        }
        truncateLegacyEncryptedTables();
    }

    /**
     * Get a descriptor for the legacy sstable at the given version.
     */
    protected Descriptor getDescriptor(File dir) throws IOException
    {
        Path file = Files.list(dir.toPath())
                .findFirst()
                .orElseThrow(() -> new RuntimeException(String.format("No files for path=%s", dir.absolutePath())));

        // ignore intentionally empty directory .keep files
        return ".keep".equals(file.toFile().getName()) ? null : Descriptor.fromFilename(new File(file));
    }

    @Test
    public void testLoadLegacyCqlTables()
    {
        DatabaseDescriptor.setColumnIndexCacheSizeInKiB(99999);
        CacheService.instance.invalidateKeyCache();
        doTestLegacyCqlTables();
    }

    @Test
    public void testLoadLegacyCqlTablesShallow()
    {
        DatabaseDescriptor.setColumnIndexCacheSizeInKiB(0);
        CacheService.instance.invalidateKeyCache();
        doTestLegacyCqlTables();
    }

    @Test
    public void testMutateMetadata()
    {
        SoftAssertions assertions = new SoftAssertions();
        // we need to make sure we write old version metadata in the format for that version
        for (String legacyVersion : legacyVersions)
            assertions.assertThatCode(() -> {
                logger.info("Loading legacy version: {}", legacyVersion);
                truncateLegacyTables(legacyVersion);
                loadLegacyTables(legacyVersion);
                CacheService.instance.invalidateKeyCache();

                for (ColumnFamilyStore cfs : Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStores())
                {
                    for (SSTableReader sstable : cfs.getLiveSSTables())
                    {
                        sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, 1234, NO_PENDING_REPAIR, false);
                        sstable.reloadSSTableMetadata();
                        assertEquals(1234, sstable.getRepairedAt());
                        if (sstable.descriptor.version.hasPendingRepair())
                            assertEquals(NO_PENDING_REPAIR, sstable.getPendingRepair());
                    }

                    boolean isTransient = false;
                    for (SSTableReader sstable : cfs.getLiveSSTables())
                    {
                        TimeUUID random = nextTimeUUID();
                        sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, UNREPAIRED_SSTABLE, random, isTransient);
                        sstable.reloadSSTableMetadata();
                        assertEquals(UNREPAIRED_SSTABLE, sstable.getRepairedAt());
                        if (sstable.descriptor.version.hasPendingRepair())
                            assertEquals(random, sstable.getPendingRepair());
                        if (sstable.descriptor.version.hasIsTransient())
                            assertEquals(isTransient, sstable.isTransient());

                        isTransient = !isTransient;
                    }
                }
            }).describedAs(legacyVersion).doesNotThrowAnyException();
        assertions.assertAll();
    }

    @Test
    public void testMutateMetadataCSM()
    {
        SoftAssertions assertions = new SoftAssertions();
        // we need to make sure we write old version metadata in the format for that version
        for (String legacyVersion : legacyVersions)
            assertions.assertThatCode(() -> {
                truncateLegacyTables(legacyVersion);
                loadLegacyTables(legacyVersion);

                for (ColumnFamilyStore cfs : Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStores())
                {
                    // set pending
                    for (SSTableReader sstable : cfs.getLiveSSTables())
                    {
                        TimeUUID random = nextTimeUUID();
                        try
                        {
                            cfs.mutateRepaired(Collections.singleton(sstable), UNREPAIRED_SSTABLE, random, false);
                            if (!sstable.descriptor.version.hasPendingRepair())
                                fail("We should fail setting pending repair on unsupported sstables " + sstable);
                        }
                        catch (IllegalStateException e)
                        {
                            if (sstable.descriptor.version.hasPendingRepair())
                                fail("We should succeed setting pending repair on " + legacyVersion + " sstables, failed on " + sstable);
                        }
                    }
                    // set transient
                    for (SSTableReader sstable : cfs.getLiveSSTables())
                    {
                        try
                        {
                            cfs.mutateRepaired(Collections.singleton(sstable), UNREPAIRED_SSTABLE, nextTimeUUID(), true);
                            if (!sstable.descriptor.version.hasIsTransient())
                                fail("We should fail setting pending repair on unsupported sstables " + sstable);
                        }
                        catch (IllegalStateException e)
                        {
                            if (sstable.descriptor.version.hasIsTransient())
                                fail("We should succeed setting pending repair on " + legacyVersion + " sstables, failed on " + sstable);
                        }
                    }
                }
            }).describedAs(legacyVersion).doesNotThrowAnyException();
        assertions.assertAll();
    }

    @Test
    public void testMutateLevel()
    {
        // we need to make sure we write old version metadata in the format for that version
        SoftAssertions assertions = new SoftAssertions();
        for (String legacyVersion : legacyVersions)
            assertions.assertThatCode(() -> {
                logger.info("Loading legacy version: {}", legacyVersion);
                truncateLegacyTables(legacyVersion);
                loadLegacyTables(legacyVersion);
                CacheService.instance.invalidateKeyCache();

                for (ColumnFamilyStore cfs : Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStores())
                {
                    for (SSTableReader sstable : cfs.getLiveSSTables())
                    {
                        sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 1234);
                        sstable.reloadSSTableMetadata();
                        assertEquals(1234, sstable.getSSTableLevel());
                    }
                }
            }).describedAs(legacyVersion).doesNotThrowAnyException();
        assertions.assertAll();
    }

    private void doTestLegacyCqlTables()
    {
        for (String legacyVersion : legacyVersions)
        {
            // the versions before cc have no clust_be_index_summary table
            if (legacyVersion.compareTo("cc") >= 0)
            {
                logger.info("Loading legacy version: {}", legacyVersion);
                truncateLegacyTables(legacyVersion);
                loadLegacyTables(legacyVersion);
                CacheService.instance.invalidateKeyCache();
                long startCount = CacheService.instance.keyCache.size();
                verifyReads(legacyVersion);
                if (Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion)).getLiveSSTables().stream().anyMatch(sstr -> BigFormat.is(sstr.descriptor.getFormat())))
                    verifyCache(legacyVersion, startCount);
                compactLegacyTables(legacyVersion);
                verifyReads(legacyVersion);
            }
        }
    }

    @Test
    public void testStreamLegacyCqlTables()
    {
        SoftAssertions assertions = new SoftAssertions();

        for (String legacyVersion : legacyVersions)
            if (!legacyVersion.equals("ca") && !legacyVersion.equals("cb") && !legacyVersion.startsWith("a") && !legacyVersion.startsWith("b"))
                assertions.assertThatCode(() -> {
                    streamLegacyTables(legacyVersion);
                    verifyReads(legacyVersion);
                }).describedAs(legacyVersion).doesNotThrowAnyException();

        assertions.assertAll();
    }

    /**
     * Reads encrypted DSE 6.8 sstables ({@code bb}, trie-indexed, encrypted with the {@code Encryptor} compressor), or
     * {@code bb} sstables never upgraded on an HCD 1.2 node, read by HCD 2.0 (HCD 1.2 itself writes {@code cc}).
     * {@code bb} encrypts the data file, the partition and row indexes and the metadata (Statistics.db).
     * <p>
     * The fixtures were written with {@link KeyProviderFactoryStub}, whose class name is stored in their
     * CompressionInfo.db, so that nested class must keep its name. {@code legacy_encrypted_table_pk} has 5 partitions
     * {@code "0"}..{@code "4"}; {@code legacy_encrypted_table_pk_ck} has the same 5 partitions with 50 rows each,
     * clustering {@code i + longString}; every {@code val} is {@code "foo bar baz"}.
     * <p>
     * Streaming of these sstables is not covered, like for all the other {@code a*}/{@code b*} versions
     * (see {@link #testStreamLegacyCqlTables()}).
     */
    @Test
    public void testEncryptedTables() throws Exception
    {
        createLegacyEncryptedTables();

        // read the legacy sstables as they are
        for (String table : LEGACY_ENCRYPTED_TABLES)
            loadLegacyTableByName(LEGACY_ENCRYPTED_VERSION, table);
        verifyLegacyEncryptedReads();
        for (String table : LEGACY_ENCRYPTED_TABLES)
        {
            ColumnFamilyStore cfs = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(table);
            assertThat(cfs.getLiveSSTables()).isNotEmpty();
            for (SSTableReader sstable : cfs.getLiveSSTables())
                verifyLegacyEncryptedSSTable(sstable, table.endsWith("_ck"));
        }

        // Compaction and upgradesstables must turn the legacy sstables into sstables of another version: when bb is the
        // current BTI version (-Dcassandra.trie_index_format_version=bb, e.g. the sai-legacy test targets), flushes
        // write bb and upgradesstables, which compares with the latest BTI version, skips them.
        Assume.assumeFalse("bb is the current BTI version, nothing to upgrade the legacy sstables to",
                           LEGACY_ENCRYPTED_VERSION.equals(BtiFormat.getInstance().getLatestVersion().version));

        // major compaction over two copies of the legacy sstables plus a new sstable, into the current version
        truncateLegacyEncryptedTables();
        for (String table : LEGACY_ENCRYPTED_TABLES)
        {
            loadLegacyTableByName(LEGACY_ENCRYPTED_VERSION, table);
            loadLegacyTableByName(LEGACY_ENCRYPTED_VERSION, table);
            insertEncryptedSensitivePartition(table);
            ColumnFamilyStore cfs = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(table);
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            assertThat(cfs.getLiveSSTables().stream().filter(s -> s.descriptor.version.version.equals(LEGACY_ENCRYPTED_VERSION)).count()).isGreaterThanOrEqualTo(2);
            assertThat(cfs.getLiveSSTables().stream().filter(s -> s.descriptor.version.isLatestVersion()).count()).isEqualTo(1);
        }
        // the reads merge two overlapping legacy sstables and one of the current version
        verifyLegacyEncryptedReads();
        verifyEncryptedSensitivePartitionReads();
        for (String table : LEGACY_ENCRYPTED_TABLES)
        {
            ColumnFamilyStore cfs = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(table);
            cfs.forceMajorCompaction();
            // a single output sstable with one data directory and one UCS shard, as configured by every test yaml
            assertThat(cfs.getLiveSSTables()).hasSize(1);
            verifyUpgradedEncryptedSSTable(Iterables.getOnlyElement(cfs.getLiveSSTables()), true);
        }
        verifyLegacyEncryptedReads();
        verifyEncryptedSensitivePartitionReads();

        // the equivalent of nodetool upgradesstables, which only rewrites sstables older than the current version
        truncateLegacyEncryptedTables();
        for (String table : LEGACY_ENCRYPTED_TABLES)
        {
            loadLegacyTableByName(LEGACY_ENCRYPTED_VERSION, table);
            ColumnFamilyStore cfs = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(table);
            Set<Descriptor> before = cfs.getLiveSSTables().stream().map(s -> s.descriptor).collect(Collectors.toSet());
            assertThat(cfs.sstablesRewrite(true, Long.MAX_VALUE, false, 1)).isEqualTo(CompactionManager.AllSSTableOpStatus.SUCCESSFUL);
            // one output sstable per input with one data directory and one UCS shard, as configured by every test yaml
            assertThat(cfs.getLiveSSTables()).hasSameSizeAs(before);
            for (SSTableReader sstable : cfs.getLiveSSTables())
            {
                assertThat(before).describedAs("%s was not rewritten", sstable.descriptor).doesNotContain(sstable.descriptor);
                verifyUpgradedEncryptedSSTable(sstable, false);
            }
        }
        verifyLegacyEncryptedReads();
    }

    private static final String[] LEGACY_ENCRYPTED_TABLES = { "legacy_encrypted_table_pk", "legacy_encrypted_table_pk_ck" };
    private static final String LEGACY_ENCRYPTED_VERSION = "bb";
    private static final String LEGACY_ENCRYPTED_VALUE = "foo bar baz";
    // sorts after the partition keys of the fixtures with the order preserving partitioner of the tests, so it is
    // stored in full as the last key of the partition index; it gets a row index in the _ck table, as its partition
    // (~60KiB) is larger than column_index_size
    private static final String SENSITIVE_KEY = "zz-sensitive-partition-key-of-an-encrypted-table";
    private static final String SENSITIVE_VALUE = "sensitive-value-of-an-encrypted-table";
    // deterministic timestamps of the fixtures, as written by DSE
    private static final long LEGACY_ENCRYPTED_PK_MIN_TIMESTAMP = 1744786768950000L;
    private static final long LEGACY_ENCRYPTED_PK_MAX_TIMESTAMP = 1744786769128001L;
    private static final long LEGACY_ENCRYPTED_PK_CK_MIN_TIMESTAMP = 1744786768953000L;
    private static final long LEGACY_ENCRYPTED_PK_CK_MAX_TIMESTAMP = 1744786769162000L;

    private static void createLegacyEncryptedTables()
    {
        QueryProcessor.executeInternal(String.format("CREATE TABLE IF NOT EXISTS %s.legacy_encrypted_table_pk (pk text PRIMARY KEY, val text) %s",
                                                     LEGACY_TABLES_KEYSPACE, localSystemKeyEncryptionCompressionSuffix("Encryptor")));
        QueryProcessor.executeInternal(String.format("CREATE TABLE IF NOT EXISTS %s.legacy_encrypted_table_pk_ck (pk text, ck text, val text, PRIMARY KEY (pk, ck)) %s",
                                                     LEGACY_TABLES_KEYSPACE, localSystemKeyEncryptionCompressionSuffix("Encryptor")));
    }

    // the same options as stored in the CompressionInfo.db of the fixtures
    private static String localSystemKeyEncryptionCompressionSuffix(String className)
    {
        return String.format(" WITH compression = " +
                             "{'class' : '%s', " +
                             "'cipher_algorithm' : 'AES/ECB/PKCS5Padding', " +
                             "'secret_key_strength' : 128, " +
                             "'key_provider' : '%s'}", className, KeyProviderFactoryStub.class.getName());
    }

    public static class KeyProviderFactoryStub implements IKeyProviderFactory
    {
        @Override
        public IKeyProvider getKeyProvider(Map<String, String> options)
        {
            return new KeyProviderStub();
        }

        @Override
        public Set<String> supportedOptions()
        {
            return Collections.emptySet();
        }
    }

    public static class KeyProviderStub implements IKeyProvider
    {
        @Override
        public SecretKey getSecretKey(String cipherName, int keyStrength)
        {
            byte[] bytes = new byte[keyStrength / 8];
            Arrays.fill(bytes, (byte) 6);
            return new SecretKeySpec(bytes, cipherName.replaceAll("/.*", ""));
        }
    }

    // does nothing if testEncryptedTables did not run (the tables do not exist)
    private static void truncateLegacyEncryptedTables()
    {
        for (String table : LEGACY_ENCRYPTED_TABLES)
        {
            if (Schema.instance.getTableMetadata(LEGACY_TABLES_KEYSPACE, table) != null)
                Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(table).truncateBlocking();
        }
        // loading from the data directory must not list the truncated sstables while they are deleted
        LifecycleTransaction.waitForDeletions();
        CacheService.instance.invalidateKeyCache();
    }

    private static List<String> legacyEncryptedClusterings()
    {
        List<String> clusterings = new ArrayList<>();
        for (int ck = 0; ck < 50; ck++)
            clusterings.add(ck + longString);
        Collections.sort(clusterings); // UTF8Type sorts ASCII strings like String does
        return clusterings;
    }

    private static void verifyLegacyEncryptedReads()
    {
        CacheService.instance.invalidateKeyCache();
        List<String> clusterings = legacyEncryptedClusterings();

        // full scans, in the order of the order preserving partitioner of the tests
        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * FROM legacy_tables.legacy_encrypted_table_pk");
        List<String> keys = new ArrayList<>();
        for (UntypedResultSet.Row row : rs)
        {
            if (SENSITIVE_KEY.equals(row.getString("pk")))
                continue;
            keys.add(row.getString("pk"));
            assertEquals(LEGACY_ENCRYPTED_VALUE, row.getString("val"));
        }
        assertThat(keys).containsExactly("0", "1", "2", "3", "4");

        rs = QueryProcessor.executeInternal("SELECT * FROM legacy_tables.legacy_encrypted_table_pk_ck");
        List<String> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : rs)
        {
            if (SENSITIVE_KEY.equals(row.getString("pk")))
                continue;
            rows.add(row.getString("pk") + ':' + row.getString("ck"));
            assertEquals(LEGACY_ENCRYPTED_VALUE, row.getString("val"));
        }
        List<String> expectedRows = new ArrayList<>();
        for (int pk = 0; pk < 5; pk++)
            for (String ck : clusterings)
                expectedRows.add(pk + ":" + ck);
        assertThat(rows).containsExactlyElementsOf(expectedRows);

        for (int pk = 0; pk < 5; pk++)
        {
            String pkValue = Integer.toString(pk);

            // point reads by partition key
            rs = QueryProcessor.executeInternal("SELECT val FROM legacy_tables.legacy_encrypted_table_pk WHERE pk = ?", pkValue);
            assertEquals(1, rs.size());
            assertEquals(LEGACY_ENCRYPTED_VALUE, rs.one().getString("val"));

            rs = QueryProcessor.executeInternal("SELECT ck FROM legacy_tables.legacy_encrypted_table_pk_ck WHERE pk = ?", pkValue);
            List<String> partition = new ArrayList<>();
            rs.forEach(row -> partition.add(row.getString("ck")));
            assertThat(partition).isEqualTo(clusterings);

            // point read of a row and slices, which go through the row index (each partition, ~60KiB, is larger
            // than column_index_size)
            for (int ck : new int[]{ 0, 4, 25, 49 })
            {
                String ckValue = ck + longString;
                rs = QueryProcessor.executeInternal("SELECT val FROM legacy_tables.legacy_encrypted_table_pk_ck WHERE pk = ? AND ck = ?", pkValue, ckValue);
                assertEquals(1, rs.size());
                assertEquals(LEGACY_ENCRYPTED_VALUE, rs.one().getString("val"));

                rs = QueryProcessor.executeInternal("SELECT ck, val FROM legacy_tables.legacy_encrypted_table_pk_ck WHERE pk = ? AND ck >= ?", pkValue, ckValue);
                List<String> slice = new ArrayList<>();
                rs.forEach(row -> {
                    slice.add(row.getString("ck"));
                    assertEquals(LEGACY_ENCRYPTED_VALUE, row.getString("val"));
                });
                assertThat(slice).isEqualTo(clusterings.stream().filter(c -> c.compareTo(ckValue) >= 0).collect(Collectors.toList()))
                                 .isNotEmpty();

                rs = QueryProcessor.executeInternal("SELECT ck, val FROM legacy_tables.legacy_encrypted_table_pk_ck WHERE pk = ? AND ck < ? ORDER BY ck DESC", pkValue, ckValue);
                List<String> reversedSlice = new ArrayList<>();
                rs.forEach(row -> {
                    reversedSlice.add(row.getString("ck"));
                    assertEquals(LEGACY_ENCRYPTED_VALUE, row.getString("val"));
                });
                List<String> expectedReversedSlice = clusterings.stream().filter(c -> c.compareTo(ckValue) < 0).collect(Collectors.toList());
                Collections.reverse(expectedReversedSlice);
                assertThat(reversedSlice).isEqualTo(expectedReversedSlice);
            }
        }

        // no sstable may have been marked suspect by any of the reads
        for (String table : LEGACY_ENCRYPTED_TABLES)
        {
            for (SSTableReader sstable : Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(table).getLiveSSTables())
                assertThat(sstable.isMarkedSuspect()).describedAs(sstable.toString()).isFalse();
        }
    }

    private static void insertEncryptedSensitivePartition(String table)
    {
        if (table.endsWith("_ck"))
        {
            for (int ck = 0; ck < 50; ck++)
                QueryProcessor.executeInternal("INSERT INTO legacy_tables.legacy_encrypted_table_pk_ck (pk, ck, val) VALUES (?, ?, ?)",
                                               SENSITIVE_KEY, ck + longString, SENSITIVE_VALUE);
        }
        else
        {
            QueryProcessor.executeInternal("INSERT INTO legacy_tables.legacy_encrypted_table_pk (pk, val) VALUES (?, ?)",
                                           SENSITIVE_KEY, SENSITIVE_VALUE);
        }
    }

    private static void verifyEncryptedSensitivePartitionReads()
    {
        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT val FROM legacy_tables.legacy_encrypted_table_pk WHERE pk = ?", SENSITIVE_KEY);
        assertEquals(1, rs.size());
        assertEquals(SENSITIVE_VALUE, rs.one().getString("val"));

        rs = QueryProcessor.executeInternal("SELECT ck, val FROM legacy_tables.legacy_encrypted_table_pk_ck WHERE pk = ? AND ck >= ?", SENSITIVE_KEY, "4" + longString);
        List<String> slice = new ArrayList<>();
        rs.forEach(row -> {
            slice.add(row.getString("ck"));
            assertEquals(SENSITIVE_VALUE, row.getString("val"));
        });
        assertThat(slice).isEqualTo(legacyEncryptedClusterings().stream().filter(c -> c.compareTo("4" + longString) >= 0).collect(Collectors.toList()));
    }

    /**
     * Byte sequences always present in the plaintext form of the metadata (Statistics.db) of any sstable of the
     * encrypted tables: the partitioner class name (validation metadata) and the key type (serialization header).
     */
    private static List<byte[]> plaintextMetadataMarkers(SSTableReader sstable)
    {
        return Arrays.asList(utf8(sstable.getPartitioner().getClass().getCanonicalName()),
                             utf8(UTF8Type.class.getName()));
    }

    /**
     * {@code key} as written by {@link ByteBufferUtil#writeWithShortLength}, the form of the keys in the partition
     * index footer (first and last key, see {@code PartitionIndexBuilder.complete}) and in the row index (the key
     * precedes the entry of each indexed partition, see {@code BtiTableWriter.IndexWriter.append}).
     */
    private static byte[] withShortLength(String... keys)
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            for (String key : keys)
                ByteBufferUtil.writeWithShortLength(ByteBufferUtil.bytes(key), out);
            return out.toByteArray();
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    private static byte[] utf8(String s)
    {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Checks a {@code bb} sstable as loaded from the fixtures.
     * <p>
     * The sstablemetadata tool ({@link org.apache.cassandra.tools.SSTableMetadataViewer}) is not invoked here, because
     * its static initialisation runs {@code DatabaseDescriptor.toolInitialization()}, which asserts when the daemon
     * is already initialised in this JVM. The code path it uses on these sstables is exercised instead: the
     * deserialization of the (encrypted) Statistics.db through the metadata serializer, and
     * {@code readKeyRange} for the first and last keys, which {@code bb} does not store in its metadata.
     * The jvm-dtest {@code SSTableEncryptionTest} runs the real tool on encrypted sstables of the current version.
     */
    private static void verifyLegacyEncryptedSSTable(SSTableReader sstable, boolean hasClustering) throws IOException
    {
        Descriptor descriptor = sstable.descriptor;
        assertEquals(LEGACY_ENCRYPTED_VERSION, descriptor.version.version);
        assertEquals(BtiFormat.NAME, descriptor.getFormat().name());
        assertThat(descriptor.version.indicesAreEncrypted()).isTrue();
        assertThat(descriptor.version.metadataIsEncrypted()).isTrue();
        assertThat(descriptor.version.hasKeyRange()).isFalse();
        assertThat(sstable.getCompressionMetadata().compressor().encryptionOnly()).isNotNull();
        assertThat(sstable.isMarkedSuspect()).isFalse();

        IPartitioner partitioner = sstable.getPartitioner();
        Pair<DecoratedKey, DecoratedKey> keyRange = descriptor.getFormat().getReaderFactory().readKeyRange(descriptor, partitioner);
        assertThat(keyRange).isNotNull();
        assertEquals(sstable.getFirst(), keyRange.left);
        assertEquals(sstable.getLast(), keyRange.right);
        // the order preserving partitioner of the tests
        assertEquals("0", UTF8Type.instance.compose(keyRange.left.getKey()));
        assertEquals("4", UTF8Type.instance.compose(keyRange.right.getKey()));

        // a fresh deserialization, independent from the one done when the reader was opened
        StatsComponent statsComponent = StatsComponent.load(descriptor, MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER);
        ValidationMetadata validation = statsComponent.validationMetadata();
        assertEquals(partitioner.getClass().getCanonicalName(), validation.partitioner);
        assertEquals(0.01, validation.bloomFilterFPChance, 0.0);

        StatsMetadata stats = statsComponent.statsMetadata();
        assertEquals(5, stats.estimatedPartitionSize.count());
        assertEquals(hasClustering ? 250 : 5, stats.totalRows);
        assertEquals(hasClustering ? LEGACY_ENCRYPTED_PK_CK_MIN_TIMESTAMP : LEGACY_ENCRYPTED_PK_MIN_TIMESTAMP, stats.minTimestamp);
        assertEquals(hasClustering ? LEGACY_ENCRYPTED_PK_CK_MAX_TIMESTAMP : LEGACY_ENCRYPTED_PK_MAX_TIMESTAMP, stats.maxTimestamp);
        assertEquals(stats.minTimestamp, sstable.getSSTableMetadata().minTimestamp);
        assertEquals(stats.maxTimestamp, sstable.getSSTableMetadata().maxTimestamp);
        assertThat(stats.firstKey).isNull();
        assertThat(stats.lastKey).isNull();

        SerializationHeader.Component header = statsComponent.serializationHeader();
        assertEquals(UTF8Type.instance, header.getKeyType());
        assertEquals(hasClustering ? Collections.singletonList(UTF8Type.instance) : Collections.emptyList(), header.getClusteringTypes());
        assertThat(header.getRegularColumns()).containsOnlyKeys(ByteBufferUtil.bytes("val"));
        assertThat(header.getStaticColumns()).isEmpty();

        // The rows returned by the queries contain these values, the files do not.
        assertAbsent(descriptor.fileFor(SSTableFormat.Components.DATA), Arrays.asList(utf8(LEGACY_ENCRYPTED_VALUE), utf8(longString.substring(0, 100))));
        // The plaintext partition index ends with the first and last keys, "0" and "4", written with their length.
        assertAbsent(descriptor.fileFor(BtiFormat.Components.PARTITION_INDEX), Collections.singletonList(withShortLength("0", "4")));
        // The plaintext row index holds the key of each indexed partition, written with its length, before the entry of
        // the partition (see PartitionIterator.readNext); all partitions of the _ck table are indexed, the ones of the
        // _pk table are not (its row index is empty).
        File rowIndex = descriptor.fileFor(BtiFormat.Components.ROW_INDEX);
        if (hasClustering)
        {
            assertThat(rowIndex.length()).isGreaterThan(0);
            assertAbsent(rowIndex, Arrays.asList(withShortLength("0"), withShortLength("1"), withShortLength("2"), withShortLength("3"), withShortLength("4")));
        }
        else
        {
            assertEquals(0, rowIndex.length());
        }
        assertAbsent(descriptor.fileFor(SSTableFormat.Components.STATS), plaintextMetadataMarkers(sstable));
    }

    private static void verifyUpgradedEncryptedSSTable(SSTableReader sstable, boolean hasSensitivePartition)
    {
        Descriptor descriptor = sstable.descriptor;
        assertThat(descriptor.version.isLatestVersion()).describedAs(descriptor.toString()).isTrue();
        assertEquals(DatabaseDescriptor.getSelectedSSTableFormat().name(), descriptor.getFormat().name());
        assertThat(sstable.getCompressionMetadata().compressor().encryptionOnly()).isNotNull();
        assertThat(sstable.getCompressionMetadata().parameters.asMap()).containsEntry("key_provider", KeyProviderFactoryStub.class.getName());

        List<byte[]> plaintext = new ArrayList<>(Arrays.asList(utf8(LEGACY_ENCRYPTED_VALUE), utf8(longString.substring(0, 100))));
        if (hasSensitivePartition)
        {
            plaintext.add(utf8(SENSITIVE_KEY));
            plaintext.add(utf8(SENSITIVE_VALUE));
            // the sensitive key is the last key, stored in full in the plaintext partition index; the sensitive
            // partition of the _ck table is indexed, so its key is in the plaintext row index too
            assertEquals(SENSITIVE_KEY, UTF8Type.instance.compose(sstable.getLast().getKey()));
        }
        else
        {
            // the first and last keys, "0" and "4", are in the footer of the plaintext partition index
            assertEquals("0", UTF8Type.instance.compose(sstable.getFirst().getKey()));
            assertEquals("4", UTF8Type.instance.compose(sstable.getLast().getKey()));
        }
        assertAbsent(descriptor.fileFor(SSTableFormat.Components.DATA), plaintext);
        // The guards below are for the current version of the big format, which encrypts neither its indexes nor its
        // metadata; note that no CI configuration selects the big format with these tests.
        if (descriptor.version.indicesAreEncrypted())
        {
            for (Component component : sstable.components())
            {
                if (component.equals(BtiFormat.Components.PARTITION_INDEX) || component.equals(BtiFormat.Components.ROW_INDEX))
                    assertAbsent(descriptor.fileFor(component), plaintext);
                if (component.equals(BtiFormat.Components.PARTITION_INDEX) && !hasSensitivePartition)
                    assertAbsent(descriptor.fileFor(component), Collections.singletonList(withShortLength("0", "4")));
            }
        }
        if (descriptor.version.metadataIsEncrypted())
            assertAbsent(descriptor.fileFor(SSTableFormat.Components.STATS), plaintextMetadataMarkers(sstable));
    }

    private static void assertAbsent(File file, List<byte[]> plaintext)
    {
        assertThat(file.exists()).describedAs(file.toString()).isTrue();
        byte[] bytes;
        try
        {
            bytes = Files.readAllBytes(file.toPath());
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
        for (byte[] value : plaintext)
            assertThat(Bytes.indexOf(bytes, value)).describedAs("0x%s in %s", ByteBufferUtil.bytesToHex(ByteBuffer.wrap(value)), file).isEqualTo(-1);
    }

    @Test
    public void testReadableVersions()
    {
        StorageCompatibilityMode mode = DatabaseDescriptor.getStorageCompatibilityMode();
        Assume.assumeFalse("CASSANDRA_4 doesn't read big oa", mode == StorageCompatibilityMode.CASSANDRA_4);
        assertThat(legacyVersions).containsExactlyElementsOf(getShippedLegacyVersions());

        try
        {
            for (String legacyVersion : getNewerLegacyVersions())
            {
                File file = getTestDataTableDir(legacyVersion, String.format("legacy_%s_simple", legacyVersion)).tryList()[0];
                DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.CASSANDRA_4);
                if (BigFormat.is(getLegacyFormat(legacyVersion)))
                    assertThatThrownBy(() -> Descriptor.fromFilename(file))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("is newer than storage_compatibility_mode CASSANDRA_4");
                else
                    assertThat(Descriptor.fromFilename(file).version.isNewerThanStorageCompatibilityMode()).describedAs(legacyVersion).isFalse();
                DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
                assertThat(Descriptor.fromFilename(file).version.isNewerThanStorageCompatibilityMode()).describedAs(legacyVersion).isTrue();
                DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.UPGRADING);
                assertThat(Descriptor.fromFilename(file).version.isNewerThanStorageCompatibilityMode()).describedAs(legacyVersion).isFalse();
            }
        }
        finally
        {
            DatabaseDescriptor.setStorageCompatibilityMode(mode);
        }
    }

    /**
     * HCD_1 reads the {@code oa} and {@code da} sstables, warns that a downgrade isn't possible while they exist when
     * they are added, and {@code nodetool upgradesstables -a} rewrites them in versions it writes.
     */
    @Test
    public void testRewriteNewerVersionsForStorageCompatibilityMode() throws Exception
    {
        Assume.assumeTrue("HCD_1 reads the versions newer than those it writes", DatabaseDescriptor.getStorageCompatibilityMode() == StorageCompatibilityMode.HCD_1);

        skipNoSpamLoggerInterval();
        ListAppender<ILoggingEvent> warnings = new ListAppender<>();
        warnings.start();
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Tracker.class)).addAppender(warnings);
        List<String> newerVersions = getNewerLegacyVersions();
        try
        {
            for (String legacyVersion : newerVersions)
                loadLegacyTables(legacyVersion);
        }
        finally
        {
            ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Tracker.class)).detachAppender(warnings);
            warnings.stop();
        }
        List<ColumnFamilyStore> tables = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStores().stream()
                                                 .filter(cfs -> !cfs.getLiveSSTables().isEmpty())
                                                 .collect(Collectors.toList());
        assertThat(tables).isNotEmpty();
        for (ColumnFamilyStore cfs : tables)
        {
            for (SSTableReader sstable : cfs.getLiveSSTables())
                assertThat(sstable.descriptor.version.isNewerThanStorageCompatibilityMode()).describedAs(sstable.toString()).isTrue();
            assertThat(warnings.list).describedAs(cfs.name)
                                     .filteredOn(e -> isNewerVersionsWarning(e, cfs.name))
                                     .hasSize(1);
        }

        for (ColumnFamilyStore cfs : tables)
        {
            assertEquals(CompactionManager.AllSSTableOpStatus.SUCCESSFUL, cfs.sstablesRewrite(false, Long.MAX_VALUE, false, 1));
            for (SSTableReader sstable : cfs.getLiveSSTables())
            {
                assertThat(sstable.descriptor.version).describedAs(sstable.toString()).isEqualTo(DatabaseDescriptor.getSelectedSSTableFormat().getLatestVersion());
                assertThat(sstable.descriptor.version.isNewerThanStorageCompatibilityMode()).describedAs(sstable.toString()).isFalse();
            }
        }
        for (String legacyVersion : newerVersions)
            verifyReads(legacyVersion);
    }

    /**
     * HCD_1 warns about the newer sstables added to a table at most once an hour.
     */
    @Test
    public void testNewerVersionsWarningInterval()
    {
        Assume.assumeTrue("HCD_1 reads the versions newer than those it writes", DatabaseDescriptor.getStorageCompatibilityMode() == StorageCompatibilityMode.HCD_1);

        String table = "legacy_oa_simple";
        skipNoSpamLoggerInterval();
        ListAppender<ILoggingEvent> warnings = new ListAppender<>();
        warnings.start();
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Tracker.class)).addAppender(warnings);
        try
        {
            Predicate<ILoggingEvent> warning = e -> isNewerVersionsWarning(e, table);
            loadLegacyTable("oa", "simple");
            truncateLegacyTables("oa");
            loadLegacyTable("oa", "simple");
            assertThat(warnings.list).filteredOn(warning).hasSize(1);

            skipNoSpamLoggerInterval();
            truncateLegacyTables("oa");
            loadLegacyTable("oa", "simple");
            assertThat(warnings.list).filteredOn(warning).hasSize(2);
        }
        finally
        {
            ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Tracker.class)).detachAppender(warnings);
            warnings.stop();
        }
    }

    /**
     * A bti "da" sstable is streamed entire, by partitions or not at all, depending on the messaging version of the
     * peer.
     */
    @Test
    public void testBtiDaStreamPerPeerMessagingVersion() throws Exception
    {
        loadLegacyTable("da", "simple");
        ColumnFamilyStore cfs = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore("legacy_da_simple");
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.2");
        EndpointMessagingVersions versions = MessagingService.instance().versions;
        // an sstable of a version newer than the one written goes by partitions to every peer
        boolean entire = sstable.descriptor.version.version.compareTo(BtiFormat.getInstance().getLatestVersion().version) <= 0
                         || DatabaseDescriptor.getStorageCompatibilityModeForWrites() != StorageCompatibilityMode.HCD_1
                            && !CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION.isPresent();
        try
        {
            assertOutgoingStream(cfs, FBUtilities.getBroadcastAddressAndPort(), entire, null);
            for (int version : new int[]{ MessagingService.VERSION_50, MessagingService.VERSION_DS_21 })
            {
                versions.set(peer, version);
                assertOutgoingStream(cfs, peer, entire, null);
            }
            versions.set(peer, MessagingService.VERSION_DS_20);
            assertOutgoingStream(cfs, peer, false, null);
            for (int version : new int[]{ MessagingService.VERSION_DS_12, MessagingService.VERSION_DSE_68 })
            {
                versions.set(peer, version);
                assertOutgoingStream(cfs, peer, false, "Cannot stream " + sstable.descriptor + " to " + peer + ", whose messaging version " + version + " doesn't read it");
            }
            versions.reset(peer);
            assertOutgoingStream(cfs, peer, false, "Cannot stream " + sstable.descriptor + " to " + peer + ", whose messaging version isn't known yet");
            InetAddressAndPort scoped = InetAddressAndPort.getByName("fe80::1%1");
            assertOutgoingStream(cfs, scoped, false, "Cannot stream " + sstable.descriptor + " to " + scoped + ", whose messaging version isn't known yet");
        }
        finally
        {
            versions.reset(peer);
        }
    }

    private static void assertOutgoingStream(ColumnFamilyStore cfs, InetAddressAndPort peer, boolean entire, @Nullable String refusal)
    {
        IPartitioner p = cfs.getPartitioner();
        List<Range<Token>> ranges = Collections.singletonList(new Range<>(p.getMinimumToken(), p.getMinimumToken()));
        StreamSession session = new StreamSession(StreamOperation.OTHER, peer, new NettyStreamingConnectionFactory(), null,
                                                  MessagingService.current_version, false, 0, null, PreviewKind.NONE);
        Collection<OutgoingStream> streams = cfs.getStreamManager().createOutgoingStreams(session, RangesAtEndpoint.toDummyList(ranges),
                                                                                         NO_PENDING_REPAIR, PreviewKind.NONE);
        try
        {
            assertThat(streams).hasSize(1);
            CassandraOutgoingFile stream = (CassandraOutgoingFile) streams.iterator().next();
            assertThat(stream.computeShouldStreamEntireSSTables()).describedAs(peer.toString()).isEqualTo(entire);
            if (refusal == null)
                stream.checkCanSend();
            else
                assertThatThrownBy(stream::checkCanSend).isInstanceOf(IllegalStateException.class)
                                                        .hasMessageContaining(refusal)
                                                        .hasMessageContaining("make the sending node write bti cc");
        }
        finally
        {
            streams.forEach(OutgoingStream::finish);
        }
    }

    /**
     * An entire sstable of a version newer than the releases of HCD_1 read is received in HCD_1 with a warning.
     */
    @Test
    public void testEntireSSTableStreamOfNewerVersion() throws Exception
    {
        int streamed = 0;
        for (String legacyVersion : getNewerLegacyVersions())
        {
            // a version newer than the one written is streamed by partitions
            if (legacyVersion.compareTo(getLegacyFormat(legacyVersion).getLatestVersion().version) <= 0)
            {
                streamEntireSSTableOfNewerVersion(legacyVersion);
                streamed++;
            }
        }
        Assume.assumeTrue("every newer version is streamed by partitions", streamed > 0);
    }

    private void streamEntireSSTableOfNewerVersion(String legacyVersion) throws Exception
    {
        StorageCompatibilityMode mode = DatabaseDescriptor.getStorageCompatibilityMode();
        String table = String.format("legacy_%s_simple", legacyVersion);
        File testDataDir = new File(tempFolder.newFolder("streamed-" + legacyVersion, LEGACY_TABLES_KEYSPACE, table));
        copySstablesToTestData(legacyVersion, table, testDataDir);
        SSTableReader sstable = SSTableReader.open(null, getDescriptor(testDataDir));
        skipNoSpamLoggerInterval();
        ListAppender<ILoggingEvent> warnings = new ListAppender<>();
        warnings.start();
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Tracker.class)).addAppender(warnings);
        try
        {
            IPartitioner p = sstable.getPartitioner();
            List<Range<Token>> ranges = Collections.singletonList(new Range<>(p.getMinimumToken(), p.getMinimumToken()));
            CassandraOutgoingFile stream = new CassandraOutgoingFile(StreamOperation.OTHER,
                                                                     sstable.ref(),
                                                                     sstable.getPositionsForRanges(ranges),
                                                                     ranges,
                                                                     sstable.estimatedKeysForRanges(ranges));
            assertThat(stream.computeShouldStreamEntireSSTables()).isTrue();

            DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
            new StreamPlan(StreamOperation.OTHER).transferStreams(FBUtilities.getBroadcastAddressAndPort(), Collections.singletonList(stream))
                                                 .execute()
                                                 .get();
            ColumnFamilyStore cfs = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(table);
            assertThat(cfs.getLiveSSTables()).isNotEmpty().allMatch(s -> s.descriptor.version.version.equals(legacyVersion));
            assertThat(warnings.list).filteredOn(e -> isNewerVersionsWarning(e, table))
                                     .hasSize(1);
        }
        finally
        {
            ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Tracker.class)).detachAppender(warnings);
            warnings.stop();
            DatabaseDescriptor.setStorageCompatibilityMode(mode);
            sstable.selfRef().release();
        }
    }

    private static boolean isNewerVersionsWarning(ILoggingEvent event, String table)
    {
        return event.getFormattedMessage().contains(LEGACY_TABLES_KEYSPACE + '.' + table + " of versions newer than a release of storage_compatibility_mode HCD_1");
    }

    // moves the clock of NoSpamLogger past the interval of the warnings logged earlier for the same tables
    private static void skipNoSpamLoggerInterval()
    {
        long offset = TimeUnit.HOURS.toNanos(2) * ++noSpamLoggerIntervalsSkipped;
        NoSpamLogger.unsafeSetClock(() -> Clock.Global.nanoTime() + offset);
    }

    @AfterClass
    public static void resetNoSpamLoggerClock()
    {
        NoSpamLogger.unsafeSetClock(Clock.Global::nanoTime);
    }

    @Test
    public void testInaccurateSSTableMinMax()
    {
        QueryProcessor.executeInternal("CREATE TABLE legacy_tables.legacy_mc_inaccurate_min_max (k int, c1 int, c2 int, c3 int, v int, primary key (k, c1, c2, c3))");
        loadLegacyTable("mc", "inaccurate_min_max");

        /*
         sstable has the following mutations:
            INSERT INTO legacy_tables.legacy_mc_inaccurate_min_max (k, c1, c2, c3, v) VALUES (100, 4, 4, 4, 4)
            DELETE FROM legacy_tables.legacy_mc_inaccurate_min_max WHERE k=100 AND c1<3
         */

        String query = "SELECT * FROM legacy_tables.legacy_mc_inaccurate_min_max WHERE k=100 AND c1=1 AND c2=1";
        List<Unfiltered> unfiltereds = SinglePartitionSliceCommandTest.getUnfilteredsFromSinglePartition(query);
        Assert.assertEquals(2, unfiltereds.size());
        Assert.assertTrue(unfiltereds.get(0).isRangeTombstoneMarker());
        Assert.assertTrue(((RangeTombstoneMarker) unfiltereds.get(0)).isOpen(false));
        Assert.assertTrue(unfiltereds.get(1).isRangeTombstoneMarker());
        Assert.assertTrue(((RangeTombstoneMarker) unfiltereds.get(1)).isClose(false));
    }

    @Test
    public void testVerifyOldSimpleSSTables()
    {
        verifyOldSSTables("simple");
    }

    @Test
    public void testVerifyOldTupleSSTables()
    {
        verifyOldSSTables("tuple");
    }

    @Test
    public void testVerifyOldDroppedTupleSSTables()
    {
        try {
            for (String legacyVersion : legacyVersions)
            {
                QueryProcessor.executeInternal(String.format("ALTER TABLE legacy_tables.legacy_%s_tuple DROP val", legacyVersion));
                QueryProcessor.executeInternal(String.format("ALTER TABLE legacy_tables.legacy_%s_tuple DROP val2", legacyVersion));
                QueryProcessor.executeInternal(String.format("ALTER TABLE legacy_tables.legacy_%s_tuple DROP val3", legacyVersion));
                QueryProcessor.executeInternal(String.format("ALTER TABLE legacy_tables.legacy_%s_tuple DROP val4", legacyVersion));
            }

            verifyOldSSTables("tuple");
        }
        finally
        {
            for (String legacyVersion : legacyVersions)
            {
                // re-add the columns with the types createTables gave them
                boolean allFrozen = legacyVersion.startsWith("m");
                alterTableAddColumn(legacyVersion, "val frozen<tuple<set<int>,set<text>>>");
                alterTableAddColumn(legacyVersion, allFrozen ? "val2 frozen<tuple<set<int>,set<text>>>"
                                                             : "val2 tuple<set<int>,set<text>>");
                alterTableAddColumn(legacyVersion, String.format("val3 frozen<legacy_%s_tuple_udt>", legacyVersion));
                alterTableAddColumn(legacyVersion, String.format(allFrozen ? "val4 frozen<legacy_%s_tuple_udt>"
                                                                           : "val4 legacy_%s_tuple_udt", legacyVersion));
            }
        }
    }

    private static void alterTableAddColumn(String legacyVersion, String column_definition)
    {
        QueryProcessor.executeInternal(String.format("ALTER TABLE legacy_tables.legacy_%s_tuple ADD %s", legacyVersion, column_definition));
    }

    private void verifyOldSSTables(String tableSuffix)
    {
        SoftAssertions assertions = new SoftAssertions();
        for (String legacyVersion : legacyVersions)
            assertions.assertThatCode(() -> {
                ColumnFamilyStore cfs = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_%s", legacyVersion, tableSuffix));
                loadLegacyTable(legacyVersion, tableSuffix);

                for (SSTableReader sstable : cfs.getLiveSSTables())
                {
                    try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().checkVersion(true).build()))
                    {
                        verifier.verify();
                        if (!sstable.descriptor.version.isLatestVersion())
                            fail("Verify should throw RuntimeException for old sstables " + sstable);
                    }
                    catch (RuntimeException e)
                    {
                    }
                }
                // make sure we don't throw any exception if not checking version:
                for (SSTableReader sstable : cfs.getLiveSSTables())
                {
                    try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().checkVersion(false).build()))
                    {
                        verifier.verify();
                    }
                    catch (Throwable e)
                    {
                        fail("Verify should throw RuntimeException for old sstables " + sstable);
                    }
                }
            }).describedAs(legacyVersion).doesNotThrowAnyException();
        assertions.assertAll();
    }

    @Test
    public void testPendingAntiCompactionOldSSTables()
    {
        SoftAssertions assertions = new SoftAssertions();
        for (String legacyVersion : legacyVersions)
            assertions.assertThatCode(() -> {
                ColumnFamilyStore cfs = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion));
                loadLegacyTable(legacyVersion, "simple");

                boolean shouldFail = !cfs.getLiveSSTables().stream().allMatch(sstable -> sstable.descriptor.version.hasPendingRepair());
                IPartitioner p = Iterables.getFirst(cfs.getLiveSSTables(), null).getPartitioner();
                Range<Token> r = new Range<>(p.getMinimumToken(), p.getMinimumToken());
                PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, singleton(r), nextTimeUUID(), 0, 0);
                PendingAntiCompaction.AcquireResult res = acquisitionCallable.call();
                assertEquals(shouldFail, res == null);
                if (res != null)
                    res.abort();
            }).describedAs(legacyVersion).doesNotThrowAnyException();
        assertions.assertAll();
    }

    @Test
    public void testAutomaticUpgrade()
    {
        SoftAssertions assertions = new SoftAssertions();
        for (String legacyVersion : legacyVersions)
            assertions.assertThatCode(() -> {
                logger.info("Loading legacy version: {}", legacyVersion);
                truncateLegacyTables(legacyVersion);
                loadLegacyTables(legacyVersion);
                ColumnFamilyStore cfs = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion));
                // there should be no compactions to run with auto upgrades disabled:
                assertTrue(cfs.getCompactionStrategy().getNextBackgroundTasks(0).isEmpty());
            }).describedAs(legacyVersion).doesNotThrowAnyException();
        assertions.assertAll();

        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(true);
        for (String legacyVersion : legacyVersions)
            assertions.assertThatCode(() -> {
                logger.info("Loading legacy version: {}", legacyVersion);
                truncateLegacyTables(legacyVersion);
                loadLegacyTables(legacyVersion);
                ColumnFamilyStore cfs = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion));
                if (cfs.getLiveSSTables().stream().anyMatch(s -> !s.descriptor.version.isLatestVersion()))
                    assertTrue(cfs.metric.oldVersionSSTableCount.getValue() > 0);
                while (cfs.getLiveSSTables().stream().anyMatch(s -> !s.descriptor.version.isLatestVersion()))
                {
                    CompactionManager.instance.submitBackground(cfs);
                    Thread.sleep(100);
                }
                assertEquals(0, (int) cfs.metric.oldVersionSSTableCount.getValue());
            }).describedAs(legacyVersion).doesNotThrowAnyException();
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(false);
        assertions.assertAll();
    }

    private void streamLegacyTables(String legacyVersion) throws Exception
    {
        logger.info("Streaming legacy version {}", legacyVersion);
        streamLegacyTable("legacy_%s_simple", legacyVersion);
        streamLegacyTable("legacy_%s_simple_counter", legacyVersion);
        streamLegacyTable("legacy_%s_clust", legacyVersion);
        streamLegacyTable("legacy_%s_clust_counter", legacyVersion);
        streamLegacyTable("legacy_%s_tuple", legacyVersion);
        // TODO – add clust_be_index_summary test data for aa-cb
        if (!legacyVersion.equals("ca") && !legacyVersion.equals("cb") && !legacyVersion.startsWith("a") && !legacyVersion.startsWith("b"))
            streamLegacyTable("legacy_%s_clust_be_index_summary", legacyVersion);
    }

    private void streamLegacyTable(String tablePattern, String legacyVersion) throws Exception
    {
        String table = String.format(tablePattern, legacyVersion);
        // streaming can mutate test data (rewrite IndexSummary, so we have to copy them)
        File testDataDir = new File(tempFolder.newFolder(LEGACY_TABLES_KEYSPACE, table));
        copySstablesToTestData(legacyVersion, table, testDataDir);
        Descriptor descriptor = getDescriptor(testDataDir);
        if (null != descriptor)
        {
            SSTableReader sstable = SSTableReader.open(null, descriptor);
            try
            {
                IPartitioner p = sstable.getPartitioner();
                List<Range<Token>> ranges = new ArrayList<>();
                ranges.add(new Range<>(p.getMinimumToken(), p.getToken(ByteBufferUtil.bytes("100"))));
                ranges.add(new Range<>(p.getToken(ByteBufferUtil.bytes("100")), p.getMinimumToken()));

                List<OutgoingStream> streams = Lists.newArrayList(new CassandraOutgoingFile(StreamOperation.OTHER,
                        sstable.ref(),
                        sstable.getPositionsForRanges(ranges),
                        ranges,
                        sstable.estimatedKeysForRanges(ranges)));

                new StreamPlan(StreamOperation.OTHER).transferStreams(FBUtilities.getBroadcastAddressAndPort(), streams).execute().get();

                // an sstable of a version newer than the one written is received in the version written
                if (descriptor.version.version.compareTo(descriptor.getFormat().getLatestVersion().version) > 0)
                    assertThat(Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(table).getLiveSSTables())
                    .isNotEmpty()
                    .allMatch(s -> s.descriptor.version.isLatestVersion());
            }
            finally
            {
                sstable.selfRef().release();
            }
        }
    }

    public static void truncateLegacyTables(String legacyVersion)
    {
        logger.info("Truncating legacy version {}", legacyVersion);
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion)).truncateBlocking();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple_counter", legacyVersion)).truncateBlocking();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_clust", legacyVersion)).truncateBlocking();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_clust_counter", legacyVersion)).truncateBlocking();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_tuple", legacyVersion)).truncateBlocking();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_clust_be_index_summary", legacyVersion)).truncateBlocking();
        LifecycleTransaction.waitForDeletions();
        CacheService.instance.invalidateCounterCache();
        CacheService.instance.invalidateKeyCache();
    }

    private static void compactLegacyTables(String legacyVersion)
    {
        logger.info("Compacting legacy version {}", legacyVersion);
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion)).forceMajorCompaction();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple_counter", legacyVersion)).forceMajorCompaction();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_clust", legacyVersion)).forceMajorCompaction();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_clust_counter", legacyVersion)).forceMajorCompaction();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_tuple", legacyVersion)).forceMajorCompaction();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_clust_be_index_summary", legacyVersion)).forceMajorCompaction();
    }

    public static void loadLegacyTables(String legacyVersion)
    {
        logger.info("Preparing legacy version {}", legacyVersion);
        loadLegacyTable(legacyVersion, "simple");
        loadLegacyTable(legacyVersion, "simple_counter");
        loadLegacyTable(legacyVersion, "clust");
        loadLegacyTable(legacyVersion, "clust_counter");
        loadLegacyTable(legacyVersion, "tuple");

        // TODO – add clust_be_index_summary test data for aa-cb
        if (!legacyVersion.equals("ca") && !legacyVersion.equals("cb") && !legacyVersion.startsWith("a") && !legacyVersion.startsWith("b"))
            loadLegacyTable(legacyVersion, "clust_be_index_summary");
    }

    private static void verifyCache(String legacyVersion, long startCount)
    {
        // Only perform test if format uses cache.
        SSTableReader sstable = Iterables.getFirst(Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion)).getLiveSSTables(), null);
        if (!(sstable instanceof KeyCacheSupport) || DatabaseDescriptor.getKeyCacheSizeInMiB() == 0)
            return;

        //For https://issues.apache.org/jira/browse/CASSANDRA-10778
        //Validate whether the key cache successfully saves in the presence of old keys as
        //well as loads the correct number of keys
        long endCount = CacheService.instance.keyCache.size();
        Assert.assertTrue(endCount > startCount);
        try
        {
            CacheService.instance.keyCache.submitWrite(Integer.MAX_VALUE).get(60, TimeUnit.MINUTES);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
        CacheService.instance.invalidateKeyCache();
        Assert.assertEquals(startCount, CacheService.instance.keyCache.size());
        CacheService.instance.keyCache.loadSaved();
        Assert.assertEquals(endCount, CacheService.instance.keyCache.size());
    }

    private static void verifyReads(String legacyVersion)
    {
        for (int ck = 0; ck < 50; ck++)
        {
            String ckValue = ck + longString;
            for (int pk = 0; pk < 5; pk++)
            {
                logger.debug("for pk={} ck={}", pk, ck);

                String pkValue = Integer.toString(pk);
                if (ck == 0)
                {
                    readSimpleTable(legacyVersion, pkValue);
                    readSimpleCounterTable(legacyVersion, pkValue);
                }

                readClusteringTable("legacy_%s_clust", legacyVersion, ck, ckValue, pkValue);
                // the versions before cc have no clust_be_index_summary table
                if (!legacyVersion.equals("ca") && !legacyVersion.equals("cb") && !legacyVersion.startsWith("a") && !legacyVersion.startsWith("b"))
                    readClusteringTable("legacy_%s_clust_be_index_summary", legacyVersion, ck, ckValue, pkValue);
                readClusteringCounterTable(legacyVersion, ckValue, pkValue);
            }
        }
    }

    private static void readClusteringCounterTable(String legacyVersion, String ckValue, String pkValue)
    {
        logger.debug("Read legacy_{}_clust_counter", legacyVersion);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_clust_counter WHERE pk=? AND ck=?", legacyVersion), pkValue, ckValue);
        Assert.assertNotNull(rs);
        Assert.assertEquals(String.format("Read legacy_%s_clust_counter", legacyVersion), 1, rs.size());
        Assert.assertEquals(String.format("Read legacy_%s_clust_counter", legacyVersion), 1L, rs.one().getLong("val"));
    }

    private static void readClusteringTable(String tableName, String legacyVersion, int ck, String ckValue, String pkValue)
    {
        logger.debug("Read legacy_{}_clust", legacyVersion);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables." + tableName + " WHERE pk=? AND ck=?", legacyVersion), pkValue, ckValue);
        assertLegacyClustRows(1, rs);

        String ckValue2 = (ck < 10 ? 40 : ck - 1) + longString;
        String ckValue3 = (ck > 39 ? 10 : ck + 1) + longString;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_clust WHERE pk=? AND ck IN (?, ?, ?)", legacyVersion), pkValue, ckValue, ckValue2, ckValue3);
        assertLegacyClustRows(3, rs);
    }

    private static void readSimpleCounterTable(String legacyVersion, String pkValue)
    {
        logger.debug("Read legacy_{}_simple_counter", legacyVersion);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_simple_counter WHERE pk=?", legacyVersion), pkValue);
        Assert.assertNotNull(rs);
        Assert.assertEquals(1, rs.size());
        Assert.assertEquals(1L, rs.one().getLong("val"));
    }

    private static void readSimpleTable(String legacyVersion, String pkValue)
    {
        logger.debug("Read simple: legacy_{}_simple", legacyVersion);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_simple WHERE pk=?", legacyVersion), pkValue);
        Assert.assertNotNull(rs);
        Assert.assertEquals(1, rs.size());
        Assert.assertEquals("foo bar baz", rs.one().getString("val"));
    }

    private static void createKeyspace()
    {
        QueryProcessor.executeInternal("CREATE KEYSPACE legacy_tables WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
    }

    private static void createTables(String legacyVersion)
    {
        QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_simple (pk text PRIMARY KEY, val text)", legacyVersion));
        QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_simple_counter (pk text PRIMARY KEY, val counter)", legacyVersion));
        QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_clust (pk text, ck text, val text, PRIMARY KEY (pk, ck))", legacyVersion));
        QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_clust_counter (pk text, ck text, val counter, PRIMARY KEY (pk, ck))", legacyVersion));
        QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_clust_be_index_summary (pk text, ck text, val text, PRIMARY KEY (pk, ck))", legacyVersion));

        QueryProcessor.executeInternal(String.format("CREATE TYPE legacy_tables.legacy_%s_tuple_udt (name tuple<text,text>)", legacyVersion));

        if (legacyVersion.startsWith("m"))
        {
            // sstable formats possibly from 3.0.x would have had a schema with everything frozen
            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%1$s_tuple (pk text PRIMARY KEY, " +
                    "val frozen<tuple<set<int>,set<text>>>, val2 frozen<tuple<set<int>,set<text>>>, val3 frozen<legacy_%1$s_tuple_udt>, val4 frozen<legacy_%1$s_tuple_udt>, extra text)", legacyVersion));
        }
        else
        {
            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%1$s_tuple (pk text PRIMARY KEY, " +
                "val frozen<tuple<set<int>,set<text>>>, val2 tuple<set<int>,set<text>>, val3 frozen<legacy_%1$s_tuple_udt>, val4 legacy_%1$s_tuple_udt, extra text)", legacyVersion));
        }
    }

    private static void truncateTables(String legacyVersion)
    {
        QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_simple", legacyVersion));
        QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_simple_counter", legacyVersion));
        QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_clust", legacyVersion));
        QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_clust_counter", legacyVersion));
        QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_clust_be_index_summary", legacyVersion));
        CacheService.instance.invalidateCounterCache();
        CacheService.instance.invalidateKeyCache();
    }

    private static void assertLegacyClustRows(int count, UntypedResultSet rs)
    {
        Assert.assertNotNull(rs);
        Assert.assertEquals(count, rs.size());
        for (int i = 0; i < count; i++)
        {
            for (UntypedResultSet.Row r : rs)
            {
                Assert.assertEquals(128, r.getString("val").length());
            }
        }
    }

    private static void loadLegacyTable(String legacyVersion, String tableSuffix)
    {
        loadLegacyTableByName(legacyVersion, String.format("legacy_%s_%s", legacyVersion, tableSuffix));
    }

    private static void loadLegacyTableByName(String legacyVersion, String table)
    {
        // ignore if no sstables are in this legacyVersion directory
        getTestDataTableDir(legacyVersion, table).forEach(f -> logger.info(f.toString()));
        if (0 == getTestDataTableDir(legacyVersion, table).tryList(f -> f.name().endsWith(".db")).length)
            return;

        logger.info("Loading legacy table {}", table);

        ColumnFamilyStore cfs = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(table);

        for (File cfDir : cfs.getDirectories().getCFDirectories())
        {
            try
            {
                copySstablesToTestData(legacyVersion, table, cfDir);
            }
            catch (IOException e)
            {
                throw new AssertionError(e);
            }
        }

        if (legacyVersion.startsWith("m") && legacyVersion.compareTo("me") <= 0)
        {
            // sstables <= me are potentially broken, pretend offline upgrade where the user ran the scrub's header fix
            FBUtilities.setPreviousReleaseVersionString("3.0.25");
            SSTableHeaderFix.fixNonFrozenUDTIfUpgradeFrom30();
        }

        int s0 = cfs.getLiveSSTables().size();
        cfs.loadNewSSTables();
        int s1 = cfs.getLiveSSTables().size();
        assertThat(s1).isGreaterThan(s0);
    }

    /**
     * Generates sstables for CQL tables (see {@link #createTables(String)}) in <i>current</i>
     * sstable format (version) into {@code test/data/legacy-sstables/VERSION}, where
     * {@code VERSION} matches {@link Version#version BigFormat.latestVersion.getVersion()}.
     *
     * Sequence numbers are changed to represent the C* version used when creating the SSTable.
     * <p>
     * Run this test alone (e.g. from your IDE) when a new version is introduced or format changed
     * during development. I.e. remove the {@code @Ignore} annotation temporarily.
     * </p>
     */
    @Ignore // TODO: Currently this test needs to be ran alone to avoid unwanted compactions, flushes, etc to interfere
    @Test
    public void testGenerateSstables() throws Throwable
    {
        SSTableFormat<?, ?> format = DatabaseDescriptor.getSelectedSSTableFormat();
        Random rand = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 128; i++)
        {
            sb.append((char)('a' + rand.nextInt(26)));
        }
        String randomString = sb.toString();

        for (int pk = 0; pk < 5; pk++)
        {
            String valPk = Integer.toString(pk);
            QueryProcessor.executeInternal(String.format("INSERT INTO legacy_tables.legacy_%s_simple (pk, val) VALUES ('%s', '%s')",
                                                         format.getLatestVersion(), valPk, "foo bar baz"));

            QueryProcessor.executeInternal(String.format("UPDATE legacy_tables.legacy_%s_simple_counter SET val = val + 1 WHERE pk = '%s'",
                                                         format.getLatestVersion(), valPk));

            QueryProcessor.executeInternal(
                    String.format("INSERT INTO legacy_tables.legacy_%s_tuple (pk, val, val2, val3, val4, extra)"
                                    + " VALUES ('%s', ({1,2,3},{'a','b','c'}), ({1,2,3},{'a','b','c'}), {name: ('abc','def')}, {name: ('abc','def')}, '%s')",
                                  format.getLatestVersion(), valPk, randomString));

            for (int ck = 0; ck < 50; ck++)
            {
                String valCk = Integer.toString(ck);

                QueryProcessor.executeInternal(String.format("INSERT INTO legacy_tables.legacy_%s_clust (pk, ck, val) VALUES ('%s', '%s', '%s')",
                                                             format.getLatestVersion(), valPk, valCk + longString, randomString));

                QueryProcessor.executeInternal(String.format("UPDATE legacy_tables.legacy_%s_clust_counter SET val = val + 1 WHERE pk = '%s' AND ck='%s'",
                                                             format.getLatestVersion(), valPk, valCk + longString));

                // note: to emulate BE for offsets in Summary you can comment temporary the following line:
                // offset = Integer.reverseBytes(offset);
                // in org.apache.cassandra.io.sstable.indexsummary.IndexSummary.IndexSummarySerializer.serialize
                QueryProcessor.executeInternal(String.format("INSERT INTO legacy_tables.legacy_%s_clust_be_index_summary (pk, ck, val) VALUES ('%s', '%s', '%s')",
                                                             format.getLatestVersion(), valPk, valCk + longString, randomString));

            }
        }

        StorageService.instance.forceKeyspaceFlush(LEGACY_TABLES_KEYSPACE, ColumnFamilyStore.FlushReason.UNIT_TESTS);

        File ksDir = new File(LEGACY_SSTABLE_ROOT, String.format("%s/legacy_tables", format.getLatestVersion()));
        ksDir.tryCreateDirectories();
        copySstablesFromTestData(format.getLatestVersion(), "legacy_%s_simple", ksDir);
        copySstablesFromTestData(format.getLatestVersion(), "legacy_%s_simple_counter", ksDir);
        copySstablesFromTestData(format.getLatestVersion(), "legacy_%s_clust", ksDir);
        copySstablesFromTestData(format.getLatestVersion(), "legacy_%s_clust_counter", ksDir);
        copySstablesFromTestData(format.getLatestVersion(), "legacy_%s_tuple", ksDir);
        copySstablesFromTestData(format.getLatestVersion(), "legacy_%s_clust_be_index_summary", ksDir);
    }

    public static void copySstablesFromTestData(Version legacyVersion, String tablePattern, File ksDir) throws IOException
    {
        copySstablesFromTestData(legacyVersion, tablePattern, ksDir, LEGACY_TABLES_KEYSPACE);
    }

    public static void copySstablesFromTestData(Version legacyVersion, String tablePattern, File ksDir, String ks) throws IOException
    {
        String table = String.format(tablePattern, legacyVersion);
        File cfDir = new File(ksDir, table);
        cfDir.tryCreateDirectory();

        for (File srcDir : Keyspace.open(ks).getColumnFamilyStore(table).getDirectories().getCFDirectories())
        {
            for (File sourceFile : srcDir.tryList())
            {
                // Sequence IDs represent the C* version used when creating the SSTable, i.e. with #testGenerateSstables() (if not uuid based)
                String newSeqId = FBUtilities.getReleaseVersionString().split("-")[0].replaceAll("[^0-9]", "");
                File target = new File(cfDir, sourceFile.name().replace(legacyVersion + "-1-", legacyVersion + "-" + newSeqId + "-"));
                copyFile(sourceFile, target);
            }
        }
    }

    private static void copySstablesToTestData(String legacyVersion, String table, File targetDir) throws IOException
    {
        File testDataTableDir = getTestDataTableDir(legacyVersion, table);
        Assert.assertTrue("The table directory " + testDataTableDir + " was not found", testDataTableDir.isDirectory());
        for (File sourceTestFile : testDataTableDir.tryList())
            copyFileToDir(sourceTestFile, targetDir);
    }

    private static File getTestDataTableDir(File parentDir, String legacyVersion, String table)
    {
        return new File(parentDir, String.format("%s/legacy_tables/%s", legacyVersion, table));
    }

    private static File getTestDataTableDir(String legacyVersion, String table)
    {
        return getTestDataTableDir(LEGACY_SSTABLE_ROOT, legacyVersion, table);
    }

    public static void copyFileToDir(File sourceFile, File targetDir) throws IOException
    {
        copyFile(sourceFile,  new File(targetDir, sourceFile.name()));
    }

    public static void copyFile(File sourceFile, File targetFile) throws IOException
    {
        byte[] buf = new byte[65536];
        if (sourceFile.isFile())
        {
            int rd;
            try (FileInputStreamPlus is = new FileInputStreamPlus(sourceFile);
                 FileOutputStreamPlus os = new FileOutputStreamPlus(targetFile);)
            {
                while ((rd = is.read(buf)) >= 0)
                    os.write(buf, 0, rd);
            }
        }
    }
}
