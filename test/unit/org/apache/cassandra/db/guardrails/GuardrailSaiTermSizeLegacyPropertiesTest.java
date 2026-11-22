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

package org.apache.cassandra.db.guardrails;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import com.google.common.primitives.Floats;
import org.junit.Test;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static java.nio.ByteBuffer.allocate;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_MAX_STRING_TERM_SIZE;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_MAX_VECTOR_TERM_SIZE;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VALIDATE_MAX_TERM_SIZE_AT_COORDINATOR;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.BYTES;
import static org.apache.cassandra.index.sai.NotIndexedValues.NGRAM_ANALYZER;
import static org.apache.cassandra.index.sai.NotIndexedValues.NGRAM_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

/**
 * How the legacy SAI term size system properties interact with the {@link Guardrails#saiStringTermSize} guardrail,
 * which applies by default (see {@link IndexContext#maxTermSize()}):
 * <ul>
 *   <li>{@code cassandra.sai.validate_max_term_size_at_coordinator=false} disables the coordinator check, so oversized
 *   terms are written but not indexed;</li>
 *   <li>{@code cassandra.sai.validate_max_term_size_at_coordinator=true} rejects terms over the maximum term size with
 *   an {@link InvalidRequestException}, whoever writes them;</li>
 *   <li>{@code cassandra.sai.max_string_term_size_kb}, if set, is the maximum size of an indexed term, and client
 *   writes of larger terms are rejected;</li>
 *   <li>without a guardrail failure threshold, the maximum size of an indexed term is the default of that property,
 *   except for vectors, which have none.</li>
 * </ul>
 */
public class GuardrailSaiTermSizeLegacyPropertiesTest extends ValueThresholdTester
{
    private static final int WARN_THRESHOLD = 1024; // bytes
    private static final int FAIL_THRESHOLD = WARN_THRESHOLD * 4; // bytes

    public GuardrailSaiTermSizeLegacyPropertiesTest()
    {
        super(WARN_THRESHOLD + "B",
              FAIL_THRESHOLD + "B",
              Guardrails.saiStringTermSize,
              Guardrails::setSaiStringTermSizeThreshold,
              Guardrails::getSaiStringTermSizeWarnThreshold,
              Guardrails::getSaiStringTermSizeFailThreshold,
              bytes -> new DataStorageSpec.LongBytesBound(bytes, BYTES).toString(),
              size -> new DataStorageSpec.LongBytesBound(size).toBytes());
    }

    @Override
    protected int warnThreshold()
    {
        return WARN_THRESHOLD;
    }

    @Override
    protected int failThreshold()
    {
        return FAIL_THRESHOLD;
    }

    @Test
    public void testCoordinatorValidationDisabled() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(SAI_VALIDATE_MAX_TERM_SIZE_AT_COORDINATOR, false))
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
            createIndex("CREATE INDEX ON %s (v) USING 'sai'");

            // no warning nor failure, but the oversized term is not indexed (the max term size is the failure threshold)
            ByteBuffer large = allocate(WARN_THRESHOLD + 1);
            ByteBuffer oversized = allocate(FAIL_THRESHOLD + 1);
            assertValid("INSERT INTO %s (k, v) VALUES (0, ?)", large);
            assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", oversized);
            assertIndexedRows(large, 1);
            assertIndexedRows(oversized, 0);
        }
    }

    @Test
    public void testLegacyCoordinatorValidation() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(SAI_VALIDATE_MAX_TERM_SIZE_AT_COORDINATOR, true))
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
            createIndex("CREATE INDEX ON %s (v) USING 'sai'");

            // the guardrail isn't applied, terms over the max term size are rejected for any user
            assertValid("INSERT INTO %s (k, v) VALUES (0, ?)", allocate(WARN_THRESHOLD + 1));
            assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", allocate(FAIL_THRESHOLD));
            for (ClientState state : new ClientState[]{ userClientState, superClientState, systemClientState })
            {
                assertThatThrownBy(() -> execute(state, "INSERT INTO %s (k, v) VALUES (2, ?)",
                                                 Collections.singletonList(allocate(FAIL_THRESHOLD + 1))))
                .isInstanceOf(InvalidRequestException.class)
                .isNotInstanceOf(GuardrailViolatedException.class)
                .hasMessageContaining("Term of column v exceeds the byte limit for index");
            }
        }
    }

    @Test
    public void testExcludedUsersWriteOversizedTermsWithoutIndexingThem() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE INDEX ON %s (v) USING 'sai'");

        // the guardrail doesn't apply to superusers and internal writes, but oversized terms are still not indexed
        ByteBuffer oversized1 = allocate(FAIL_THRESHOLD + 1);
        ByteBuffer oversized2 = allocate(FAIL_THRESHOLD + 2);
        assertValid(() -> execute(superClientState, "INSERT INTO %s (k, v) VALUES (0, ?)", Collections.singletonList(oversized1)));
        assertValid(() -> execute(systemClientState, "INSERT INTO %s (k, v) VALUES (1, ?)", Collections.singletonList(oversized2)));
        assertIndexedRows(oversized1, 0);
        assertIndexedRows(oversized2, 0);
    }

    @Test
    public void testMaxTermSizeProperty() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(SAI_MAX_STRING_TERM_SIZE, 2))
        {
            // The property overrides the failure threshold as the max size of an indexed term. The guardrail still
            // applies, and client writes of terms over the max size are rejected rather than left out of the index.
            createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
            createIndex("CREATE INDEX ON %s (v) USING 'sai'");

            ByteBuffer small = allocate(2048);
            ByteBuffer large = allocate(2049);
            assertWarns(() -> execute("INSERT INTO %s (k, v) VALUES (1, ?)", small), "Value of column 'v' has size 2048");
            assertThatThrownBy(() -> execute("INSERT INTO %s (k, v) VALUES (2, ?)", large))
            .isInstanceOf(InvalidRequestException.class)
            .isNotInstanceOf(GuardrailViolatedException.class)
            .hasMessage("Term of column v exceeds the byte limit for index. Term size 2.001KiB. Max allowed size 2.000KiB.");
            assertFails("v", "INSERT INTO %s (k, v) VALUES (3, ?)", allocate(FAIL_THRESHOLD + 1));
            assertIndexedRows(small, 1);

            // writes the guardrail doesn't apply to are accepted, but their terms over the max size aren't indexed
            assertValid(() -> execute(superClientState, "INSERT INTO %s (k, v) VALUES (4, ?)", Collections.singletonList(large)));
            assertIndexedRows(large, 0);
        }
    }

    @Test
    public void testDisabledGuardrailFallsBackToDefaultMaxTermSize() throws Throwable
    {
        guardrails().setSaiStringTermSizeThreshold(null, null);
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE INDEX ON %s (v) USING 'sai'");

        // without a failure threshold, client writes of terms over the default max size are rejected
        assertEquals(IndexContext.MAX_STRING_TERM_SIZE, 8192);
        ByteBuffer max = allocate(IndexContext.MAX_STRING_TERM_SIZE);
        ByteBuffer oversized = allocate(IndexContext.MAX_STRING_TERM_SIZE + 1);
        assertValid("INSERT INTO %s (k, v) VALUES (0, ?)", max);
        assertThatThrownBy(() -> execute("INSERT INTO %s (k, v) VALUES (1, ?)", oversized))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Term of column v exceeds the byte limit for index");
        assertValid(() -> execute(systemClientState, "INSERT INTO %s (k, v) VALUES (1, ?)", Collections.singletonList(oversized)));
        assertIndexedRows(max, 1);
        assertIndexedRows(oversized, 0);
    }

    /**
     * An analyzed value larger than the max term size, but whose terms are all small, is accepted, and indexed alike in
     * the memtable, in flushed sstables and after compaction.
     */
    @Test
    public void testAnalyzedValueOverMaxTermSizeWithSmallTerms() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        disableCompaction();

        String document = words(FAIL_THRESHOLD + 2000);
        execute("INSERT INTO %s (k, v) VALUES (0, ?)", ByteBufferUtil.bytes(document));
        assertAnalyzedRows("w7", 1);
    }

    /**
     * An analyzed value whose terms add up to the max analyzed size (cassandra.sai.max_analyzed_size_kb) isn't indexed,
     * alike in the memtable, in flushed sstables and after compaction.
     */
    @Test
    public void testAnalyzedValueOverMaxAnalyzedSize() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        String index = createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        disableCompaction();

        // the write is accepted, with a warning that the value isn't indexed, given once by the coordinator and the replica
        String document = words(IndexContext.MAX_ANALYZED_SIZE + 4000);
        assertWarnsNotIndexed(IndexContext.ANALYZED_VALUE_NOT_INDEXED_WARNING, index,
                              () -> execute("INSERT INTO %s (k, v) VALUES (0, ?)", ByteBufferUtil.bytes(document)));
        assertAnalyzedRows("w7", 0);
    }

    /**
     * An analyzed value under the term size warning threshold whose terms add up to the max analyzed size isn't indexed,
     * and the replica warns, for an internal write.
     */
    @Test
    public void testReplicaAnalyzedValueOverMaxAnalyzedSizeInternal() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        String index = createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'StorageAttachedIndex' WITH OPTIONS = " + NGRAM_ANALYZER);

        assertWarnsNotIndexed(IndexContext.ANALYZED_VALUE_NOT_INDEXED_WARNING, index,
                              () -> executeInternal("INSERT INTO %s (k, v) VALUES (0, ?)", NGRAM_VALUE));
        assertEquals(1, rows("SELECT k FROM %s WHERE k = 0"));
        assertEquals(0, rows("SELECT k FROM %s WHERE v : 'ab'"));
    }

    /**
     * The same as {@link #testReplicaAnalyzedValueOverMaxAnalyzedSizeInternal}, for a client write applied locally.
     */
    @Test
    public void testReplicaAnalyzedValueOverMaxAnalyzedSize() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        String index = createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'StorageAttachedIndex' WITH OPTIONS = " + NGRAM_ANALYZER);

        assertWarnsNotIndexed(IndexContext.ANALYZED_VALUE_NOT_INDEXED_WARNING, index,
                              () -> execute("INSERT INTO %s (k, v) VALUES (0, ?)", ByteBufferUtil.bytes(NGRAM_VALUE)));
        assertEquals(1, rows("SELECT k FROM %s WHERE k = 0"));
        assertEquals(0, rows("SELECT k FROM %s WHERE v : 'ab'"));
    }

    /**
     * Replacing or deleting a value that isn't indexed because of its analyzed size, in the same memtable, doesn't warn.
     */
    @Test
    public void testAnalyzedValueOverMaxAnalyzedSizeReplacedOrDeleted() throws Throwable
    {
        for (String memtable : new String[]{ "skiplist", "trie" })
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, v text) WITH memtable = '" + memtable + "'");
            String index = createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'StorageAttachedIndex' WITH OPTIONS = " + NGRAM_ANALYZER);

            ByteBuffer value = ByteBufferUtil.bytes(NGRAM_VALUE);
            assertWarnsNotIndexed(IndexContext.ANALYZED_VALUE_NOT_INDEXED_WARNING, index,
                                  () -> execute("INSERT INTO %s (k, v) VALUES (0, ?)", value));
            assertValid(() -> execute("INSERT INTO %s (k, v) VALUES (0, ?)", ByteBufferUtil.bytes("ab")));
            assertEquals(memtable, 1, rows("SELECT k FROM %s WHERE v : 'ab'"));

            assertWarnsNotIndexed(IndexContext.ANALYZED_VALUE_NOT_INDEXED_WARNING, index,
                                  () -> execute("INSERT INTO %s (k, v) VALUES (1, ?)", value));
            assertValid(() -> execute(userClientState, "DELETE FROM %s WHERE k = 1"));
            assertEquals(memtable, 0, rows("SELECT k FROM %s WHERE k = 1"));
        }
    }

    /**
     * A value that has already expired when it's written isn't indexed either, but doesn't warn, inserted or updating a
     * row in the memtable, and as a collection element.
     */
    @Test
    public void testExpiredAnalyzedValue() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text, s set<text>)");
        String index = createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'StorageAttachedIndex' WITH OPTIONS = " + NGRAM_ANALYZER);
        String setIndex = createIndex("CREATE CUSTOM INDEX ON %s (s) USING 'StorageAttachedIndex' WITH OPTIONS = " + NGRAM_ANALYZER);
        long past = FBUtilities.nowInSeconds() - 10;

        assertWarnsNotIndexed(IndexContext.ANALYZED_VALUE_NOT_INDEXED_WARNING, index,
                              () -> executeInternal("INSERT INTO %s (k, v) VALUES (0, ?)", NGRAM_VALUE));
        assertWarnsNotIndexed(IndexContext.ANALYZED_VALUE_NOT_INDEXED_WARNING, "s", setIndex,
                              () -> executeInternal("INSERT INTO %s (k, s) VALUES (1, ?)", Collections.singleton(NGRAM_VALUE)));

        assertNoWarnings(() -> executeInternalWithNowInSec("INSERT INTO %s (k, v, s) VALUES (2, ?, ?) USING TTL 1",
                                                           past, NGRAM_VALUE, Collections.singleton(NGRAM_VALUE)));

        executeInternal("INSERT INTO %s (k, v, s) VALUES (3, 'ab', {'ab'})");
        assertNoWarnings(() -> executeInternalWithNowInSec("UPDATE %s USING TTL 1 SET v = ?, s = s + ? WHERE k = 3",
                                                           past, NGRAM_VALUE, Collections.singleton(NGRAM_VALUE)));
    }

    private void executeInternalWithNowInSec(String query, long nowInSec, Object... values)
    {
        QueryProcessor.executeInternalWithNowInSec(formatQuery(query), nowInSec, values);
    }

    private static void assertNoWarnings(CheckedFunction write) throws Throwable
    {
        ClientWarn.instance.captureWarnings();
        try
        {
            write.apply();
            assertThat(ClientWarn.instance.getWarnings()).isNull();
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
        }
    }

    /** Checks that the write gives exactly the warning of the given format that a value of v isn't indexed. */
    private void assertWarnsNotIndexed(String format, String index, CheckedFunction write) throws Throwable
    {
        assertWarnsNotIndexed(format, "v", index, write);
    }

    private void assertWarnsNotIndexed(String format, String column, String index, CheckedFunction write) throws Throwable
    {
        ClientWarn.instance.captureWarnings();
        try
        {
            write.apply();
            assertThat(ClientWarn.instance.getWarnings()).containsExactly(String.format(format, column, keyspace(), index));
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
        }
    }

    /**
     * A value of a non-tokenizing analyzed index (case_sensitive, normalize) is a single term, so it is only bounded by
     * the max term size, not by the max analyzed size (8KiB), with the default thresholds (failure over 8KiB) and with
     * a raised failure threshold.
     */
    @Test
    public void testNonTokenizingAnalyzedValueSizes() throws Throwable
    {
        int[] sizes = { 8191, 8192, 10000 };
        for (String options : new String[]{ "{'case_sensitive': 'false'}", "{'normalize': 'true'}" })
        {
            for (int failThreshold : new int[]{ 8192, 16384 })
            {
                guardrails().setSaiStringTermSizeThreshold("1KiB", failThreshold + "B");
                createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
                createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'StorageAttachedIndex' WITH OPTIONS = " + options);
                disableCompaction();

                for (int i = 0; i < sizes.length; i++)
                {
                    ByteBuffer value = value(sizes[i]);
                    String query = "INSERT INTO %s (k, v) VALUES (" + i + ", ?)";
                    if (sizes[i] > failThreshold)
                        assertThatThrownBy(() -> execute(query, value)).isInstanceOf(GuardrailViolatedException.class);
                    else
                        execute(query, value);
                }

                String description = options + " with a failure threshold of " + failThreshold + ": ";
                assertValueRows(description + "memtable", sizes, failThreshold);
                flush();
                assertValueRows(description + "flushed", sizes, failThreshold);
                execute(systemClientState, "INSERT INTO %s (k, v) VALUES (100, 'other')");
                flush();
                compact();
                assertValueRows(description + "compacted", sizes, failThreshold);
            }
        }
    }

    /**
     * A failure threshold over the largest term the sstable index writers can hold (32766 bytes) doesn't let a client
     * write a larger term, which would be found in memtables and flushed sstables, then fail every compaction.
     */
    @Test
    public void testFailThresholdOverMaxOnDiskTermSize() throws Throwable
    {
        assertEquals(32766, IndexContext.MAX_ON_DISK_TERM_SIZE);
        for (String options : new String[]{ "{}", "{'case_sensitive': 'false'}" })
        {
            guardrails().setSaiStringTermSizeThreshold("1KiB", "64KiB");
            createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
            createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'StorageAttachedIndex' WITH OPTIONS = " + options);
            disableCompaction();

            for (int size : new int[]{ 32800, 40000 })
            {
                ByteBuffer value = value(size);
                assertThatThrownBy(() -> execute("INSERT INTO %s (k, v) VALUES (1, ?)", value))
                .as(options + ", term of " + size + " bytes")
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining("Term of column v exceeds the byte limit for index");
            }

            ByteBuffer value = value(32700);
            execute("INSERT INTO %s (k, v) VALUES (0, ?)", value);
            String query = "SELECT k FROM %s WHERE v = ?";
            assertEquals(1, ((ResultMessage.Rows) execute(systemClientState, query, Collections.singletonList(value))).result.size());
            flush();
            assertEquals(1, ((ResultMessage.Rows) execute(systemClientState, query, Collections.singletonList(value))).result.size());
            execute(systemClientState, "INSERT INTO %s (k, v) VALUES (100, 'other')");
            flush();
            compact();
            assertEquals(1, ((ResultMessage.Rows) execute(systemClientState, query, Collections.singletonList(value))).result.size());
        }
    }

    /**
     * Map entries are held by the sstable index writers byte-comparable encoded, with separators and an extra byte per
     * run of zeros, so a term under 32766 bytes can still be too large for them: such writes are rejected too.
     */
    @Test
    public void testMapEntriesOverMaxOnDiskTermSize() throws Throwable
    {
        MapType<String, String> type = MapType.getInstance(UTF8Type.instance, UTF8Type.instance, true);
        // entry values of letters, then of alternating zeros, and whether they are accepted
        Object[][] cases = { { value(32750), true }, { value(32757), false }, { value(32759), false },
                             { alternatingZeros(21800), true }, { alternatingZeros(21840), false }, { alternatingZeros(22000), false } };
        for (Object[] c : cases)
        {
            ByteBuffer entryValue = (ByteBuffer) c[0];
            boolean accepted = (Boolean) c[1];
            guardrails().setSaiStringTermSizeThreshold("1KiB", "64KiB");
            createTable("CREATE TABLE %s (k int PRIMARY KEY, v map<text, text>)");
            createIndex("CREATE CUSTOM INDEX ON %s (ENTRIES(v)) USING 'StorageAttachedIndex'");
            disableCompaction();

            ByteBuffer value = type.decompose(Collections.singletonMap("k", UTF8Type.instance.compose(entryValue)));
            String description = "entry value of " + entryValue.remaining() + " bytes";
            if (accepted)
            {
                execute("INSERT INTO %s (k, v) VALUES (0, ?)", value);
                assertFoundEverywhere(description, "SELECT k FROM %s WHERE v['k'] = ?", entryValue,
                                      type.decompose(Collections.singletonMap("k", "other")));
            }
            else
            {
                assertThatThrownBy(() -> execute("INSERT INTO %s (k, v) VALUES (0, ?)", value))
                .as(description)
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining("Term of column v exceeds the byte limit for index");
            }
        }
    }

    /**
     * A term can be larger than the value it comes from (lower-casing U+023A turns 2 bytes into 3): the max term size
     * applies to the term, whatever the size of the value.
     */
    @Test
    public void testAnalyzedTermOverMaxOnDiskTermSize() throws Throwable
    {
        // values of 21800, 22000 and 32000 bytes, whose terms are of 32700, 33000 and 48000 bytes
        for (int count : new int[]{ 10900, 11000, 16000 })
        {
            guardrails().setSaiStringTermSizeThreshold("1KiB", "64KiB");
            createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
            createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'StorageAttachedIndex' WITH OPTIONS = {'case_sensitive': 'false'}");
            disableCompaction();

            ByteBuffer value = ByteBufferUtil.bytes(new String(new char[count]).replace('\0', '\u023A'));
            String description = "value of " + value.remaining() + " bytes";
            if (count * 3 <= IndexContext.MAX_ON_DISK_TERM_SIZE)
            {
                execute("INSERT INTO %s (k, v) VALUES (0, ?)", value);
                assertFoundEverywhere(description, "SELECT k FROM %s WHERE v = ?", value, ByteBufferUtil.bytes("other"));
            }
            else
            {
                assertThatThrownBy(() -> execute("INSERT INTO %s (k, v) VALUES (0, ?)", value))
                .as(description)
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining("Term of column v exceeds the byte limit for index");
            }
        }
    }

    /**
     * varint and decimal values are indexed with a fixed size whatever their own size, so they have no max term size:
     * only the guardrail applies to them.
     */
    @Test
    public void testNumericValuesHaveNoMaxTermSize() throws Throwable
    {
        for (String type : new String[]{ "varint", "decimal" })
        {
            for (int size : new int[]{ 20000, 40000, 10000 })
            {
                // the last size is with the guardrail disabled, where literal terms are bounded by 8KiB
                if (size == 10000)
                    guardrails().setSaiStringTermSizeThreshold(null, null);
                else
                    guardrails().setSaiStringTermSizeThreshold("1KiB", "64KiB");
                createTable("CREATE TABLE %s (k int PRIMARY KEY, v " + type + ")");
                createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'StorageAttachedIndex'");
                disableCompaction();

                byte[] bytes = new byte[size];
                Arrays.fill(bytes, (byte) 1);
                BigInteger integer = new BigInteger(bytes);
                ByteBuffer value = type.equals("varint") ? IntegerType.instance.decompose(integer)
                                                         : DecimalType.instance.decompose(new BigDecimal(integer, 2));
                ByteBuffer other = type.equals("varint") ? IntegerType.instance.decompose(BigInteger.ONE)
                                                         : DecimalType.instance.decompose(BigDecimal.ONE);
                execute("INSERT INTO %s (k, v) VALUES (0, ?)", value);
                assertFoundEverywhere(type + " value of " + value.remaining() + " bytes", "SELECT k FROM %s WHERE v = ?", value, other);
            }
        }
    }

    /** Checks that row 0 is found everywhere: memtable, flushed sstable, and compaction with another sstable. */
    private void assertFoundEverywhere(String description, String query, ByteBuffer queryValue, ByteBuffer otherValue) throws Throwable
    {
        assertEquals(description + ", memtable", 1, rows(query, queryValue));
        flush();
        assertEquals(description + ", flushed", 1, rows(query, queryValue));
        execute(systemClientState, "INSERT INTO %s (k, v) VALUES (100, ?)", Collections.singletonList(otherValue));
        flush();
        compact();
        assertEquals(description + ", compacted", 1, rows(query, queryValue));
    }

    private static ByteBuffer alternatingZeros(int size)
    {
        byte[] bytes = new byte[size];
        for (int i = 1; i < size; i += 2)
            bytes[i] = 'a';
        return ByteBuffer.wrap(bytes);
    }

    /**
     * The legacy coordinator check (validate_max_term_size_at_coordinator=true) rejects exactly the analyzed values
     * that indexing would leave out, whose terms add up to the max analyzed size (8KiB), whatever the guardrail.
     */
    @Test
    public void testLegacyCoordinatorValidationOfAnalyzedValues() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(SAI_VALIDATE_MAX_TERM_SIZE_AT_COORDINATOR, true))
        {
            for (String failThreshold : new String[]{ "8KiB", "16KiB" })
            {
                // 1170 terms of 7 bytes add up to 8190 bytes, plus a last term of 1 to 3 bytes; 1700 terms to 11900 bytes
                for (String document : new String[]{ terms(1170, "b"), terms(1170, "bb"), terms(1170, "bbb"), terms(1700, "") })
                {
                    guardrails().setSaiStringTermSizeThreshold("1KiB", failThreshold);
                    createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
                    createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
                    disableCompaction();

                    int termsSize = document.replace(" ", "").length();
                    String query = "INSERT INTO %s (k, v) VALUES (0, ?)";
                    if (termsSize < IndexContext.MAX_ANALYZED_SIZE)
                    {
                        execute(query, ByteBufferUtil.bytes(document));
                        assertAnalyzedRows("w000007", 1);
                    }
                    else
                    {
                        ClientWarn.instance.captureWarnings();
                        try
                        {
                            assertThatThrownBy(() -> execute(query, ByteBufferUtil.bytes(document)))
                            .as("fail threshold " + failThreshold + ", terms adding up to " + termsSize + " bytes")
                            .isInstanceOf(InvalidRequestException.class)
                            .hasMessage("Term's analyzed size for column v exceeds the cumulative limit for index. Max allowed size 8.000KiB.");
                            assertThat(ClientWarn.instance.getWarnings()).isNull();
                        }
                        finally
                        {
                            ClientWarn.instance.resetWarnings();
                        }
                    }
                }
            }
        }
    }

    private static String terms(int count, String last)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++)
            sb.append(String.format("w%06d ", i));
        return sb.append(last).toString();
    }

    private void assertValueRows(String description, int[] sizes, int failThreshold)
    {
        for (int size : sizes)
        {
            ResultMessage.Rows rows = (ResultMessage.Rows) execute(systemClientState, "SELECT k FROM %s WHERE v = ?",
                                                                   Collections.singletonList(value(size)));
            assertEquals(description + ", value of " + size + " bytes", size > failThreshold ? 0 : 1, rows.result.size());
        }
    }

    private static ByteBuffer value(int size)
    {
        byte[] bytes = new byte[size];
        Arrays.fill(bytes, (byte) 'a');
        return ByteBuffer.wrap(bytes);
    }

    private static String words(int size)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; sb.length() < size; i++)
            sb.append('w').append(i).append(' ');
        return sb.toString();
    }

    /** Checks the rows found for the analyzed term in the memtable, a flushed sstable, and after compaction. */
    private void assertAnalyzedRows(String term, int expected) throws Throwable
    {
        String query = "SELECT k FROM %s WHERE v : '" + term + "'";
        assertEquals(expected, rows(query));
        flush();
        assertEquals(expected, rows(query));
        execute(systemClientState, "INSERT INTO %s (k, v) VALUES (1, 'other')");
        flush();
        compact();
        assertEquals(expected, rows(query));
    }

    private int rows(String query)
    {
        return ((ResultMessage.Rows) execute(systemClientState, query)).result.size();
    }

    /**
     * Without a vector failure threshold, vectors of any size are indexed, in memtables, flushed sstables and compacted
     * sstables alike.
     */
    @Test
    public void testDisabledVectorGuardrailIndexesAnySize() throws Throwable
    {
        try
        {
            guardrails().setSaiVectorTermSizeThreshold(null, null);
            checkVectorIndexing(20_000, true);
        }
        finally
        {
            guardrails().setSaiVectorTermSizeThreshold("16KiB", "32KiB");
        }
    }

    /**
     * Vectors over the maximum size set by the legacy property are rejected for clients and left out of the index in
     * memtables, flushed sstables and compacted sstables alike.
     */
    @Test
    public void testMaxVectorTermSizeProperty() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(SAI_MAX_VECTOR_TERM_SIZE, 16))
        {
            // no warning threshold, so that a client write is only rejected by the max term size
            guardrails().setSaiVectorTermSizeThreshold(null, "32KiB");
            checkVectorIndexing(20_000, false);
        }
        finally
        {
            guardrails().setSaiVectorTermSizeThreshold("16KiB", "32KiB");
        }
    }

    /** Writes two vectors of the given size in bytes internally, and checks whether they are indexed everywhere. */
    private void checkVectorIndexing(int size, boolean indexed) throws Throwable
    {
        int dimensions = size / Float.BYTES;
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, " + dimensions + ">)");
        createIndex("CREATE INDEX ON %s (v) USING 'sai' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        disableCompaction();

        VectorType<Float> type = VectorType.getInstance(FloatType.instance, dimensions);
        ByteBuffer vector = type.decompose(Floats.asList(new float[dimensions]));
        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        String query = "SELECT k FROM %s ORDER BY v ANN OF ? LIMIT 10";

        if (!indexed)
        {
            assertThatThrownBy(() -> execute(insert, ByteBufferUtil.bytes(0), vector))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageContaining("Term of column v exceeds the byte limit for index");
        }

        execute(systemClientState, insert, Arrays.asList(ByteBufferUtil.bytes(0), vector));
        assertEquals(indexed ? 1 : 0, rows(query, vector));
        flush();
        assertEquals(indexed ? 1 : 0, rows(query, vector));
        execute(systemClientState, insert, Arrays.asList(ByteBufferUtil.bytes(1), vector));
        flush();
        assertEquals(indexed ? 2 : 0, rows(query, vector));
        compact();
        assertEquals(indexed ? 2 : 0, rows(query, vector));
    }

    private int rows(String query, ByteBuffer value)
    {
        return ((ResultMessage.Rows) execute(systemClientState, query, Collections.singletonList(value))).result.size();
    }

    private void assertValid(String query, ByteBuffer value) throws Throwable
    {
        assertValid(() -> execute(query, value));
    }

    /** Checks the rows the index finds for the value, in the memtable and then in an sstable. */
    private void assertIndexedRows(ByteBuffer value, int expected) throws Throwable
    {
        beforeAndAfterFlush(() -> {
            ResultMessage.Rows rows = (ResultMessage.Rows) execute("SELECT k FROM %s WHERE v = ?", value);
            assertEquals(expected, rows.result.size());
        });
    }
}
