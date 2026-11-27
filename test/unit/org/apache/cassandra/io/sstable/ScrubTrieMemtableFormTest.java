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

package org.apache.cassandra.io.sstable;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.memtable.TrieDecodedForm;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.OutputHandler;

import static org.apache.cassandra.config.CassandraRelevantProperties.COMPACTION_REWRITE_TRIE_FORM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Scrub and upgradesstables with the trie form option, and compaction with the trie form property, rewrite the keys of
 * sstables written by a skip-list memtable into the form the table's trie memtable reads them back in, and refuse
 * partitions that hold a key in two forms.
 */
@RunWith(Parameterized.class)
public class ScrubTrieMemtableFormTest extends CQLTester
{
    private static final ByteBuffer EMPTY = ByteBufferUtil.EMPTY_BYTE_BUFFER;

    @Parameterized.Parameter
    public String memtable;

    @Parameterized.Parameter(1)
    public boolean decodesBounds;

    @Parameterized.Parameter(2)
    public boolean decodesCellPaths;

    @Parameterized.Parameters(name = "memtable={0}")
    public static Collection<Object[]> parameters()
    {
        return List.of(new Object[]{ "trie", true, true },
                       new Object[]{ "trie_stage3", true, false },
                       new Object[]{ "trie_stage2", false, false });
    }

    @Test
    public void testScrub() throws Throwable
    {
        writeKeysInBothForms();

        Set<String> sstables = dataFiles();
        List<byte[]> contents = dataFileContents();
        scrub(false, true);
        assertThat(dataFiles()).isNotEqualTo(sstables);
        assertThat(dataFileContents()).containsExactlyElementsOf(contents);
        assertKeysNotRewritten();

        scrub(true, true);
        assertKeysRewritten();
        compact();
        assertKeysRewritten();
    }

    @Test
    public void testScrubWithoutValidation() throws Throwable
    {
        writeKeysInBothForms();
        scrub(true, false);
        assertKeysRewritten();
    }

    @Test
    public void testCompaction() throws Throwable
    {
        writeKeysInBothForms();
        assertThatThrownBy(this::compact).hasStackTraceContaining("Terms must be added in lexicographic ascending order");
        assertKeysNotRewritten();

        try (WithProperties properties = new WithProperties().set(COMPACTION_REWRITE_TRIE_FORM, true))
        {
            compact();
        }
        assertThat(getCurrentColumnFamilyStore().getLiveSSTables()).hasSize(1);
        assertKeysRewritten();
    }

    @Test
    public void testUpgradeSSTables() throws Throwable
    {
        writeKeysInBothForms();
        getCurrentColumnFamilyStore().sstablesRewrite(true, Long.MAX_VALUE, false, true, 1);
        assertKeysRewritten();
    }

    @Test
    public void testUpgradeSSTablesWithOverlappingTombstones() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck frozen<tuple<int, int>>, v int, PRIMARY KEY (pk, ck)) WITH memtable = 'skiplist' " +
                    "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'provide_overlapping_tombstones': 'row'}");
        disableCompaction();
        // the largest sstable, rewritten first, holds a range that the next one's covers up to a bound that decodes
        execute("DELETE FROM %s USING TIMESTAMP 10 WHERE pk = 0 AND ck >= (null, 0) AND ck <= (1, 0)");
        for (int pk = 100; pk < 120; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, (0, 0), 0)", pk);
        flush();
        execute("DELETE FROM %s USING TIMESTAMP 20 WHERE pk = 0 AND ck >= (null, 0) AND ck <= (blobAsInt(0x), 5)");
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (0, (null, 8), 99) USING TIMESTAMP 5");
        flush();
        execute("ALTER TABLE %s WITH memtable = '" + memtable + "'");
        assertEmpty(execute("SELECT v FROM %s WHERE pk = 0"));

        getCurrentColumnFamilyStore().sstablesRewrite(true, Long.MAX_VALUE, false, true, 1);
        assertEmpty(execute("SELECT v FROM %s WHERE pk = 0"));
    }

    @Test
    public void testDecimalKeys() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck decimal, s set<decimal>, PRIMARY KEY (pk, ck)) WITH memtable = 'skiplist'");
        disableCompaction();
        execute("INSERT INTO %s (pk, ck, s) VALUES (0, 1.1, {1.1})");
        flush();
        execute("ALTER TABLE %s WITH memtable = '" + memtable + "'");

        scrub(false, true);
        assertRows(execute("SELECT ck, s FROM %s"), row(new BigDecimal("1.1"), set(new BigDecimal("1.1"))));
        scrub(true, true);
        assertRows(execute("SELECT ck, s FROM %s"), row(new BigDecimal("1.10"), set(new BigDecimal(decodesCellPaths ? "1.10" : "1.1"))));
    }

    @Test
    public void testRefusesKeysInBothForms() throws Throwable
    {
        // a row and a point deletion of the other form of its key
        assertRefused(true,
                      "INSERT INTO %s (pk, ck, v) VALUES (0, (1, null), 1) USING TIMESTAMP 10",
                      "DELETE FROM %s USING TIMESTAMP 20 WHERE pk = 0 AND ck = (1, blobAsInt(0x))");
        // a row before a range whose lower bound ends with an empty value, which decodes to the row's key
        assertRefused(decodesBounds,
                      "INSERT INTO %s (pk, ck, v) VALUES (0, (1, null), 1) USING TIMESTAMP 10",
                      "DELETE FROM %s USING TIMESTAMP 20 WHERE pk = 0 AND ck >= (1, blobAsInt(0x)) AND ck <= (2, 0)");
        // the same range alone, as when the row is in another sstable
        assertRefused(decodesBounds,
                      "DELETE FROM %s USING TIMESTAMP 20 WHERE pk = 0 AND ck >= (1, blobAsInt(0x)) AND ck <= (2, 0)");
        // a row that moves before another row
        assertRefused(true,
                      "INSERT INTO %s (pk, ck, v) VALUES (0, (null, 9), 1)",
                      "INSERT INTO %s (pk, ck, v) VALUES (0, (blobAsInt(0x), 1), 2)");
        // a range that becomes empty
        assertRefused(decodesBounds,
                      "DELETE FROM %s USING TIMESTAMP 20 WHERE pk = 0 AND ck > (null, 1) AND ck <= (blobAsInt(0x), 1)");
        // two map keys that become equal
        assertRefused(decodesCellPaths,
                      "UPDATE %s SET m[(null, 1)] = 1 WHERE pk = 0 AND ck = (0, 0)",
                      "UPDATE %s SET m = m + {(blobAsInt(0x), 1): 2} WHERE pk = 0 AND ck = (0, 0)");
    }

    /**
     * Writes the statements through a skip-list memtable, then checks that rewriting the sstable fails and leaves it in
     * place if {@code refused}, and succeeds otherwise, and that it succeeds once the partition is deleted and garbage
     * collected.
     */
    private void assertRefused(boolean refused, String... statements) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck frozen<tuple<int, int>>, v int, m map<frozen<tuple<int, int>>, int>, " +
                    "PRIMARY KEY (pk, ck)) WITH memtable = 'skiplist'");
        disableCompaction();
        for (String statement : statements)
            execute(statement);
        flush();
        execute("ALTER TABLE %s WITH memtable = '" + memtable + "'");

        if (!refused)
        {
            scrub(true, true);
            return;
        }

        Set<String> sstables = dataFiles();
        assertThatThrownBy(() -> scrub(true, true)).isInstanceOf(TrieDecodedForm.RefusedException.class)
                                                   .hasMessageContaining(KEYSPACE + '.' + currentTable())
                                                   .hasMessageContaining("partition 0 ");
        assertThatThrownBy(() -> getCurrentColumnFamilyStore().scrub(true, IScrubber.options().rewriteTrieForm(true).build(), 1))
        .hasStackTraceContaining(TrieDecodedForm.RefusedException.class.getName());
        assertThatThrownBy(() -> getCurrentColumnFamilyStore().sstablesRewrite(true, Long.MAX_VALUE, false, true, 1))
        .hasStackTraceContaining(TrieDecodedForm.RefusedException.class.getName());
        try (WithProperties properties = new WithProperties().set(COMPACTION_REWRITE_TRIE_FORM, true))
        {
            assertThatThrownBy(() -> getCurrentColumnFamilyStore().forceMajorCompaction())
            .hasStackTraceContaining(TrieDecodedForm.RefusedException.class.getName());
            assertThat(dataFiles()).isEqualTo(sstables);

            // garbage collection doesn't rewrite, even with the property
            execute("DELETE FROM %s WHERE pk = 0");
            flush();
            getCurrentColumnFamilyStore().garbageCollect(CompactionParams.TombstoneOption.ROW, 1);
        }
        scrub(true, true);
    }

    /**
     * Writes, through a skip-list memtable, rows whose clustering keys and map keys hold empty ints in a tuple or user
     * type, and a range tombstone, then switches the table to the trie memtable, deletes and updates them, and indexes
     * the table with SAI.
     */
    private void writeKeysInBothForms() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int, b frozen<tuple<int, int>>)");
        createTable("CREATE TABLE %s (pk int, ck frozen<" + type + ">, v int, s set<frozen<tuple<int, int>>>, " +
                    "m map<frozen<tuple<int, int>>, int>, PRIMARY KEY (pk, ck)) WITH memtable = 'skiplist'");
        disableCompaction();

        execute("INSERT INTO %s (pk, ck, v) VALUES (0, ?, 1)", deleted());
        execute("INSERT INTO %s (pk, ck, v) VALUES (0, ?, 2)", updated());
        execute("INSERT INTO %s (pk, ck, v, s, m) VALUES (0, ?, 3, ?, ?)", withCollections(),
                set(tuple(EMPTY, i(3)), tuple(i(3), i(3))), map(tuple(EMPTY, i(4)), 4, tuple(i(4), i(4)), 44));
        execute("DELETE FROM %s USING TIMESTAMP 100 WHERE pk = 0 AND ck >= ? AND ck <= ?", rangeDeleted(), rangeDeleted());
        flush();

        execute("ALTER TABLE %s WITH memtable = '" + memtable + "'");
        execute("DELETE FROM %s WHERE pk = 0 AND ck = ?", deleted());
        execute("UPDATE %s SET v = 20 WHERE pk = 0 AND ck = ?", updated());
        execute("UPDATE %s SET s = s - ? WHERE pk = 0 AND ck = ?", set(tuple(EMPTY, i(3))), withCollections());
        execute("UPDATE %s SET m[?] = 40 WHERE pk = 0 AND ck = ?", tuple(EMPTY, i(4)), withCollections());
        execute("INSERT INTO %s (pk, ck, v) VALUES (0, ?, 6) USING TIMESTAMP 50", rangeDeleted());
        flush();
        // SAI refuses writes of such keys, so the index is created afterwards
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        assertKeysNotRewritten();
    }

    private static ByteBuffer deleted()
    {
        return udt(EMPTY, tuple(i(1), i(1)));
    }

    private static ByteBuffer updated()
    {
        return udt(i(1), tuple(EMPTY, i(2)));
    }

    private static ByteBuffer withCollections()
    {
        return udt(i(2), tuple(i(2), i(2)));
    }

    private static ByteBuffer rangeDeleted()
    {
        return udt(EMPTY, tuple(i(6), i(6)));
    }

    private void assertKeysNotRewritten() throws Throwable
    {
        assertRowsIgnoringOrder(execute("SELECT v FROM %s"), row(1), row(2), row(20), row(3), row(6));
        assertCollections(decodesCellPaths ? 2 : 1, decodesCellPaths ? new Integer[]{ 4, 40, 44 } : new Integer[]{ 40, 44 });
    }

    private void assertKeysRewritten() throws Throwable
    {
        if (decodesBounds)
            assertRows(execute("SELECT v FROM %s"), row(20), row(3));
        else
            assertRows(execute("SELECT v FROM %s"), row(6), row(20), row(3));
        assertCollections(1, 40, 44);
        assertRows(execute("SELECT pk FROM %s WHERE v = 20"), row(0));
        assertEmpty(execute("SELECT pk FROM %s WHERE v = 2"));
    }

    private void assertCollections(int setSize, Integer... mapValues) throws Throwable
    {
        UntypedResultSet.Row row = execute("SELECT s, m FROM %s WHERE pk = 0 AND ck = ?", withCollections()).one();
        assertThat(row.getSet("s", BytesType.instance)).hasSize(setSize);
        assertThat(row.getMap("m", BytesType.instance, Int32Type.instance).values()).containsExactlyInAnyOrder(mapValues);
    }

    private void scrub(boolean rewriteTrieForm, boolean checkData) throws Throwable
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        IScrubber.Options options = IScrubber.options().checkData(checkData).rewriteTrieForm(rewriteTrieForm).build();
        for (SSTableReader sstable : new ArrayList<>(cfs.getLiveSSTables()))
        {
            try (LifecycleTransaction txn = cfs.getTracker().tryModify(Collections.singletonList(sstable), OperationType.SCRUB);
                 IScrubber scrubber = sstable.descriptor.getFormat().getScrubber(cfs, txn, new OutputHandler.LogOutput(), options))
            {
                assertThat(scrubber.scrubWithResult().badPartitions).as("partitions skipped while scrubbing %s", sstable).isZero();
            }
        }
        assertSSTablesValid();
    }

    private Set<String> dataFiles()
    {
        return getCurrentColumnFamilyStore().getLiveSSTables().stream().map(SSTableReader::getFilename).collect(Collectors.toSet());
    }

    private List<byte[]> dataFileContents() throws Exception
    {
        List<byte[]> contents = new ArrayList<>();
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables().stream()
                                                                  .sorted(Comparator.comparingLong(SSTableReader::getMaxTimestamp))
                                                                  .collect(Collectors.toList()))
            contents.add(Files.readAllBytes(Paths.get(sstable.getFilename())));
        return contents;
    }

    /** Checks that the clusterings of each sstable are values of its header's types, in strictly increasing order. */
    private void assertSSTablesValid()
    {
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
        {
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                while (scanner.hasNext())
                {
                    try (UnfilteredRowIterator partition = scanner.next())
                    {
                        Unfiltered previous = null;
                        while (partition.hasNext())
                        {
                            Unfiltered unfiltered = partition.next();
                            for (int i = 0; i < unfiltered.clustering().size(); i++)
                                sstable.header.clusteringTypes().get(i).validate(unfiltered.clustering().bufferAt(i));
                            if (previous != null)
                                assertThat(sstable.metadata().comparator.compare(previous, unfiltered)).isNegative();
                            previous = unfiltered;
                        }
                    }
                }
            }
        }
    }

    private static ByteBuffer udt(ByteBuffer a, ByteBuffer b)
    {
        return TupleType.buildValue(a, b);
    }

    private static ByteBuffer tuple(ByteBuffer... components)
    {
        return TupleType.buildValue(components);
    }

    private static ByteBuffer i(int value)
    {
        return Int32Type.instance.decompose(value);
    }
}
