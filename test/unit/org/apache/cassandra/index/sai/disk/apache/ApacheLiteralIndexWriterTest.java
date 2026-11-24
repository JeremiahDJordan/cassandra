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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.inject.Expression.expr;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Literal indexes and per-sstable components written in Apache Cassandra 5.0's format, read back.
 */
public class ApacheLiteralIndexWriterTest extends SAITester
{
    private Injection writeApacheFormat;

    @Before
    public void writeApacheFormat() throws Throwable
    {
        writeApacheFormat = Injections.newCustom("write_apache_format")
                                      .add(InvokePointBuilder.newInvokePoint().onClass(IndexDescriptor.class).onMethod("versionForNewComponents").atEntry())
                                      .add(ActionBuilder.newActionBuilder().actions().doReturn(expr(Version.class.getName() + ".APACHE_AA")))
                                      .build();
        Injections.inject(writeApacheFormat);
    }

    @After
    public void stopApacheFormat()
    {
        writeApacheFormat.disable();
    }

    @Test
    public void testSkinny() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, s text, fl frozen<list<decimal>>, m map<text, decimal>)");
        String s = createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        String fl = createIndex("CREATE CUSTOM INDEX ON %s(FULL(fl)) USING 'StorageAttachedIndex'");
        String m = createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(m)) USING 'StorageAttachedIndex'");
        // posting lists of several blocks, and decimals of a scale other than that of their canonical form (2.5
        // against 2.50), which Apache indexes as written
        for (int pk = 0; pk < 300; pk++)
        {
            BigDecimal decimal = new BigDecimal(pk % 3 + ".5");
            execute("INSERT INTO %s (pk, s, fl, m) VALUES (?, ?, ?, ?)", pk, "s" + (pk % 2), List.of(decimal), Map.of("k", decimal));
        }
        flush();

        assertWrittenInApacheFormat(s, fl, m);
        assertRowCount(execute("SELECT pk FROM %s WHERE s = 's1'"), 150);
        assertRows(execute("SELECT pk FROM %s WHERE s = 's1' AND pk = 251"), row(251));
        assertRowCount(execute("SELECT pk FROM %s WHERE fl = ?", List.of(new BigDecimal("2.5"))), 100);
        assertRowCount(execute("SELECT pk FROM %s WHERE m['k'] = ?", new BigDecimal("2.5")), 100);
    }

    @Test
    public void testWide() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck bigint, s text, PRIMARY KEY (pk, ck))");
        String index = createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        for (int pk = 0; pk < 20; pk++)
            for (long ck = 0; ck < 5; ck++)
                execute("INSERT INTO %s (pk, ck, s) VALUES (?, ?, ?)", pk, ck, "s" + ck);
        // several segments per index
        long lastValidSegmentRowId = SegmentBuilder.updateLastValidSegmentRowId(30);
        try
        {
            flush();
        }
        finally
        {
            SegmentBuilder.updateLastValidSegmentRowId(lastValidSegmentRowId);
        }

        assertWrittenInApacheFormat(index);
        assertRowCount(execute("SELECT pk, ck FROM %s WHERE s = 's3'"), 20);
        assertRows(execute("SELECT pk, ck FROM %s WHERE s = 's3' AND pk = 7"), row(7, 3L));
    }

    @Test
    public void testClusteringWithNullFields() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck tuple<int, int>, ck2 frozen<list<tuple<int, int>>>, a text, b text, PRIMARY KEY (pk, ck, ck2))");
        String a = createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        String b = createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, ck, ck2, a, b) VALUES (0, (null, 3), [(null, 1)], 'x', 'y')");
        execute("INSERT INTO %s (pk, ck, ck2, a, b) VALUES (0, (null, 2), [(null, 1)], 'y', 'x')");
        execute("INSERT INTO %s (pk, ck, ck2, a, b) VALUES (0, (null, 1), [(null, 1)], 'x', 'x')");
        flush();

        assertWrittenInApacheFormat(a, b);
        assertRows(execute("SELECT ck, ck2 FROM %s WHERE a = 'x' AND b = 'x'"), row(tuple(null, 1), list(tuple(null, 1))));

        // keys from other sstables look their rows up by clustering
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());
        SSTableContext context = SSTableContext.create(sstable, StorageAttachedIndexGroup.getIndexGroup(cfs).descriptorFor(sstable).perSSTableComponents());
        try
        {
            List<PrimaryKey> keys = new ArrayList<>();
            try (PrimaryKeyMap map = context.primaryKeyMapFactory().newPerSSTablePrimaryKeyMap())
            {
                for (long rowId = 0; rowId < map.count(); rowId++)
                {
                    PrimaryKey key = map.primaryKeyFromRowId(rowId).loadDeferred();
                    keys.add(context.primaryKeyFactory().create(key.partitionKey(), key.clustering()));
                }
            }
            assertThat(keys).hasSize(3);
            try (PrimaryKeyMap map = context.primaryKeyMapFactory().newPerSSTablePrimaryKeyMap())
            {
                for (int rowId = 0; rowId < keys.size(); rowId++)
                    assertThat(map.exactRowIdOrInvertedCeiling(keys.get(rowId))).as("row %d", rowId).isEqualTo(rowId);
            }
        }
        finally
        {
            context.close();
        }
    }

    private void assertWrittenInApacheFormat(String... indexes)
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        assertThat(cfs.getLiveSSTables()).isNotEmpty();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IndexDescriptor descriptor = group.descriptorFor(sstable);
            assertThat(descriptor.perSSTableComponents().version()).isEqualTo(Version.APACHE_AA);
            assertThat(descriptor.perSSTableComponents().isValid(true)).isTrue();
            for (String index : indexes)
            {
                IndexComponents.ForRead components = descriptor.perIndexComponents(((StorageAttachedIndex) cfs.indexManager.getIndexByName(index)).getIndexContext());
                assertThat(components.version()).isEqualTo(Version.APACHE_AA);
                assertThat(components.isComplete()).isTrue();
                assertThat(components.isValid(true)).isTrue();
            }
        }
    }
}
