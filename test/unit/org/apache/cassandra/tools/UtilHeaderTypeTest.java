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
package org.apache.cassandra.tools;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The types {@link Util#metadataFromSSTable} reads from sstable headers.
 */
public class UtilHeaderTypeTest extends CQLTester
{
    private static final String M = "org.apache.cassandra.db.marshal.";
    // how Apache Cassandra records frozen<tuple<int, text>>, and a DataStax node a dropped non-frozen UDT (x int, y text)
    private static final String SCALAR_TUPLE = M + "TupleType(" + M + "Int32Type," + M + "UTF8Type)";
    // how Apache Cassandra records frozen<tuple<set<int>, set<text>>>
    private static final String COLLECTION_TUPLE = M + "TupleType(" + M + "SetType(" + M + "Int32Type)," + M + "SetType(" + M + "UTF8Type))";
    // how a DataStax node records a dropped non-frozen UDT (name tuple<text, text>)
    private static final String DROPPED_UDT = M + "TupleType(" + M + "FrozenType(" + M + "TupleType(" + M + "UTF8Type," + M + "UTF8Type)))";
    // how Apache Cassandra records a non-frozen UDT (name tuple<text, text>)
    private static final String UDT = M + "UserType(ks,74797065,6e616d65:" + M + "TupleType(" + M + "UTF8Type," + M + "UTF8Type))";

    private static AbstractType<?> fix(boolean tuplesAreFrozen, String type)
    {
        return Util.fixHeaderType(tuplesAreFrozen, UTF8Type.instance.decompose("val"), TypeParser.parse(type), false, false);
    }

    @Test
    public void testOnlyFrozenPlainTuplesVersions()
    {
        SSTableFormat<?, ?> big = DatabaseDescriptor.getSSTableFormats().get("big");
        for (String version : new String[]{ "ma", "mb", "mc", "md", "me", "na" })
            assertThat(Util.hasOnlyFrozenPlainTuples(big.getVersion(version))).as(version).isTrue();
        for (String version : new String[]{ "nb", "oa" })
            assertThat(Util.hasOnlyFrozenPlainTuples(big.getVersion(version))).as(version).isFalse();
        SSTableFormat<?, ?> bti = DatabaseDescriptor.getSSTableFormats().get("bti");
        for (String version : new String[]{ "aa", "cc", "da" })
            assertThat(Util.hasOnlyFrozenPlainTuples(bti.getVersion(version))).as(version).isFalse();
    }

    @Test
    public void testPlainTuples()
    {
        assertThat(fix(true, SCALAR_TUPLE).isMultiCell()).isFalse();
        assertThat(fix(true, COLLECTION_TUPLE).isMultiCell()).isFalse();
        assertThat(fix(true, COLLECTION_TUPLE).subTypes()).allMatch(t -> !t.isMultiCell());

        // without the version, only multi-cell subtypes tell a frozen tuple from a dropped non-frozen UDT
        assertThat(fix(false, SCALAR_TUPLE).isMultiCell()).isTrue();
        assertThat(fix(false, COLLECTION_TUPLE).isMultiCell()).isFalse();
        assertThat(fix(false, DROPPED_UDT).isMultiCell()).isTrue();
    }

    @Test
    public void testNonFrozenUdt()
    {
        for (boolean tuplesAreFrozen : new boolean[]{ true, false })
        {
            AbstractType<?> fixed = fix(tuplesAreFrozen, UDT);
            assertThat(fixed.isUDT()).isTrue();
            assertThat(fixed.isMultiCell()).isTrue();
            assertThat(fixed.subTypes()).allMatch(t -> !t.isMultiCell());
        }
    }

    /**
     * A node that opens an sstable after a non-frozen UDT column is dropped records the column as a multi-cell tuple in
     * the headers it writes, which must stay complex for the other columns to be read.
     */
    @Test
    public void testDroppedNonFrozenUdt() throws Throwable
    {
        String udt = createType("CREATE TYPE %s (x int, y text)");
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, a int, u " + udt + ", z int)");
        disableCompaction();
        execute("INSERT INTO %s (pk, a, u, z) VALUES (0, 1, {x: 1, y: 'one'}, 10)");
        flush();
        execute("ALTER TABLE %s DROP u");
        execute("INSERT INTO %s (pk, a, z) VALUES (1, 2, 20)");
        flush();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        // reopen the sstables with the schema after the drop, as a restart does
        cfs.clearUnsafe();
        cfs.loadNewSSTables();
        cfs.forceMajorCompaction();

        assertThat(cfs.getLiveSSTables()).hasSize(1);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        TableMetadata metadata = Util.metadataFromSSTable(sstable.descriptor);
        ColumnMetadata u = metadata.getColumn(UTF8Type.instance.decompose("u"));
        assertThat(u.type.isTuple()).isTrue();
        assertThat(u.isComplex()).isTrue();
    }
}
