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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SAI ANN indexes can only be created on vectors of floats. Vectors of other types only exist with
 * {@code cassandra.float_only_vectors=false}: by default, the vector type itself rejects them.
 */
public class VectorNonFloatIndexTest extends SAITester
{
    static
    {
        // read once, when the vector type is first used
        CassandraRelevantProperties.VECTOR_FLOAT_ONLY.setBoolean(false);
    }

    // CQLTester#createIndex doesn't wrap the exception here
    @Test
    public void cannotCreateIndexOnNonFloatVector()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<int, 1>)");

        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'"))
            .isInstanceOf(InvalidRequestException.class).hasMessage(StorageAttachedIndex.VECTOR_NON_FLOAT_ERROR);
    }

    @Test
    public void cannotCreateIndexOnOtherNonFloatVectors()
    {
        createTable("CREATE TABLE %s (k int, c int, i vector<int, 2>, d vector<double, 2>, b vector<bigint, 3>, " +
                    "t vector<text, 2>, s vector<int, 2> static, PRIMARY KEY (k, c))");

        for (String column : new String[]{ "i", "d", "b", "t", "s" })
        {
            assertThatThrownBy(() -> createIndex("CREATE INDEX ON %s(" + column + ") USING 'sai'"))
                .describedAs(column)
                .isInstanceOf(InvalidRequestException.class).hasMessage(StorageAttachedIndex.VECTOR_NON_FLOAT_ERROR);
            assertThatThrownBy(() -> createIndex("CREATE INDEX ON %s(" + column + ") USING 'sai' WITH OPTIONS = {'similarity_function' : 'euclidean'}"))
                .describedAs(column)
                .isInstanceOf(InvalidRequestException.class).hasMessage(StorageAttachedIndex.VECTOR_NON_FLOAT_ERROR);
        }
        assertThat(getCurrentColumnFamilyStore().metadata().indexes.size()).isZero();
    }

    @Test
    public void canCreateIndexOnFloatVector()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 2>, i int)");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        createIndex("CREATE INDEX ON %s(i) USING 'sai'");

        execute("INSERT INTO %s (k, v, i) VALUES (0, [1.0, 2.0], 0)");
        execute("INSERT INTO %s (k, v, i) VALUES (1, [2.0, 1.0], 1)");
        assertRows(execute("SELECT k FROM %s ORDER BY v ANN OF [1.0, 2.0] LIMIT 1"), row(0));
    }

    /**
     * An index on a vector of non-float elements created before the check, such as by an earlier version, keeps
     * working, and doesn't block other schema changes: the check only applies to CREATE INDEX of such an index.
     */
    @Test
    public void existingIndexOnNonFloatVectorIsKept()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<int, 2>, i int)");

        // add the index to the schema without CREATE INDEX, as schema propagation of an index created by an earlier
        // version would
        TableMetadata table = getCurrentColumnFamilyStore().metadata();
        Map<String, String> options = new HashMap<>();
        options.put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
        options.put(IndexTarget.TARGET_OPTION_NAME, "v");
        // with cosine similarity, the default, the int values would be rejected as zero vectors: the vector index reads
        // the elements as floats
        options.put("similarity_function", "euclidean");
        String index = "existing_int_vector_index";
        IndexMetadata indexMetadata = IndexMetadata.fromSchemaMetadata(index, IndexMetadata.Kind.CUSTOM, options);
        SchemaTestUtil.announceTableUpdate(table.withSwapped(table.indexes.with(indexMetadata)));
        waitForIndexQueryable(index);

        execute("INSERT INTO %s (k, v, i) VALUES (0, [1, 2], 0)");
        execute("INSERT INTO %s (k, v, i) VALUES (1, [2, 1], 1)");
        flush();
        assertRowCount(execute("SELECT k FROM %s ORDER BY v ANN OF [1, 2] LIMIT 1"), 1);

        // other DDL on the table, including another index, is still accepted
        execute("ALTER TABLE %s ADD j int");
        execute("ALTER TABLE %s WITH comment = 'still valid'");
        createIndex("CREATE INDEX ON %s(i) USING 'sai'");
        assertThat(getCurrentColumnFamilyStore().metadata().indexes.get(index)).isPresent();
        assertRows(execute("SELECT k FROM %s WHERE i = 1"), row(1));

        // but another index on the column isn't
        assertThatThrownBy(() -> createIndex("CREATE INDEX ON %s(v) USING 'sai' WITH OPTIONS = {'similarity_function' : 'euclidean'}"))
            .isInstanceOf(InvalidRequestException.class).hasMessage(StorageAttachedIndex.VECTOR_NON_FLOAT_ERROR);
    }
}
