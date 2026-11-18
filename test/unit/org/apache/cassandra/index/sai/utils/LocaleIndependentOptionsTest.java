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

package org.apache.cassandra.index.sai.utils;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.db.filter.ANNOptions;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.analyzer.AnalyzerEqOperatorSupport;
import org.apache.cassandra.index.sai.analyzer.JSONAnalyzerParser;
import org.apache.cassandra.index.sai.analyzer.LuceneAnalyzer;
import org.apache.cassandra.index.sai.analyzer.filter.BuiltInAnalyzers;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.vector.OptimizeFor;
import org.apache.cassandra.index.sai.disk.vector.VectorSourceModel;
import org.apache.cassandra.schema.IndexMetadata;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SAI index options are case-insensitive, and must be parsed the same whatever the JVM's default locale is
 * (CASSANDRA-19953). In Turkish and Azeri, {@code "i".toUpperCase()} is {@code "İ"}, so for example
 * {@code "cosine".toUpperCase()} isn't {@code "COSINE"}.
 */
public class LocaleIndependentOptionsTest
{
    private static final Locale TURKISH = Locale.forLanguageTag("tr");

    private Locale defaultLocale;

    @Before
    public void setTurkishLocale()
    {
        defaultLocale = Locale.getDefault();
        Locale.setDefault(TURKISH);
        // make sure the locale actually changes upper-casing, or this test would prove nothing
        assertThat("cosine".toUpperCase()).isNotEqualTo("COSINE");
    }

    @After
    public void restoreLocale()
    {
        Locale.setDefault(defaultLocale);
    }

    @Test
    public void testSimilarityFunction()
    {
        for (VectorSimilarityFunction function : VectorSimilarityFunction.values())
        {
            Map<String, String> options = new HashMap<>();
            options.put(IndexWriterConfig.SIMILARITY_FUNCTION, function.name().toLowerCase(Locale.US));
            assertThat(IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options).getSimilarityFunction())
            .isEqualTo(function);
        }
    }

    @Test
    public void testSourceModel()
    {
        for (VectorSourceModel model : VectorSourceModel.values())
        {
            String option = model.name().toLowerCase(Locale.US);
            assertThat(VectorSourceModel.fromString(option)).isEqualTo(model);

            Map<String, String> options = new HashMap<>();
            options.put(IndexWriterConfig.SOURCE_MODEL, option);
            assertThat(IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options).getSourceModel())
            .isEqualTo(model);
        }
    }

    @Test
    public void testOptimizeFor()
    {
        for (OptimizeFor optimizeFor : OptimizeFor.values())
            assertThat(OptimizeFor.fromString(optimizeFor.name().toLowerCase(Locale.US))).isEqualTo(optimizeFor);
    }

    @Test
    public void testEqualsBehaviourWhenAnalyzed()
    {
        for (AnalyzerEqOperatorSupport.Value value : AnalyzerEqOperatorSupport.Value.values())
        {
            Map<String, String> options = new HashMap<>();
            options.put(LuceneAnalyzer.INDEX_ANALYZER, "standard");
            options.put(AnalyzerEqOperatorSupport.OPTION, value.name().toLowerCase(Locale.US));
            assertThat(AnalyzerEqOperatorSupport.fromMap(options)).isEqualTo(value);
        }
    }

    @Test
    public void testIndexClassAliases()
    {
        // IndexMetadata registers its aliases in a static initializer, which this test class doesn't trigger before
        // switching the locale, so this also covers the registration of the aliases
        String sai = StorageAttachedIndex.class.getCanonicalName();
        for (String alias : new String[]{ "sai", "SAI", "StorageAttachedIndex", "storageattachedindex", "STORAGEATTACHEDINDEX" })
            assertThat(IndexMetadata.expandAliases(alias)).as(alias).isEqualTo(sai);
    }

    @Test
    public void testUsePruning()
    {
        for (String value : new String[]{ "true", "TRUE", "false", "FALSE" })
            assertThat(ANNOptions.fromMap(Map.of(ANNOptions.USE_PRUNING_OPTION_NAME, value)).usePruning)
            .isEqualTo(Boolean.parseBoolean(value));
    }

    @Test
    public void testBuiltInAnalyzers() throws Exception
    {
        for (BuiltInAnalyzers analyzer : BuiltInAnalyzers.values())
        {
            // a built-in analyzer is returned without a custom analyzer config
            assertThat(JSONAnalyzerParser.parse(analyzer.name().toLowerCase(Locale.US)).right)
            .as("Built-in analyzer %s", analyzer)
            .isNull();
        }
    }
}
