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

package org.apache.cassandra.index.sai.analyzer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Locale;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.analyzer.filter.BasicResultFilters;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the non-tokenizing analyzer
 */
public class NonTokenizingAnalyzerTest
{
    @Test
    public void asciiAnalyzer() throws Exception
    {
        NonTokenizingOptions options = NonTokenizingOptions.getDefaultOptions();
        options.setCaseSensitive(false);
        options.setAscii(true);
        NonTokenizingAnalyzer analyzer = new NonTokenizingAnalyzer(UTF8Type.instance, options);

        String testString = "Éppinger";
        ByteBuffer toAnalyze = ByteBuffer.wrap(testString.getBytes());
        analyzer.reset(toAnalyze);
        ByteBuffer analyzed = null;

        while (analyzer.hasNext())
        {
            analyzed = analyzer.next();
        }

        String good = "eppinger";

        String result = ByteBufferUtil.string(analyzed);

        assertEquals(good, result);
    }

    @Test
    public void asciiAnalyzerFalse() throws Exception
    {
        NonTokenizingOptions options = NonTokenizingOptions.getDefaultOptions();
        options.setCaseSensitive(true);
        options.setAscii(false);
        NonTokenizingAnalyzer analyzer = new NonTokenizingAnalyzer(UTF8Type.instance, options);

        String testString = "Éppinger";
        ByteBuffer toAnalyze = ByteBuffer.wrap(testString.getBytes());
        analyzer.reset(toAnalyze);
        ByteBuffer analyzed = null;

        while (analyzer.hasNext())
        {
            analyzed = analyzer.next();
        }

        String good = "Éppinger";

        String result = ByteBufferUtil.string(analyzed);

        assertEquals(good, result);
    }

    @Test
    public void caseInsensitiveAnalyzer() throws Exception
    {
        NonTokenizingOptions options = NonTokenizingOptions.getDefaultOptions();
        options.setCaseSensitive(false);
        NonTokenizingAnalyzer analyzer = new NonTokenizingAnalyzer(UTF8Type.instance, options);

        String testString = "Nip it in the bud";
        ByteBuffer toAnalyze = ByteBuffer.wrap(testString.getBytes());
        analyzer.reset(toAnalyze);
        ByteBuffer analyzed = null;
        
        while (analyzer.hasNext())
        {
            analyzed = analyzer.next();
        }
        
        assertEquals(testString.toLowerCase(), ByteBufferUtil.string(analyzed));
    }

    @Test
    public void caseSensitiveAnalyzer() throws Exception
    {
        NonTokenizingOptions options = NonTokenizingOptions.getDefaultOptions();
        NonTokenizingAnalyzer analyzer = new NonTokenizingAnalyzer(UTF8Type.instance, options);

        String testString = "Nip it in the bud";
        ByteBuffer toAnalyze = ByteBuffer.wrap(testString.getBytes());
        analyzer.reset(toAnalyze);
        ByteBuffer analyzed = null;
        
        while (analyzer.hasNext())
        {
            analyzed = analyzer.next();
        }
        
        assertNotEquals(testString.toLowerCase(), ByteBufferUtil.string(analyzed));
    }

    @Test
    public void testMaxTermsSize()
    {
        assertEquals(30, factory(Map.of("case_sensitive", "false")).maxTermsSize(10));
        assertEquals(30, factory(Map.of("normalize", "true")).maxTermsSize(10));
        assertEquals(20, factory(Map.of("ascii", "true")).maxTermsSize(10));
        assertEquals(180, factory(Map.of("case_sensitive", "false", "normalize", "true", "ascii", "true")).maxTermsSize(10));
        assertEquals(180, factory(Map.of("case_sensitive", "false", "normalize", "true", "ascii", "true")).maxTermSize(10));
    }

    /**
     * The expansion of each filter on every code point, at most the factor of {@link NonTokenizingAnalyzer#maxExpansion()}:
     * lower-casing in the locales with special rules, also before an accent, NFC normalization and ASCII folding.
     */
    @Test
    public void testFilterExpansions()
    {
        Locale[] locales = { Locale.ROOT, new Locale("lt"), new Locale("tr"), new Locale("az") };
        char[] folded = new char[4];
        for (int codePoint = 0; codePoint <= Character.MAX_CODE_POINT; codePoint++)
        {
            if (Character.getType(codePoint) == Character.SURROGATE)
                continue;

            String value = new String(Character.toChars(codePoint));
            int size = utf8Size(value);
            for (Locale locale : locales)
            {
                assertTrue(value, utf8Size(value.toLowerCase(locale)) <= 3 * size);
                assertTrue(value, utf8Size((value + '\u0300').toLowerCase(locale)) - 2 <= 3 * size);
            }
            assertTrue(value, utf8Size(Normalizer.normalize(value, Normalizer.Form.NFC)) <= 3 * size);
            if (value.length() == 1)
            {
                int length = BasicResultFilters.foldToASCII(value.toCharArray(), 0, folded, 0, 1);
                assertTrue(value, utf8Size(new String(folded, 0, length)) <= 2 * size);
            }
        }
    }

    private static AbstractAnalyzer.AnalyzerFactory factory(Map<String, String> options)
    {
        return AbstractAnalyzer.fromOptions("v", UTF8Type.instance, options);
    }

    private static int utf8Size(String value)
    {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }
}
