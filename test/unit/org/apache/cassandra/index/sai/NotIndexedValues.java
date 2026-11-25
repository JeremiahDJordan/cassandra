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

package org.apache.cassandra.index.sai;

/**
 * A value that SAI writes but doesn't index, which the coordinator accepts.
 */
public final class NotIndexedValues
{
    /** The options of an index whose analyzer makes the grams of 2 to 8 characters of a value. */
    public static final String NGRAM_ANALYZER =
    "{'index_analyzer': '{\"tokenizer\":{\"name\":\"keyword\"}, " +
    "\"filters\":[{\"name\":\"ngram\", \"args\":{\"minGramSize\":\"2\", \"maxGramSize\":\"8\"}}]}'}";

    /** 512 letters, whose grams add up to 17752 bytes, over the max analyzed size. */
    public static final String NGRAM_VALUE = letters(512, 0);

    private NotIndexedValues()
    {
    }

    /** The given number of letters, from the alphabet starting at the given offset. */
    public static String letters(int count, int offset)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++)
            sb.append((char) ('a' + (i + offset) % 26));
        return sb.toString();
    }
}
