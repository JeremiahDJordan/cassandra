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

package org.apache.cassandra.index.sai.disk.apache;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.mutable.MutableLong;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.trie.TrieTermsDictionaryWriter;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;

/**
 * Writes the terms dictionary and posting lists of a literal index segment the way Apache Cassandra 5.0 does
 * (cassandra-5.0.9 {@code LiteralIndexWriter}).
 */
public class ApacheLiteralIndexWriter
{
    private ApacheLiteralIndexWriter()
    {
    }

    public static SegmentMetadata.ComponentMetadataMap writeCompleteSegment(IndexComponents.ForWrite components, TermsIterator terms) throws IOException
    {
        SegmentMetadata.ComponentMetadataMap metadata = new SegmentMetadata.ComponentMetadataMap();

        try (TrieTermsDictionaryWriter termsDictionaryWriter = new TrieTermsDictionaryWriter(components);
             ApachePostingsWriter postingsWriter = new ApachePostingsWriter(components))
        {
            // Terms and postings writers are opened in append mode with pointers at the end of their respective files.
            long termsOffset = termsDictionaryWriter.getStartOffset();
            long postingsOffset = postingsWriter.getStartOffset();

            while (terms.hasNext())
            {
                var term = terms.next();
                try (PostingList postings = terms.postings())
                {
                    long offset = postingsWriter.write(postings);
                    termsDictionaryWriter.add(term, offset);
                }
            }
            MutableLong footerPointer = new MutableLong();
            long termsRoot = termsDictionaryWriter.complete(footerPointer);
            postingsWriter.complete();

            long termsLength = termsDictionaryWriter.getFilePointer() - termsOffset;
            long postingsLength = postingsWriter.getFilePointer() - postingsOffset;

            Map<String, String> map = new HashMap<>(2);
            map.put(SAICodecUtils.FOOTER_POINTER, footerPointer.getValue().toString());

            // Postings list file pointers are stored directly in TERMS_DATA, so a root is not needed.
            metadata.put(IndexComponentType.POSTING_LISTS, -1, postingsOffset, postingsLength);
            metadata.put(IndexComponentType.TERMS_DATA, termsRoot, termsOffset, termsLength, map);
        }
        return metadata;
    }
}
