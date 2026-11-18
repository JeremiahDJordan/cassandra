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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Predicate;
import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;

import org.apache.cassandra.cql3.LikePatterns;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.FeatureNeedsIndexRebuildException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.IndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.InvertedIndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.TermsReader;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.metrics.MulticastQueryEventListeners;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.index.sai.utils.AutomatonQueries;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

/**
 * Searches a segment of a literal index written by Apache Cassandra 5.0: a trie terms dictionary of Apache's
 * byte-comparable terms (see {@link ApacheTermType}), with Apache's postings. Ranges, {@code LIKE} and ordering work on
 * the values decoded from the terms.
 */
public class ApacheLiteralIndexSearcher extends IndexSearcher
{
    private final TermsReader reader;
    private final ApacheTermType termType;
    private final QueryEventListener.TrieIndexEventListener perColumnEventListener;

    ApacheLiteralIndexSearcher(SSTableContext sstableContext,
                               PerIndexFiles perIndexFiles,
                               SegmentMetadata segmentMetadata,
                               IndexContext indexContext) throws IOException
    {
        super(sstableContext.primaryKeyMapFactory(), perIndexFiles, segmentMetadata, indexContext);
        this.termType = ApacheTermType.of(indexContext.getValidator());
        this.perColumnEventListener = (QueryEventListener.TrieIndexEventListener) indexContext.getColumnQueryMetrics();

        SegmentMetadata.ComponentMetadata termsMetadata = segmentMetadata.componentMetadatas.get(IndexComponentType.TERMS_DATA);
        Map<String, String> attributes = termsMetadata.attributes;
        String footerPointer = attributes.get(SAICodecUtils.FOOTER_POINTER);
        this.reader = new TermsReader(indexContext,
                                      perIndexFiles.termsData(),
                                      ApacheTermType.VERSION,
                                      perIndexFiles.postingLists(),
                                      termsMetadata.root,
                                      footerPointer == null ? -1 : Long.parseLong(footerPointer),
                                      Version.APACHE_AA);
    }

    @Override
    public long indexFileCacheSize()
    {
        return 0;
    }

    @Override
    public KeyRangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange, QueryContext context, boolean defer) throws IOException
    {
        return toPrimaryKeyIterator(searchPostings(expression, context), context);
    }

    private PostingList searchPostings(Expression expression, QueryContext context)
    {
        QueryEventListener.TrieIndexEventListener listener = MulticastQueryEventListeners.of(context, perColumnEventListener);
        switch (expression.getOp())
        {
            case EQ:
            case MATCH:
            case CONTAINS_KEY:
            case CONTAINS_VALUE:
                return reader.exactMatch(termType.asComparableBytes(expression.lower.value.encoded), listener, context);
            case RANGE:
            {
                // The terms of strings are ordered like the values, so the scan can be bounded by the terms of the
                // bounds (the filter applies their inclusiveness). Other terms are filtered from all of them.
                boolean bounded = termType.isOrderedString();
                ByteComparable lower = bounded && expression.lower != null ? termType.asComparableBytes(expression.lower.value.encoded) : null;
                ByteComparable upper = bounded && expression.upper != null ? termType.asComparableBytes(expression.upper.value.encoded) : null;
                return reader.filteredRangeMatch(decoding(expression::isSatisfiedBy), lower, upper, listener, context);
            }
            case PREFIX:
            {
                byte[] prefix = ByteBufferUtil.getArray(expression.lower.value.encoded);
                return patternMatch(prefix, value -> startsWith(value, prefix), listener, context);
            }
            case AUTOMATON:
            {
                byte[] prefix = expression.getAutomatonCommonPrefix();
                return patternMatch(prefix, value -> startsWith(value, prefix) && LikePatterns.accepts(expression.getAutomaton(), value), listener, context);
            }
            default:
                throw new IllegalArgumentException(indexContext.logMessage("Unsupported expression: " + expression));
        }
    }

    /**
     * The postings of the terms whose value starts with the provided prefix and is accepted by the provided filter.
     */
    private PostingList patternMatch(byte[] prefix, Predicate<ByteBuffer> filter, QueryEventListener.TrieIndexEventListener listener, QueryContext context)
    {
        ByteComparable lower = null, upper = null;
        if (termType.isOrderedString() && prefix.length > 0)
        {
            // All the values starting with the prefix are in [prefix, nextOf(prefix)), and so are their terms.
            lower = termType.asComparableBytes(ByteBuffer.wrap(prefix));
            byte[] upperBytes = AutomatonQueries.prefixUpperBound(prefix);
            upper = upperBytes == null ? null : termType.asComparableBytes(ByteBuffer.wrap(upperBytes));
        }
        return reader.filteredRangeMatch(decoding(filter), lower, upper, listener, context);
    }

    private Predicate<byte[]> decoding(Predicate<ByteBuffer> valueFilter)
    {
        return term -> valueFilter.test(termType.decodeLiteral(term));
    }

    private static boolean startsWith(ByteBuffer value, byte[] prefix)
    {
        return value.remaining() >= prefix.length
               && ByteBufferUtil.compareSubArrays(value, value.position(), ByteBuffer.wrap(prefix), 0, prefix.length) == 0;
    }

    @Override
    public CloseableIterator<PrimaryKeyWithSortKey> orderBy(Orderer orderer, Expression slice, AbstractBounds<PartitionPosition> keyRange, QueryContext context, int limit) throws IOException
    {
        if (orderer.isBM25())
            throw new FeatureNeedsIndexRebuildException(String.format(QueryController.INDEX_VERSION_DOES_NOT_SUPPORT_BM25, indexContext.getIndexName()));

        Predicate<ByteBuffer> filter = null;
        if (slice != null && slice.getOp() == Expression.Op.PREFIX)
        {
            byte[] prefix = ByteBufferUtil.getArray(slice.lower.value.encoded);
            filter = value -> startsWith(value, prefix);
        }
        else if (slice != null && slice.getOp() == Expression.Op.AUTOMATON)
        {
            filter = value -> LikePatterns.accepts(slice.getAutomaton(), value);
        }

        // The terms must come in the order of the raw bytes of the values (how our terms, and those of memtables,
        // compare, see IndexSearcher#encode). Apache's terms are in that order (strings and frozen values are
        // escaped, booleans are a single byte), except that a reversed type stores them in reverse.
        boolean ascendingTerms = orderer.isAscending() != termType.isReversed();
        TermsIterator terms = new DecodedTerms(reader.allTerms(ascendingTerms), termType, filter);
        return toMetaSortedIterator(new InvertedIndexSearcher.RowIdWithTermsIterator(terms), context);
    }

    @Override
    public CloseableIterator<PrimaryKeyWithSortKey> orderResultsBy(SSTableReader reader, QueryContext context, List<PrimaryKey> keys, Orderer orderer, int limit) throws IOException
    {
        if (orderer.isBM25())
            throw new InvalidRequestException(String.format(QueryController.INDEX_VERSION_DOES_NOT_SUPPORT_BM25, indexContext.getIndexName()));
        return super.orderResultsBy(reader, context, keys, orderer, limit);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this).add("indexContext", indexContext).toString();
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(reader);
    }

    /**
     * The terms of a terms dictionary, with the value of each term in place of the term (encoded like
     * {@code IndexSearcher#encode} does), and only those accepted by a filter, if any.
     */
    private static class DecodedTerms implements TermsIterator
    {
        private final TermsIterator terms;
        private final ApacheTermType termType;
        @Nullable
        private final Predicate<ByteBuffer> filter;
        private ByteComparable next;

        DecodedTerms(TermsIterator terms, ApacheTermType termType, @Nullable Predicate<ByteBuffer> filter)
        {
            this.terms = terms;
            this.termType = termType;
            this.filter = filter;
        }

        @Override
        public boolean hasNext()
        {
            while (next == null && terms.hasNext())
            {
                ByteBuffer value = termType.decodeLiteral(ByteSourceInverse.readBytes(terms.next().asComparableBytes(ApacheTermType.VERSION)));
                if (filter == null || filter.test(value))
                    next = v -> ByteSource.preencoded(value);
            }
            return next != null;
        }

        @Override
        public ByteComparable next()
        {
            if (!hasNext())
                throw new NoSuchElementException();
            ByteComparable term = next;
            next = null;
            return term;
        }

        @Override
        public PostingList postings() throws IOException
        {
            return terms.postings();
        }

        @Override
        public ByteBuffer getMinTerm()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteBuffer getMaxTerm()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException
        {
            terms.close();
        }
    }
}
