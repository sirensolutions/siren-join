/**
 * Copyright (c) 2016, SIREn Solutions. All Rights Reserved.
 *
 * This file is part of the SIREn project.
 *
 * SIREn is a free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * SIREn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package solutions.siren.join.index.query;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LRUQueryCache;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.UsageTrackingQueryCachingPolicy;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.RamUsageEstimator;

import org.elasticsearch.common.logging.Loggers;

import solutions.siren.join.action.terms.collector.BytesRefTermsSet;
import solutions.siren.join.action.terms.collector.TermsSet;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * Specialization for a disjunction over many terms, encoded in a byte array, which scans the dictionary
 * using a {@link TermsEnum} to collect documents ids.
 * It behaves like a {@link ConstantScoreQuery} over a {@link BooleanQuery} containing only
 * {@link org.apache.lucene.search.BooleanClause.Occur#SHOULD} clauses.
 */
public class TermsEnumTermsQuery extends Query implements Accountable {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TermsEnumTermsQuery.class);

  /**
   * Reference to the encoded list of terms for late decoding.
   */
  private byte[] encodedTerms;

  /**
   * The set of terms after decoding
   */
  private BytesRefTermsSet termsSet;

  /**
   * The field to enumerate
   */
  protected String field;

  /**
   * The cache key for this query
   */
  protected final long cacheKey;

  private static final Logger logger = Loggers.getLogger(TermsEnumTermsQuery.class);

  /**
   * Creates a new {@link TermsEnumTermsQuery} from the given field data.
   */
  public TermsEnumTermsQuery(final byte[] encodedTerms, final String field, final long cacheKey) {
    this.encodedTerms = encodedTerms;
    this.cacheKey = cacheKey;
    this.field = field;
  }

  @Override
  public long ramBytesUsed() {
    BytesRefTermsSet termsSet = this.getTermsSet();
    return BASE_RAM_BYTES_USED + termsSet.size() * 8;
  }

  @Override
  public String toString(String defaultField) {
    BytesRefTermsSet termsSet = this.getTermsSet();
    final StringBuilder sb = new StringBuilder("TermsEnumTermsQuery:");
    return sb
            .append(defaultField)
            .append(":")
            // Do not serialise the full array, but instead the number of elements - see issue #168
            .append("[size=" + termsSet.size() + "]")
            .toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (cacheKey != ((TermsEnumTermsQuery) obj).cacheKey) { // relies on the cache key instead of the encodedTerms for equality
      return false;
    }
    if (!field.equals(((TermsEnumTermsQuery) obj).field)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hashcode = 31 * ((int) cacheKey); // relies on the cache key instead of the encodedTerms for hashcode
    hashcode = 31 * hashcode + field.hashCode();
    return hashcode;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }

  /**
   * Returns the set of terms. This method will perform a late-decoding of the encoded terms, and will release the
   * byte array. This method needs to be synchronized as each segment thread will call it concurrently.
   */
  protected synchronized BytesRefTermsSet getTermsSet() {
    if (encodedTerms != null) { // late decoding of the encoded terms
      long start = System.nanoTime();
      termsSet = (BytesRefTermsSet) TermsSet.readFrom(new BytesRef(encodedTerms));
      logger.debug("{}: Deserialized {} terms - took {} ms", new Object[] { Thread.currentThread().getName(), termsSet.size(), (System.nanoTime() - start) / 1000000 });
      encodedTerms = null; // release reference to the byte array to be able to reclaim memory
    }
    return termsSet;
  }

  public DocIdSet getDocIdSet(LeafReaderContext context) throws IOException {
    final Terms terms = context.reader().terms(field);
    // make sure the field exists
    if (terms == null) return null;

    final BytesRefTermsSet termsSet = this.getTermsSet();
    // make sure there are terms to filter on
    if (termsSet == null || termsSet.isEmpty()) return null;

    SeekingTermSetTermsEnum termsEnum = new SeekingTermSetTermsEnum(terms.iterator(), termsSet);

    DocIdSetBuilder builder = new DocIdSetBuilder(context.reader().maxDoc());
    PostingsEnum docs = null;
    while (termsEnum.next() != null) {
      docs = termsEnum.postings(docs, PostingsEnum.NONE);
      builder.add(docs);
    }

    return builder.build();
  }

  @Override
  public Weight createWeight(final IndexSearcher searcher, final boolean needsScores) throws IOException {
    return new ConstantScoreWeight(new CacheKeyFieldDataTermsQuery(cacheKey)) {

      @Override
      public void extractTerms(Set<Term> terms) {
        // no-op
        // This query is for abuse cases when the number of terms is too high to
        // run efficiently as a BooleanQuery. So likewise we hide its terms in
        // order to protect highlighters
      }

      private Scorer scorer(DocIdSet set) throws IOException {
        if (set == null) {
          return null;
        }
        final DocIdSetIterator disi = set.iterator();
        if (disi == null) {
          return null;
        }
        return new ConstantScoreScorer(this, score(), disi);
      }

      @Override
      public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
        final Scorer scorer = scorer(TermsEnumTermsQuery.this.getDocIdSet(context));
        if (scorer == null) {
          return null;
        }
        return new DefaultBulkScorer(scorer);
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        return scorer(TermsEnumTermsQuery.this.getDocIdSet(context));
      }
    };
  }

  /**
   * <p>
   *   This query will be returned by the {@link ConstantScoreWeight} instead of the {@link TermsEnumTermsQuery}
   *   and used by the
   *   {@link LRUQueryCache.CachingWrapperWeight} to cache the query.
   *   This is necessary in order to avoid caching the byte array and long hash set, which is not memory friendly
   *   and not very efficient.
   * </p>
   * <p>
   *   Extends MultiTermQuery in order to be detected as "costly" query by {@link UsageTrackingQueryCachingPolicy}
   *   and trigger early caching.
   * </p>
   */
  private static class CacheKeyFieldDataTermsQuery extends MultiTermQuery {

    private final long cacheKey;

    public CacheKeyFieldDataTermsQuery(long cacheKey) {
      super("");
      this.cacheKey = cacheKey;
    }

    @Override
    public String toString(String field) {
      final StringBuilder sb = new StringBuilder("CacheKeyFieldDataTermsQuery:");
      return sb.append(field).append(":").append("[cacheKey=" + cacheKey + "]").toString();
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof CacheKeyFieldDataTermsQuery)) return false;
      CacheKeyFieldDataTermsQuery other = (CacheKeyFieldDataTermsQuery) o;
      return super.equals(o) && this.cacheKey == other.cacheKey;
    }

    @Override
    protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
      return null;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((int) cacheKey);
      return result;
    }

  }

  static class SeekingTermSetTermsEnum extends FilteredTermsEnum {

    private final BytesRefHash terms;
    private final int[] ords;
    private final int lastElement;

    private final BytesRef lastTerm;
    private final BytesRef spare = new BytesRef();

    private BytesRef seekTerm;
    private int upto = 0;

    SeekingTermSetTermsEnum(TermsEnum tenum, BytesRefTermsSet termsSet) {
      super(tenum);
      this.terms = termsSet.getBytesRefHash();
      this.ords = this.terms.sort();
      lastElement = terms.size() - 1;
      lastTerm = terms.get(ords[lastElement], new BytesRef());
      seekTerm = terms.get(ords[upto], spare);
    }

    @Override
    protected BytesRef nextSeekTerm(BytesRef currentTerm) throws IOException {
      BytesRef temp = seekTerm;
      seekTerm = null;
      return temp;
    }

    @Override
    protected AcceptStatus accept(BytesRef term) throws IOException {
      if (term.compareTo(lastTerm) > 0) {
        return AcceptStatus.END;
      }

      BytesRef currentTerm = terms.get(ords[upto], spare);
      if (term.compareTo(currentTerm) == 0) {
        if (upto == lastElement) {
          return AcceptStatus.YES;
        } else {
          seekTerm = terms.get(ords[++upto], spare);
          return AcceptStatus.YES_AND_SEEK;
        }
      } else {
        if (upto == lastElement) {
          return AcceptStatus.NO;
        } else { // Our current term doesn't match the the given term.
          int cmp;
          do { // We maybe are behind the given term by more than one step. Keep incrementing till we're the same or higher.
            if (upto == lastElement) {
              return AcceptStatus.NO;
            }
            // typically the terms dict is a superset of query's terms so it's unusual that we have to skip many of
            // our terms so we don't do a binary search here
            seekTerm = terms.get(ords[++upto], spare);
          } while ((cmp = seekTerm.compareTo(term)) < 0);
          if (cmp == 0) {
            if (upto == lastElement) {
              return AcceptStatus.YES;
            }
            seekTerm = terms.get(ords[++upto], spare);
            return AcceptStatus.YES_AND_SEEK;
          } else {
            return AcceptStatus.NO_AND_SEEK;
          }
        }
      }
    }

  }

}