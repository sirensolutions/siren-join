package solutions.siren.join.index.query;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.*;

import com.carrotsearch.hppc.LongHashSet;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

/**
 * Specialization for a disjunction over many terms that behaves like a
 * {@link ConstantScoreQuery} over a {@link BooleanQuery} containing only
 * {@link org.apache.lucene.search.BooleanClause.Occur#SHOULD} clauses.
 * <p>For instance in the following example, both @{code q1} and {@code q2}
 * would yield the same scores:
 * <pre class="prettyprint">
 * Query q1 = new TermsQuery(new Term("field", "foo"), new Term("field", "bar"));
 *
 * BooleanQuery bq = new BooleanQuery();
 * bq.add(new TermQuery(new Term("field", "foo")), Occur.SHOULD);
 * bq.add(new TermQuery(new Term("field", "bar")), Occur.SHOULD);
 * Query q2 = new ConstantScoreQuery(bq);
 * </pre>
 * <p>When there are few terms, this query executes like a regular disjunction.
 * However, when there are many terms, instead of merging iterators on the fly,
 * it will populate a bit set with matching docs and return a {@link Scorer}
 * over this bit set.
 * <p>NOTE: This query produces scores that are equal to its boost
 */
public abstract class FieldDataTermsQuery extends Query implements Accountable {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FieldDataTermsQuery.class);

  final IndexFieldData fieldData;
  final int cacheKey;

  /**
   * Get a {@link FieldDataTermsQuery} that filters on non-floating point numeric terms found in a hppc
   * {@link LongHashSet}.
   *
   * @param fieldData The fielddata for the field.
   * @param terms     A {@link LongHashSet} of terms.
   * @param cacheKey  A unique key to use for caching this query.
   * @return the query.
   */
  public static FieldDataTermsQuery newLongs(IndexNumericFieldData fieldData, LongHashSet terms, int cacheKey) {
    return new LongsFieldDataTermsQuery(fieldData, terms, cacheKey);
  }

  /**
   * Get a {@link FieldDataTermsQuery} that filters on non-numeric terms found in a hppc {@link LongHashSet} of
   * {@link BytesRef}.
   *
   * @param fieldData The fielddata for the field.
   * @param terms     An {@link LongHashSet} of terms.
   * @param cacheKey  A unique key to use for caching this query.
   * @return the query.
   */
  public static FieldDataTermsQuery newBytes(IndexFieldData fieldData, LongHashSet terms, int cacheKey) {
    return new BytesFieldDataTermsQuery(fieldData, terms, cacheKey);
  }

  /**
   * Creates a new {@link FieldDataTermsQuery} from the given field data.
   */
  public FieldDataTermsQuery(final IndexFieldData fieldData, final int cacheKey) {
    this.fieldData = fieldData;
    this.cacheKey = cacheKey;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    return true;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }

  public abstract DocIdSet getDocIdSet(LeafReaderContext context) throws IOException;

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
        final Scorer scorer = scorer(FieldDataTermsQuery.this.getDocIdSet(context));
        if (scorer == null) {
          return null;
        }
        return new DefaultBulkScorer(scorer);
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        return scorer(FieldDataTermsQuery.this.getDocIdSet(context));
      }
    };
  }

  /**
   * Filters on non-floating point numeric fields.
   */
  protected static class LongsFieldDataTermsQuery extends FieldDataTermsQuery {

    final LongHashSet terms;

    /**
     * Creates a new {@link FieldDataTermsQuery} from the given field data.
     *
     * @param fieldData
     */
    public LongsFieldDataTermsQuery(IndexFieldData fieldData, LongHashSet terms, int cacheKey) {
      super(fieldData, cacheKey);
      this.terms = terms;
    }

    @Override
    public long ramBytesUsed() {
      return BASE_RAM_BYTES_USED + (terms != null ? terms.size() * 8 : 0);
    }

    @Override
    public boolean equals(Object obj) {
      if (super.equals(obj) == false) {
        return false;
      }
      return terms.equals(((LongsFieldDataTermsQuery) obj).terms);
    }

    @Override
    public int hashCode() {
      int hashcode = super.hashCode();
      hashcode = 31 * hashcode + (terms != null ? terms.hashCode() : 0);
      return hashcode;
    }

    @Override
    public String toString(String defaultField) {
      final StringBuilder sb = new StringBuilder("LongsFieldDataTermsQuery:");
      return sb
              .append(defaultField)
              .append(":")
              // Do not serialise the full array, but instead the number of bytes - see issue #168
              .append("[size=" + (terms != null ? terms.size() * 8 : "0") + "]")
              .toString();
    }

    @Override
    public DocIdSet getDocIdSet(LeafReaderContext context) throws IOException {
      // make sure there are terms to filter on
      if (terms == null || terms.isEmpty()) return null;

      IndexNumericFieldData numericFieldData = (IndexNumericFieldData) fieldData;
      if (!numericFieldData.getNumericType().isFloatingPoint()) {
        final SortedNumericDocValues values = numericFieldData.load(context).getLongValues(); // load fielddata
        return new DocValuesDocIdSet(context.reader().maxDoc(), context.reader().getLiveDocs()) {
          @Override
          protected boolean matchDoc(int doc) {
            values.setDocument(doc);
            final int numVals = values.count();
            for (int i = 0; i < numVals; i++) {
              if (terms.contains(values.valueAt(i))) {
                return true;
              }
            }

            return false;
          }
        };
      }

      // only get here if wrong fielddata type in which case
      // no docs will match so we just return null.
      return null;
    }

  }

  /**
   * Filters on non-numeric fields. Uses Sip hash to hash byte values before comparison.
   */
  protected static class BytesFieldDataTermsQuery extends FieldDataTermsQuery {

    final LongHashSet terms;

    /**
     * Creates a new {@link BytesFieldDataTermsQuery} from the given field data.
     *
     * @param fieldData
     */
    public BytesFieldDataTermsQuery(IndexFieldData fieldData, LongHashSet terms, int cacheKey) {
      super(fieldData, cacheKey);
      this.terms = terms;
    }

    @Override
    public long ramBytesUsed() {
      return BASE_RAM_BYTES_USED + (terms != null ? terms.size() * 8 : 0);
    }

    @Override
    public boolean equals(Object obj) {
      if (super.equals(obj) == false) {
        return false;
      }
      return terms.equals(((BytesFieldDataTermsQuery) obj).terms);
    }

    @Override
    public int hashCode() {
      int hashcode = super.hashCode();
      hashcode = 31 * hashcode + (terms != null ? terms.hashCode() : 0);
      return hashcode;
    }

    @Override
    public String toString(String defaultField) {
      final StringBuilder sb = new StringBuilder("BytesFieldDataTermsQuery:");
      return sb
              .append(defaultField)
              .append(":")
              // Do not serialise the full array, but instead the number of bytes - see issue #168
              .append("[size=" + (terms != null ? terms.size() * 8 : "0") + "]")
              .toString();
    }

    @Override
    public DocIdSet getDocIdSet(LeafReaderContext context) throws IOException {
      // make sure there are terms to filter on
      if (terms == null || terms.isEmpty()) return null;

      final SortedBinaryDocValues values = fieldData.load(context).getBytesValues(); // load fielddata
      return new DocValuesDocIdSet(context.reader().maxDoc(), context.reader().getLiveDocs()) {
        @Override
        protected boolean matchDoc(int doc) {
          values.setDocument(doc);
          final int numVals = values.count();
          for (int i = 0; i < numVals; i++) {
            final BytesRef term = values.valueAt(i);
            long termHash = FieldDataTermsQueryHelper.hash(term);
            if (terms.contains(termHash)) {
              return true;
            }
          }

          return false;
        }
      };
    }

  }

  /**
   * This query will be returned by the {@link ConstantScoreWeight} instead of the {@link FieldDataTermsQuery}
   * and used by the
   * {@link org.apache.lucene.search.LRUQueryCache.CachingWrapperWeight} to cache the query.
   * This is necessary in order to avoid caching the byte array and long hash set, which is not memory friendly
   * and not very efficient.
   */
  private static class CacheKeyFieldDataTermsQuery extends Query {

    private final int cacheKey;

    public CacheKeyFieldDataTermsQuery(int cacheKey) {
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
    public int hashCode() {
      return super.hashCode() ^ cacheKey;
    }

  }

}
