/**
 * Copyright (c) 2015, SIREn Solutions. All Rights Reserved.
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

import com.carrotsearch.hppc.LongHashSet;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.hppc.LongOpenHashSet;
import org.elasticsearch.common.hppc.ObjectOpenHashSet;
import org.elasticsearch.common.lucene.docset.MatchDocIdSet;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.io.IOException;

public abstract class FieldDataTermsFilter extends org.elasticsearch.index.search.FieldDataTermsFilter {

  final IndexFieldData fieldData;

  protected FieldDataTermsFilter(final IndexFieldData fieldData) {
    super(fieldData);
    this.fieldData = fieldData;
  }

  /**
   * Get a {@link FieldDataTermsFilter} that filters on non-floating point numeric terms found in a hppc
   * {@link LongOpenHashSet}.
   *
   * @param fieldData The fielddata for the field.
   * @param terms     A {@link LongOpenHashSet} of terms.
   * @return the filter.
   */
  public static FieldDataTermsFilter newLongs(IndexNumericFieldData fieldData, LongHashSet terms) {
    return new LongsFieldDataFilter(fieldData, terms);
  }

  /**
   * Get a {@link FieldDataTermsFilter} that filters on non-numeric terms found in a hppc {@link ObjectOpenHashSet} of
   * {@link BytesRef}.
   *
   * @param fieldData The fielddata for the field.
   * @param terms     An {@link ObjectOpenHashSet} of terms.
   * @return the filter.
   */
  public static FieldDataTermsFilter newBytes(IndexFieldData fieldData, LongHashSet terms) {
    return new BytesFieldDataFilter(fieldData, terms);
  }

  /**
   * Filters on non-floating point numeric fields.
   */
  protected static class LongsFieldDataFilter extends FieldDataTermsFilter {

    final LongHashSet terms;

    protected LongsFieldDataFilter(IndexNumericFieldData fieldData, LongHashSet terms) {
      super(fieldData);
      this.terms = terms;
    }

    @Override
    public boolean equals(Object obj) {
      if (super.equals(obj) == false) {
        return false;
      }
      return terms.equals(((LongsFieldDataFilter) obj).terms);
    }

    @Override
    public int hashCode() {
      int hashcode = super.hashCode();
      hashcode = 31 * hashcode + (terms != null ? terms.hashCode() : 0);
      return hashcode;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("LongsFieldDataFilter:");
      return sb
        .append(fieldData.getFieldNames().indexName())
        .append(":")
        // Do not serialise the full array, but instead the number of bytes - see issue #168
        .append("[size=" + (terms != null ? terms.size() * 8 : "0") + "]")
        .toString();
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
      // make sure there are terms to filter on
      if (terms == null || terms.isEmpty()) return null;

      IndexNumericFieldData numericFieldData = (IndexNumericFieldData) fieldData;
      if (!numericFieldData.getNumericType().isFloatingPoint()) {
        final SortedNumericDocValues values = numericFieldData.load(context).getLongValues(); // load fielddata
        return new MatchDocIdSet(context.reader().maxDoc(), acceptDocs) {
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
  protected static class BytesFieldDataFilter extends FieldDataTermsFilter {

    final LongHashSet terms;

    protected BytesFieldDataFilter(IndexFieldData fieldData, LongHashSet terms) {
      super(fieldData);
      this.terms = terms;
    }

    @Override
    public boolean equals(Object obj) {
      if (super.equals(obj) == false) {
        return false;
      }
      return terms.equals(((FieldDataTermsFilter.BytesFieldDataFilter) obj).terms);
    }

    @Override
    public int hashCode() {
      int hashcode = super.hashCode();
      hashcode = 31 * hashcode + (terms != null ? terms.hashCode() : 0);
      return hashcode;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("BytesFieldDataFilter:");
      return sb
        .append(fieldData.getFieldNames().indexName())
        .append(":")
        // Do not serialise the full array, but instead the number of bytes - see issue #168
        .append("[size=" + (terms != null ? terms.size() * 8 : "0") + "]")
        .toString();
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
      // make sure there are terms to filter on
      if (terms == null || terms.isEmpty()) return null;

      final SortedBinaryDocValues values = fieldData.load(context).getBytesValues(); // load fielddata
      return new MatchDocIdSet(context.reader().maxDoc(), acceptDocs) {
        @Override
        protected boolean matchDoc(int doc) {
          values.setDocument(doc);
          final int numVals = values.count();
          for (int i = 0; i < numVals; i++) {
            final BytesRef term = values.valueAt(i);
            long termHash = BinaryTermsFilterHelper.hash(term);
            if (terms.contains(termHash)) {
              return true;
            }
          }

          return false;
        }
      };
    }
  }

}
