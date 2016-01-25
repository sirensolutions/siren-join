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
package solutions.siren.join.action.terms.collector;

import org.apache.lucene.index.LeafReaderContext;
import solutions.siren.join.index.query.FieldDataTermsQueryHelper;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

/**
 * A stream of terms coming for a given document and field. A {@link TermStream} is a reusable object
 * used in combination with {@link HitStream#getTermStream(TermStream)}.
 */
abstract class TermStream {

  /**
   * Instantiates a new reusable {@link TermStream} based on the field type.
   */
  public static TermStream get(IndexReader reader, IndexFieldData indexFieldData) {
    if (indexFieldData instanceof IndexNumericFieldData) {
      IndexNumericFieldData numFieldData = (IndexNumericFieldData) indexFieldData;
      if (!numFieldData.getNumericType().isFloatingPoint()) {
        return new LongTermStream(reader, numFieldData);
      }
      else {
        throw new UnsupportedOperationException("Streaming floating points is unsupported");
      }
    }
    else {
      return new BytesTermStream(reader, indexFieldData);
    }
  }

  protected final IndexReader reader;

  protected TermStream(IndexReader reader) {
    this.reader = reader;
  }

  /**
   * Returns true if there is at least one remaining term in the stream.
   */
  public abstract boolean hasNext();

  /**
   * Move to the next term in the stream, and returns its long value (i.e., hash for string field type).
   */
  public abstract long next();

  /**
   * Set the stream to the given document.
   * @see HitStream#getTermStream(TermStream)
   */
  protected abstract void set(int atomicReaderId, int atomicDocId);

  /**
   * A term stream for numeric long values.
   */
  private static class LongTermStream extends TermStream {

    private final IndexNumericFieldData fieldData;
    private int lastAtomicReaderId = -1;
    private SortedNumericDocValues values;
    private int count;

    protected LongTermStream(IndexReader reader, IndexNumericFieldData fieldData) {
      super(reader);
      this.fieldData = fieldData;
    }

    @Override
    protected void set(int atomicReaderId, int atomicDocId) {
      // loading values from field data cache is costly,
      // therefore we load values from cache only if new atomic reader id
      if (lastAtomicReaderId != atomicReaderId) {
        LeafReaderContext leafReader = reader.leaves().get(atomicReaderId);
        this.values = this.fieldData.load(leafReader).getLongValues();
      }
      this.values.setDocument(atomicDocId);
      this.count = 0;
      this.lastAtomicReaderId = atomicReaderId;
    }

    @Override
    public boolean hasNext() {
      if (this.count < this.values.count()) {
        return true;
      }
      return false;
    }

    @Override
    public long next() {
      return this.values.valueAt(this.count++);
    }

  }

  /**
   * A term stream for string values. It computes a Sip hash of the term.
   */
  private static class BytesTermStream extends TermStream {

    private final IndexFieldData fieldData;
    private int lastAtomicReaderId = -1;
    private SortedBinaryDocValues values;
    private int count;

    protected BytesTermStream(IndexReader reader, IndexFieldData fieldData) {
      super(reader);
      this.fieldData = fieldData;
    }

    @Override
    protected void set(int atomicReaderId, int atomicDocId) {
      // loading values from field data cache is costly,
      // therefore we load values from cache only if new atomic reader id
      if (lastAtomicReaderId != atomicReaderId) {
        LeafReaderContext leafReader = reader.leaves().get(atomicReaderId);
        this.values = this.fieldData.load(leafReader).getBytesValues();
      }
      this.values.setDocument(atomicDocId);
      this.count = 0;
      this.lastAtomicReaderId = atomicReaderId;
    }

    @Override
    public boolean hasNext() {
      if (this.count < this.values.count()) {
        return true;
      }
      return false;
    }

    @Override
    public long next() {
      final BytesRef term = values.valueAt(this.count++);
      return FieldDataTermsQueryHelper.hash(term);
    }

  }

}