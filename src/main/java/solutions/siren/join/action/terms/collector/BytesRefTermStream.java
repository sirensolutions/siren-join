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
package solutions.siren.join.action.terms.collector;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.LegacyNumericUtils;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

/**
 * A stream of terms coming for a given document and field. A {@link BytesRefTermStream} is a reusable object
 * used in combination with {@link HitStream#getTermStream(TermStream)}.
 */
abstract class BytesRefTermStream extends TermStream {

  protected final IndexFieldData fieldData;

  protected BytesRefTermStream(IndexReader reader, IndexFieldData indexFieldData) {
    super(reader);
    this.fieldData = indexFieldData;
  }

  /**
   * Returns true if there is at least one remaining term in the stream.
   */
  public abstract boolean hasNext();

  /**
   * Move to the next term in the stream, and returns its long value (i.e., hash for string field type).
   */
  public abstract BytesRef next();

  /**
   * Set the stream to the given document.
   * @see HitStream#getTermStream(TermStream)
   */
  protected abstract void set(int atomicReaderId, int atomicDocId);

  private static class BytesBytesRefTermStream extends BytesRefTermStream {

    private int lastAtomicReaderId = -1;
    private SortedBinaryDocValues values;
    private int count;

    protected BytesBytesRefTermStream(IndexReader reader, IndexFieldData indexFieldData) {
      super(reader, indexFieldData);
    }

    @Override
    public boolean hasNext() {
      if (this.count < this.values.count()) {
        return true;
      }
      return false;
    }

    @Override
    public BytesRef next() {
      return values.valueAt(this.count++);
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

  }

  private static abstract class NumericBytesRefTermStream extends BytesRefTermStream {

    private int lastAtomicReaderId = -1;
    protected SortedNumericDocValues values;
    protected int count;

    protected NumericBytesRefTermStream(IndexReader reader, IndexFieldData indexFieldData) {
      super(reader, indexFieldData);
    }

    @Override
    public boolean hasNext() {
      if (this.count < this.values.count()) {
        return true;
      }
      return false;
    }

    @Override
    protected void set(int atomicReaderId, int atomicDocId) {
      // loading values from field data cache is costly,
      // therefore we load values from cache only if new atomic reader id
      if (lastAtomicReaderId != atomicReaderId) {
        LeafReaderContext leafReader = reader.leaves().get(atomicReaderId);
        this.values = ((IndexNumericFieldData) this.fieldData).load(leafReader).getLongValues();
      }
      this.values.setDocument(atomicDocId);
      this.count = 0;
      this.lastAtomicReaderId = atomicReaderId;
    }

  }

  private static class IntegerBytesRefTermStream extends NumericBytesRefTermStream {

    protected IntegerBytesRefTermStream(IndexReader reader, IndexFieldData indexFieldData) {
      super(reader, indexFieldData);
    }

    @Override
    public BytesRef next() {
      BytesRefBuilder b = new BytesRefBuilder();
      LegacyNumericUtils.intToPrefixCoded((int) values.valueAt(this.count++), 0, b);
      return b.toBytesRef();
    }

  }

  private static class LongBytesRefTermStream extends NumericBytesRefTermStream {

    protected LongBytesRefTermStream(IndexReader reader, IndexFieldData indexFieldData) {
      super(reader, indexFieldData);
    }

    @Override
    public BytesRef next() {
      BytesRefBuilder b = new BytesRefBuilder();
      LegacyNumericUtils.longToPrefixCoded((int) values.valueAt(this.count++), 0, b);
      return b.toBytesRef();
    }

  }

  /**
   * Instantiates a new reusable {@link BytesRefTermStream} based on the field type.
   */
  public static BytesRefTermStream get(IndexReader reader, IndexFieldData indexFieldData) {
    if (indexFieldData instanceof IndexNumericFieldData) {
      IndexNumericFieldData numFieldData = (IndexNumericFieldData) indexFieldData;
      switch (numFieldData.getNumericType()) {

        case INT:
          return new IntegerBytesRefTermStream(reader, numFieldData);

        case LONG:
          return new LongBytesRefTermStream(reader, numFieldData);

        default:
          throw new UnsupportedOperationException("Streaming numeric type '" + numFieldData.getNumericType().name() + "' is unsupported");

      }
    }
    else {
      return new BytesBytesRefTermStream(reader, indexFieldData);
    }
  }

}