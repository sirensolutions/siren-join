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

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.Loggers;
import solutions.siren.join.action.terms.TermsByQueryRequest;
import solutions.siren.join.common.Bytes;

import java.io.IOException;

/**
 * A set of bytes ref terms.
 */
public class BytesRefTermsSet extends TermsSet {

  private transient Counter bytesUsed;
  private transient ByteBlockPool pool;
  private transient BytesRefHash set;

  /**
   * The size of the header: four bytes for the terms encoding ordinal,
   * 1 byte for the {@link #isPruned} flag, and four bytes for the size.
   */
  private static final int HEADER_SIZE = 9;

  private static final Logger logger = Loggers.getLogger(BytesRefTermsSet.class);

  public BytesRefTermsSet(final CircuitBreaker breaker) {
    super(breaker);
    this.bytesUsed = Counter.newCounter();
    this.pool = new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(bytesUsed));
    this.set = new BytesRefHash(pool);
  }

  /**
   * Constructor based on a byte array containing the encoded set of terms.
   * Used in {@link solutions.siren.join.index.query.TermsEnumTermsQuery}.
   */
  public BytesRefTermsSet(BytesRef bytes) {
    super(null);
    this.readFromBytes(bytes);
  }

  public void add(BytesRef term) {
    this.set.add(term);
  }

  public boolean contains(BytesRef term) {
    return this.set.find(term) != -1;
  }

  @Override
  protected void addAll(TermsSet terms) {
    if (!(terms instanceof BytesRefTermsSet)) {
      throw new UnsupportedOperationException("Invalid type: BytesRefTermsSet expected.");
    }

    BytesRefHash input = ((BytesRefTermsSet) terms).set;
    BytesRef reusable = new BytesRef();
    for (int i = 0; i < input.size(); i++) {
      input.get(i, reusable);
      set.add(reusable);
    }
  }

  public BytesRefHash getBytesRefHash() {
    return set;
  }

  @Override
  public int size() {
    return this.set.size();
  }

  /**
   * Return the memory usage of this object in bytes.
   */
  public long ramBytesUsed() {
    return bytesUsed.get();
  }

  @Override
  public void readFrom(StreamInput in) throws IOException {
    this.setIsPruned(in.readBoolean());
    int size = in.readInt();

    bytesUsed = Counter.newCounter();
    pool = new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(bytesUsed));
    set = new BytesRefHash(pool);

    for (long i = 0; i < size; i++) {
      set.add(in.readBytesRef());
    }
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    // Encode flag
    out.writeBoolean(this.isPruned());

    // Encode size of list
    out.writeInt(set.size());

    // Encode BytesRefs
    BytesRef reusable = new BytesRef();
    for (int i = 0; i < this.set.size(); i++) {
      this.set.get(i, reusable);
      out.writeBytesRef(reusable);
    }
  }

  @Override
  public BytesRef writeToBytes() {
    long start = System.nanoTime();
    int size = set.size();

    BytesRef bytes = new BytesRef(new byte[HEADER_SIZE + (int) bytesUsed.get()]);

    // Encode encoding type
    Bytes.writeInt(bytes, this.getEncoding().ordinal());

    // Encode flag
    bytes.bytes[bytes.offset++] = (byte) (this.isPruned() ? 1 : 0);

    // Encode size of the set
    Bytes.writeInt(bytes, size);

    // Encode longs
    BytesRef reusable = new BytesRef();
    for (int i = 0; i < this.set.size(); i++) {
      this.set.get(i, reusable);
      Bytes.writeBytesRef(reusable, bytes);
    }

    logger.debug("Serialized {} terms - took {} ms", this.size(), (System.nanoTime() - start) / 1000000);

    bytes.length = bytes.offset;
    bytes.offset = 0;
    return bytes;
  }

  private void readFromBytes(BytesRef bytes) {
    // Read pruned flag
    this.setIsPruned(bytes.bytes[bytes.offset++] == 1 ? true : false);

    // Read size fo the set
    int size = Bytes.readInt(bytes);

    // Read terms
    bytesUsed = Counter.newCounter();
    pool = new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(bytesUsed));
    set = new BytesRefHash(pool);

    BytesRef reusable = new BytesRef();
    for (int i = 0; i < size; i++) {
      Bytes.readBytesRef(bytes, reusable);
      set.add(reusable);
    }
  }

  @Override
  public TermsByQueryRequest.TermsEncoding getEncoding() {
    return TermsByQueryRequest.TermsEncoding.BYTES;
  }

  @Override
  public void release() {
    if (set != null) {
      set.close();
    }
  }

}
