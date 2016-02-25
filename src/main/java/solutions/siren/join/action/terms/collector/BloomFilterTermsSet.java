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

import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import solutions.siren.join.action.terms.TermsByQueryRequest;
import solutions.siren.join.common.Bytes;

import java.io.IOException;
import java.math.RoundingMode;

public class BloomFilterTermsSet extends TermsSet {

  private transient LongBloomFilter set;

  private static final double DEFAULT_FPP = 0.03;

  /**
   * The size of the header: 4 bytes for the terms encoding ordinal, 1 byte for the {@link #isPruned} flag,
   * 4 bytes for the number of hashing functions ,4 bytes for the hash type, 4 bytes for the number of longs.
   */
  private static final int HEADER_SIZE = 17;

  private static final ESLogger logger = Loggers.getLogger(BloomFilterTermsSet.class);

  /**
   * Constructor used by {@link solutions.siren.join.action.terms.TermsByQueryShardResponse}
   */
  public BloomFilterTermsSet(final CircuitBreaker breaker) {
    super(breaker);
  }

  public BloomFilterTermsSet(final long expectedElements, final CircuitBreaker breakerService) {
    super(breakerService);
    this.set = new CircuitBreakerLongBloomFilter(Math.toIntExact(expectedElements), DEFAULT_FPP);
  }

  /**
   * Constructor based on a byte array containing the encoded set of terms.
   * Used in {@link solutions.siren.join.index.query.FieldDataTermsQuery}.
   */
  public BloomFilterTermsSet(BytesRef bytes) {
    super(null);
    this.readFromBytes(bytes);
  }

  @Override
  public void add(long term) {
    this.set.put(term);
  }

  @Override
  public boolean contains(long term) {
    return this.set.mightContain(term);
  }

  @Override
  protected void addAll(TermsSet terms) {
    if (!(terms instanceof BloomFilterTermsSet)) {
      throw new UnsupportedOperationException("Invalid type: BloomFilterTermsSet expected.");
    }
    this.set.merge(((BloomFilterTermsSet) terms).set);
  }

  @Override
  public int size() {
    return this.set.estimateCardinality();
  }

  @Override
  public void readFrom(StreamInput in) throws IOException {
    // Decode flag
    this.setIsPruned(in.readBoolean());

    // Decode bloom filter
    int numberOfHashFunctions = in.readVInt();
    int hashType = in.readVInt();
    int numLongs = in.readVInt();

    // Adjust breaker
    long memSize = numLongs * 8;
    breaker.addEstimateBytesAndMaybeBreak(memSize, "<terms_set>");

    try {
      long[] data = new long[numLongs];
      for (int i = 0; i < numLongs; i++) {
        data[i] = in.readLong();
      }
      set = new CircuitBreakerLongBloomFilter(new LongBloomFilter.BitArray(data), numberOfHashFunctions, LongBloomFilter.Hashing.fromType(hashType));
    }
    catch (OutOfMemoryError e) {
      // Ensure no change is done if we hit an OOM.
      breaker.addWithoutBreaking(-memSize);
      throw e;
    }
  }

  /**
   * Serialize the list of terms to the {@link StreamOutput}.
   * <br>
   * Given the low performance of {@link org.elasticsearch.common.io.stream.BytesStreamOutput} when writing a large number
   * of longs (5 to 10 times slower than writing directly to a byte[]), we use a small buffer of 8kb
   * to optimise the throughput. 8kb seems to be the optimal buffer size, larger buffer size did not improve
   * the throughput.
   */
  @Override
  public void writeTo(StreamOutput out) throws IOException {
    // Encode flag
    out.writeBoolean(this.isPruned());

    // Encode bloom filter
    out.writeVInt(set.numHashFunctions);
    out.writeVInt(set.hashing.type()); // hashType
    out.writeVInt(set.bits.data.length);
    BytesRef buffer = new BytesRef(new byte[1024 * 8]);
    for (long l : set.bits.data) {
      Bytes.writeLong(buffer, l);
      if (buffer.offset == buffer.length) {
        out.write(buffer.bytes, 0, buffer.offset);
        buffer.offset = 0;
      }
    }
    // flush the remaining bytes from the buffer
    out.write(buffer.bytes, 0, buffer.offset);
  }

  @Override
  public BytesRef writeToBytes() {
    long start = System.nanoTime();

    BytesRef bytes = new BytesRef(new byte[HEADER_SIZE + set.bits.data.length * 8]);

    // Encode encoding type
    Bytes.writeInt(bytes, this.getEncoding().ordinal());

    // Encode flag
    bytes.bytes[bytes.offset++] = (byte) (this.isPruned() ? 1 : 0);

    Bytes.writeInt(bytes, set.numHashFunctions);
    Bytes.writeInt(bytes, set.hashing.type()); // hashType
    Bytes.writeInt(bytes, set.bits.data.length);
    for (long l : set.bits.data) {
      Bytes.writeLong(bytes, l);
    }

    logger.debug("Serialized {} terms - took {} ms", this.size(), (System.nanoTime() - start) / 1000000);

    bytes.length = bytes.offset;
    bytes.offset = 0;
    return bytes;
  }

  private void readFromBytes(BytesRef bytes) {
    // Read pruned flag
    this.setIsPruned(bytes.bytes[bytes.offset++] == 1 ? true : false);

    // Decode bloom filter
    int numberOfHashFunctions = Bytes.readInt(bytes);
    int hashType = Bytes.readInt(bytes);
    int numLongs = Bytes.readInt(bytes);
    long[] data = new long[numLongs];
    for (int i = 0; i < numLongs; i++) {
      data[i] = Bytes.readLong(bytes);
    }
    set = new LongBloomFilter(new LongBloomFilter.BitArray(data), numberOfHashFunctions, LongBloomFilter.Hashing.fromType(hashType));
  }

  @Override
  public TermsByQueryRequest.TermsEncoding getEncoding() {
    return TermsByQueryRequest.TermsEncoding.BLOOM;
  }

  @Override
  public void release() {
    if (set != null) {
      set.release();
    }
  }

  private final class CircuitBreakerLongBloomFilter extends LongBloomFilter {

    CircuitBreakerLongBloomFilter(int expectedInsertions, double fpp) {
      super(expectedInsertions, fpp);
    }

    CircuitBreakerLongBloomFilter(BitArray bits, int numHashFunctions, Hashing hashing) {
      super(bits, numHashFunctions, hashing);
    }

    @Override
    protected BitArray createBitArray(long numBits) {
      int memSize = Ints.checkedCast(LongMath.divide(numBits, 64, RoundingMode.CEILING)) * 8;
      // Adjust the breaker with the new memory size
      breaker.addEstimateBytesAndMaybeBreak(memSize, "<terms_set>");

      try {
        return new BitArray(numBits);
      }
      catch (OutOfMemoryError e) {
        // Ensure no change is done if we hit an OOM.
        breaker.addWithoutBreaking(-memSize);
        throw e;
      }
    }

    @Override
    protected void release() {
      int memSize = bits.data.length * 8;

      super.release();

      // Adjust breaker
      breaker.addWithoutBreaking(-memSize);
    }

  }

}
