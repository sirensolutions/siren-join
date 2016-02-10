package solutions.siren.join.action.terms.collector;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongScatterSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import solutions.siren.join.action.terms.TermsByQueryRequest;
import solutions.siren.join.index.query.FieldDataTermsQueryHelper;

import java.io.IOException;
import java.util.Iterator;

public class LongTermsSet extends TermsSet {

  private transient LongHashSet set;

  /**
   * The size of the header: four bytes for the terms encoding ordinal,
   * 1 byte for the {@link #isPruned} flag, and four bytes for the size.
   */
  private static final int HEADER_SIZE = 9;

  private static final ESLogger logger = Loggers.getLogger(LongTermsSet.class);

//  /**
//   * Default constructor
//   */
//  public LongTermsSet() {}

  public LongTermsSet(final CircuitBreakerService breakerService) {
    super(breakerService);
  }

//  public LongTermsSet(final int expectedElements) {
//    this.set = new LongHashSet(expectedElements);
//  }

  public LongTermsSet(final int expectedElements, final CircuitBreakerService breakerService) {
    super(breakerService);
    this.set = new LongHashSet(expectedElements);
    this.adjustBreaker(estimatedRamBytesUsed());
  }

  /**
   * Constructor based on a byte array containing the encoded set of terms.
   * Used in {@link solutions.siren.join.index.query.FieldDataTermsQuery}.
   */
  public LongTermsSet(BytesRef bytes) {
    super(null);
    this.readFromBytes(bytes);
  }

  /**
   * Used by unit-tests
   */
  public LongHashSet getLongHashSet() {
    return set;
  }

  @Override
  public void add(long term) {
    this.set.add(term);
  }

  @Override
  public boolean contains(long term) {
    return this.set.contains(term);
  }

  @Override
  protected void addAll(TermsSet terms) {
    if (!(terms instanceof LongTermsSet)) {
      throw new UnsupportedOperationException("Invalid type: LongTermSet expected.");
    }
    long oldMemSize = this.estimatedRamBytesUsed();
    this.set.addAll(((LongTermsSet) terms).set);
    this.adjustBreaker(this.estimatedRamBytesUsed() - oldMemSize);
  }

  @Override
  public int size() {
    return this.set.size();
  }

  @Override
  public void readFrom(StreamInput in) throws IOException {
    this.setIsPruned(in.readBoolean());
    int size = in.readInt();
    set = new LongHashSet(size);
    this.adjustBreaker(estimatedRamBytesUsed());
    for (long i = 0; i < size; i++) {
      set.add(in.readLong());
    }
  }

  /**
   * Serialize the list of terms to the {@link StreamOutput}.
   * <br>
   * Given the low performance of {@link org.elasticsearch.common.io.stream.BytesStreamOutput} when writing a large number
   * of longs (5 to 10 times slower than writing directly to a byte[]), we use a small buffer of 8kb
   * to optimise the throughput. 8kb seems to be the optimal buffer size, larger buffer size did not improve
   * the throughput.
   *
   * @param out the output
   */
  @Override
  public void writeTo(StreamOutput out) throws IOException {
    // Encode flag
    out.writeBoolean(this.isPruned());

    // Encode size of list
    out.writeInt(set.size());

    // Encode longs
    BytesRef buffer = new BytesRef(new byte[1024 * 8]);
    Iterator<LongCursor> it = set.iterator();
    while (it.hasNext()) {
      FieldDataTermsQueryHelper.writeLong(buffer, it.next().value);
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
    int size = set.size();

    BytesRef bytes = new BytesRef(new byte[HEADER_SIZE + 8 * size]);

    // Encode encoding type
    FieldDataTermsQueryHelper.writeInt(bytes, this.getEncoding().ordinal());

    // Encode flag
    bytes.bytes[bytes.offset++] = (byte) (this.isPruned() ? 1 : 0);

    // Encode size of the set
    FieldDataTermsQueryHelper.writeInt(bytes, size);

    // Encode longs
    for (LongCursor i : set) {
      FieldDataTermsQueryHelper.writeLong(bytes, i.value);
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
    int size = FieldDataTermsQueryHelper.readInt(bytes);

    // Read terms

    // Scatter set is slightly more efficient than the hash set, but should be used only for lookups,
    // not for merging
    set = new LongScatterSet(size);
    this.adjustBreaker(estimatedRamBytesUsed());
    for (int i = 0; i < size; i++) {
      set.add(FieldDataTermsQueryHelper.readLong(bytes));
    }
  }

  @Override
  public TermsByQueryRequest.TermsEncoding getEncoding() {
    return TermsByQueryRequest.TermsEncoding.LONG;
  }

  @Override
  protected long estimatedRamBytesUsed() {
    return this.set != null ? this.set.keys.length * 8 : 0;
  }

}
