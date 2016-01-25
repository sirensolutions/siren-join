package solutions.siren.join.action.terms.collector;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import solutions.siren.join.action.terms.TermsByQueryRequest;
import solutions.siren.join.index.query.FieldDataTermsQueryHelper;

import java.io.IOException;

/**
 * A set of terms.
 */
public abstract class TermsSet implements Streamable {

  /**
   * A flag to indicate if the set of terms has been pruned
   */
  private boolean isPruned = false;

  public static TermsSet newTermsSet(int expectedElements, TermsByQueryRequest.TermsEncoding termsEncoding) {
    switch (termsEncoding) {
      case LONG:
        return new LongTermsSet(expectedElements);
      case INTEGER:
        return new IntegerTermsSet(expectedElements);
      default:
        throw new IllegalArgumentException("[termsByQuery] Invalid terms encoding: " + termsEncoding.name());
    }
  }

  public static TermsSet readFrom(byte[] in, int offset) {
    TermsByQueryRequest.TermsEncoding termsEncoding = TermsByQueryRequest.TermsEncoding.values()[FieldDataTermsQueryHelper.readInt(in, offset)];
    switch (termsEncoding) {
      case INTEGER:
        return new IntegerTermsSet(in, offset + 4);
      case LONG:
        return new LongTermsSet(in, offset + 4);
      default:
        throw new IllegalArgumentException("[termsByQuery] Invalid terms encoding: " + termsEncoding.name());
    }
  }

  public void setIsPruned(boolean isPruned) {
    this.isPruned = isPruned;
  }

  /**
   * Returns true if the set of terms has been pruned.
   */
  public boolean isPruned() {
    return isPruned;
  }

  public abstract void add(long term);

  public abstract boolean contains(long term);

  protected abstract void addAll(TermsSet terms);

  /**
   * Called to merge {@link TermsSet} from other shards.
   *
   * @param other The {@link TermsSet} to merge with
   */
  public void merge(TermsSet other) {
    this.addAll(other);
    this.isPruned |= other.isPruned;
  }

  /**
   * Shortcut for <code>size() == 0</code>.
   */
  public boolean isEmpty() {
    return this.size() == 0;
  }

  /**
   * The number of terms in the {@link TermsSet}.
   *
   * @return The number of terms
   */
  public abstract int size();

  /**
   * The size of the {@link TermsSet} in bytes.
   *
   * @return The size in bytes
   */
  public abstract int getSizeInBytes();

  /**
   * Deserialize the set of terms from the {@link StreamInput}.
   *
   * @param in the input
   */
  public abstract void readFrom(StreamInput in) throws IOException;

  /**
   * Serialize the set of terms to the {@link StreamOutput}.
   *
   * @param out the output
   */
  public abstract void writeTo(StreamOutput out) throws IOException;

  /**
   * Encodes the set of terms into a byte array. The first four bytes should be the ordinal of the
   * {@link solutions.siren.join.action.terms.TermsByQueryRequest.TermsEncoding} returned by
   * {@link #getEncoding()}.
   */
  public abstract byte[] writeToBytes();

  /**
   * Returns the type of encoding for the terms.
   */
  public abstract TermsByQueryRequest.TermsEncoding getEncoding();

}
