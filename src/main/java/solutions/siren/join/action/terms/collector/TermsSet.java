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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import solutions.siren.join.action.terms.TermsByQueryRequest;
import solutions.siren.join.common.Bytes;

import java.io.IOException;

/**
 * A set of terms.
 */
public abstract class TermsSet implements Streamable {

  /**
   * A flag to indicate if the set of terms has been pruned
   */
  private boolean isPruned = false;

  protected final CircuitBreaker breaker;

  protected TermsSet(final CircuitBreaker breaker) {
    this.breaker = breaker;
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
   * {@link TermsByQueryRequest.TermsEncoding} returned by
   * {@link #getEncoding()}. Used by {@link solutions.siren.join.action.terms.TermsByQueryResponse}.
   */
  public abstract BytesRef writeToBytes();

  /**
   * Returns the type of encoding for the terms.
   */
  public abstract TermsByQueryRequest.TermsEncoding getEncoding();

  /**
   * Removes all elements from the set and additionally releases any internal buffers.
   */
  public abstract void release();

  /**
   * Used by {@link solutions.siren.join.action.terms.TransportTermsByQueryAction}
   */
  public static TermsSet newTermsSet(long expectedElements, TermsByQueryRequest.TermsEncoding termsEncoding, CircuitBreaker breaker) {
    switch (termsEncoding) {
      case LONG:
        return new LongTermsSet(expectedElements, breaker);
      case INTEGER:
        return new IntegerTermsSet(expectedElements, breaker);
      case BLOOM:
        return new BloomFilterTermsSet(expectedElements, breaker);
      case BYTES:
        return new BytesRefTermsSet(breaker);
      default:
        throw new IllegalArgumentException("[termsByQuery] Invalid terms encoding: " + termsEncoding.name());
    }
  }

  /**
   * Used by {@link solutions.siren.join.index.query.FieldDataTermsQuery} to decode encoded terms.
   */
  public static TermsSet readFrom(BytesRef in) {
    TermsByQueryRequest.TermsEncoding termsEncoding = TermsByQueryRequest.TermsEncoding.values()[Bytes.readInt(in)];
    switch (termsEncoding) {
      case INTEGER:
        return new IntegerTermsSet(in);
      case LONG:
        return new LongTermsSet(in);
      case BLOOM:
        return new BloomFilterTermsSet(in);
      case BYTES:
        return new BytesRefTermsSet(in);
      default:
        throw new IllegalArgumentException("[termsByQuery] Invalid terms encoding: " + termsEncoding.name());
    }
  }

}
