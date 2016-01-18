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
package solutions.siren.join.action.terms;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import solutions.siren.join.action.terms.collector.TermsCollector;
import solutions.siren.join.index.query.FieldDataTermsQueryHelper;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Gathers and stores terms for a {@link TermsByQueryRequest}.
 */
public class TermsResponse implements Streamable {

  protected static final ESLogger logger = Loggers.getLogger(TermsResponse.class);

  private transient TermsCollector.TermsCollection terms;

  /**
   * Default constructor
   */
  TermsResponse() {}

  /**
   * Constructor used after terms collection in
   * {@link TransportTermsByQueryAction#shardOperation(TermsByQueryShardRequest)}
   */
  TermsResponse(TermsCollector.TermsCollection terms) {
    this.terms = terms;
  }

  /**
   * Constructor used during merging of shard responses in
   * {@link TransportTermsByQueryAction#newResponse(TermsByQueryRequest, AtomicReferenceArray, ClusterState)}
   */
  TermsResponse(int numTerms) {
    this.terms = new TermsCollector.TermsCollection(numTerms);
  }

  /**
   * Called to merging {@link TermsResponse} from other shards.
   *
   * @param other The {@link TermsResponse} to merge with
   */
  public void merge(TermsResponse other) {
    this.terms.merge(other.terms);
  }

  /**
   * Deserialize
   *
   * @param in the input
   * @throws IOException
   */
  @Override
  public void readFrom(StreamInput in) throws IOException {
    boolean isPruned = in.readBoolean();
    int size = in.readInt();
    LongHashSet termsHash = new LongHashSet(size);
    for (long i = 0; i < size; i++) {
      termsHash.add(in.readLong());
    }
    this.terms = new TermsCollector.TermsCollection(termsHash, isPruned);
  }

  /**
   * Serialize the list of terms to the {@link StreamOutput}.
   * <br>
   * Given the low performance of {@link BytesStreamOutput} when writing a large number
   * of longs (5 to 10 times slower than writing directly to a byte[]), we use a small buffer of 8kb
   * to optimise the throughput. 8kb seems to be the optimal buffer size, larger buffer size did not improve
   * the throughput.
   *
   * @param out the output
   */
  @Override
  public void writeTo(StreamOutput out) throws IOException {
    byte[] buffer = new byte[1024 * 8];
    int offset = 0;

    // Encode flag
    out.writeBoolean(this.terms.isPruned());

    // Encode size of list
    out.writeInt(this.terms.getTermsHash().size());

    // Encode longs
    Iterator<LongCursor> it = this.terms.getTermsHash().iterator();
    while (it.hasNext()) {
      FieldDataTermsQueryHelper.writeLong(buffer, offset, it.next().value);
      offset += 8;
      if (offset == buffer.length) {
        out.write(buffer, 0, offset);
        offset = 0;
      }
    }

    // flush the remaining bytes from the buffer
    out.write(buffer, 0, offset);
  }

  /**
   * The number of terms in the {@link TermsResponse}.
   *
   * @return The number of terms
   */
  public int size() {
    return this.terms.getTermsHash().size();
  }

  /**
   * The size of the {@link TermsResponse} in bytes.
   *
   * @return The size in bytes
   */
  public long getSizeInBytes() {
    return this.terms.getTermsHash().size() * 8;
  }

  /**
   * Returns the the terms.
   *
   * @return The terms
   */
  public LongHashSet getTerms() {
    return this.terms.getTermsHash();
  }

  /**
   * Returns true if the set of terms has been pruned.
   */
  public boolean isPruned() {
    return this.terms.isPruned();
  }


  /**
   * Encodes the list of terms into a byte array.
   */
  public byte[] getBytes() {
    long start = System.nanoTime();

    int size = this.terms.getTermsHash().size();
    byte[] bytes = new byte[size * 8 + 4];
    int offset = 0;

    // Encode size of list
    FieldDataTermsQueryHelper.writeInt(bytes, 0, size);
    offset += 4;

    // Encode longs
    for (LongCursor i : this.terms.getTermsHash()) {
      FieldDataTermsQueryHelper.writeLong(bytes, offset, i.value);
      offset += 8;
    }

    logger.debug("Serialized {} terms - took {} ms", this.size(), (System.nanoTime() - start) / 1000000);
    return bytes;
  }

}