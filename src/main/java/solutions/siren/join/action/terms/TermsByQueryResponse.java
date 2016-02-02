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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import solutions.siren.join.action.terms.collector.IntegerTermsSet;
import solutions.siren.join.action.terms.collector.LongTermsSet;
import solutions.siren.join.action.terms.collector.TermsSet;

import java.io.IOException;
import java.util.List;

/**
 * The response of the terms by query action.
 */
public class TermsByQueryResponse extends BroadcastResponse {

  /**
   * The set of terms that has been retrieved
   */
  private BytesRef encodedTerms;

  /**
   * The number of terms
   */
  private int size;

  /**
   * The type of encoding used
   */
  private TermsByQueryRequest.TermsEncoding termsEncoding;

  /**
   * Has the terms set been pruned ?
   */
  private boolean isPruned;

  /**
   * How long it took to retrieve the terms.
   */
  private long tookInMillis;

  /**
   * Default constructor
   */
  TermsByQueryResponse() {}

  /**
   * Main constructor
   *
   * @param termsSet    the merged terms
   * @param tookInMillis     the time in millis it took to retrieve the terms.
   * @param totalShards      the number of shards the request executed on
   * @param successfulShards the number of shards the request executed on successfully
   * @param failedShards     the number of failed shards
   * @param shardFailures    the failures
   */
  TermsByQueryResponse(TermsSet termsSet, long tookInMillis, int totalShards, int successfulShards, int failedShards,
                       List<ShardOperationFailedException> shardFailures) {
    super(totalShards, successfulShards, failedShards, shardFailures);
    this.encodedTerms = termsSet.writeToBytes();
    this.termsEncoding = termsSet.getEncoding();
    this.size = termsSet.size();
    this.isPruned = termsSet.isPruned();
    this.tookInMillis = tookInMillis;
  }

  /**
   * Gets the time it took to execute the terms by query action.
   */
  public long getTookInMillis() {
    return this.tookInMillis;
  }

  /**
   * Gets the merged terms
   *
   * @return the terms
   */
  public BytesRef getEncodedTermsSet() {
    return encodedTerms;
  }

  /**
   * Gets the number of terms
   */
  public int getSize() {
    return size;
  }

  /**
   * Returns true if the set of terms has been pruned.
   */
  public boolean isPruned() {
    return isPruned;
  }

  /**
   * Deserialize
   *
   * @param in the input
   * @throws IOException
   */
  @Override
  public void readFrom(StreamInput in) throws IOException {
    super.readFrom(in);

    isPruned = in.readBoolean();
    size = in.readVInt();
    termsEncoding = TermsByQueryRequest.TermsEncoding.values()[in.readVInt()];
    encodedTerms = in.readBytesRef();
  }

  /**
   * Serialize
   *
   * @param out the output
   * @throws IOException
   */
  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);

    // Encode flag
    out.writeBoolean(isPruned);
    // Encode size
    out.writeVInt(size);
    // Encode type of encoding
    out.writeVInt(termsEncoding.ordinal());
    // Encode terms
    out.writeBytesRef(encodedTerms);
  }

}
