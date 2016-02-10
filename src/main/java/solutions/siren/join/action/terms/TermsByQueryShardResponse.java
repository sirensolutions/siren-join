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

import org.elasticsearch.action.support.broadcast.BroadcastShardResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import solutions.siren.join.action.terms.collector.IntegerTermsSet;
import solutions.siren.join.action.terms.collector.LongTermsSet;
import solutions.siren.join.action.terms.collector.TermsSet;

import java.io.IOException;

/**
 * Internal terms by query response of a shard terms by query request executed directly against a specific shard.
 */
class TermsByQueryShardResponse extends BroadcastShardResponse {

  private TermsSet termsSet;
  private final CircuitBreakerService breakerService;

  /**
   * Default constructor
   */
  TermsByQueryShardResponse(final CircuitBreakerService breakerService) {
    this.breakerService = breakerService;
  }

  /**
   * Main constructor
   *
   * @param shardId the id of the shard the request executed on
   * @param termsSet the terms gathered from the shard
   */
  public TermsByQueryShardResponse(ShardId shardId, TermsSet termsSet) {
    super(shardId);
    this.termsSet = termsSet;
    this.breakerService = null;
  }

  /**
   * Gets the gathered terms.
   *
   * @return the {@link TermsSet}
   */
  public TermsSet getTerms() {
    return this.termsSet;
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

    TermsByQueryRequest.TermsEncoding termsEncoding = TermsByQueryRequest.TermsEncoding.values()[in.readVInt()];
    switch (termsEncoding) {

      case LONG:
        termsSet = new LongTermsSet(breakerService);
        termsSet.readFrom(in);
        return;

      case INTEGER:
        termsSet = new IntegerTermsSet(breakerService);
        termsSet.readFrom(in);
        return;

      default:
        throw new IOException("[termsByQuery] Invalid type of terms encoding: " + termsEncoding.name());

    }
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

    // Encode type of encoding
    out.writeVInt(termsSet.getEncoding().ordinal());
    // Encode terms
    termsSet.writeTo(out);
  }
}