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
package solutions.siren.siren.join.action.terms;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * Internal terms by query response of a shard terms by query request executed directly against a specific shard.
 */
class TermsByQueryShardResponse extends BroadcastShardOperationResponse {

  private TermsResponse termsResponse;

  /**
   * Default constructor
   */
  TermsByQueryShardResponse() {}

  /**
   * Main constructor
   *
   * @param shardId       the id of the shard the request executed on
   * @param termsResponse the terms gathered from the shard
   */
  public TermsByQueryShardResponse(ShardId shardId, TermsResponse termsResponse) {
    super(shardId);
    this.termsResponse = termsResponse;
  }

  /**
   * Gets the gathered terms.
   *
   * @return the {@link TermsResponse}
   */
  public TermsResponse getTermsResponse() {
    return this.termsResponse;
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
    termsResponse = new TermsResponse();
    termsResponse.readFrom(in);
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
    termsResponse.writeTo(out);
  }
}