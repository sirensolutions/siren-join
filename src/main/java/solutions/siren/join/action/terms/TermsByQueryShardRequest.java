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
package solutions.siren.join.action.terms;

import org.elasticsearch.action.support.broadcast.BroadcastShardRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.AliasFilter;

import java.io.IOException;

/**
 * Internal terms by query request executed directly against a specific index shard.
 */
public class TermsByQueryShardRequest extends BroadcastShardRequest {

  @Nullable
  private AliasFilter filteringAliases;
  private TermsByQueryRequest request;

  /**
   * Default constructor
   */
  public TermsByQueryShardRequest() {
    filteringAliases = new AliasFilter(null, Strings.EMPTY_ARRAY);
  }

  /**
   * Main Constructor
   *
   * @param shardId          the id of the shard the request is for
   * @param filteringAliases optional aliases
   * @param request          the original {@link TermsByQueryRequest}
   */
  public TermsByQueryShardRequest(ShardId shardId, AliasFilter filteringAliases, TermsByQueryRequest request) {
    super(shardId, request);
    this.filteringAliases = filteringAliases;
    this.request = request;
  }

  /**
   * Gets the filtering aliases
   *
   * @return the filtering aliases
   */
  public AliasFilter filteringAliases() {
    return filteringAliases;
  }

  /**
   * Gets the original {@link TermsByQueryRequest}
   *
   * @return the request
   */
  public TermsByQueryRequest request() {
    return request;
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
    request = new TermsByQueryRequest();
    request.readFrom(in);

    if (in.readBoolean()) {
      filteringAliases = new AliasFilter(in);
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
    request.writeTo(out);

    if (filteringAliases == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      filteringAliases.writeTo(out);
    }
  }
}