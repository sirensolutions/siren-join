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
package solutions.siren.join.action.admin.cache;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import solutions.siren.join.action.coordinate.execution.FilterJoinCache;

import java.io.IOException;

public class StatsFilterJoinCacheNodeResponse extends BaseNodeResponse {

  private long timestamp;
  private FilterJoinCache.FilterJoinCacheStats cacheStats;

  StatsFilterJoinCacheNodeResponse() {}

  StatsFilterJoinCacheNodeResponse(DiscoveryNode node, long timestamp, FilterJoinCache.FilterJoinCacheStats cacheStats) {
    super(node);
    this.timestamp = timestamp;
    this.cacheStats = cacheStats;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public FilterJoinCache.FilterJoinCacheStats getCacheStats() {
    return this.cacheStats;
  }

  public static StatsFilterJoinCacheNodeResponse readNodeStats(StreamInput in) throws IOException {
    StatsFilterJoinCacheNodeResponse nodeStats = new StatsFilterJoinCacheNodeResponse();
    nodeStats.readFrom(in);
    return nodeStats;
  }

  @Override
  public void readFrom(StreamInput in) throws IOException {
    super.readFrom(in);
    timestamp = in.readVLong();
    cacheStats = new FilterJoinCache.FilterJoinCacheStats();
    cacheStats.readFrom(in);
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeVLong(timestamp);
    cacheStats.writeTo(out);
  }

}
