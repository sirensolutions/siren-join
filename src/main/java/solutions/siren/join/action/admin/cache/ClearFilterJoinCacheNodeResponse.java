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

import java.io.IOException;

public class ClearFilterJoinCacheNodeResponse extends BaseNodeResponse {

  private long timestamp;

  ClearFilterJoinCacheNodeResponse() {}

  ClearFilterJoinCacheNodeResponse(DiscoveryNode node, long timestamp) {
    super(node);
    this.timestamp = timestamp;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public static ClearFilterJoinCacheNodeResponse readNodeInfo(StreamInput in) throws IOException {
    ClearFilterJoinCacheNodeResponse nodeInfo = new ClearFilterJoinCacheNodeResponse();
    nodeInfo.readFrom(in);
    return nodeInfo;
  }

  @Override
  public void readFrom(StreamInput in) throws IOException {
    super.readFrom(in);
    timestamp = in.readVLong();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeVLong(timestamp);
  }

}
