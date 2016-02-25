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

import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class ClearFilterJoinCacheResponse extends BaseNodesResponse<ClearFilterJoinCacheNodeResponse> implements ToXContent {

  ClearFilterJoinCacheResponse() {}

  ClearFilterJoinCacheResponse(ClusterName clusterName, ClearFilterJoinCacheNodeResponse[] nodes) {
    super(clusterName, nodes);
  }

  @Override
  public void readFrom(StreamInput in) throws IOException {
    super.readFrom(in);
    nodes = new ClearFilterJoinCacheNodeResponse[in.readVInt()];
    for (int i = 0; i < nodes.length; i++) {
      nodes[i] = ClearFilterJoinCacheNodeResponse.readNodeInfo(in);
    }
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeVInt(nodes.length);
    for (ClearFilterJoinCacheNodeResponse node : nodes) {
      node.writeTo(out);
    }
  }

  @Override
  public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
    builder.field("cluster_name", getClusterName().value());

    builder.startObject("nodes");
    for (ClearFilterJoinCacheNodeResponse node : this) {
      builder.startObject(node.getNode().getName(), XContentBuilder.FieldCaseConversion.NONE);
      builder.field("timestamp", node.getTimestamp());
      builder.endObject();
    }
    builder.endObject();

    return builder;
  }

}
