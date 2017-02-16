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

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ClearFilterJoinCacheResponse extends BaseNodesResponse<ClearFilterJoinCacheNodeResponse> implements ToXContent {

  ClearFilterJoinCacheResponse() {}

  ClearFilterJoinCacheResponse(ClusterName clusterName, List<ClearFilterJoinCacheNodeResponse> nodes, List<FailedNodeException> failures) {
    super(clusterName, nodes, failures);
  }

  @Override
  public List<ClearFilterJoinCacheNodeResponse> readNodesFrom(StreamInput in) throws IOException {
    int size = in.readVInt();
    List<ClearFilterJoinCacheNodeResponse> nodes = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      nodes.add(ClearFilterJoinCacheNodeResponse.readNodeInfo(in));
    }

    return nodes;
  }

  @Override
  public void writeNodesTo(StreamOutput out, List<ClearFilterJoinCacheNodeResponse> nodes) throws IOException {
    out.writeVInt(nodes.size());
    for (ClearFilterJoinCacheNodeResponse node : nodes) {
      node.writeTo(out);
    }
  }

  @Override
  public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
    builder.field("cluster_name", getClusterName().value());

    builder.startObject("nodes");
    for (ClearFilterJoinCacheNodeResponse node : getNodes()) {
      builder.startObject(node.getNode().getName());
      builder.field("timestamp", node.getTimestamp());
      builder.endObject();
    }
    builder.endObject();

    return builder;
  }

}