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
package solutions.siren.join.action.admin.version;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportGetIndicesVersionAction extends TransportBroadcastByNodeAction<GetIndicesVersionRequest, GetIndicesVersionResponse, ShardIndexVersion> {

  private final IndicesService indicesService;
  private final IndexVersionService indexVersionService;

  @Inject
  public TransportGetIndicesVersionAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
          TransportService transportService, IndicesService indicesService,
          ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
          IndexVersionService indexVersionService) {
    super(settings, GetIndicesVersionAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
            GetIndicesVersionRequest::new, ThreadPool.Names.MANAGEMENT);
    this.indicesService = indicesService;
    this.indexVersionService = indexVersionService;
  }

  @Override
  protected ShardIndexVersion readShardResult(StreamInput in) throws IOException {
    return ShardIndexVersion.readShardIndexVersion(in);
  }

  @Override
  protected GetIndicesVersionResponse newResponse(GetIndicesVersionRequest request, int totalShards, int successfulShards, int failedShards, List<ShardIndexVersion> shardVersions, List<ShardOperationFailedException> shardFailures, ClusterState clusterState) {
    return new GetIndicesVersionResponse(shardVersions.toArray(new ShardIndexVersion[shardVersions.size()]), totalShards, successfulShards, failedShards, shardFailures);
  }

  @Override
  protected GetIndicesVersionRequest readRequestFrom(StreamInput in) throws IOException {
    GetIndicesVersionRequest request = new GetIndicesVersionRequest();
    request.readFrom(in);
    return request;
  }

  @Override
  protected ShardIndexVersion shardOperation(GetIndicesVersionRequest request, ShardRouting shardRouting) throws IOException {
    IndexService indexService = indicesService.indexServiceSafe(shardRouting.index());
    IndexShard indexShard = indexService.getShard(shardRouting.id());

    // Get the IndexVersionShardService associated to this shard
    long version = indexVersionService.getVersion(indexService.index(), indexShard.shardId());
    return new ShardIndexVersion(indexShard.routingEntry(), version);
  }

  /**
   * Goes across *all* shards, i.e., primaries and replicas.
   *
   * When elasticsearch will have resolved issue #10708, we will be able to compute an index version based on the
   * primary shards only.
   */
  @Override
  protected ShardsIterator shards(ClusterState clusterState, GetIndicesVersionRequest request, String[] concreteIndices) {
    return clusterState.routingTable().allShards(concreteIndices);
  }

  @Override
  protected ClusterBlockException checkGlobalBlock(ClusterState state, GetIndicesVersionRequest request) {
    return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
  }

  @Override
  protected ClusterBlockException checkRequestBlock(ClusterState state, GetIndicesVersionRequest request, String[] concreteIndices) {
    return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
  }

}