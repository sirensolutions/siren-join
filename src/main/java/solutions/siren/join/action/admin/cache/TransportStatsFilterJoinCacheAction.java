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

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class TransportStatsFilterJoinCacheAction extends TransportNodesAction<StatsFilterJoinCacheRequest,
        StatsFilterJoinCacheResponse, StatsFilterJoinCacheNodeRequest, StatsFilterJoinCacheNodeResponse> {

  private final ClusterService clusterService;
  private final FilterJoinCacheService cacheService;

  @Inject
  public TransportStatsFilterJoinCacheAction(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                                ClusterService clusterService, FilterJoinCacheService cacheService,
                                                TransportService transportService, ActionFilters actionFilters,
                                                IndexNameExpressionResolver indexNameExpressionResolver) {
    super(settings, StatsFilterJoinCacheAction.NAME, clusterName, threadPool, clusterService, transportService,
            actionFilters, indexNameExpressionResolver, StatsFilterJoinCacheRequest.class,
            StatsFilterJoinCacheNodeRequest.class, ThreadPool.Names.MANAGEMENT);
    this.cacheService = cacheService;
    this.clusterService = clusterService;
  }

  @Override
  protected StatsFilterJoinCacheResponse newResponse(StatsFilterJoinCacheRequest request, AtomicReferenceArray nodesResponses) {
    final List<StatsFilterJoinCacheNodeResponse> nodeStats = new ArrayList<>();
    for (int i = 0; i < nodesResponses.length(); i++) {
      Object resp = nodesResponses.get(i);
      if (resp instanceof StatsFilterJoinCacheNodeResponse) {
        nodeStats.add((StatsFilterJoinCacheNodeResponse) resp);
      }
    }
    return new StatsFilterJoinCacheResponse(clusterName, nodeStats.toArray(new StatsFilterJoinCacheNodeResponse[nodeStats.size()]));
  }

  @Override
  protected StatsFilterJoinCacheNodeRequest newNodeRequest(String nodeId, StatsFilterJoinCacheRequest request) {
    return new StatsFilterJoinCacheNodeRequest(nodeId, request);
  }

  @Override
  protected StatsFilterJoinCacheNodeResponse newNodeResponse() {
    return new StatsFilterJoinCacheNodeResponse();
  }

  @Override
  protected StatsFilterJoinCacheNodeResponse nodeOperation(StatsFilterJoinCacheNodeRequest request) {
    logger.debug("Clearing filter join cache on node {}", clusterService.localNode());
    return new StatsFilterJoinCacheNodeResponse(clusterService.localNode(), System.currentTimeMillis(), cacheService.getStats());
  }

  @Override
  protected boolean accumulateExceptions() {
    return false;
  }

}
