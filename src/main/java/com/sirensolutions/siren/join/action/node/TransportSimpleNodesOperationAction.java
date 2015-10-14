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
package com.sirensolutions.siren.join.action.node;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesOperationAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class TransportSimpleNodesOperationAction extends TransportNodesOperationAction<NodesSimpleRequest, NodesSimpleResponse, NodeSimpleRequest, NodeSimpleResponse> {

  private final IndicesService indicesService;

  @Inject
  protected TransportSimpleNodesOperationAction(final Settings settings, final ClusterName clusterName, final ThreadPool threadPool,
                                                final ClusterService clusterService, final TransportService transportService,
                                                IndicesService indicesService, final ActionFilters actionFilters) {
    super(settings, NodesSimpleAction.NAME, clusterName, threadPool, clusterService, transportService, actionFilters);
    this.indicesService = indicesService;
  }

  @Override
  protected String executor() {
    return ThreadPool.Names.SEARCH;
  }

  @Override
  protected NodesSimpleRequest newRequest() {
    return new NodesSimpleRequest();
  }

  @Override
  protected NodesSimpleResponse newResponse(final NodesSimpleRequest request, final AtomicReferenceArray nodesResponses) {
    final List<NodeSimpleResponse> nodeSimpleResponses = Lists.newArrayList();
    for (int i = 0; i < nodesResponses.length(); i++) {
      Object resp = nodesResponses.get(i);
      if (resp instanceof NodeSimpleResponse) {
        nodeSimpleResponses.add((NodeSimpleResponse) resp);
      }
    }
    return new NodesSimpleResponse(clusterName, nodeSimpleResponses.toArray(new NodeSimpleResponse[nodeSimpleResponses.size()]));
  }

  @Override
  protected NodeSimpleRequest newNodeRequest() {
    return new NodeSimpleRequest();
  }

  @Override
  protected NodeSimpleRequest newNodeRequest(final String nodeId, final NodesSimpleRequest request) {
    return new NodeSimpleRequest(nodeId, request);
  }

  @Override
  protected NodeSimpleResponse newNodeResponse() {
    return new NodeSimpleResponse();
  }

  @Override
  protected NodeSimpleResponse nodeOperation(final NodeSimpleRequest request) throws ElasticsearchException {
    return new NodeSimpleResponse();
  }

  @Override
  protected boolean accumulateExceptions() {
    return false;
  }

}
