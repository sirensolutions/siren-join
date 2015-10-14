/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.action.node;

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
