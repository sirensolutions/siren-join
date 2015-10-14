/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.action.coordinate;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The transport action for a coordinated multi-search. It converts the filter joins defined in all the requests into
 * binary terms filters, reusing cached computation when possible.
 * <br>
 * The {@link #doExecuteRequest(MultiSearchRequest, ActionListener, List)} is a copy of
 * {@link TransportMultiSearchAction#doExecute(MultiSearchRequest, ActionListener)} where we instantiate a
 * {@link CoordinateMultiSearchResponse} instead of a {@link MultiSearchResponse}.
 *
 * @see TransportMultiSearchAction
 */
public class TransportCoordinateMultiSearchAction extends BaseTransportCoordinateSearchAction<MultiSearchRequest, MultiSearchResponse> {

  private final ClusterService clusterService;

  private final TransportSearchAction searchAction;

  @Inject
  public TransportCoordinateMultiSearchAction(Settings settings, ThreadPool threadPool,
                                              TransportService transportService, ClusterService clusterService,
                                              TransportSearchAction search, ActionFilters actionFilters,
                                              Client client) {
    super(settings, CoordinateMultiSearchAction.NAME, threadPool, transportService, actionFilters, client);
    this.searchAction = search;
    this.clusterService = clusterService;
  }

  @Override
  public MultiSearchRequest newRequestInstance() {
    return new MultiSearchRequest();
  }

  @Override
  protected void doExecute(final MultiSearchRequest request, final ActionListener<MultiSearchResponse> listener) {
    logger.debug("{}: Execute coordinated multi-search action", Thread.currentThread().getName());

    List<CoordinateSearchMetadata> metadatas = new ArrayList<>(request.requests().size());
    this.doExecuteFilterJoins(request, metadatas);
    this.doExecuteRequest(request, listener, metadatas);

    logger.debug("{}: Coordinated multi-search action completed", Thread.currentThread().getName());
  }

  private void doExecuteFilterJoins(final MultiSearchRequest request,
                                    final List<CoordinateSearchMetadata> metadatas) {
    FilterJoinCache cache = FilterJoinCache.getInstance();

    for (int i = 0; i < request.requests().size(); i++) {
      // Parse query source
      Tuple<XContentType, Map<String, Object>> parsedSource = this.parseSource(request.requests().get(i).source());
      Map<String, Object> map = parsedSource.v2();

      // Query planning and execution of filter joins
      SourceMapVisitor mapVisitor = new SourceMapVisitor(map);
      mapVisitor.traverse();
      FilterJoinVisitor joinVisitor = new CachedFilterJoinVisitor(client, mapVisitor.getFilterJoinTree(), cache);
      joinVisitor.traverse();
      metadatas.add(joinVisitor.getMetadata());

      // Filter joins have been replaced by a binary terms filter
      // Rebuild the query source, and delegate the execution of the search action
      request.requests().get(i).source(this.buildSource(parsedSource.v1().xContent(), map));
    }
  }

  private void doExecuteRequest(final MultiSearchRequest request, final ActionListener<MultiSearchResponse> listener,
                                final List<CoordinateSearchMetadata> metadatas) {
    ClusterState clusterState = clusterService.state();
    clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);

    final AtomicArray<CoordinateMultiSearchResponse.Item> responses = new AtomicArray<>(request.requests().size());
    final AtomicInteger counter = new AtomicInteger(responses.length());
    for (int i = 0; i < responses.length(); i++) {
      final int index = i;
      SearchRequest searchRequest = new SearchRequest(request.requests().get(i), request);
      searchAction.execute(searchRequest, new ActionListener<SearchResponse>() {

        @Override
        public void onResponse(SearchResponse searchResponse) {
          responses.set(index, new CoordinateMultiSearchResponse.Item(new CoordinateSearchResponse(searchResponse, metadatas.get(index)), null));
          if (counter.decrementAndGet() == 0) {
            finishHim();
          }
        }

        @Override
        public void onFailure(Throwable e) {
          responses.set(index, new CoordinateMultiSearchResponse.Item(null, ExceptionsHelper.detailedMessage(e)));
          if (counter.decrementAndGet() == 0) {
            finishHim();
          }
        }

        private void finishHim() {
          listener.onResponse(new CoordinateMultiSearchResponse(responses.toArray(new CoordinateMultiSearchResponse.Item[responses.length()])));
        }

      });
    }
  }

}
