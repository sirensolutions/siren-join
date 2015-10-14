/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.action.coordinate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;

/**
 * The transport action for a coordinated search.
 */
public class TransportCoordinateSearchAction extends BaseTransportCoordinateSearchAction<SearchRequest, SearchResponse> {

  private final TransportSearchAction searchAction;

  @Inject
  public TransportCoordinateSearchAction(Settings settings, ThreadPool threadPool,
                                         TransportService transportService, ActionFilters actionFilters,
                                         TransportSearchAction searchAction, Client client) {
    super(settings, CoordinateSearchAction.NAME, threadPool, transportService, actionFilters, client);
    this.searchAction = searchAction;
  }

  @Override
  public SearchRequest newRequestInstance() {
    return this.searchAction.newRequestInstance();
  }

  @Override
  protected void doExecute(final SearchRequest request, final ActionListener<SearchResponse> listener) {
    logger.debug("{}: Execute coordinated search action", Thread.currentThread().getName());

    // A reference to the listener that will be used - can be overwritten to reference a CoordinateSearchListener
    ActionListener<SearchResponse> actionListener = listener;

    // Retrieve the singleton instance of the filterjoin cache
    FilterJoinCache cache = FilterJoinCache.getInstance();

    // Parse query source
    Tuple<XContentType, Map<String, Object>> parsedSource = this.parseSource(request.source());
    if (parsedSource != null) { // can be null if this is a uri search (query parameter in extraSource)
      Map<String, Object> map = parsedSource.v2();

      // Query planning and execution of filter joins
      SourceMapVisitor mapVisitor = new SourceMapVisitor(map);
      mapVisitor.traverse();
      FilterJoinVisitor joinVisitor = new CachedFilterJoinVisitor(client, mapVisitor.getFilterJoinTree(), cache);
      joinVisitor.traverse();

      // Wraps the listener with our own to inject metadata information in the response
      CoordinateSearchListener coordinateSearchListener = new CoordinateSearchListener(listener);
      coordinateSearchListener.setMetadata(joinVisitor.getMetadata());
      actionListener = coordinateSearchListener;

      // Filter joins have been replaced by a binary terms filter
      // Rebuild the query source, and delegate the execution of the search action
      request.source(this.buildSource(parsedSource.v1().xContent(), map));
    }

    // Delegate the execution of the request to the original search action
    this.searchAction.execute(request, actionListener);

    logger.debug("{}: Coordinated search action completed", Thread.currentThread().getName());
  }

  /**
   * Wrapper around a listener that is responsible for injecting the coordinate search metadata
   * into the search response.
   */
  public static class CoordinateSearchListener implements ActionListener<SearchResponse> {

    private final ActionListener<SearchResponse> actionListener;

    private CoordinateSearchMetadata metadata;

    public CoordinateSearchListener(final ActionListener<SearchResponse> listener) {
      this.actionListener = listener;
    }

    public void setMetadata(CoordinateSearchMetadata metadata) {
      this.metadata = metadata;
    }

    @Override
    public final void onResponse(SearchResponse response) {
      CoordinateSearchResponse r = new CoordinateSearchResponse(response, metadata);
      this.actionListener.onResponse(r);
    }

    @Override
    public final void onFailure(Throwable e) {
      this.actionListener.onFailure(e);
    }

  }

}
