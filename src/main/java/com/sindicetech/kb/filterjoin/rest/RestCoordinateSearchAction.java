/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.rest;

import com.sindicetech.kb.filterjoin.action.coordinate.CoordinateSearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.exists.RestExistsAction;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.rest.action.support.RestStatusToXContentListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestCoordinateSearchAction extends BaseRestHandler {

  @Inject
  public RestCoordinateSearchAction(final Settings settings, final RestController controller, final Client client) {
    super(settings, controller, client);
    controller.registerHandler(GET, "/_coordinate_search", this);
    controller.registerHandler(POST, "/_coordinate_search", this);
    controller.registerHandler(GET, "/{index}/_coordinate_search", this);
    controller.registerHandler(POST, "/{index}/_coordinate_search", this);
    controller.registerHandler(GET, "/{index}/{type}/_coordinate_search", this);
    controller.registerHandler(POST, "/{index}/{type}/_coordinate_search", this);
    controller.registerHandler(GET, "/_coordinate_search/template", this);
    controller.registerHandler(POST, "/_coordinate_search/template", this);
    controller.registerHandler(GET, "/{index}/_coordinate_search/template", this);
    controller.registerHandler(POST, "/{index}/_coordinate_search/template", this);
    controller.registerHandler(GET, "/{index}/{type}/_coordinate_search/template", this);
    controller.registerHandler(POST, "/{index}/{type}/_coordinate_search/template", this);

    // TODO: Redirects to original rest exists action, therefore it will not support filterjoin filter. It would be better to have our own coordinate exists action.
    RestExistsAction restExistsAction = new RestExistsAction(settings, controller, client);
    controller.registerHandler(GET, "/_coordinate_search/exists", restExistsAction);
    controller.registerHandler(POST, "/_coordinate_search/exists", restExistsAction);
    controller.registerHandler(GET, "/{index}/_coordinate_search/exists", restExistsAction);
    controller.registerHandler(POST, "/{index}/_coordinate_search/exists", restExistsAction);
    controller.registerHandler(GET, "/{index}/{type}/_coordinate_search/exists", restExistsAction);
    controller.registerHandler(POST, "/{index}/{type}/_coordinate_search/exists", restExistsAction);
  }

  @Override
  public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
    SearchRequest searchRequest;
    searchRequest = RestSearchAction.parseSearchRequest(request);
    searchRequest.listenerThreaded(false);
    client.execute(CoordinateSearchAction.INSTANCE, searchRequest, new RestStatusToXContentListener<SearchResponse>(channel));
  }

}
