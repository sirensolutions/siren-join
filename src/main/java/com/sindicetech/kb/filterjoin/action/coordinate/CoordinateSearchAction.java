/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.action.coordinate;

import org.elasticsearch.action.ClientAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;

public class CoordinateSearchAction extends ClientAction<SearchRequest, SearchResponse, SearchRequestBuilder> {

  public static final CoordinateSearchAction INSTANCE = new CoordinateSearchAction();
  public static final String NAME = "indices:data/read/coordinate-search";

  private CoordinateSearchAction() {
    super(NAME);
  }

  @Override
  public SearchRequestBuilder newRequestBuilder(Client client) {
    return new CoordinateSearchRequestBuilder(client);
  }

  @Override
  public SearchResponse newResponse() {
    return new SearchResponse();
  }
}
