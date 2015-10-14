/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.action.coordinate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;

public class CoordinateMultiSearchRequestBuilder extends MultiSearchRequestBuilder {

  public CoordinateMultiSearchRequestBuilder(final Client client) {
    super(client);
  }

  @Override
  protected void doExecute(final ActionListener<MultiSearchResponse> listener) {
    client.execute(CoordinateMultiSearchAction.INSTANCE, this.request(), listener);
  }

}
