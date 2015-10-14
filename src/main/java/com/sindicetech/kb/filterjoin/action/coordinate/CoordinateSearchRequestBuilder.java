/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.action.coordinate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;

public class CoordinateSearchRequestBuilder extends SearchRequestBuilder {

  public CoordinateSearchRequestBuilder(final Client client) {
    super(client);
  }

  @Override
  protected void doExecute(final ActionListener<SearchResponse> listener) {
    client.execute(CoordinateSearchAction.INSTANCE, this.request(), listener);
  }

}
