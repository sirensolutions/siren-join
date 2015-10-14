/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.action.coordinate;

import org.elasticsearch.action.ClientAction;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.Client;

public class CoordinateMultiSearchAction extends ClientAction<MultiSearchRequest, MultiSearchResponse, MultiSearchRequestBuilder> {

  public static final CoordinateMultiSearchAction INSTANCE = new CoordinateMultiSearchAction();
  public static final String NAME = "indices:data/read/coordinate-msearch";

  private CoordinateMultiSearchAction() {
    super(NAME);
  }

  @Override
  public MultiSearchRequestBuilder newRequestBuilder(Client client) {
    return new CoordinateMultiSearchRequestBuilder(client);
  }

  @Override
  public MultiSearchResponse newResponse() {
    return new CoordinateMultiSearchResponse();
  }

}
