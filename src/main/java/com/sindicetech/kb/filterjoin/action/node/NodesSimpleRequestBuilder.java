/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.action.node;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.Client;

public class NodesSimpleRequestBuilder extends ActionRequestBuilder<NodesSimpleRequest, NodesSimpleResponse, NodesSimpleRequestBuilder, Client> {

  public NodesSimpleRequestBuilder(Client client) {
    super(client, new NodesSimpleRequest());
  }

  @Override
  protected void doExecute(final ActionListener<NodesSimpleResponse> listener) {
    client.execute(NodesSimpleAction.INSTANCE, request, listener);
  }
}
