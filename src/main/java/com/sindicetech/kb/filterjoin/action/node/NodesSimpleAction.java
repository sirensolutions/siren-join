/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.action.node;

import org.elasticsearch.action.ClientAction;
import org.elasticsearch.client.Client;

public class NodesSimpleAction extends ClientAction<NodesSimpleRequest, NodesSimpleResponse, NodesSimpleRequestBuilder> {

  public static final NodesSimpleAction INSTANCE = new NodesSimpleAction();
  public static final String NAME = "nodes:simple";

  private NodesSimpleAction() {
    super(NodesSimpleAction.NAME);
  }

  /**
   * Creates a new request builder given the client provided as argument
   *
   * @param client
   */
  @Override
  public NodesSimpleRequestBuilder newRequestBuilder(final Client client) {
    return new NodesSimpleRequestBuilder(client);
  }

  /**
   * Creates a new response instance.
   */
  @Override
  public NodesSimpleResponse newResponse() {
    return new NodesSimpleResponse();
  }
}
