/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.action.terms;

import org.elasticsearch.action.ClientAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportRequestOptions;

/**
 * The action to request terms by query
 */
public class TermsByQueryAction extends ClientAction<TermsByQueryRequest, TermsByQueryResponse, TermsByQueryRequestBuilder> {

  public static final TermsByQueryAction INSTANCE = new TermsByQueryAction();
  public static final String NAME = "indices:data/read/search/termsbyquery";

  /**
   * Default constructor
   */
  private TermsByQueryAction() {
    super(NAME);
  }

  /**
   * Gets a new {@link TermsByQueryResponse} object
   *
   * @return the new {@link TermsByQueryResponse}.
   */
  @Override
  public TermsByQueryResponse newResponse() {
    return new TermsByQueryResponse();
  }

  /**
   * Set transport options specific to a terms by query request.
   * Enabling compression here does not really reduce data transfer, even increase it on the contrary.
   *
   * @param settings node settings
   * @return the request options.
   */
  @Override
  public TransportRequestOptions transportOptions(Settings settings) {
    TransportRequestOptions opts = new TransportRequestOptions();
    opts.withType(TransportRequestOptions.Type.REG);
    return opts;
  }

  /**
   * Get a new {@link TermsByQueryRequestBuilder}
   *
   * @param client the client responsible for executing the request.
   * @return the new {@link TermsByQueryRequestBuilder}
   */
  @Override
  public TermsByQueryRequestBuilder newRequestBuilder(Client client) {
    return new TermsByQueryRequestBuilder(client);
  }

}
