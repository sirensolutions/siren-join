/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.action.terms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * A terms by query action request builder. This is an internal api.
 */
public class TermsByQueryRequestBuilder extends BroadcastOperationRequestBuilder<TermsByQueryRequest, TermsByQueryResponse, TermsByQueryRequestBuilder, Client> {

  public TermsByQueryRequestBuilder(Client client) {
    super(client, new TermsByQueryRequest());
  }

  /**
   * The types of documents the query will run against. Defaults to all types.
   */
  public TermsByQueryRequestBuilder setTypes(String... types) {
    request.types(types);
    return this;
  }

  /**
   * A comma separated list of routing values to control the shards the search will be executed on.
   */
  public TermsByQueryRequestBuilder setRouting(String routing) {
    request.routing(routing);
    return this;
  }

  /**
   * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to <tt>_local</tt> to prefer local
   * shards, <tt>_primary</tt> to execute only on primary shards, _shards:x,y to operate on shards x & y, or a custom value, which
   * guarantees that the same order will be used across different requests.
   */
  public TermsByQueryRequestBuilder setPreference(String preference) {
    request.preference(preference);
    return this;
  }

  /**
   * The routing values to control the shards that the search will be executed on.
   */
  public TermsByQueryRequestBuilder setRouting(String... routing) {
    request.routing(routing);
    return this;
  }

  /**
   * The field to extract terms from.
   */
  public TermsByQueryRequestBuilder setField(String field) {
    request.field(field);
    return this;
  }

  /**
   * The query source to execute.
   *
   * @see org.elasticsearch.index.query.QueryBuilders
   */
  public TermsByQueryRequestBuilder setQuery(QueryBuilder queryBuilder) {
    request.query(queryBuilder);
    return this;
  }

  /**
   * The ordering to use before performing the term cut.
   */
  public TermsByQueryRequestBuilder setOrderBy(TermsByQueryRequest.Ordering ordering) {
    request.orderBy(ordering);
    return this;
  }

  /**
   * The max number of terms collected per shard
   */
  public TermsByQueryRequestBuilder setMaxTermsPerShard(int maxTermsPerShard) {
    request.maxTermsPerShard(maxTermsPerShard);
    return this;
  }

  /**
   * Executes the the request
   */
  @Override
  protected void doExecute(ActionListener<TermsByQueryResponse> listener) {
    client.execute(TermsByQueryAction.INSTANCE, request, listener);
  }
}
