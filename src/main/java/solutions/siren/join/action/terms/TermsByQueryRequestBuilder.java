/**
 * Copyright (c) 2015, SIREn Solutions. All Rights Reserved.
 *
 * This file is part of the SIREn project.
 *
 * SIREn is a free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * SIREn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package solutions.siren.join.action.terms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * A terms by query action request builder. This is an internal api.
 */
public class TermsByQueryRequestBuilder extends BroadcastOperationRequestBuilder<TermsByQueryRequest, TermsByQueryResponse, TermsByQueryRequestBuilder> {

  public TermsByQueryRequestBuilder(ElasticsearchClient client, TermsByQueryAction action) {
    super(client, action, new TermsByQueryRequest());
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

  @Override
  public void execute(ActionListener<TermsByQueryResponse> listener) {
    client.execute(TermsByQueryAction.INSTANCE, request, listener);
  }

}
