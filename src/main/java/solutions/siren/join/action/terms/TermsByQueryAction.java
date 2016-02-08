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

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportRequestOptions;

/**
 * The action to request terms by query
 */
public class TermsByQueryAction extends Action<TermsByQueryRequest, TermsByQueryResponse, TermsByQueryRequestBuilder> {

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
    return TransportRequestOptions.builder()
            .withType(TransportRequestOptions.Type.REG)
            .build();
  }

  /**
   * Get a new {@link TermsByQueryRequestBuilder}
   *
   * @param client the client responsible for executing the request.
   * @return the new {@link TermsByQueryRequestBuilder}
   */
  @Override
  public TermsByQueryRequestBuilder newRequestBuilder(ElasticsearchClient client) {
    return new TermsByQueryRequestBuilder(client, this);
  }

}
