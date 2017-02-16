/**
 * Copyright (c) 2016, SIREn Solutions. All Rights Reserved.
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
package solutions.siren.join.rest;

import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.rest.action.search.RestMultiSearchAction;
import org.elasticsearch.search.SearchRequestParsers;

import solutions.siren.join.action.coordinate.CoordinateMultiSearchAction;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestCoordinateMultiSearchAction extends BaseRestHandler {

  private final boolean allowExplicitIndex;
  private final SearchRequestParsers searchRequestParsers;

  @Inject
  public RestCoordinateMultiSearchAction(final Settings settings, final RestController controller, SearchRequestParsers searchRequestParsers) {
    super(settings);
    controller.registerHandler(GET, "/_coordinate_msearch", this);
    controller.registerHandler(POST, "/_coordinate_msearch", this);
    controller.registerHandler(GET, "/{index}/_coordinate_msearch", this);
    controller.registerHandler(POST, "/{index}/_coordinate_msearch", this);
    controller.registerHandler(GET, "/{index}/{type}/_coordinate_msearch", this);
    controller.registerHandler(POST, "/{index}/{type}/_coordinate_msearch", this);

    this.allowExplicitIndex = settings.getAsBoolean("rest.action.multi.allow_explicit_index", true);
    this.searchRequestParsers = searchRequestParsers;
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
    MultiSearchRequest multiSearchRequest = RestMultiSearchAction.parseRequest(request, allowExplicitIndex, searchRequestParsers, parseFieldMatcher);
    return channel -> client.execute(CoordinateMultiSearchAction.INSTANCE, multiSearchRequest, new RestToXContentListener<>(channel));
  }

  @Override
  public boolean canTripCircuitBreaker() {
    return false;
  }
}