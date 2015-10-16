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
package solutions.siren.siren.join.rest;

import solutions.siren.siren.join.action.coordinate.CoordinateMultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestToXContentListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestCoordinateMultiSearchAction extends BaseRestHandler {

  private final boolean allowExplicitIndex;

  @Inject
  public RestCoordinateMultiSearchAction(final Settings settings, final RestController controller, final Client client) {
    super(settings, controller, client);
    controller.registerHandler(GET, "/_coordinate_msearch", this);
    controller.registerHandler(POST, "/_coordinate_msearch", this);
    controller.registerHandler(GET, "/{index}/_coordinate_msearch", this);
    controller.registerHandler(POST, "/{index}/_coordinate_msearch", this);
    controller.registerHandler(GET, "/{index}/{type}/_coordinate_msearch", this);
    controller.registerHandler(POST, "/{index}/{type}/_coordinate_msearch", this);

    this.allowExplicitIndex = settings.getAsBoolean("rest.action.multi.allow_explicit_index", true);
  }

  @Override
  public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) throws Exception {
    MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
    multiSearchRequest.listenerThreaded(false);

    String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
    String[] types = Strings.splitStringByCommaToArray(request.param("type"));
    IndicesOptions indicesOptions = IndicesOptions.fromRequest(request, multiSearchRequest.indicesOptions());
    multiSearchRequest.add(RestActions.getRestContent(request), indices, types,
      request.param("search_type"), request.param("routing"), indicesOptions, allowExplicitIndex);

    client.execute(CoordinateMultiSearchAction.INSTANCE, multiSearchRequest, new RestToXContentListener<MultiSearchResponse>(channel));
  }

}
