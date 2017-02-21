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

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.rest.action.admin.indices.RestTypesExistsAction;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.SearchRequestParsers;
import solutions.siren.join.action.coordinate.CoordinateSearchAction;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestCoordinateSearchAction extends BaseRestHandler {

  private SearchRequestParsers searchRequestParsers;

  @Inject
  public RestCoordinateSearchAction(final Settings settings, final RestController controller, final SearchRequestParsers searchRequestParsers) {
    super(settings);
    controller.registerHandler(GET, "/_coordinate_search", this);
    controller.registerHandler(POST, "/_coordinate_search", this);
    controller.registerHandler(GET, "/{index}/_coordinate_search", this);
    controller.registerHandler(POST, "/{index}/_coordinate_search", this);
    controller.registerHandler(GET, "/{index}/{type}/_coordinate_search", this);
    controller.registerHandler(POST, "/{index}/{type}/_coordinate_search", this);
    controller.registerHandler(GET, "/_coordinate_search/template", this);
    controller.registerHandler(POST, "/_coordinate_search/template", this);
    controller.registerHandler(GET, "/{index}/_coordinate_search/template", this);
    controller.registerHandler(POST, "/{index}/_coordinate_search/template", this);
    controller.registerHandler(GET, "/{index}/{type}/_coordinate_search/template", this);
    controller.registerHandler(POST, "/{index}/{type}/_coordinate_search/template", this);

    // TODO: Redirects to original rest exists action, therefore it will not support filterjoin filter. It would be better to have our own coordinate exists action.

    controller.registerHandler(GET, "/_coordinate_search/exists", this);
    controller.registerHandler(POST, "/_coordinate_search/exists", this);
    controller.registerHandler(GET, "/{index}/_coordinate_search/exists", this);
    controller.registerHandler(POST, "/{index}/_coordinate_search/exists", this);
    controller.registerHandler(GET, "/{index}/{type}/_coordinate_search/exists", this);
    controller.registerHandler(POST, "/{index}/{type}/_coordinate_search/exists", this);

    this.searchRequestParsers = searchRequestParsers;
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
    SearchRequest searchRequest = new SearchRequest();
    BytesReference content = request.content().length() > 0 ? request.content() : XContentFactory.jsonBuilder().startObject().endObject().bytes();
    RestSearchAction.parseSearchRequest(searchRequest, request, searchRequestParsers ,parseFieldMatcher, content);
    return channel -> client.execute(CoordinateSearchAction.INSTANCE, searchRequest, new RestStatusToXContentListener<>(channel));
  }

  @Override
  public boolean canTripCircuitBreaker() {
    return false;
  }
}
