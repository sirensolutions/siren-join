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

import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import solutions.siren.join.action.admin.cache.StatsFilterJoinCacheAction;
import solutions.siren.join.action.admin.cache.StatsFilterJoinCacheRequest;

import java.io.IOException;

public class RestStatsFilterJoinCacheAction extends BaseRestHandler {

  @Inject
  public RestStatsFilterJoinCacheAction(final Settings settings, final RestController controller) {
    super(settings);
    controller.registerHandler(RestRequest.Method.POST, "/_filter_join/cache/stats", this);
    controller.registerHandler(RestRequest.Method.GET, "/_filter_join/cache/stats", this);
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
    StatsFilterJoinCacheRequest statsFilterJoinCacheRequest = new StatsFilterJoinCacheRequest();
    return (consumer) -> client.doExecute(StatsFilterJoinCacheAction.INSTANCE, statsFilterJoinCacheRequest,
            new RestToXContentListener<>(consumer));
  }

  @Override
  public boolean canTripCircuitBreaker() {
    return false;
  }
}
