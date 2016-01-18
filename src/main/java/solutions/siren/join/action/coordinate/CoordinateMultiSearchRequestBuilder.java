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
package solutions.siren.join.action.coordinate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.client.ElasticsearchClient;

public class CoordinateMultiSearchRequestBuilder extends MultiSearchRequestBuilder {

  public CoordinateMultiSearchRequestBuilder(final ElasticsearchClient client) {
    // hack to be able to subclass MultiSearchRequestBuilder: the action instance is only used in #execute which we overwrite
    super(client, MultiSearchAction.INSTANCE);
  }

  @Override
  public void execute(final ActionListener<MultiSearchResponse> listener) {
    client.execute(CoordinateMultiSearchAction.INSTANCE, beforeExecute(request), listener);
  }

}
