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
package solutions.siren.join.action.coordinate;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;

public class CoordinateMultiSearchAction extends Action<MultiSearchRequest, MultiSearchResponse, MultiSearchRequestBuilder> {

  public static final CoordinateMultiSearchAction INSTANCE = new CoordinateMultiSearchAction();
  public static final String NAME = "indices:data/read/coordinate-msearch";

  private CoordinateMultiSearchAction() {
    super(NAME);
  }

  @Override
  public MultiSearchRequestBuilder newRequestBuilder(ElasticsearchClient client) {
    return new CoordinateMultiSearchRequestBuilder(client);
  }

  @Override
  public MultiSearchResponse newResponse() {
    return new CoordinateMultiSearchResponse();
  }

}
