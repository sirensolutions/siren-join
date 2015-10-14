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
package com.sindicetech.kb.filterjoin.action.coordinate;

import org.elasticsearch.action.ClientAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;

public class CoordinateSearchAction extends ClientAction<SearchRequest, SearchResponse, SearchRequestBuilder> {

  public static final CoordinateSearchAction INSTANCE = new CoordinateSearchAction();
  public static final String NAME = "indices:data/read/coordinate-search";

  private CoordinateSearchAction() {
    super(NAME);
  }

  @Override
  public SearchRequestBuilder newRequestBuilder(Client client) {
    return new CoordinateSearchRequestBuilder(client);
  }

  @Override
  public SearchResponse newResponse() {
    return new SearchResponse();
  }
}
