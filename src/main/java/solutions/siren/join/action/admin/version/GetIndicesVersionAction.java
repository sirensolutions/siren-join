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
package solutions.siren.join.action.admin.version;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Action to compute the version of a set of indices.
 */
public class GetIndicesVersionAction extends Action<GetIndicesVersionRequest, GetIndicesVersionResponse, GetIndicesVersionRequestBuilder> {

  public static final GetIndicesVersionAction INSTANCE = new GetIndicesVersionAction();
  public static final String NAME = "indices:admin/version/get";

  private GetIndicesVersionAction() {
    super(NAME);
  }

  @Override
  public GetIndicesVersionResponse newResponse() {
    return new GetIndicesVersionResponse();
  }

  @Override
  public GetIndicesVersionRequestBuilder newRequestBuilder(ElasticsearchClient client) {
    return new GetIndicesVersionRequestBuilder(client, this);
  }

}