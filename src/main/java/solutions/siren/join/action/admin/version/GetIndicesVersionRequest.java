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

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.Strings;

/**
 * Request to compute the version of a set of indices.
 */
public class GetIndicesVersionRequest extends BroadcastRequest<GetIndicesVersionRequest> {

  public GetIndicesVersionRequest() {
    this(Strings.EMPTY_ARRAY);
  }

  public GetIndicesVersionRequest(String... indices) {
    super(indices);
  }

  /**
   * Constructor used internally to execute a request that originates from a parent request.
   * This is required for Shield compatibility. This will copy the context and headers (which contain the Shield tokens)
   * of the original request to the new request.
   */
  public GetIndicesVersionRequest(ActionRequest originalRequest, String... indices) {
    super(indices);
    this.indices(indices);
  }

}