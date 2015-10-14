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
package com.sindicetech.kb.filterjoin.action.node;

import org.elasticsearch.action.ClientAction;
import org.elasticsearch.client.Client;

public class NodesSimpleAction extends ClientAction<NodesSimpleRequest, NodesSimpleResponse, NodesSimpleRequestBuilder> {

  public static final NodesSimpleAction INSTANCE = new NodesSimpleAction();
  public static final String NAME = "nodes:simple";

  private NodesSimpleAction() {
    super(NodesSimpleAction.NAME);
  }

  /**
   * Creates a new request builder given the client provided as argument
   *
   * @param client
   */
  @Override
  public NodesSimpleRequestBuilder newRequestBuilder(final Client client) {
    return new NodesSimpleRequestBuilder(client);
  }

  /**
   * Creates a new response instance.
   */
  @Override
  public NodesSimpleResponse newResponse() {
    return new NodesSimpleResponse();
  }
}
