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

import org.elasticsearch.action.support.nodes.NodeOperationResponse;
import org.elasticsearch.action.support.nodes.NodesOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class NodeSimpleResponse extends NodeOperationResponse {

  public NodeSimpleResponse() {}

  @Override
  public void readFrom(StreamInput in) throws IOException {
    super.readFrom(in);
  }
  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
  }

}
