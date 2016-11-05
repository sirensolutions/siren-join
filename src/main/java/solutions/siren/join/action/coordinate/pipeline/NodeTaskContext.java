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
package solutions.siren.join.action.coordinate.pipeline;

import org.elasticsearch.client.Client;
import solutions.siren.join.action.coordinate.model.FilterJoinNode;
import solutions.siren.join.action.coordinate.execution.FilterJoinVisitor;

/**
 * The context of a {@link NodeTask} which holds references to objects that are necessary for the processing of a node.
 * This context will be provided to the {@link NodeTask} when the {@link NodePipelineManager} starts the execution of a
 * {@link NodeTask}.
 */
public class NodeTaskContext {

  private Client client;
  private FilterJoinNode node;
  private FilterJoinVisitor visitor;

  public NodeTaskContext(Client client, FilterJoinNode node, FilterJoinVisitor visitor) {
    this.client = client;
    this.node = node;
    this.visitor = visitor;
  }

  public Client getClient() {
    return client;
  }

  public FilterJoinNode getNode() {
    return node;
  }

  public FilterJoinVisitor getVisitor() {
    return visitor;
  }

}
