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
package solutions.siren.join.action.coordinate.execution;

import solutions.siren.join.action.coordinate.model.AbstractNode;
import solutions.siren.join.action.coordinate.model.FilterJoinNode;
import solutions.siren.join.action.coordinate.model.RootNode;
import solutions.siren.join.index.query.FilterJoinBuilder;

import java.util.*;

/**
 *  Visitor that will convert the source map into a tree of {@link AbstractNode}s. The tree will
 *  be composed of a root, {@link RootNode}, with one of more {@link FilterJoinNode}s as children.
 */
@SuppressWarnings("unchecked")
public class SourceMapVisitor {

  private final RootNode root;
  private Deque<AbstractNode> queue = new LinkedList<>();

  public SourceMapVisitor(Map map) {
    this.root = new RootNode(map);
    this.queue.offer(this.root);
  }

  public RootNode getFilterJoinTree() {
    return root;
  }

  public void traverse() {
    this.visit(root.getSourceMap());
  }

  private void visit(Map map) {
    Set<Map.Entry> entries = map.entrySet();

    // if map contains a filter join, create a filter join node
    if (map.containsKey(FilterJoinBuilder.NAME)) {
      FilterJoinNode node = new FilterJoinNode(map, (Map<String, Object>) map.get(FilterJoinBuilder.NAME));
      queue.peek().addChild(node);
      queue.addFirst(node);
    }

    // traverse map
    for (Map.Entry entry : entries) {
      this.visit(entry.getValue());
    }

    // if the map contains a filter join, remove the latest filter join node from the queue
    if (map.containsKey(FilterJoinBuilder.NAME)) {
      queue.poll();
    }

  }

  private void visit(List array) {
    for (Object obj : array) {
      this.visit(obj);
    }
  }

  private void visit(Object value) {
    if (value instanceof Map) {
      this.visit((Map<String, Object>) value);
    }
    else if (value instanceof List) {
      this.visit((List) value);
    }
  }

}
