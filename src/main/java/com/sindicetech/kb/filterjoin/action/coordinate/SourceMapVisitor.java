/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.action.coordinate;

import com.sindicetech.kb.filterjoin.index.query.FilterJoinBuilder;

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
