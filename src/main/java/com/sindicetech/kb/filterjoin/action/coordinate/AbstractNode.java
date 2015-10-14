/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.action.coordinate;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract node of the abstract syntax tree.
 */
public abstract class AbstractNode {

  private final List<AbstractNode> children = new ArrayList<>();

  public void addChild(AbstractNode child) {
    this.children.add(child);
  }

  public boolean hasChildren() {
    return !this.children.isEmpty();
  }

  public List<AbstractNode> getChildren() {
    return this.children;
  }

}
