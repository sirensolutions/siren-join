/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.action.coordinate;

import java.util.Map;

/**
 * The root node of the abstract syntax tree. It contains a reference to the source map.
 */
public class RootNode extends AbstractNode {

  private final Map<String, Object> self;

  public RootNode(Map<String, Object> self) {
    this.self = self;
  }

  public Map<String, Object> getSourceMap() {
    return self;
  }

}
