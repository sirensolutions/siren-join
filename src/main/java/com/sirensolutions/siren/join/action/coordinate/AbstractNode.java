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
package com.sirensolutions.siren.join.action.coordinate;

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
