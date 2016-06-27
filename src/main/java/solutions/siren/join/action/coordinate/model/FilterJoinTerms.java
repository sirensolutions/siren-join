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
package solutions.siren.join.action.coordinate.model;

import org.apache.lucene.util.BytesRef;

/**
 * Holder for the terms computed during the processing of a {@link FilterJoinNode}.
 */
public class FilterJoinTerms {

  public FilterJoinTerms() {}

  /**
   * The set of encoded terms from the {@link solutions.siren.join.action.terms.TermsByQueryResponse}
   */
  private BytesRef encodedTerms;

  /**
   * The size of the set of terms (number of terms)
   */
  private int size;

  /**
   * The flag to indicate if the set of terms has been pruned
   */
  private boolean isPruned = false;

  /**
   * The time it took to retrieve the terms
   */
  private long tookInMillis = 0;

  /**
   * The flag to indicate if we hit the cache
   */
  private boolean cacheHit = false;

  /**
   * To be used by subclasses to set the encoded terms, for example if the encoded terms were
   * cached.
   */
  public void setEncodedTerms(final BytesRef encodedTerms) {
    this.encodedTerms = encodedTerms;
  }

  /**
   * To be used by subclasses to set the size, for example if the encoded terms were
   * cached.
   */
  public void setSize(int size) {
    this.size = size;
  }

  /**
   * To be used by subclasses to set the flag, for example if the encoded terms were
   * cached.
   */
  public void setPruned(boolean isPruned) {
    this.isPruned = isPruned;
  }

  public void setTookInMillis(long tookInMillis) {
    this.tookInMillis = tookInMillis;
  }

  public void setCacheHit(boolean cacheHit) {
    this.cacheHit = cacheHit;
  }

  public BytesRef getEncodedTerms() {
    return encodedTerms;
  }

  public int getSize() {
    return size;
  }

  public boolean isPruned() {
    return isPruned;
  }

  public long getTookInMillis() {
    return tookInMillis;
  }

  public boolean cacheHit() {
    return cacheHit;
  }

}