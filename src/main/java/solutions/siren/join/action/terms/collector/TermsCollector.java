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
package solutions.siren.join.action.terms.collector;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.internal.SearchContext;
import solutions.siren.join.common.Math;

import java.io.IOException;

/**
 * Collects terms for a given field based on a {@link HitStream}.
 */
public abstract class TermsCollector {

  protected final SearchContext context;
  protected final IndexFieldData indexFieldData;
  protected final CircuitBreaker breaker;

  protected int expectedTerms = -1;
  protected int maxTerms = Integer.MAX_VALUE;

  public TermsCollector(final IndexFieldData indexFieldData, final SearchContext context,
                        final CircuitBreaker breaker) {
    this.indexFieldData = indexFieldData;
    this.context = context;
    this.breaker = breaker;
  }

  /**
   * Sets the expected number of terms to collect. Used to instantiate {@link TermsSet}. Default to number of hits.
   */
  public void setExpectedTerms(long expectedTerms) {
    this.expectedTerms = Math.toIntExact(expectedTerms);
  }

  /**
   * Sets the maximum number of terms to collect.
   */
  public void setMaxTerms(int maxTerms) {
    this.maxTerms = maxTerms;
  }

  /**
   * Collects the terms into a {@link TermsSet}.
   */
  public abstract TermsSet collect(HitStream hitStream) throws IOException;

}
