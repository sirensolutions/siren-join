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
package solutions.siren.join.action.terms.collector;

import com.carrotsearch.hppc.LongHashSet;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.internal.SearchContext;
import solutions.siren.join.common.Math;

import java.io.IOException;

/**
 * Collects terms for a given field based on a {@link HitStream}.
 */
public abstract class TermsCollector {

  private final SearchContext context;
  private final IndexFieldData indexFieldData;
  private final CircuitBreaker breaker;

  private int expectedTerms = -1;
  private int maxTerms = Integer.MAX_VALUE;

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

  protected abstract TermsSet newTermsSet(final int expectedElements, final CircuitBreaker breaker);

  /**
   * Collects the terms into a {@link LongHashSet}.
   */
  public TermsSet collect(HitStream hitStream) throws IOException {
    hitStream.initialize(); // initialise the stream
    int nHits = hitStream.getHits();
    TermsSet terms = this.newTermsSet(this.expectedTerms != -1 ? this.expectedTerms : nHits, breaker);
    try {
      TermStream reusableTermStream = TermStream.get(context.searcher().getIndexReader(), indexFieldData);

      while (terms.size() < this.maxTerms && hitStream.hasNext()) {
        hitStream.next();
        reusableTermStream = hitStream.getTermStream(reusableTermStream);

        while (terms.size() < this.maxTerms && reusableTermStream.hasNext()) {
          terms.add(reusableTermStream.next());
        }
      }

      boolean isPruned = hitStream.getTotalHits() > hitStream.getHits();
      isPruned |= this.maxTerms < nHits;
      terms.setIsPruned(isPruned);
      return terms;
    }
    catch (Throwable t) {
      // If something happens during the term collection, release the terms set and adjust the circuit breaker
      terms.release();
      throw t;
    }
  }

}
