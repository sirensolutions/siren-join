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

import com.carrotsearch.hppc.LongHashSet;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * Collects numeric terms for a given field based on a {@link HitStream}.
 */
public abstract class NumericTermsCollector extends TermsCollector {

  public NumericTermsCollector(final IndexFieldData indexFieldData, final SearchContext context,
                               final CircuitBreaker breaker) {
    super(indexFieldData, context, breaker);
  }

  protected abstract NumericTermsSet newTermsSet(final int expectedElements, final CircuitBreaker breaker);

  /**
   * Collects the terms into a {@link LongHashSet}.
   */
  public NumericTermsSet collect(HitStream hitStream) throws IOException {
    hitStream.initialize(); // initialise the stream
    int nHits = hitStream.getHits();
    NumericTermsSet terms = this.newTermsSet(this.expectedTerms != -1 ? this.expectedTerms : nHits, breaker);
    try {
      NumericTermStream reusableTermStream = NumericTermStream.get(context.searcher().getIndexReader(), indexFieldData);

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
