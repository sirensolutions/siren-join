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
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * Collects terms for a given field based on a {@link HitStream}.
 */
public abstract class TermsCollector {

  private final SearchContext context;
  private final IndexFieldData indexFieldData;
  private final CircuitBreakerService breakerService;

  private int maxTerms = Integer.MAX_VALUE;

  public TermsCollector(final IndexFieldData indexFieldData, final SearchContext context,
                        final CircuitBreakerService breakerService) {
    this.indexFieldData = indexFieldData;
    this.context = context;
    this.breakerService = breakerService;
  }

  public void setMaxTerms(int maxTerms) {
    this.maxTerms = maxTerms;
  }

  protected abstract TermsSet newTermsSet(final int expectedElements, final CircuitBreakerService breakerService);

  /**
   * Collects the terms into a {@link LongHashSet}.
   */
  public TermsSet collect(HitStream hitStream) throws IOException {
    hitStream.initialize(); // initialise the stream
    int nHits = hitStream.getHits();
    TermsSet terms = this.newTermsSet(this.maxTerms < nHits ? this.maxTerms : nHits, breakerService);
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

}
