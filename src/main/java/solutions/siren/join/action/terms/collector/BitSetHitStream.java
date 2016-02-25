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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link HitStream} implementation based on bitsets.
 */
public class BitSetHitStream extends HitStream {

  private int count = 0;
  private int currentAtomicReaderId = 0;
  private int currentAtomicDocId = -1;

  public BitSetHitStream(final Query query, final IndexSearcher searcher) throws IOException {
    // wraps the query into a ConstantScoreQuery since we do not need the score
    super(new ConstantScoreQuery(query), new LimitedBitSetHitCollector(searcher.getIndexReader().leaves().size()), searcher);
  }

  @Override
  public int getTotalHits() {
    return ((LimitedBitSetHitCollector) this.getCollector()).getTotalHits();
  }

  @Override
  public int getHits() {
    return this.getTotalHits();
  }

  @Override
  public boolean hasNext() {
    if (this.count < this.getHits()) {
      return true;
    }
    return false;
  }

  @Override
  public void next() {
    LimitedBitSetHitCollector collector = (LimitedBitSetHitCollector) this.getCollector();
    int atomicDocId = this.currentAtomicDocId;

    if (currentAtomicReaderId < collector.getFixedSets().size()) {
      do {
        FixedBitSet bitSet = collector.getFixedSets().get(currentAtomicReaderId);
        if (atomicDocId == DocIdSetIterator.NO_MORE_DOCS) { // we start a new reader, reset the doc id
          atomicDocId = -1;
        }
        atomicDocId = atomicDocId + 1 < bitSet.length() ? bitSet.nextSetBit(atomicDocId + 1) : DocIdSetIterator.NO_MORE_DOCS;
      } while (atomicDocId == DocIdSetIterator.NO_MORE_DOCS && ++currentAtomicReaderId < collector.getFixedSets().size());
    }

    this.currentAtomicDocId = atomicDocId;
    this.count++;
  }

  @Override
  public int getAtomicDocId() {
    return currentAtomicDocId;
  }

  @Override
  public int getAtomicReaderId() {
    return currentAtomicReaderId;
  }

  private static class LimitedBitSetHitCollector implements Collector {

    /** The total number of documents that the collector encountered. */
    private int totalHits;

    /** Bitset for each atomic reader, ordered by atomic reader id */
    private final List<FixedBitSet> fixedBitSets;

    /** The current bitset being read */
    private FixedBitSet current;

    public LimitedBitSetHitCollector(int numSegments) {
      this.fixedBitSets = new ArrayList<>(numSegments);
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      current = new FixedBitSet(context.reader().maxDoc());
      fixedBitSets.add(context.ord, current);

      return new LeafCollector() {

        @Override
        public void setScorer(Scorer scorer) throws IOException {}

        @Override
        public void collect(int doc) throws IOException {
          current.set(doc);
          totalHits++;
        }

      };
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    /**
     * The BitSets for each atomic reader
     */
    public List<FixedBitSet> getFixedSets() {
      return fixedBitSets;
    }

    public int getTotalHits() {
      return this.totalHits;
    }

  }

}
