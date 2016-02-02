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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * A {@link HitStream} implementation based on a {@link TopDocsCollector}.
 */
public class TopHitStream extends HitStream {

  private final int numHit;
  private ScoreDoc[] topHits;

  private int count = 0;
  private int currentAtomicReaderId = 0;
  private int currentAtomicDocId = -1;

  public TopHitStream(final int numHit, final Query query, final IndexSearcher searcher) throws IOException {
    super(query, new TopHitCollector(new HitQueue(numHit)), searcher);
    this.numHit = numHit;
  }

  @Override
  public void initialize() throws IOException {
    super.initialize();
    this.topHits = ((TopHitCollector) this.getCollector()).topDocs().scoreDocs;
    // Sort the hits by atomicReaderId in order to optimise lookup to field data cache. If the atomicReaderIds
    // are random, the atomic reader will likely change for each hit which is not very optimal when loading
    // values for the field data cache.
    // At this stage, we do not really care anymore about the order of the documents, so it is fine to reorder it.
    Arrays.sort(this.topHits, new Comparator<ScoreDoc>() {

      @Override
      public int compare(final ScoreDoc o1, final ScoreDoc o2) {
        return ((ScoreHit) o1).atomicReaderId - ((ScoreHit) o2).atomicReaderId;
      }

    });
  }

  @Override
  public int getTotalHits() {
    return ((TopHitCollector) this.getCollector()).getTotalHits();
  }

  @Override
  public int getHits() {
    return this.topHits.length;
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
    ScoreHit scoreHit = (ScoreHit) this.topHits[this.count];
    this.currentAtomicReaderId = scoreHit.atomicReaderId;
    this.currentAtomicDocId = scoreHit.doc;
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

  /**
   * A {@link TopDocsCollector} over the
   * {@link TopHitStream.ScoreHit}s.
   */
  private static class TopHitCollector extends TopDocsCollector<ScoreHit> {

    /** The identifier of the current atomic reader */
    private int currentAtomicReaderId;

    /** The priority queue */
    private ScoreHit pqTop;

    public TopHitCollector(HitQueue hq) {
      super(hq);
      // HitQueue implements getSentinelObject to return a ScoreHit, so we know
      // that at this point top() is already initialized.
      pqTop = pq.top();
    }

    @Override
    public boolean needsScores() {
      return true;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      currentAtomicReaderId = context.ord;
      final int docBase = context.docBase;
      return new LeafCollector() {

        Scorer scorer;

        @Override
        public void setScorer(Scorer scorer) throws IOException {
          this.scorer = scorer;
        }

        @Override
        public void collect(int doc) throws IOException {
          float score = scorer.score();

          // This collector cannot handle these scores:
          assert score != Float.NEGATIVE_INFINITY;
          assert !Float.isNaN(score);

          totalHits++;
          if (score <= pqTop.score) {
            // Since docs are returned in-order (i.e., increasing doc Id), a document
            // with equal score to pqTop.score cannot compete since HitQueue favors
            // documents with lower doc Ids. Therefore reject those docs too.
            return;
          }
          pqTop.atomicReaderId = currentAtomicReaderId;
          pqTop.doc = doc;
          pqTop.score = score;
          pqTop = pq.updateTop();
        }

      };
    }

  }

  /**
   * A {@link ScoreDoc} with an additional metadata to track the atomic reader id.
   */
  private static class ScoreHit extends ScoreDoc {

    /** the atomic reader id associated to the document's id */
    public int atomicReaderId;

    /** Constructs a ScoreHit. */
    public ScoreHit(int atomicReaderId, int docId, float score) {
      super(docId, score);
      this.atomicReaderId = atomicReaderId;
    }

  }

  /**
   * A priority queue over the {@link TopHitStream.ScoreHit}s.
   * ORder by taking into account the atomic reader id and the doc id.
   */
  private static class HitQueue extends PriorityQueue<ScoreHit> {

    HitQueue(int size) {
      super(size, true);
    }

    @Override
    protected ScoreHit getSentinelObject() {
      // Always set the doc Id to MAX_VALUE so that it won't be favored by
      // lessThan. This generally should not happen since if score is not NEG_INF,
      // TopScoreDocCollector will always add the object to the queue.
      return new ScoreHit(Integer.MAX_VALUE, Integer.MAX_VALUE, Float.NEGATIVE_INFINITY);
    }

    @Override
    protected final boolean lessThan(ScoreHit hitA, ScoreHit hitB) {
      if (hitA.score == hitB.score)
        return hitA.atomicReaderId > hitB.atomicReaderId & hitA.doc > hitB.doc;
      else
        return hitA.score < hitB.score;
    }

  }

}
