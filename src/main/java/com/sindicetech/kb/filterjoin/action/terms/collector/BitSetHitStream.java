/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.action.terms.collector;

import org.apache.lucene.index.AtomicReaderContext;
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
        atomicDocId = atomicDocId + 1 < bitSet.length() ? bitSet.nextSetBit(atomicDocId + 1) : -1;
      } while (atomicDocId == -1 && ++currentAtomicReaderId < collector.getFixedSets().size());
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

  private static class LimitedBitSetHitCollector extends Collector {

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
    public void collect(int doc) throws IOException {
      current.set(doc);
      totalHits++;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
      current = new FixedBitSet(context.reader().maxDoc());
      fixedBitSets.add(context.ord, current);
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {}

    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
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
