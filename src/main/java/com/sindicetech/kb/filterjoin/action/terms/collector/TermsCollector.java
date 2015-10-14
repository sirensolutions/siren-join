/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.action.terms.collector;

import com.carrotsearch.hppc.LongHashSet;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * Collects terms for a given field based on a {@link HitStream}.
 */
public class TermsCollector {

  private final SearchContext context;
  private final IndexFieldData indexFieldData;

  private int maxTerms = Integer.MAX_VALUE;

  public TermsCollector(IndexFieldData indexFieldData, SearchContext context) {
    this.indexFieldData = indexFieldData;
    this.context = context;
  }

  public void setMaxTerms(int maxTerms) {
    this.maxTerms = maxTerms;
  }

  /**
   * Collects the terms into a {@link LongHashSet}.
   */
  public TermsCollection collect(HitStream hitStream) throws IOException {
    hitStream.initialize(); // initialise the stream
    int nHits = hitStream.getHits();
    LongHashSet terms = new LongHashSet(this.maxTerms < nHits ? this.maxTerms : nHits);
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
    return new TermsCollection(terms, isPruned);
  }

  /**
   * Represents a collection of terms
   */
  public static class TermsCollection {

    /**
     * The set of terms
     */
    private LongHashSet termsHash;

    /**
     * A flag to indicate if the set of terms has been pruned
     */
    private boolean isPruned = false;

    public TermsCollection(LongHashSet terms, boolean isPruned) {
      this.termsHash = terms;
      this.isPruned = isPruned;
    }

    public TermsCollection(int numTerms) {
      this.termsHash = new LongHashSet(numTerms);
    }

    public LongHashSet getTermsHash() {
      return this.termsHash;
    }

    public boolean isPruned() {
      return isPruned;
    }

    public void merge(TermsCollection other) {
      if (termsHash == null) {
        // probably never hit this since we init terms to known size before merge
        termsHash = new LongHashSet(other.termsHash.size());
      }
      termsHash.addAll(other.termsHash);
      this.isPruned |= other.isPruned;
    }

  }

}
