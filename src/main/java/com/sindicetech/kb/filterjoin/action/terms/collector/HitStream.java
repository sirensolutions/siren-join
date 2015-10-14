/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.action.terms.collector;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.fielddata.IndexFieldData;

import java.io.IOException;

/**
 * A stream over the search hits. This stream is low-level and operates on a segment-level.
 */
public abstract class HitStream {

  private final Query query;
  protected final IndexSearcher searcher;
  private final Collector collector;

  public HitStream(Query query, Collector collector, IndexSearcher searcher) throws IOException {
    this.query = query;
    this.searcher = searcher;
    this.collector = collector;
  }

  /**
   * Initialize the stream by executing the search and collecting hits
   */
  public void initialize() throws IOException {
    this.searcher.search(query, this.collector);
  }

  /**
   * Returns the total number of documents that the collector encountered.
   */
  public abstract int getTotalHits();

  /**
   * Returns the total number of documents in the stream.
   */
  public abstract int getHits();

  /**
   * Returns the hit collector associated to this stream.
   */
  protected Collector getCollector() {
    return collector;
  }

  /**
   * Returns true if the stream has at least one remaining hit, false otherwise.
   */
  public abstract boolean hasNext();

  /**
   * Move to the next hit in the stream.
   */
  public abstract void next();

  /**
   * Returns the document id of the current hit. The document id is local to an atomic reader.
   * @see #getAtomicReaderId
   */
  protected abstract int getAtomicDocId();

  /**
   * Returns the atomic reader if of the current hit.
   */
  protected abstract int getAtomicReaderId();

  /**
   * Get the {@link TermStream} for the current hit.
   */
  public TermStream getTermStream(TermStream reusableTermStream) {
    reusableTermStream.set(this.getAtomicReaderId(), this.getAtomicDocId());
    return reusableTermStream;
  }

}
