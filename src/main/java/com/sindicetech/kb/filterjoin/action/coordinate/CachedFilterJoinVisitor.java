/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.action.coordinate;

import com.sindicetech.kb.filterjoin.action.terms.TermsByQueryAction;
import com.sindicetech.kb.filterjoin.action.terms.TermsByQueryRequest;
import com.sindicetech.kb.filterjoin.action.terms.TermsByQueryResponse;
import org.elasticsearch.client.Client;

/**
 * This visitor will cache the filter join and their resulting terms. This is useful when traversing
 * more than one tree with overlapping filter joins, as it will avoid computing multiple times the same
 * filter join.
 */
public class CachedFilterJoinVisitor extends FilterJoinVisitor {

  /**
   * The filter join cache
   */
  private final FilterJoinCache cache;

  public CachedFilterJoinVisitor(Client client, RootNode root, FilterJoinCache cache) {
    super(client, root);
    this.cache = cache;
  }

  /**
   * Execute a terms by query action
   */
  @Override
  protected void executeAsyncOperation(FilterJoinNode node) {
    // Check cache
    FilterJoinCache.CacheEntry cacheEntry = this.cache.get(node);

    if (cacheEntry == null) { // if cache miss
      logger.debug("Executing async terms by query action");
      // Create term by query request (can be an expensive operation - do it only if cache miss)
      final TermsByQueryRequest termsByQueryReq = this.getTermsByQueryRequest(node);
      TermsByQueryActionListener listener = new CachedTermsByQueryActionListener(node);
      node.setActionListener(listener);
      node.setState(FilterJoinNode.State.RUNNING); // set state before execution to avoid race conditions
      client.execute(TermsByQueryAction.INSTANCE, termsByQueryReq, listener);
    }
    else { // if cache hit
      logger.debug("Cache hit for terms by query action");
      TermsByQueryActionListener listener = new CachedTermsByQueryActionListener(cacheEntry.encodedTerms, cacheEntry.size, cacheEntry.isPruned, true, node);
      node.setActionListener(listener);
      node.setState(FilterJoinNode.State.COMPLETED); // set state before unblocking the queue to avoid race conditions
      this.blockingQueue.offer(0);
    }
  }

  @Override
  protected CoordinateSearchMetadata.Action recordMetadata(FilterJoinNode node) {
    CoordinateSearchMetadata.Action action = super.recordMetadata(node);
    CachedTermsByQueryActionListener listener = (CachedTermsByQueryActionListener) node.getActionListener();
    action.setCacheHit(listener.cacheHit);
    return action;
  }

  /**
   * On response, it caches the filter join with the associated list of encoded terms.
   */
  private class CachedTermsByQueryActionListener extends TermsByQueryActionListener {

    /**
     * The flag to indicate if we hit the cache
     */
    private boolean cacheHit = false;

    public CachedTermsByQueryActionListener(final FilterJoinNode node) {
      super(node);
    }

    /**
     * Constructor to be used when retrieving the list of encoded terms from the cache.
     */
    public CachedTermsByQueryActionListener(final byte[] encodedTerms, int size, boolean isPruned, boolean cacheHit,
                                            final FilterJoinNode node) {
      super(node);
      this.setEncodedTerms(encodedTerms);
      this.setSize(size);
      this.setPruned(isPruned);
      this.cacheHit = cacheHit;
    }

    @Override
    public void onResponse(final TermsByQueryResponse termsByQueryResponse) {
      super.onResponse(termsByQueryResponse);
      // We cache the list of encoded terms instead of the {@link TermsByQueryResponse} to save the
      // byte serialization computation
      CachedFilterJoinVisitor.this.cache.put(this.getNode(), this.getEncodedTerms(), this.getSize(), this.isPruned());
    }

  }

}
