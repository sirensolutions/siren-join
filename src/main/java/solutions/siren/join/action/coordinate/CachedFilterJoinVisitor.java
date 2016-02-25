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
package solutions.siren.join.action.coordinate;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import solutions.siren.join.action.terms.TermsByQueryResponse;
import org.elasticsearch.client.Client;
import solutions.siren.join.action.terms.collector.TermsSet;

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

  private static final ESLogger logger = Loggers.getLogger(CachedFilterJoinVisitor.class);

  public CachedFilterJoinVisitor(Client client, RootNode root, FilterJoinCache cache) {
    super(client, root);
    this.cache = cache;
  }

  @Override
  protected void executeAsyncOperation(FilterJoinNode node) {
    // Check cache
    FilterJoinCache.CacheEntry cacheEntry = this.cache.get(node);

    if (cacheEntry == null) { // if cache miss
      logger.debug("Cache miss for terms by query action: {}", node.getCacheId());
      logger.debug("Executing async actions");
      node.setState(FilterJoinNode.State.RUNNING); // set state before execution to avoid race conditions
      // Create term by query request (can be an expensive operation - do it only if cache miss)
      TermsByQueryActionListener listener = new CachedTermsByQueryActionListener(node);
      node.setActionListener(listener);
      new AsyncCardinalityEstimationAction(client, node, listener).start();
    }
    else { // if cache hit
      logger.debug("Cache hit for terms by query action: {}", node.getCacheId());
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
    public CachedTermsByQueryActionListener(final BytesRef encodedTerms, int size, boolean isPruned, boolean cacheHit,
                                            final FilterJoinNode node) {
      super(node);
      this.setEncodedTerms(encodedTerms);
      this.setSize(size);
      this.setPruned(isPruned);
      this.cacheHit = cacheHit;
    }

    @Override
    public void onResponse(final TermsByQueryResponse termsByQueryResponse) {
      // We cache the list of encoded terms instead of the {@link TermsByQueryResponse} to save the
      // byte serialization computation
      CachedFilterJoinVisitor.this.cache.put(this.getNode(), termsByQueryResponse.getEncodedTermsSet(),
              termsByQueryResponse.getSize(), termsByQueryResponse.isPruned());

      super.onResponse(termsByQueryResponse);
    }

  }

}
