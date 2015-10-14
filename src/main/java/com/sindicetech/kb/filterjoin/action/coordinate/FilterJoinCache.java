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
package com.sindicetech.kb.filterjoin.action.coordinate;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.concurrent.TimeUnit;

/**
 * The filter join cache. It is thread-safe (use a {@link Cache} underneath).
 * The cache is based on a unique {@link FilterJoinNode}'s cache id, computed based on the source map
 * of the filter join.
 */
public class FilterJoinCache {

  private final Cache<Integer, CacheEntry> cache;

  // TODO: Make this configurable
  private static final int CACHE_SIZE = 16;
  private static final int CACHE_ENTRY_EXPIRATION = 10;

  /**
   * The singleton instance of the cache
   */
  private final static FilterJoinCache instance = new FilterJoinCache();

  protected static final ESLogger logger = Loggers.getLogger(FilterJoinCache.class);

  public static FilterJoinCache getInstance() {
    return instance;
  }

  private FilterJoinCache() {
    // TODO: Maybe add an eviction rule based on weight (based on the size of the bytes)
    this.cache = CacheBuilder.newBuilder()
            .maximumSize(CACHE_SIZE)
            .expireAfterWrite(CACHE_ENTRY_EXPIRATION, TimeUnit.MINUTES)
            .build();
  }

  /**
   * Caches the provided list of encoded terms for the given filter join node.
   */
  public void put(final FilterJoinNode node, final byte[] encodedTerms, final int size, final boolean isPruned) {
    this.cache.put(node.getCacheId(), new CacheEntry(encodedTerms, size, isPruned));
  }

  /**
   * Retrieves the list of encoded terms for the given filter join node.
   */
  public CacheEntry get(final FilterJoinNode node) {
    CacheEntry entry = this.cache.getIfPresent(node.getCacheId());
    return entry;
  }

  /**
   * Invalidate all cache entries
   */
  public void invalidateAll() {
    this.cache.invalidateAll();
  }

  /**
   * A cache entry is composed of the set of terms (encoded), a flag to indicate
   * if the set of terms has been pruned, the size in number of terms.
   */
  static class CacheEntry {

    final byte[] encodedTerms;
    final int size;
    final boolean isPruned;

    private CacheEntry(byte[] encodedTerms, int size, boolean isPruned) {
      this.encodedTerms = encodedTerms;
      this.size = size;
      this.isPruned = isPruned;
    }

  }

}
