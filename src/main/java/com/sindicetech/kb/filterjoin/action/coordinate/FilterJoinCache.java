/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
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
