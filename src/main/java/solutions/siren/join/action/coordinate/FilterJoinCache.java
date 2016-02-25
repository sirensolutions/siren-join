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
package solutions.siren.join.action.coordinate;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.Weigher;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * The filter join cache. It is thread-safe (use a {@link Cache} underneath).
 * The cache is based on a unique {@link FilterJoinNode}'s cache id, computed based on the source map
 * of the filter join.
 */
public class FilterJoinCache {

  private final Cache<Integer, CacheEntry> cache;

  /**
   * The maximum size (in bytes) of the cache. Default to 256MB.
   */
  private static final int DEFAULT_CACHE_SIZE = 262144;

  /**
   * The time (in second) before a cache entry expires. Default to 360 seconds.
   */
  private static final long DEFAULT_CACHE_EXPIRE = 360;

  public final static String SIREN_FILTERJOIN_CACHE_ENABLED = "siren.filterjoin.cache.enabled";
  public final static String SIREN_FILTERJOIN_CACHE_SIZE = "siren.filterjoin.cache.size";
  public final static String SIREN_FILTERJOIN_CACHE_EXPIRE = "siren.filterjoin.cache.expire";

  private static final ESLogger logger = Loggers.getLogger(FilterJoinCache.class);

  public FilterJoinCache(Settings settings) {
    boolean isEnabled = settings.getAsBoolean(SIREN_FILTERJOIN_CACHE_ENABLED, true);
    long size = settings.getAsInt(SIREN_FILTERJOIN_CACHE_SIZE, DEFAULT_CACHE_SIZE);
    long duration = settings.getAsLong(SIREN_FILTERJOIN_CACHE_EXPIRE, DEFAULT_CACHE_EXPIRE);

    if (isEnabled) {
      this.cache = CacheBuilder.newBuilder()
              .recordStats()
              .maximumWeight(size)
              .weigher(new CacheEntryWeigher())
              .expireAfterWrite(duration, TimeUnit.SECONDS)
              .build();
    }
    else {
      this.cache = CacheBuilder.newBuilder().maximumSize(0).build();
    }
  }

  /**
   * Caches the provided list of encoded terms for the given filter join node.
   */
  public void put(final FilterJoinNode node, final BytesRef encodedTerms, final int size, final boolean isPruned) {
    logger.debug("{}: New cache entry {}", Thread.currentThread().getName(), node.getCacheId());
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
    logger.debug("{}: Invalidate all cache entries", Thread.currentThread().getName());
    this.cache.invalidateAll();
  }

  /**
   * Returns a current snapshot of this cache's cumulative statistics. All stats are initialized
   * to zero, and are monotonically increasing over the lifetime of the cache.
   */
  public FilterJoinCacheStats getStats() {
    return new FilterJoinCacheStats(cache.size(), cache.stats());
  }

  /**
   * Returns the approximate number of entries in this cache.
   */
  public long getSize() {
    return cache.size();
  }

  /**
   * A cache entry is composed of the set of terms (encoded), a flag to indicate
   * if the set of terms has been pruned, the size in number of terms.
   */
  static class CacheEntry {

    final BytesRef encodedTerms;
    final int size;
    final boolean isPruned;

    private CacheEntry(BytesRef encodedTerms, int size, boolean isPruned) {
      this.encodedTerms = encodedTerms;
      this.size = size;
      this.isPruned = isPruned;
    }

  }

  static class CacheEntryWeigher implements Weigher<Integer, CacheEntry> {

    @Override
    public int weigh(Integer key, CacheEntry value) {
      return value.encodedTerms.length;
    }

  }

  public static class FilterJoinCacheStats implements Streamable {

    private CacheStats cacheStats;
    private long size;

    public FilterJoinCacheStats() {}

    public FilterJoinCacheStats(long size, CacheStats stats) {
      this.cacheStats = stats;
      this.size = size;
    }

    public long getSize() {
      return size;
    }

    public CacheStats getCacheStats() {
      return cacheStats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
      size = in.readVLong();
      long hitCount = in.readVLong();
      long misscount = in.readVLong();
      long loadSuccessCount = in.readVLong();
      long loadExceptionCount = in.readVLong();
      long totalLoadTime = in.readVLong();
      long evictionCount = in.readVLong();
      cacheStats = new CacheStats(hitCount, misscount, loadSuccessCount, loadExceptionCount, totalLoadTime, evictionCount);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
      out.writeVLong(size);
      out.writeVLong(cacheStats.hitCount());
      out.writeVLong(cacheStats.missCount());
      out.writeVLong(cacheStats.loadSuccessCount());
      out.writeVLong(cacheStats.loadExceptionCount());
      out.writeVLong(cacheStats.totalLoadTime());
      out.writeVLong(cacheStats.evictionCount());

    }
  }

}
