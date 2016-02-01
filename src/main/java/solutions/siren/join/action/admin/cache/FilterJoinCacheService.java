package solutions.siren.join.action.admin.cache;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import solutions.siren.join.action.coordinate.FilterJoinCache;

public class FilterJoinCacheService extends AbstractComponent {

  private final FilterJoinCache cache;

  @Inject
  public FilterJoinCacheService(Settings settings) {
    super(settings);
    this.cache = new FilterJoinCache(settings);
  }

  public FilterJoinCache getCacheInstance() {
    return this.cache;
  }

  public void clear() {
    cache.invalidateAll();
  }

  public FilterJoinCache.FilterJoinCacheStats getStats() {
    return cache.getStats();
  }

}
