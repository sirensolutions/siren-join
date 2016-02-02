package solutions.siren.join.modules;

import org.elasticsearch.common.inject.AbstractModule;
import solutions.siren.join.action.admin.cache.FilterJoinCacheService;

public class FilterJoinCacheModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(FilterJoinCacheService.class).asEagerSingleton();
  }

}
