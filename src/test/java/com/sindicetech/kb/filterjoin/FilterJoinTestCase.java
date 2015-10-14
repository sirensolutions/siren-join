/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin;

import com.sindicetech.kb.filterjoin.action.coordinate.FilterJoinCache;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Before;

public class FilterJoinTestCase extends ElasticsearchIntegrationTest {

  @Override
  protected Settings nodeSettings(int nodeOrdinal) {
    return ImmutableSettings.settingsBuilder()
      .put("path.data", "./target/elasticsearch-test/data/")
      .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
      .put(super.nodeSettings(nodeOrdinal)).build();
  }

  /**
   * Elasticsearch test framework will randomly use a transport client instead of a node.
   * It is necessary to explicitly tell Elasticsearch to load plugins for transport clients, otherwise
   * tests will fail randomly.
   */
  @Override
  protected Settings transportClientSettings() {
    return ImmutableSettings.settingsBuilder()
      .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
      .put(super.transportClientSettings()).build();
  }

  @Before
  public void beforeTest() throws Exception {
    logger.info("Invalidate filter join cache before test");
    FilterJoinCache.getInstance().invalidateAll();
  }

}
