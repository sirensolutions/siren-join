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
package solutions.siren.join;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.common.settings.Settings;

import solutions.siren.join.action.admin.cache.ClearFilterJoinCacheAction;
import solutions.siren.join.action.admin.cache.ClearFilterJoinCacheRequestBuilder;
import solutions.siren.join.action.admin.cache.ClearFilterJoinCacheResponse;

import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;

public class SirenJoinTestCase extends ESIntegTestCase {

  @Override
  protected Settings nodeSettings(int nodeOrdinal) {
    return Settings.builder().put(super.nodeSettings(nodeOrdinal)).build();
  }

  @Override
  protected Collection<Class<? extends Plugin>> nodePlugins() {
    return Arrays.asList(SirenJoinPlugin.class, Netty4Plugin.class);
  }

  @Override
  protected Collection<Class<? extends Plugin>> transportClientPlugins() {
    return nodePlugins();
  }

  @Before
  public void beforeTest() throws Exception {
    logger.info("Invalidate filter join cache before test");
    ClearFilterJoinCacheResponse rsp = new ClearFilterJoinCacheRequestBuilder(client(), ClearFilterJoinCacheAction.INSTANCE).get();
  }

}