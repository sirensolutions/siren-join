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

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestHandler;

import solutions.siren.join.action.admin.cache.ClearFilterJoinCacheAction;
import solutions.siren.join.action.admin.cache.StatsFilterJoinCacheAction;
import solutions.siren.join.action.admin.cache.TransportClearFilterJoinCacheAction;
import solutions.siren.join.action.admin.cache.TransportStatsFilterJoinCacheAction;
import solutions.siren.join.action.admin.version.*;
import solutions.siren.join.action.coordinate.CoordinateMultiSearchAction;
import solutions.siren.join.action.coordinate.CoordinateSearchAction;
import solutions.siren.join.action.coordinate.TransportCoordinateMultiSearchAction;
import solutions.siren.join.action.coordinate.TransportCoordinateSearchAction;
import solutions.siren.join.action.coordinate.execution.FilterJoinCache;
import solutions.siren.join.action.terms.TermsByQueryAction;
import solutions.siren.join.action.terms.TransportTermsByQueryAction;
import solutions.siren.join.index.query.FieldDataTermsQueryBuilder;
import solutions.siren.join.index.query.FilterJoinBuilder;
import solutions.siren.join.index.query.TermsEnumTermsQueryBuilder;
import solutions.siren.join.rest.RestClearFilterJoinCacheAction;
import solutions.siren.join.rest.RestCoordinateMultiSearchAction;
import solutions.siren.join.rest.RestCoordinateSearchAction;
import solutions.siren.join.rest.RestStatsFilterJoinCacheAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * The SIREn Join plugin.
 */
public class SirenJoinPlugin extends Plugin implements ActionPlugin, SearchPlugin {

  private final boolean isEnabled;
  private final IndexVersionService indexVersionService;

  @Inject
  public SirenJoinPlugin(Settings settings) {
    if (DiscoveryNode.isDataNode(settings) || DiscoveryNode.isMasterNode(settings)) {
      this.isEnabled = "node".equals(settings.get("client.type"));
    } else {
      this.isEnabled = false;
    }

    this.indexVersionService = isEnabled ? new IndexVersionService(settings) : null;
  }

  @Override
  public void onIndexModule(IndexModule indexModule) {
    if (!this.isEnabled) return;
    IndexVersionEventListener indexVersionEventListener = indexVersionService.getOrDefault(indexModule.getIndex());

    if (indexVersionEventListener == null) {
      VersionIndexingOperationListener indexingOperationListener = new VersionIndexingOperationListener();
      IndexVersionEventListener eventListener = new IndexVersionEventListener(indexingOperationListener);
      indexModule.addIndexEventListener(eventListener);
      indexModule.addIndexOperationListener(indexingOperationListener);
      this.indexVersionService.registerIndexEventListener(indexModule.getIndex(), eventListener);
    } else {
      indexModule.addIndexEventListener(indexVersionEventListener);
      indexModule.addIndexOperationListener(indexVersionEventListener.getOperationListener());
    }
  }

  @Override
  public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
    return Arrays.asList(
            new ActionHandler<>(TermsByQueryAction.INSTANCE, TransportTermsByQueryAction.class),
            new ActionHandler<>(CoordinateSearchAction.INSTANCE, TransportCoordinateSearchAction.class),
            new ActionHandler<>(CoordinateMultiSearchAction.INSTANCE, TransportCoordinateMultiSearchAction.class),
            new ActionHandler<>(ClearFilterJoinCacheAction.INSTANCE, TransportClearFilterJoinCacheAction.class),
            new ActionHandler<>(StatsFilterJoinCacheAction.INSTANCE, TransportStatsFilterJoinCacheAction.class),
            new ActionHandler<>(GetIndicesVersionAction.INSTANCE, TransportGetIndicesVersionAction.class));
  }


  @Override
  public List<QuerySpec<?>> getQueries() {
    return Arrays.asList(new QuerySpec<>(FieldDataTermsQueryBuilder.NAME, FieldDataTermsQueryBuilder::new, FieldDataTermsQueryBuilder::fromXContent),
            new QuerySpec<>(TermsEnumTermsQueryBuilder.NAME, TermsEnumTermsQueryBuilder::new, TermsEnumTermsQueryBuilder::fromXContent),
            new QuerySpec<>(FilterJoinBuilder.NAME, FilterJoinBuilder::new, FilterJoinBuilder::fromXContent));
  }

  @Override
  public List<Class<? extends RestHandler>> getRestHandlers() {
    return Arrays.asList(RestCoordinateSearchAction.class, RestCoordinateMultiSearchAction.class,
            RestClearFilterJoinCacheAction.class, RestStatsFilterJoinCacheAction.class);
  }

  @Override
  public Collection<Module> createGuiceModules() {
    if (isEnabled) {
      return Collections.singletonList(new SirenJoinNodeModule(this.indexVersionService));
    }
    else {
      return Collections.emptyList();
    }
  }

  @Override
  public List<Setting<?>> getSettings() {
    return Arrays.asList(Setting.boolSetting(FilterJoinCache.SIREN_FILTERJOIN_CACHE_ENABLED, true, Setting.Property.NodeScope),
            Setting.intSetting(FilterJoinCache.SIREN_FILTERJOIN_CACHE_SIZE, FilterJoinCache.DEFAULT_CACHE_SIZE, Setting.Property.NodeScope)
    );
  }

}