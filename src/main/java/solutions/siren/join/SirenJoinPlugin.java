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
package solutions.siren.join;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.index.cache.IndexCacheModule;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.breaker.CircuitBreakerModule;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestModule;
import solutions.siren.join.action.admin.cache.ClearFilterJoinCacheAction;
import solutions.siren.join.action.admin.cache.StatsFilterJoinCacheAction;
import solutions.siren.join.action.admin.cache.TransportClearFilterJoinCacheAction;
import solutions.siren.join.action.admin.cache.TransportStatsFilterJoinCacheAction;
import solutions.siren.join.action.coordinate.CoordinateMultiSearchAction;
import solutions.siren.join.action.coordinate.CoordinateSearchAction;
import solutions.siren.join.action.coordinate.TransportCoordinateMultiSearchAction;
import solutions.siren.join.action.coordinate.TransportCoordinateSearchAction;
import solutions.siren.join.action.terms.TermsByQueryAction;
import solutions.siren.join.action.terms.TransportTermsByQueryAction;
import solutions.siren.join.index.query.FieldDataTermsQueryParser;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import solutions.siren.join.modules.FilterJoinCacheModule;
import solutions.siren.join.rest.RestClearFilterJoinCacheAction;
import solutions.siren.join.rest.RestCoordinateMultiSearchAction;
import solutions.siren.join.rest.RestCoordinateSearchAction;
import solutions.siren.join.rest.StatsClearFilterJoinCacheAction;

import java.util.ArrayList;
import java.util.Collection;

/**
 * The SIREn Join plugin.
 */
public class SirenJoinPlugin extends Plugin {

  @Inject
  public SirenJoinPlugin(Settings settings) {
  }

  public void onModule(ActionModule module) {
    module.registerAction(TermsByQueryAction.INSTANCE, TransportTermsByQueryAction.class);
    module.registerAction(CoordinateSearchAction.INSTANCE, TransportCoordinateSearchAction.class);
    module.registerAction(CoordinateMultiSearchAction.INSTANCE, TransportCoordinateMultiSearchAction.class);
    module.registerAction(ClearFilterJoinCacheAction.INSTANCE, TransportClearFilterJoinCacheAction.class);
    module.registerAction(StatsFilterJoinCacheAction.INSTANCE, TransportStatsFilterJoinCacheAction.class);
  }

  public void onModule(IndicesModule module) {
    module.registerQueryParser(FieldDataTermsQueryParser.class);
  }

  public void onModule(RestModule module) {
    module.addRestAction(RestCoordinateSearchAction.class);
    module.addRestAction(RestCoordinateMultiSearchAction.class);
    module.addRestAction(RestClearFilterJoinCacheAction.class);
    module.addRestAction(StatsClearFilterJoinCacheAction.class);
  }

  @Override
  public Collection<Module> nodeModules() {
    Collection<Module> modules = new ArrayList<>();
    modules.add(new FilterJoinCacheModule());
    return modules;
  }

  @Override
  public String name() {
    return "SirenJoinPlugin";
  }

  @Override
  public String description() {
    return "SIREn plugin that adds join capabilities to Elasticsearch";
  }

  @Override
  public Settings additionalSettings() {
    return Settings.builder().put(IndexCacheModule.QUERY_CACHE_EVERYTHING, true).build();
  }

}
