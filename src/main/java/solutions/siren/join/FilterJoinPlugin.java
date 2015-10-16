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

import solutions.siren.join.action.coordinate.CoordinateMultiSearchAction;
import solutions.siren.join.action.coordinate.TransportCoordinateMultiSearchAction;
import solutions.siren.join.action.coordinate.TransportCoordinateSearchAction;
import solutions.siren.join.action.terms.TermsByQueryAction;
import solutions.siren.join.action.terms.TransportTermsByQueryAction;
import solutions.siren.join.index.query.BinaryTermsFilterParser;
import solutions.siren.join.index.query.BinaryTermsQueryParser;
import solutions.siren.join.rest.RestCoordinateMultiSearchAction;
import solutions.siren.join.rest.RestCoordinateSearchAction;
import solutions.siren.join.action.coordinate.CoordinateSearchAction;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.query.IndicesQueriesModule;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

/**
 * The FilterJoin plugin.
 */
public class FilterJoinPlugin extends AbstractPlugin {

  @Inject
  public FilterJoinPlugin(Settings settings) {}

  public void onModule(ActionModule module) {
    module.registerAction(TermsByQueryAction.INSTANCE, TransportTermsByQueryAction.class);
    module.registerAction(CoordinateSearchAction.INSTANCE, TransportCoordinateSearchAction.class);
    module.registerAction(CoordinateMultiSearchAction.INSTANCE, TransportCoordinateMultiSearchAction.class);
  }

  public void onModule(IndicesQueriesModule module) {
    module.addFilter(BinaryTermsFilterParser.class);
    module.addQuery(BinaryTermsQueryParser.class);
  }

   public void onModule(RestModule module) {
     module.addRestAction(RestCoordinateSearchAction.class);
     module.addRestAction(RestCoordinateMultiSearchAction.class);
   }

  @Override
  public String name() {
    return "FilterJoinPlugin";
  }

  @Override
  public String description() {
    return "Filter join query parser and terms by query action";
  }

}
