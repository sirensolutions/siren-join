/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin;

import com.sindicetech.kb.filterjoin.action.coordinate.CoordinateMultiSearchAction;
import com.sindicetech.kb.filterjoin.action.coordinate.CoordinateSearchAction;
import com.sindicetech.kb.filterjoin.action.coordinate.TransportCoordinateMultiSearchAction;
import com.sindicetech.kb.filterjoin.action.coordinate.TransportCoordinateSearchAction;
import com.sindicetech.kb.filterjoin.action.terms.TermsByQueryAction;
import com.sindicetech.kb.filterjoin.action.terms.TransportTermsByQueryAction;
import com.sindicetech.kb.filterjoin.index.query.BinaryTermsFilterParser;
import com.sindicetech.kb.filterjoin.index.query.BinaryTermsQueryParser;
import com.sindicetech.kb.filterjoin.rest.RestCoordinateMultiSearchAction;
import com.sindicetech.kb.filterjoin.rest.RestCoordinateSearchAction;
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
