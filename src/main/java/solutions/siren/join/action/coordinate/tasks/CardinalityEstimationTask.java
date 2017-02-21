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
package solutions.siren.join.action.coordinate.tasks;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import solutions.siren.join.action.coordinate.model.FilterJoinNode;
import solutions.siren.join.action.coordinate.pipeline.NodeTask;
import solutions.siren.join.action.coordinate.pipeline.NodeTaskContext;
import solutions.siren.join.action.coordinate.pipeline.NodeTaskReporter;
import solutions.siren.join.action.terms.TermsByQueryRequest;

/**
 * Task to estimate the cardinality of a {@link FilterJoinNode}. The cardinality is based on the number of unique
 * terms in the path of the lookup index.
 */
public class CardinalityEstimationTask implements NodeTask {

  protected static final Logger logger = Loggers.getLogger(CardinalityEstimationTask.class);

  @Override
  public void execute(NodeTaskContext context, NodeTaskReporter reporter) {
    // Executes the cardinality estimation only for bloom encoding
    if (context.getNode().getTermsEncoding().equals(TermsByQueryRequest.TermsEncoding.BLOOM)) {
      this.executeCardinalityRequest(context, reporter);
    }
    else {
      reporter.success(context);
    }
  }

  protected void executeCardinalityRequest(final NodeTaskContext context, final NodeTaskReporter reporter) {
    logger.debug("Executing async cardinality action");
    final SearchRequest cardinalityRequest = this.getCardinalityRequest(context.getNode(), context.getVisitor().getParentRequest());
    context.getClient().execute(SearchAction.INSTANCE, cardinalityRequest, new ActionListener<SearchResponse>() {

      @Override
      public void onResponse(SearchResponse searchResponse) {
        Cardinality c = searchResponse.getAggregations().get(context.getNode().getLookupPath());
        context.getNode().setCardinality(c.getValue());
        reporter.success(context);
      }

      @Override
      public void onFailure(Exception e) {
        reporter.failure(e);
      }

    });
  }

  protected SearchRequest getCardinalityRequest(FilterJoinNode node, ActionRequest parentRequest) {
    String[] lookupIndices = node.getLookupIndices();
    String[] lookupTypes = node.getLookupTypes();
    String lookupPath = node.getLookupPath();

    // Build the search source with the aggregate definition
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.size(0).aggregation(AggregationBuilders.cardinality(lookupPath).field(lookupPath));

    // Build search request with reference to the parent request
    SearchRequest searchRequest = new SearchRequest();//parentRequest);
    searchRequest.indices(lookupIndices).types(lookupTypes).source(sourceBuilder);

    return searchRequest;
  }

}
