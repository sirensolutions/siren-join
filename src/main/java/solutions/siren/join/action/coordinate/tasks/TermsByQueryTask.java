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
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;

import solutions.siren.join.action.coordinate.model.FilterJoinNode;
import solutions.siren.join.action.coordinate.model.FilterJoinTerms;
import solutions.siren.join.action.coordinate.pipeline.NodeTask;
import solutions.siren.join.action.coordinate.pipeline.NodeTaskContext;
import solutions.siren.join.action.coordinate.pipeline.NodeTaskReporter;
import solutions.siren.join.action.terms.TermsByQueryAction;
import solutions.siren.join.action.terms.TermsByQueryRequest;
import solutions.siren.join.action.terms.TermsByQueryResponse;

/**
 * Task to execute a {@link TermsByQueryRequest} to retrieve the set of terms associated to a {@link FilterJoinNode}.
 * Terms will be stored in the {@link FilterJoinNode}.
 */
public class TermsByQueryTask implements NodeTask {

  protected static final Logger logger = Loggers.getLogger(TermsByQueryTask.class);

  @Override
  public void execute(final NodeTaskContext context, final NodeTaskReporter reporter) {
    logger.debug("Executing async terms by query action");
    final TermsByQueryRequest termsByQueryReq = this.getTermsByQueryRequest(context.getNode(), context.getVisitor().getParentRequest());
    context.getClient().execute(TermsByQueryAction.INSTANCE, termsByQueryReq, new ActionListener<TermsByQueryResponse>() {

      @Override
      public void onResponse(TermsByQueryResponse termsByQueryResponse) {
        FilterJoinTerms terms = new FilterJoinTerms();
        terms.setEncodedTerms(termsByQueryResponse.getEncodedTermsSet());
        terms.setPruned(termsByQueryResponse.isPruned());
        terms.setSize(termsByQueryResponse.getSize());
        terms.setTookInMillis(termsByQueryResponse.getTookInMillis());

        // We cache the list of encoded terms instead of the {@link TermsByQueryResponse} to save the
        // byte serialization computation
        context.getVisitor().getCache().put(context.getNode().getCacheId(), terms);

        // Update the node with the terms
        context.getNode().setTerms(terms);

        // reports that the task was successful
        reporter.success(context);
      }

      @Override
      public void onFailure(Exception e) {
        reporter.failure(e);
      }

    });
  }

  protected TermsByQueryRequest getTermsByQueryRequest(FilterJoinNode node, ActionRequest parentRequest) {
    String[] lookupIndices = node.getLookupIndices();
    String[] lookupTypes = node.getLookupTypes();
    String lookupPath = node.getLookupPath();
    XContentBuilder lookupQuery = node.getLookupQuery();
    TermsByQueryRequest.Ordering ordering = node.getOrderBy();
    Integer maxTermsPerShard = node.getMaxTermsPerShard();
    TermsByQueryRequest.TermsEncoding termsEncoding = node.getTermsEncoding();

    TermsByQueryRequest request = new TermsByQueryRequest(lookupIndices)
            .field(lookupPath)
            .types(lookupTypes)
            .query(lookupQuery)
            .orderBy(ordering)
            .maxTermsPerShard(maxTermsPerShard)
            .termsEncoding(termsEncoding);

    if (node.hasCardinality()) {
      request.expectedTerms(node.getCardinality());
    }

    return request;
  }

}