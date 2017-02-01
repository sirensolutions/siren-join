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
import org.elasticsearch.common.logging.Loggers;
import solutions.siren.join.action.admin.version.GetIndicesVersionAction;
import solutions.siren.join.action.admin.version.GetIndicesVersionRequest;
import solutions.siren.join.action.admin.version.GetIndicesVersionResponse;
import solutions.siren.join.action.coordinate.model.FilterJoinNode;
import solutions.siren.join.action.coordinate.pipeline.NodeTask;
import solutions.siren.join.action.coordinate.pipeline.NodeTaskContext;
import solutions.siren.join.action.coordinate.pipeline.NodeTaskReporter;

import java.util.Arrays;

/**
 * Task to retrieve the version of the lookup indices specified in a
 * {@link FilterJoinNode}. This must be executed before
 * {@link CacheLookupTask} to ensure that the {@link FilterJoinNode#getCacheId()} is correct.
 */
public class IndicesVersionTask implements NodeTask {

  protected static final Logger logger = Loggers.getLogger(IndicesVersionTask.class);

  @Override
  public void execute(final NodeTaskContext context, final NodeTaskReporter reporter) {
    logger.debug("Executing async get indices version action on indices: {}", Arrays.toString(context.getNode().getLookupIndices()));
    final GetIndicesVersionRequest indicesVersionRequest = new GetIndicesVersionRequest(context.getVisitor().getParentRequest(), context.getNode().getLookupIndices());
    context.getClient().execute(GetIndicesVersionAction.INSTANCE, indicesVersionRequest, new ActionListener<GetIndicesVersionResponse>() {

      @Override
      public void onResponse(GetIndicesVersionResponse response) {
        logger.debug("Got version {} for indices: {}", response.getVersion(), Arrays.toString(context.getNode().getLookupIndices()));
        context.getNode().setIndicesVersion(response.getVersion());
        reporter.success(context);
      }

      @Override
      public void onFailure(Exception e) {
        reporter.failure(e);
      }

    });
  }

}
