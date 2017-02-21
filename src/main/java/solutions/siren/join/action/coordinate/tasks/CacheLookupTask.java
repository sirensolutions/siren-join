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
import org.elasticsearch.common.logging.Loggers;
import solutions.siren.join.action.coordinate.execution.FilterJoinCache;
import solutions.siren.join.action.coordinate.model.FilterJoinNode;
import solutions.siren.join.action.coordinate.model.FilterJoinTerms;
import solutions.siren.join.action.coordinate.pipeline.NodeTask;
import solutions.siren.join.action.coordinate.pipeline.NodeTaskContext;
import solutions.siren.join.action.coordinate.pipeline.NodeTaskReporter;

/**
 * A task to lookup a cache entry based on the cache id of a node ({@link FilterJoinNode#getCacheId()}).
 */
public class CacheLookupTask implements NodeTask {

  protected static final Logger logger = Loggers.getLogger(CacheLookupTask.class);

  @Override
  public void execute(NodeTaskContext context, NodeTaskReporter reporter) {
    FilterJoinNode node = context.getNode();

    // Check cache
    FilterJoinCache.CacheEntry cacheEntry = context.getVisitor().getCache().get(node.getCacheId());

    if (cacheEntry == null) { // if cache miss
      logger.debug("Cache miss for terms by query action: {}", node.getCacheId());

      // report success and move to the next task
      reporter.success(context);
    }
    else { // if cache hit
      logger.debug("Cache hit for terms by query action: {}", node.getCacheId());

      // Read the terms from the cache and update the node
      FilterJoinTerms terms = new FilterJoinTerms();
      terms.setEncodedTerms(cacheEntry.encodedTerms);
      terms.setSize(cacheEntry.size);
      terms.setPruned(cacheEntry.isPruned);
      terms.setCacheHit(true);
      context.getNode().setTerms(terms);

      // force termination of the pipeline
      reporter.terminate();
    }
  }

}
