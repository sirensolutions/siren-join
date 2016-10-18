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
package solutions.siren.join.action.coordinate.execution;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.index.query.ConstantScoreQueryParser;
import solutions.siren.join.action.coordinate.model.AbstractNode;
import solutions.siren.join.action.coordinate.model.FilterJoinNode;
import solutions.siren.join.action.coordinate.model.FilterJoinTerms;
import solutions.siren.join.action.coordinate.model.RootNode;
import solutions.siren.join.action.coordinate.pipeline.NodePipelineListener;
import solutions.siren.join.action.coordinate.pipeline.NodePipelineManager;
import solutions.siren.join.action.coordinate.pipeline.NodeTaskContext;
import solutions.siren.join.action.coordinate.tasks.CacheLookupTask;
import solutions.siren.join.action.coordinate.tasks.CardinalityEstimationTask;
import solutions.siren.join.action.coordinate.tasks.IndicesVersionTask;
import solutions.siren.join.action.coordinate.tasks.TermsByQueryTask;
import solutions.siren.join.action.terms.TermsByQueryRequest;
import solutions.siren.join.index.query.FieldDataTermsQueryParser;
import solutions.siren.join.index.query.FilterJoinBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import solutions.siren.join.index.query.TermsEnumTermsQueryParser;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Visitor that will traverse the tree until all the filter join nodes have been converted
 * into field data terms queries. The visitor will execute in parallel multiple async actions
 * when it is possible.
 */
public class FilterJoinVisitor {

  protected final ActionRequest parentRequest;
  private final RootNode root;
  protected final Client client;
  protected final BlockingQueue<Integer> blockingQueue = new LinkedBlockingQueue<>();
  protected final CoordinateSearchMetadata metadata;

  /**
   * The filter join cache
   */
  private final FilterJoinCache cache;

  private static final ESLogger logger = Loggers.getLogger(FilterJoinVisitor.class);

  public FilterJoinVisitor(Client client, RootNode root, FilterJoinCache cache, ActionRequest parentRequest) {
    this.parentRequest = parentRequest;
    this.client = client;
    this.root = root;
    this.cache = cache;
    this.metadata = new CoordinateSearchMetadata();
  }

  /**
   * Unblock the parent thread of the visitor
   */
  public void unblock() {
    this.blockingQueue.offer(0);
  }

  /**
   * Returns the cache
   */
  public FilterJoinCache getCache() {
    return this.cache;
  }

  /**
   * Returns the parent request
   */
  public ActionRequest getParentRequest() {
    return this.parentRequest;
  }

  /**
   * Returns the metadata associated to this search query execution
   */
  public CoordinateSearchMetadata getMetadata() {
    return this.metadata;
  }

  /**
   * Traverse the tree until all the filter join nodes have been converted to field data terms queries.
   */
  public void traverse() {
    while (root.hasChildren()) {
      this.visit(root); // traverse the tree
      this.await(); // wait for completion of async actions
    }
  }

  /**
   * Await for the completion of an async action.
   */
  private void await() {
    try {
      // Clean up all filter join leaf nodes that have been converted
      boolean nodeRemoved = this.removeConvertedNodes(root);
      // If some nodes were removed, it means that we converted in the previous iteration
      // at least one filter join into a field data terms query. We don't have to wait since
      // we might have new filter join leaf nodes.
      if (!nodeRemoved && root.hasChildren()) {
        logger.debug("Visitor thread block - blocking queue size: {}", blockingQueue.size());
        this.blockingQueue.take();   // block until one async action is completed
        this.blockingQueue.offer(0); // add back the element to the queue, it will be removed after the node conversion
        logger.debug("Visitor thread unblock - blocking queue size: {}", blockingQueue.size());
      }
    }
    catch (InterruptedException e) {
      logger.warn("Filter join visitor thread interrupted while waiting");
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Removes all the filter join leaf nodes that were converted. Returns true if at least one node
   * has been removed.
   */
  private boolean removeConvertedNodes(AbstractNode node) {
    boolean nodeRemoved = false;
    Iterator<AbstractNode> it = node.getChildren().iterator();
    while (it.hasNext()) {
      FilterJoinNode child = (FilterJoinNode) it.next();
      if (child.getState().equals(FilterJoinNode.State.CONVERTED)) {
        it.remove();
        nodeRemoved |= true;
      }
      else {
        nodeRemoved |= this.removeConvertedNodes(child) ? true : false;
      }
    }
    return nodeRemoved;
  }

  private void visit(RootNode root) {
    for (AbstractNode child : root.getChildren()) {
      this.visit((FilterJoinNode) child, null);
    }
  }

  private void visit(FilterJoinNode node, FilterJoinNode parent) {
    if (node.hasChildren()) {
      for (AbstractNode child : node.getChildren()) {
        this.visit((FilterJoinNode) child, node);
      }
    }
    else {
      this.visitLeafNode(node, parent);
    }
  }

  private void visitLeafNode(FilterJoinNode node, FilterJoinNode parent) {
    switch (node.getState()) {
      case WAITING:
        this.executeAsyncOperation(node);
        return;

      case COMPLETED:
        this.checkForFailure(node);
        this.recordMetadata(node, parent);
        this.convertToTermsQuery(node);
        return;
    }
  }

  /**
   * Executes the pipeline of async actions to compute the terms for this node.
   */
  protected void executeAsyncOperation(final FilterJoinNode node) {
    logger.debug("Executing async actions");
    node.setState(FilterJoinNode.State.RUNNING); // set state before execution to avoid race conditions with listener

    NodePipelineManager pipeline = new NodePipelineManager();
    pipeline.addListener(new NodePipelineListener() {

      @Override
      public void onSuccess() {
        node.setState(FilterJoinNode.State.COMPLETED); // set state before unblocking the queue to avoid race conditions
        FilterJoinVisitor.this.unblock();
      }

      @Override
      public void onFailure(Throwable e) {
        node.setFailure(e);
        node.setState(FilterJoinNode.State.COMPLETED); // set state before unblocking the queue to avoid race conditions
        FilterJoinVisitor.this.unblock();
      }

    });

    // Adds the list of tasks to be executed
    pipeline.addTask(new IndicesVersionTask());
    pipeline.addTask(new CacheLookupTask());
    pipeline.addTask(new CardinalityEstimationTask());
    pipeline.addTask(new TermsByQueryTask());

    // Starts the execution of the pipeline
    pipeline.execute(new NodeTaskContext(client, node, this));
  }

  /**
   * Records metadata of each terms by query actions. This must be called before
   * converting the filter join into a field data terms query.
   * <br>
   * Returns the created action, so that subclasses, e.g., {@link CachedFilterJoinVisitor}, can extend it.
   */
  protected CoordinateSearchMetadata.Action recordMetadata(FilterJoinNode node, FilterJoinNode parent) {
    FilterJoinTerms terms = node.getTerms();

    final String[] fromIndices = node.getLookupIndices();
    final String[] fromTypes = node.getLookupTypes();
    final String[] toIndices = parent == null ? null : parent.getLookupIndices();
    final String[] toTypes = parent == null ? null : parent.getLookupTypes();

    CoordinateSearchMetadata.Relation from = new CoordinateSearchMetadata.Relation(fromIndices, fromTypes, node.getLookupPath());
    CoordinateSearchMetadata.Relation to = new CoordinateSearchMetadata.Relation(toIndices, toTypes, node.getField());

    CoordinateSearchMetadata.Action action = this.metadata.addAction(from, to);
    action.setPruned(terms.isPruned());
    action.setSize(terms.getSize());
    action.setSizeInBytes(terms.getEncodedTerms().length);
    action.setCacheHit(terms.cacheHit());
    action.setTookInMillis(terms.getTookInMillis());
    action.setTermsEncoding(node.getTermsEncoding());
    action.setOrdering(node.getOrderBy());
    action.setMaxTermsPerShard(node.getMaxTermsPerShard());

    return action;
  }

  /**
   * Checks for an action failure
   */
  private void checkForFailure(FilterJoinNode node) {
    if (node.hasFailure()) {
      logger.error("Node processing failed: {}", node.getFailure());
      throw new ElasticsearchException("Unexpected failure while processing a node", node.getFailure());
    }
  }

  /**
   * Converts a filter join into a terms query.
   */
  private void convertToTermsQuery(FilterJoinNode node) {
    Map<String, Object> parent = node.getParentSourceMap();
    FilterJoinTerms terms = node.getTerms();
    BytesRef bytes = terms.getEncodedTerms();

    // Remove the filter join from the parent
    parent.remove(FilterJoinBuilder.NAME);

    // Create the nested object for the parameters of the field data terms query
    Map<String, Object> queryParams = new HashMap<>();
    queryParams.put("value", bytes.bytes);
    // use the hash of the filter join source map as cache key - see #170
    queryParams.put("_cache_key", node.getCacheId());

    // Create the nested object for the field
    Map<String, Object> field = new HashMap<>();
    field.put(node.getField(), queryParams);

    // Create the nested object for the field data terms query
    Map<String, Object> termsQuery = new HashMap<>();
    // If bytes terms encoding is used, we switch to the terms enum based terms query
    if (node.getTermsEncoding().equals(TermsByQueryRequest.TermsEncoding.BYTES)) {
      termsQuery.put(TermsEnumTermsQueryParser.NAME, field);
    }
    else {
      termsQuery.put(FieldDataTermsQueryParser.NAME, field);
    }

    // Create the object for the constant score query
    Map<String, Object> constantScoreQueryParams = new HashMap<>();
    constantScoreQueryParams.put("filter", termsQuery);

    // Add the constant score query to the parent
    parent.put(ConstantScoreQueryParser.NAME, constantScoreQueryParams);
    node.setState(FilterJoinNode.State.CONVERTED);
    this.blockingQueue.poll();
  }

}
