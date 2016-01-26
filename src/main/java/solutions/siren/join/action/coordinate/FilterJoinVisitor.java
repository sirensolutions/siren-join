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
package solutions.siren.join.action.coordinate;

import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.fieldstats.FieldStatsResponse;
import org.elasticsearch.index.query.ConstantScoreQueryParser;
import solutions.siren.join.action.terms.TermsByQueryAction;
import solutions.siren.join.action.terms.TermsByQueryResponse;
import solutions.siren.join.action.terms.TermsByQueryRequest;
import solutions.siren.join.index.query.FieldDataTermsQueryParser;
import solutions.siren.join.index.query.FilterJoinBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;

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

  private final RootNode root;
  protected final Client client;
  protected final BlockingQueue<Integer> blockingQueue = new LinkedBlockingQueue<>();
  protected final CoordinateSearchMetadata metadata;

  private static final ESLogger logger = Loggers.getLogger(FilterJoinVisitor.class);

  public FilterJoinVisitor(Client client, RootNode root) {
    this.client = client;
    this.root = root;
    this.metadata = new CoordinateSearchMetadata();
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
      this.visit((FilterJoinNode) child);
    }
  }

  private void visit(FilterJoinNode node) {
    if (node.hasChildren()) {
      for (AbstractNode child : node.getChildren()) {
        this.visit((FilterJoinNode) child);
      }
    }
    else {
      this.visitLeafNode(node);
    }
  }

  private void visitLeafNode(FilterJoinNode node) {
    switch (node.getState()) {
      case WAITING:
        this.executeAsyncOperation(node);
        return;

      case COMPLETED:
        this.checkForFailure(node);
        this.recordMetadata(node);
        this.convertToFieldDataTermsQuery(node);
        return;
    }
  }

  /**
   * Executes the sequence of async actions to compute the terms.
   */
  protected void executeAsyncOperation(FilterJoinNode node) {
    logger.debug("Executing async actions");
    node.setState(FilterJoinNode.State.RUNNING); // set state before execution to avoid race conditions with listener
    TermsByQueryActionListener listener = new TermsByQueryActionListener(node);
    node.setActionListener(listener);
    new AsyncFilterJoinVisitorAction(client, node, listener).start();
  }

  /**
   * Records metadata of each terms by query actions. This must be called before
   * converting the filter join into a field data terms query.
   * <br>
   * Returns the created action, so that subclasses, e.g., {@link CachedFilterJoinVisitor}, can extend it.
   */
  protected CoordinateSearchMetadata.Action recordMetadata(FilterJoinNode node) {
    TermsByQueryActionListener listener = node.getActionListener();

    CoordinateSearchMetadata.Relation from = new CoordinateSearchMetadata.Relation(node.getLookupIndices(), node.getLookupTypes(), node.getLookupPath());
    CoordinateSearchMetadata.Relation to = new CoordinateSearchMetadata.Relation(null, null, node.getField());

    CoordinateSearchMetadata.Action action = this.metadata.addAction(from, to);
    action.setPruned(listener.isPruned());
    action.setSize(listener.getSize());
    action.setSizeInBytes(listener.getEncodedTerms().length);
    action.setCacheHit(false);
    action.setTookInMillis(listener.getTookInMillis());
    action.setTermsEncoding(node.getTermsEncoding());

    return action;
  }

  /**
   * Checks for an action failure
   */
  private void checkForFailure(FilterJoinNode node) {
    TermsByQueryActionListener listener = node.getActionListener();
    if (listener.hasFailure()) {
      logger.error("Terms by query action failed: {}", listener.getFailure());
      throw new ElasticsearchException("Unexpected failure while executing a terms by query action", listener.getFailure());
    }
  }

  /**
   * Converts a filter join into a field data terms query.
   */
  private void convertToFieldDataTermsQuery(FilterJoinNode node) {
    Map<String, Object> parent = node.getParentSourceMap();
    TermsByQueryActionListener listener = node.getActionListener();
    byte[] bytes = listener.getEncodedTerms();

    // Remove the filter join from the parent
    parent.remove(FilterJoinBuilder.NAME);

    // Create the nested object for the parameters of the field data terms query
    Map<String, Object> queryParams = new HashMap<>();
    queryParams.put("value", bytes);
    // use the hash of the filter join source map as cache key - see #170
    queryParams.put("_cache_key", node.getCacheId());

    // Create the nested object for the field
    Map<String, Object> field = new HashMap<>();
    field.put(node.getField(), queryParams);

    // Create the nested object for the field data terms query
    Map<String, Object> fieldDataTermsQuery = new HashMap<>();
    fieldDataTermsQuery.put(FieldDataTermsQueryParser.NAME, field);

    // Create the object for the constant score query
    Map<String, Object> constantScoreQueryParams = new HashMap<>();
    constantScoreQueryParams.put("filter", fieldDataTermsQuery);

    // Add the constant score query to the parent
    parent.put(ConstantScoreQueryParser.NAME, constantScoreQueryParams);
    node.setState(FilterJoinNode.State.CONVERTED);
    this.blockingQueue.poll();
  }

  protected static class AsyncFilterJoinVisitorAction {

    protected final Client client;
    protected final FilterJoinNode node;
    protected final TermsByQueryActionListener listener;

    protected static final ESLogger logger = Loggers.getLogger(AsyncFilterJoinVisitorAction.class);

    protected AsyncFilterJoinVisitorAction(Client client, FilterJoinNode node, TermsByQueryActionListener listener) {
      this.client = client;
      this.node = node;
      this.listener = listener;
    }

    protected void start() {
      this.executeTermsByQuery();
    }

    protected void executeTermsByQuery() {
      logger.debug("Executing async terms by query action");
      final TermsByQueryRequest termsByQueryReq = this.getTermsByQueryRequest(node);
      client.execute(TermsByQueryAction.INSTANCE, termsByQueryReq, listener);
    }

    protected TermsByQueryRequest getTermsByQueryRequest(FilterJoinNode node) {
      String[] lookupIndices = node.getLookupIndices();
      String[] lookupTypes = node.getLookupTypes();
      String lookupPath = node.getLookupPath();
      XContentBuilder lookupQuery = node.getLookupQuery();
      TermsByQueryRequest.Ordering ordering = node.getOrderBy();
      Integer maxTermsPerShard = node.getMaxTermsPerShard();
      TermsByQueryRequest.TermsEncoding termsEncoding = node.getTermsEncoding();

      return new TermsByQueryRequest(lookupIndices).field(lookupPath)
              .types(lookupTypes)
              .query(lookupQuery)
              .orderBy(ordering)
              .maxTermsPerShard(maxTermsPerShard)
              .termsEncoding(termsEncoding);
    }

  }

  public class TermsByQueryActionListener implements ActionListener<TermsByQueryResponse> {

    /**
     * The corresponding {@link FilterJoinNode}
     */
    private final FilterJoinNode node;

    /**
     * The set of encoded terms from the {@link TermsByQueryResponse}
     */
    private byte[] encodedTerms;

    /**
     * The size of the set of terms (number of terms)
     */
    private int size;

    /**
     * The flag to indicate if the set of terms has been pruned
     */
    private boolean isPruned = false;

    /**
     * The time it took to retrieve the terms
     */
    private long tookInMillis = 0;

    /**
     * Flag to indicate if there was a failure
     */
    private boolean hasFailure = false;
    private Throwable failure;

    public TermsByQueryActionListener(final FilterJoinNode node) {
      this.node = node;
    }

    public FilterJoinNode getNode() {
      return node;
    }

    /**
     * To be used by subclasses to set the encoded terms, for example if the encoded terms were
     * cached.
     */
    protected void setEncodedTerms(final byte[] encodedTerms) {
      this.encodedTerms = encodedTerms;
    }

    /**
     * To be used by subclasses to set the size, for example if the encoded terms were
     * cached.
     */
    protected void setSize(int size) {
      this.size = size;
    }

    /**
     * To be used by subclasses to set the flag, for example if the encoded terms were
     * cached.
     */
    protected void setPruned(boolean isPruned) {
      this.isPruned = isPruned;
    }

    public byte[] getEncodedTerms() {
      return encodedTerms;
    }

    public int getSize() {
      return size;
    }

    public boolean isPruned() {
      return isPruned;
    }

    public long getTookInMillis() {
      return tookInMillis;
    }

    public boolean isCompleted() {
      return this.node.getState().equals(FilterJoinNode.State.COMPLETED);
    }

    public boolean hasFailure() {
      return this.hasFailure;
    }

    public Throwable getFailure() {
      return failure;
    }

    @Override
    public void onResponse(final TermsByQueryResponse termsByQueryResponse) {
      logger.debug("Received terms by query response with {} terms", termsByQueryResponse.getTermsSet().size());
      this.encodedTerms = termsByQueryResponse.getTermsSet().writeToBytes();
      this.size = termsByQueryResponse.getTermsSet().size();
      this.isPruned = termsByQueryResponse.getTermsSet().isPruned();
      this.tookInMillis = termsByQueryResponse.getTookInMillis();
      this.node.setState(FilterJoinNode.State.COMPLETED); // set state before unblocking the queue to avoid race conditions
      FilterJoinVisitor.this.blockingQueue.offer(0);
    }

    @Override
    public void onFailure(final Throwable e) {
      this.hasFailure = true;
      this.failure = e;
      this.node.setState(FilterJoinNode.State.COMPLETED); // set state before unblocking the queue to avoid race conditions
      FilterJoinVisitor.this.blockingQueue.offer(0);
    }

  }

}
