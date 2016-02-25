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
package solutions.siren.join.action.coordinate;

import solutions.siren.join.action.terms.TermsByQueryRequest;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * A filter join node of the abstract syntax tree. It contains a reference to the source map
 * of the filter join, and a reference to the parent source map of the filter join. This node can
 * have multiple {@link FilterJoinNode.State}s:
 * <ul>
 *   <li>WAITING: original state - no async action has been send</li>
 *   <li>RUNNING: an async action has been sent</li>
 *   <li>COMPLETED: the response of the async action has been received</li>
 *   <li>CONVERTED: the terms has been extracted from the response, and the filter join has been replaced
 *   by a binary terms filter</li>
 * </ul>
 */
@SuppressWarnings("unchecked")
public class FilterJoinNode extends AbstractNode {

  private final Map<String, Object> parent;
  private final Map<String, Object> self;

  /**
   * A unique cache id based on the source map.
   */
  private final int cacheId;

  /**
   * An estimation of the cardinality of the join.
   */
  private long cardinality = 0;
  private boolean hasCardinality = false;

  private State state;
  private FilterJoinVisitor.TermsByQueryActionListener listener;

  /**
   * The various states of {@link FilterJoinNode}.
   */
  public enum State {
    WAITING,
    RUNNING,
    COMPLETED,
    CONVERTED
  }

  public FilterJoinNode(Map<String, Object> parent, Map<String, Object> self) {
    this.parent = parent;
    this.self = self;
    this.state = State.WAITING;
    // Generate the cache id based on the hashCode of the source map, before it is modified.
    // This should not be sensitive to the order of the fields, but this might be sensitive to the order of arrays.
    this.cacheId = self.hashCode();
  }

  /**
   * The cache id for this filter join node. The cache id is a unique identifier based on the source map.
   * This is currently used in {@link FilterJoinCache} to cache the list of terms resulting from a filter join,
   * and by {@link FilterJoinVisitor#convertToFieldDataTermsQuery(FilterJoinNode)} as cache key for the
   * binary terms filter.
   */
  int getCacheId() {
    return cacheId;
  }

  void setCardinality(long cardinality) {
    this.cardinality = cardinality;
    this.hasCardinality = true;
  }

  boolean hasCardinality() {
    return hasCardinality;
  }

  long getCardinality() {
    return cardinality;
  }

  public String getField() {
    return this.self.keySet().iterator().next();
  }

  public String[] getLookupIndices() {
    Map<String, Object> conf = (Map<String, Object>) this.self.get(this.getField());
    Object o = conf.get("indices");
    if (o == null) {
      return new String[0];
    }
    if (o instanceof String) {
      return new String[]{ (String) o };
    }
    if (o instanceof List) {
      return ((List<String>) o).toArray(new String[((List) o).size()]);
    }
    throw new ElasticsearchParseException("Unable to build the lookup query - Invalid 'indices' parameter.");
  }

  public String[] getLookupTypes() {
    Map<String, Object> conf = (Map<String, Object>) this.self.get(this.getField());
    Object o = conf.get("types");
    if (o == null) {
      return new String[0];
    }
    if (o instanceof String) {
      return new String[]{ (String) o };
    }
    if (o instanceof List) {
      return ((List<String>) o).toArray(new String[((List) o).size()]);
    }
    throw new ElasticsearchParseException("Unable to build the lookup query - Invalid 'types' parameter.");
  }

  public String getLookupPath() {
    Map<String, Object> conf = (Map<String, Object>) this.self.get(this.getField());
    return (String) conf.get("path");
  }

  public XContentBuilder getLookupQuery() {
    Map<String, Object> conf = (Map<String, Object>) this.self.get(this.getField());
    return this.buildQuery((Map) conf.get("query"));
  }

  public TermsByQueryRequest.Ordering getOrderBy() {
    Map<String, Object> conf = (Map<String, Object>) this.self.get(this.getField());
    String ordering = (String) conf.get("orderBy");
    if (ordering == null) {
      return null;
    }
    return TermsByQueryRequest.Ordering.valueOf(ordering.toUpperCase(Locale.ROOT));
  }

  public Integer getMaxTermsPerShard() {
    Map<String, Object> conf = (Map<String, Object>) this.self.get(this.getField());
    return (Integer) conf.get("maxTermsPerShard");
  }

  public TermsByQueryRequest.TermsEncoding getTermsEncoding() {
    Map<String, Object> conf = (Map<String, Object>) this.self.get(this.getField());
    String termsEncoding = (String) conf.get("termsEncoding");
    if (termsEncoding == null) {
      return TermsByQueryRequest.DEFAULT_TERM_ENCODING;
    }
    return TermsByQueryRequest.TermsEncoding.valueOf(termsEncoding.toUpperCase(Locale.ROOT));
  }

  private XContentBuilder buildQuery(Map query) {
    try {
      if (query == null) {
        return null;
      }
      return XContentBuilder.builder(XContentType.CBOR.xContent()).map(query);
    }
    catch (IOException e) {
      throw new ElasticsearchParseException("Unable to build the lookup query");
    }
  }

  public Map<String, Object> getSourceMap() {
    return self;
  }

  public Map<String, Object> getParentSourceMap() {
    return parent;
  }

  public void setState(State state) {
    this.state = state;
  }

  public State getState() {
    return state;
  }

  public void setActionListener(FilterJoinVisitor.TermsByQueryActionListener listener) {
    this.listener = listener;
  }

  public FilterJoinVisitor.TermsByQueryActionListener getActionListener() {
    return listener;
  }

}
