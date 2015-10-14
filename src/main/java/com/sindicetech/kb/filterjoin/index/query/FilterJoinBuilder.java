/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.index.query;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BaseFilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;

/**
 * A filter for a field based on terms coming from another set of documents.
 */
public class FilterJoinBuilder extends BaseFilterBuilder {

  private final String name;
  private String[] indices;
  private String[] types;
  private String routing;
  private String path;
  private QueryBuilder query;
  private String orderBy;
  private Integer maxTermsPerShard;

  private Boolean cache;
  private String cacheKey;
  private String filterName;

  public static final String NAME = "filterjoin";

  public FilterJoinBuilder(String name) {
    this.name = name;
  }

  /**
   * Sets the index names to lookup the terms from.
   */
  public FilterJoinBuilder indices(String... indices) {
    this.indices = indices;
    return this;
  }

  /**
   * Sets the index types to lookup the terms from.
   */
  public FilterJoinBuilder types(String... types) {
    this.types = types;
    return this;
  }

  /**
   * Sets the path within the document to lookup the terms from.
   */
  public FilterJoinBuilder path(String path) {
    this.path = path;
    return this;
  }

  /**
   * Sets the node routing used to control the shards the lookup request is executed on
   */
  public FilterJoinBuilder routing(String lookupRouting) {
    this.routing = lookupRouting;
    return this;
  }

  /**
   * Sets the query used to lookup terms with
   */
  public FilterJoinBuilder query(QueryBuilder query) {
    this.query = query;
    return this;
  }

  /**
   * Sets the ordering used to lookup terms
   */
  public FilterJoinBuilder orderBy(String orderBy) {
    this.orderBy = orderBy;
    return this;
  }

  /**
   * Sets the maximum number of terms per shard to lookup
   */
  public FilterJoinBuilder maxTermsPerShard(int maxTermsPerShard) {
    this.maxTermsPerShard = maxTermsPerShard;
    return this;
  }


  /**
   * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
   */
  public FilterJoinBuilder filterName(String filterName) {
    this.filterName = filterName;
    return this;
  }

  /**
   * Sets if the resulting filter should be cached or not
   */
  public FilterJoinBuilder cache(boolean cache) {
    this.cache = cache;
    return this;
  }

  /**
   * Sets the filter cache key
   */
  public FilterJoinBuilder cacheKey(String cacheKey) {
    this.cacheKey = cacheKey;
    return this;
  }

  @Override
  public void doXContent(XContentBuilder builder, Params params) throws IOException {
    builder.startObject(FilterJoinBuilder.NAME);

    builder.startObject(name);
    if (indices != null) {
      builder.field("indices", indices);
    }
    if (types != null) {
      builder.field("types", types);
    }
    if (routing != null) {
      builder.field("routing", routing);
    }
    builder.field("path", path);
    builder.field("query", query);
    if (orderBy != null) {
      builder.field("orderBy", orderBy);
    }
    if (maxTermsPerShard != null) {
      builder.field("maxTermsPerShard", maxTermsPerShard);
    }
    builder.endObject();

    if (filterName != null) {
      builder.field("_name", filterName);
    }
    if (cache != null) {
      builder.field("_cache", cache);
    }
    if (cacheKey != null) {
      builder.field("_cache_key", cacheKey);
    }

    builder.endObject();
  }
}