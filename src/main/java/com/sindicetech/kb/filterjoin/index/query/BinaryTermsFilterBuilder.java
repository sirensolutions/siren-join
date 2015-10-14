/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.index.query;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BaseFilterBuilder;

import java.io.IOException;

public class BinaryTermsFilterBuilder extends BaseFilterBuilder {

  private final String name;

  private final byte[] value;

  private final String cacheKey;

  public BinaryTermsFilterBuilder(String name, byte[] values, String cacheKey) {
    this.name = name;
    this.value = values;
    this.cacheKey = cacheKey;
  }

  public BinaryTermsFilterBuilder(String name, long[] values, String cacheKey) throws IOException {
    this(name, BinaryTermsFilterHelper.encode(values), cacheKey);
  }

  @Override
  public void doXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
    builder.startObject(BinaryTermsFilterParser.NAME);
      builder.startObject(name);
        builder.field("value", value);
        builder.field("_cache_key", cacheKey);
      builder.endObject();
    builder.endObject();
  }

  @Override
  public String toString() {
    try {
      XContentBuilder builder = XContentFactory.jsonBuilder();
      builder.prettyPrint();

      builder.startObject(BinaryTermsFilterParser.NAME);
        builder.startObject(name);
          // Do not serialise the full byte array, but instead the number of bytes - see issue #168
          builder.field("value", "[size=" + value.length + "]");
          builder.field("_cache_key", cacheKey);
        builder.endObject();
      builder.endObject();

      return builder.string();
    }
    catch (Exception e) {
      throw new ElasticsearchException("Failed to build filter", e);
    }
  }

}
