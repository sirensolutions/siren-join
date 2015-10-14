/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.action.terms;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * Internal terms by query request executed directly against a specific index shard.
 */
class TermsByQueryShardRequest extends BroadcastShardOperationRequest {

  @Nullable
  private String[] filteringAliases;
  private TermsByQueryRequest request;

  /**
   * Default constructor
   */
  TermsByQueryShardRequest() {}

  /**
   * Main Constructor
   *
   * @param shardId          the id of the shard the request is for
   * @param filteringAliases optional aliases
   * @param request          the original {@link TermsByQueryRequest}
   */
  public TermsByQueryShardRequest(ShardId shardId, @Nullable String[] filteringAliases, TermsByQueryRequest request) {
    super(shardId, request);
    this.filteringAliases = filteringAliases;
    this.request = request;
  }

  /**
   * Gets the filtering aliases
   *
   * @return the filtering aliases
   */
  public String[] filteringAliases() {
    return filteringAliases;
  }

  /**
   * Gets the original {@link TermsByQueryRequest}
   *
   * @return the request
   */
  public TermsByQueryRequest request() {
    return request;
  }

  /**
   * Deserialize
   *
   * @param in the input
   * @throws IOException
   */
  @Override
  public void readFrom(StreamInput in) throws IOException {
    super.readFrom(in);
    request = new TermsByQueryRequest();
    request.readFrom(in);

    if (in.readBoolean()) {
      filteringAliases = in.readStringArray();
    }
  }

  /**
   * Serialize
   *
   * @param out the output
   * @throws IOException
   */
  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    request.writeTo(out);

    if (filteringAliases == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeStringArray(filteringAliases);
    }
  }
}