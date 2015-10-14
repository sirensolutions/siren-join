/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.action.terms;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * Internal terms by query response of a shard terms by query request executed directly against a specific shard.
 */
class TermsByQueryShardResponse extends BroadcastShardOperationResponse {

  private TermsResponse termsResponse;

  /**
   * Default constructor
   */
  TermsByQueryShardResponse() {}

  /**
   * Main constructor
   *
   * @param shardId       the id of the shard the request executed on
   * @param termsResponse the terms gathered from the shard
   */
  public TermsByQueryShardResponse(ShardId shardId, TermsResponse termsResponse) {
    super(shardId);
    this.termsResponse = termsResponse;
  }

  /**
   * Gets the gathered terms.
   *
   * @return the {@link TermsResponse}
   */
  public TermsResponse getTermsResponse() {
    return this.termsResponse;
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
    termsResponse = new TermsResponse();
    termsResponse.readFrom(in);
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
    termsResponse.writeTo(out);
  }
}