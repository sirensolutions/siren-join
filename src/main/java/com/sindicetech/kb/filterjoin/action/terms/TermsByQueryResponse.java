/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.action.terms;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * The response of the terms by query action.
 */
public class TermsByQueryResponse extends BroadcastOperationResponse {

  /**
   * The set of terms that has been retrieved
   */
  private TermsResponse termsResponse;

  /**
   * How long it took to retrieve the terms.
   */
  private long tookInMillis;

  /**
   * Default constructor
   */
  TermsByQueryResponse() {}

  /**
   * Main constructor
   *
   * @param termsResponse    the merged terms
   * @param tookInMillis     the time in millis it took to retrieve the terms.
   * @param totalShards      the number of shards the request executed on
   * @param successfulShards the number of shards the request executed on successfully
   * @param failedShards     the number of failed shards
   * @param shardFailures    the failures
   */
  TermsByQueryResponse(TermsResponse termsResponse, long tookInMillis, int totalShards, int successfulShards, int failedShards,
                       List<ShardOperationFailedException> shardFailures) {
    super(totalShards, successfulShards, failedShards, shardFailures);
    this.termsResponse = termsResponse;
    this.tookInMillis = tookInMillis;
  }

  /**
   * Gets the time it took to execute the terms by query action.
   */
  public long getTookInMillis() {
    return this.tookInMillis;
  }

  /**
   * Gets the merged terms
   *
   * @return the terms
   */
  public TermsResponse getTermsResponse() {
    return termsResponse;
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
