/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.action.node;

import org.elasticsearch.action.support.nodes.NodesOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class NodesSimpleRequest extends NodesOperationRequest<NodesSimpleRequest> {

  public NodesSimpleRequest(String... nodesIds) {
    super(nodesIds);
  }

  @Override
  public void readFrom(StreamInput in) throws IOException {
    super.readFrom(in);
  }
  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
  }

}
