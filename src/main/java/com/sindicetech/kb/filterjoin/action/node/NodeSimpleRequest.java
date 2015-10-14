/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.action.node;

import org.elasticsearch.action.support.nodes.NodeOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class NodeSimpleRequest extends NodeOperationRequest {

  public NodeSimpleRequest() {}

  public NodeSimpleRequest(String nodeId, NodesSimpleRequest request) {
    super(request, nodeId);
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
