package solutions.siren.join.action.admin.cache;

import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class StatsFilterJoinCacheNodeRequest extends BaseNodeRequest {

  private StatsFilterJoinCacheRequest request;

  StatsFilterJoinCacheNodeRequest() {}

  public StatsFilterJoinCacheNodeRequest(String nodeId, StatsFilterJoinCacheRequest request) {
    super(request, nodeId);
    this.request = request;
  }

  @Override
  public void readFrom(StreamInput in) throws IOException {
    super.readFrom(in);
    request = new StatsFilterJoinCacheRequest();
    request.readFrom(in);
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    request.writeTo(out);
  }

}
