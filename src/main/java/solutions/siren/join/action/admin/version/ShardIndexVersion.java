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
package solutions.siren.join.action.admin.version;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

public class ShardIndexVersion implements Streamable {

  private ShardRouting shardRouting;

  private long version;

  ShardIndexVersion() {
  }

  ShardIndexVersion(ShardRouting shardRouting, long version) {
    this.shardRouting = shardRouting;
    this.version = version;
  }

  public ShardRouting getShardRouting() {
    return this.shardRouting;
  }

  public long getVersion() {
    return this.version;
  }

  @Override
  public void readFrom(StreamInput in) throws IOException {
    shardRouting = ShardRouting.readShardRoutingEntry(in);
    version = in.readLong();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    shardRouting.writeTo(out);
    out.writeLong(version);
  }

  public static ShardIndexVersion readShardIndexVersion(StreamInput in) throws IOException {
    ShardIndexVersion shard = new ShardIndexVersion();
    shard.readFrom(in);
    return shard;
  }

}
