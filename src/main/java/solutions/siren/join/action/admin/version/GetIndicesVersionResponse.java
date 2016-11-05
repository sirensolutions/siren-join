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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.*;

/**
 * The version for the set of indices can be retrieved with {@link #getVersion()}.
 */
public class GetIndicesVersionResponse extends BroadcastResponse {

  private ShardIndexVersion[] shards;

  private Map<String, Long> indicesVersions;

  GetIndicesVersionResponse() {}

  GetIndicesVersionResponse(ShardIndexVersion[] shards, int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures) {
    super(totalShards, successfulShards, failedShards, shardFailures);
    this.shards = shards;
  }

  /**
   * Returns the version for the set of indices.
   */
  public long getVersion() {
    long version = 1;

    ArrayList<String> indices = new ArrayList<>(getIndices().keySet());
    Collections.sort(indices);
    for (String index : indices) {
      version = 31 * version + getIndices().get(index);
    }

    return version;
  }

  /**
   * Returns a map with the version of each index.
   */
  public Map<String, Long> getIndices() {
    if (indicesVersions != null) {
      return indicesVersions;
    }
    Map<String, Long> indicesVersions = Maps.newHashMap();

    Set<String> indices = Sets.newHashSet();
    for (ShardIndexVersion shard : shards) {
      indices.add(shard.getShardRouting().getIndex());
    }

    for (String index : indices) {
      List<ShardIndexVersion> shards = new ArrayList<>();
      for (ShardIndexVersion shard : this.shards) {
        if (shard.getShardRouting().index().equals(index)) {
          shards.add(shard);
        }
      }
      indicesVersions.put(index, this.getIndexVersion(shards));
    }
    this.indicesVersions = indicesVersions;
    return indicesVersions;
  }

  /**
   * Computes a unique hash based on the version of the shards.
   */
  private long getIndexVersion(List<ShardIndexVersion> shards) {
    long version = 1;

    // order shards per their id before computing the hash
    Collections.sort(shards, new Comparator<ShardIndexVersion>() {
      @Override
      public int compare(ShardIndexVersion o1, ShardIndexVersion o2) {
        return o1.getShardRouting().id() - o2.getShardRouting().id();
      }
    });

    // compute hash
    for (ShardIndexVersion shard : shards) {
      version = 31 * version + shard.getVersion();
    }

    return version;
  }

  @Override
  public void readFrom(StreamInput in) throws IOException {
    super.readFrom(in);
    shards = new ShardIndexVersion[in.readVInt()];
    for (int i = 0; i < shards.length; i++) {
      shards[i] = ShardIndexVersion.readShardIndexVersion(in);
    }
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeVInt(shards.length);
    for (ShardIndexVersion shard : shards) {
      shard.writeTo(out);
    }
  }

}
