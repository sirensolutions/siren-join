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

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.indexing.IndexingOperationListener;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service that tracks changes on a shard index and computes a unique version number.
 * <br>
 * When elasticsearch will have resolved issue #10708, we will be able to reuse the unique sequence number associated
 * to a write operations. This will allow us to compute an index version based on the primary shards only, and not
 * the replicas as we are doing now.
 */
public class IndexVersionShardService extends AbstractIndexShardComponent implements Closeable {

  private final AtomicLong version;

  private final IndexShard indexShard;
  private final VersioningIndexingOperationListener versioningIndexingOperationListener;

  @Inject
  public IndexVersionShardService(ShardId shardId, Settings indexSettings, IndexShard indexShard) {
    super(shardId, indexSettings);
    this.indexShard = indexShard;
    this.versioningIndexingOperationListener = new VersioningIndexingOperationListener();
    indexShard.indexingService().addListener(versioningIndexingOperationListener);
    version = new AtomicLong(System.nanoTime()); // initialise version number based on time to ensure uniqueness even if shard restarted
  }

  public long getVersion() {
    return version.get();
  }

  @Override
  public void close() {
    indexShard.indexingService().removeListener(versioningIndexingOperationListener);
  }

  private class VersioningIndexingOperationListener extends IndexingOperationListener {

    @Override
    public void postIndex(Engine.Index index) {
      version.incrementAndGet();
    }

    @Override
    public void postDelete(Engine.Delete delete) {
      version.incrementAndGet();
    }

  }


}
