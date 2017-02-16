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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IndexVersionEventListener implements IndexEventListener {
	private final VersionIndexingOperationListener indexingOperationListener;

	public IndexVersionEventListener(VersionIndexingOperationListener indexingOperationListener) {
		this.indexingOperationListener = indexingOperationListener;
	}

	@Override
	public void afterIndexShardStarted(IndexShard indexShard) {
		indexingOperationListener.registerShard(indexShard.shardId());
	}

	@Override
	public void afterIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings settings) {
		indexingOperationListener.unregisterShard(shardId);
	}

	public long getVersion(ShardId shardId) {
		return indexingOperationListener.getVersion(shardId);
	}

	public VersionIndexingOperationListener getOperationListener() { return indexingOperationListener; }
}