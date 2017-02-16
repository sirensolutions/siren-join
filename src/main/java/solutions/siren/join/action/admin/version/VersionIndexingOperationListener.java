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

import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service that tracks changes on a shard index and computes a unique version number.
 * <br>
 * When elasticsearch will have resolved issue #10708, we will be able to reuse the unique sequence number associated
 * to a write operations. This will allow us to compute an index version based on the primary shards only, and not
 * the replicas as we are doing now.
 */
public class VersionIndexingOperationListener implements IndexingOperationListener {

	private final Map<ShardId, AtomicLong> versionMapping;

	public VersionIndexingOperationListener() {
		versionMapping = new ConcurrentHashMap<>();
	}

	public void registerShard(ShardId shardId) {
		if (!versionMapping.containsKey(shardId)) {
			versionMapping.put(shardId, new AtomicLong(System.nanoTime()));
		}
	}

	public void unregisterShard(ShardId shardId) {
		versionMapping.remove(shardId);
	}

	// TODO: Elasticsearch 5.3 will restore the shard information that is now missing from these callbacks.
	// Once it is out, we will be able to have a finer-grained version handling.
	// https://github.com/elastic/elasticsearch/pull/22606

	@Override
	public void postIndex(Engine.Index index, boolean created) {
		versionMapping.values().forEach(AtomicLong::incrementAndGet);
	}

	@Override
	public void postDelete(Engine.Delete delete) {
		versionMapping.values().forEach(AtomicLong::incrementAndGet);
	}

	public long getVersion(ShardId shardId) {
		if (!versionMapping.containsKey(shardId)) {
			return 0;
		}

		return versionMapping.get(shardId).get();
	}
}