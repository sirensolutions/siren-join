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

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IndexVersionService extends AbstractLifecycleComponent {

    private final Map<Index, IndexVersionEventListener> indexToVersionEventListenerMapping;

    @Inject
    public IndexVersionService(Settings settings) {
        super(settings);
        this.indexToVersionEventListenerMapping = new ConcurrentHashMap<>();
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
        this.indexToVersionEventListenerMapping.clear();
    }

    public long getVersion(Index index, ShardId shardId) {
        return indexToVersionEventListenerMapping.get(index).getVersion(shardId);
    }

    public void registerIndexEventListener(Index index, IndexVersionEventListener indexVersionEventListener) {
        if (!indexToVersionEventListenerMapping.containsKey(index)) {
            this.indexToVersionEventListenerMapping.put(index, indexVersionEventListener);
        }
    }

    public IndexVersionEventListener getOrDefault(Index index) {
        return indexToVersionEventListenerMapping.getOrDefault(index, null);
    }
}
