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
package solutions.siren.join.action.admin.cache;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import solutions.siren.join.action.coordinate.FilterJoinCache;

public class FilterJoinCacheService extends AbstractComponent {

  private final FilterJoinCache cache;

  @Inject
  public FilterJoinCacheService(Settings settings) {
    super(settings);
    this.cache = new FilterJoinCache(settings);
  }

  public FilterJoinCache getCacheInstance() {
    return this.cache;
  }

  public void clear() {
    cache.invalidateAll();
  }

  public FilterJoinCache.FilterJoinCacheStats getStats() {
    return cache.getStats();
  }

}
