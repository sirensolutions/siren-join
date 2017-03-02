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
package solutions.siren.join.common;

import com.carrotsearch.hppc.BitMixer;
import org.elasticsearch.common.bytes.BytesReference;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Collections {

  /**
   * Returns the hash of a {@link Map} object created by
   * {@link org.elasticsearch.common.xcontent.XContentHelper#convertToMap(BytesReference, boolean)} by traversing
   * nested {@link Map} and {@link List} objects.
   * <br>
   * The hash is used by {@link solutions.siren.join.action.coordinate.model.FilterJoinNode} to compute a unique
   * cache id. This method should not be used outside this scope.
   * <br>
   * The hash is computed in a similar way than in {@link java.util.AbstractMap}, i.e.,
   * the sum of the hash codes of each entry in the map. The difference is that we are mixing bits of
   * the hash codes of the entry's key and value to improve the distribution of their hash value. A similar technique
   * is used in hppc's {@link com.carrotsearch.hppc.ObjectObjectHashMap#hashCode()}.
   *
   * @see <a href="https://github.com/sirensolutions/siren-join/issues/112">Issue #112</a>
   */
  public static int hashCode(Map map) {
    int h = 0;
    Iterator<Map.Entry<Object, Object>> i = map.entrySet().iterator();
    while (i.hasNext()) {
      Map.Entry<Object, Object> entry = i.next();
      if (entry.getValue() instanceof Map) {
        h += BitMixer.mix32(entry.getKey().hashCode()) ^ BitMixer.mix32(hashCode((Map) entry.getValue()));
      }
      else if (entry.getValue() instanceof List) {
        h += BitMixer.mix32(entry.getKey().hashCode()) ^ BitMixer.mix32(hashCode((List) entry.getValue()));
      }
      else {
        h += BitMixer.mix32(entry.getKey().hashCode()) ^ BitMixer.mix32(entry.hashCode());
      }
    }
    return h;
  }

  private static int hashCode(List list) {
    int h = 1;
    for (int i = 0; i < list.size(); i++) {
      Object e = list.get(i);
      if (e instanceof List) {
        h = 31 * h + BitMixer.mix32(hashCode((List) e));
      }
      else if (e instanceof Map) {
        h = 31 * h + BitMixer.mix32(hashCode((Map) e));
      }
      else {
        h = 31 * h + BitMixer.mix32(list.get(i).hashCode());
      }
    }
    return h;
  }

}
