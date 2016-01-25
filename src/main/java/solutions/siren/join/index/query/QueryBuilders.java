/**
 * Copyright (c) 2015, SIREn Solutions. All Rights Reserved.
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
package solutions.siren.join.index.query;

import java.io.IOException;

/**
 * A static factory for simple "import static" usage.
 */
public class QueryBuilders {

  /**
   * A filter join for the provided field name. A filter join can
   * extract the terms to filter by querying another index.
   */
  public static FilterJoinBuilder filterJoin(String name) {
    return new FilterJoinBuilder(name);
  }

  /**
   * A field data terms query for the provided field name.
   */
  public static FieldDataTermsQueryBuilder fieldDataTermsQuery(String name, byte[] value, int cacheKey) {
    return new FieldDataTermsQueryBuilder(name, value, cacheKey);
  }

  /**
   * A field data terms query for the provided field name.
   */
  public static FieldDataTermsQueryBuilder fieldDataTermsQuery(String name, long[] values, int cacheKey) throws IOException {
    return new FieldDataTermsQueryBuilder(name, values, cacheKey);
  }

}
