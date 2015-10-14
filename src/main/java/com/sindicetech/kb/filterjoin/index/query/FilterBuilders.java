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
package com.sindicetech.kb.filterjoin.index.query;

import java.io.IOException;

/**
 * A static factory for simple "import static" usage.
 */
public class FilterBuilders {

  /**
   * A filter join for the provided field name. A filter join can
   * extract the terms to filter by querying another index.
   */
  public static FilterJoinBuilder filterJoin(String name) {
    return new FilterJoinBuilder(name);
  }

  /**
   * A binary terms filter for the provided field name.
   */
  public static BinaryTermsFilterBuilder binaryTermsFilter(String name, byte[] value, String cacheKey) {
    return new BinaryTermsFilterBuilder(name, value, cacheKey);
  }

  /**
   * A binary terms filter for the provided field name.
   */
  public static BinaryTermsFilterBuilder binaryTermsFilter(String name, long[] values, String cacheKey) throws IOException {
    return new BinaryTermsFilterBuilder(name, values, cacheKey);
  }

}
