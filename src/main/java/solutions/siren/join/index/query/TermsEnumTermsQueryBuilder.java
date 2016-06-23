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
package solutions.siren.join.index.query;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import solutions.siren.join.common.Bytes;

import java.io.IOException;

public class TermsEnumTermsQueryBuilder extends QueryBuilder {

  /**
   * The target field
   */
  private final String name;

  /**
   * The list of long terms encoded in a byte array
   */
  private final byte[] value;

  /**
   * A unique cache key for the query
   */
  private final long cacheKey;

  public TermsEnumTermsQueryBuilder(String name, byte[] values, long cacheKey) {
    this.name = name;
    this.value = values;
    this.cacheKey = cacheKey;
  }

  public TermsEnumTermsQueryBuilder(String name, BytesRef[] values, long cacheKey) throws IOException {
    this(name, Bytes.encode(values), cacheKey);
  }

  @Override
  public void doXContent(XContentBuilder builder, Params params) throws IOException {
    builder.startObject(TermsEnumTermsQueryParser.NAME);
      builder.startObject(name);
        builder.field("value", value);
        builder.field("_cache_key", cacheKey);
      builder.endObject();
    builder.endObject();
  }

}
