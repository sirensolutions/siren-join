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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BaseFilterBuilder;

import java.io.IOException;

public class BinaryTermsFilterBuilder extends BaseFilterBuilder {

  private final String name;

  private final byte[] value;

  private final String cacheKey;

  public BinaryTermsFilterBuilder(String name, byte[] values, String cacheKey) {
    this.name = name;
    this.value = values;
    this.cacheKey = cacheKey;
  }

  public BinaryTermsFilterBuilder(String name, long[] values, String cacheKey) throws IOException {
    this(name, BinaryTermsFilterHelper.encode(values), cacheKey);
  }

  @Override
  public void doXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
    builder.startObject(BinaryTermsFilterParser.NAME);
      builder.startObject(name);
        builder.field("value", value);
        builder.field("_cache_key", cacheKey);
      builder.endObject();
    builder.endObject();
  }

  @Override
  public String toString() {
    try {
      XContentBuilder builder = XContentFactory.jsonBuilder();
      builder.prettyPrint();

      builder.startObject(BinaryTermsFilterParser.NAME);
        builder.startObject(name);
          // Do not serialise the full byte array, but instead the number of bytes - see issue #168
          builder.field("value", "[size=" + value.length + "]");
          builder.field("_cache_key", cacheKey);
        builder.endObject();
      builder.endObject();

      return builder.string();
    }
    catch (Exception e) {
      throw new ElasticsearchException("Failed to build filter", e);
    }
  }

}
