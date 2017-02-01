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

import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import solutions.siren.join.common.Bytes;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class TermsEnumTermsQueryBuilder extends AbstractQueryBuilder<TermsEnumTermsQueryBuilder> {

  public static final String NAME = "termsenum_terms";

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

  public TermsEnumTermsQueryBuilder(StreamInput in) throws IOException {
    super(in);
    this.name = in.readString();
    this.value = in.readByteArray();
    this.cacheKey = in.readLong();
  }

  @Override
  protected void doWriteTo(StreamOutput out) throws IOException {
    out.writeString(name);
    out.writeByteArray(value);
    out.writeLong(cacheKey);
  }

  public static Optional<TermsEnumTermsQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
    XContentParser parser = parseContext.parser();

    XContentParser.Token token = parser.nextToken();
    if (token != XContentParser.Token.FIELD_NAME) {
        throw new ParsingException(parser.getTokenLocation(), "[termsenum_terms] a field name is required");
    }
    String fieldName = parser.currentName();

    String queryName = null;
    byte[] value = null;
    Long cacheKey = null;

    token = parser.nextToken();
    if (token == XContentParser.Token.START_OBJECT) {
      String currentFieldName = null;
      while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
        if (token == XContentParser.Token.FIELD_NAME) {
          currentFieldName = parser.currentName();
        } else {
          if ("value".equals(currentFieldName)) {
              value = parser.binaryValue();
          } else if ("_name".equals(currentFieldName)) {
              queryName = parser.text();
          } else if ("_cache_key".equals(currentFieldName) || "_cacheKey".equals(currentFieldName)) {
              cacheKey = parser.longValue();
          } else {
            throw new ParsingException(parser.getTokenLocation(), "[termsenum_terms] filter does not support [" + currentFieldName + "]");
          }
        }
      }
      parser.nextToken();
    } else {
      value = parser.binaryValue();
      // move to the next token
      parser.nextToken();
    }

    if (value == null) {
      throw new ParsingException(parser.getTokenLocation(), "[termsenum_terms] a binary value is required");
    }
    if (cacheKey == null) { // cache key is mandatory - see #170
      throw new ParsingException(parser.getTokenLocation(), "[termsenum_terms] a cache key is required");
    }

    if (fieldName == null) {
      throw new ParsingException(parser.getTokenLocation(), "[termsenum_terms] a field name is required");
    }

    TermsEnumTermsQueryBuilder queryBuilder = new TermsEnumTermsQueryBuilder(fieldName, value, cacheKey);

    if (queryName != null) {
      queryBuilder.queryName(queryName);
    }


    return Optional.of(queryBuilder);
  }

  @Override
  public void doXContent(XContentBuilder builder, Params params) throws IOException {
    builder.startObject(TermsEnumTermsQueryBuilder.NAME);
      builder.startObject(name);
        builder.field("value", value);
        builder.field("_cache_key", cacheKey);
      builder.endObject();
    builder.endObject();
  }

  @Override
  protected Query doToQuery(QueryShardContext context) throws IOException {
    MappedFieldType fieldType = context.fieldMapper(name);

    if (fieldType == null) {
      return new MatchNoDocsQuery("Field " + name + " has no indexed type");
    }

    return new TermsEnumTermsQuery(value, name, cacheKey);
  }


  @Override
  protected boolean doEquals(TermsEnumTermsQueryBuilder other) {
    return Objects.equals(name, other.name)
            && Objects.equals(value, other.value)
            && Objects.equals(cacheKey, other.cacheKey);
  }

  @Override
  protected int doHashCode() {
    return Objects.hash(name, value, cacheKey);
  }

  @Override
  public String getWriteableName() {
    return NAME;
  }
}
