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

import org.apache.lucene.queryparser.classic.MapperQueryParser;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.StringFieldMapper;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import solutions.siren.join.common.Bytes;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class FieldDataTermsQueryBuilder extends AbstractQueryBuilder<FieldDataTermsQueryBuilder> {

  public static final String NAME = "fielddata_terms";

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

  public FieldDataTermsQueryBuilder(String name, byte[] values, long cacheKey) {
    this.name = name;
    this.value = values;
    this.cacheKey = cacheKey;
  }

  public FieldDataTermsQueryBuilder(String name, long[] values, long cacheKey) throws IOException {
    this(name, Bytes.encode(values), cacheKey);
  }

  public FieldDataTermsQueryBuilder(StreamInput in) throws IOException {
    super(in);
    name = in.readString();
    value = in.readByteArray();
    cacheKey = in.readLong();
  }

  @Override
  protected void doWriteTo(StreamOutput out) throws IOException {
    out.writeString(name);
    out.writeByteArray(value);
    out.writeLong(cacheKey);
  }

  @Override
  public void doXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
    builder.startObject(FieldDataTermsQueryBuilder.NAME);
      builder.startObject(name);
        builder.field("value", value);
        builder.field("_cache_key", cacheKey);
      builder.endObject();
    builder.endObject();
  }

  public static Optional<FieldDataTermsQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
    XContentParser parser = parseContext.parser();

    XContentParser.Token token = parser.nextToken();
    if (token != XContentParser.Token.FIELD_NAME) {
      throw new ParsingException(parser.getTokenLocation(), "[fielddata_terms] a field name is required");
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
            throw new ParsingException(parser.getTokenLocation(), "[fielddata_terms] filter does not support [" + currentFieldName + "]");
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
      throw new ParsingException(parser.getTokenLocation(), "[fielddata_terms] a binary value is required");
    }
    if (cacheKey == null) { // cache key is mandatory - see #170
      throw new ParsingException(parser.getTokenLocation(), "[fielddata_terms] a cache key is required");
    }

    if (fieldName == null) {
      throw new ParsingException(parser.getTokenLocation(), "[fielddata_terms] a field name is required");
    }

    FieldDataTermsQueryBuilder queryBuilder = new FieldDataTermsQueryBuilder(fieldName, value, cacheKey);
    queryBuilder.queryName(queryName);

    return Optional.of(queryBuilder);

  }

  @Override
  protected Query doToQuery(QueryShardContext context) throws IOException {
    MappedFieldType mappedFieldType = context.fieldMapper(name);

    if (mappedFieldType == null) {
      return new MatchNoDocsQuery("Field " + name + " has no indexed type");
    }

    return toFieldDataTermsQuery(mappedFieldType, context.getForField(mappedFieldType), value, cacheKey);
  }

  private final Query toFieldDataTermsQuery(MappedFieldType fieldType, IndexFieldData fieldData,
                                            byte[] encodedTerms, long cacheKey) {
    Query query;

    if (fieldType instanceof NumberFieldMapper.NumberFieldType) {
      query = FieldDataTermsQuery.newLongs(encodedTerms, (IndexNumericFieldData) fieldData, cacheKey);
    } else if (fieldType instanceof StringFieldMapper.StringFieldType || fieldType instanceof KeywordFieldMapper.KeywordFieldType) {
      query = FieldDataTermsQuery.newBytes(encodedTerms, fieldData, cacheKey);
    } else {
      throw new ElasticsearchParseException("[fielddata_terms] query does not support field data type " + fieldType.typeName());
    }

    return query;
  }

  @Override
  protected boolean doEquals(FieldDataTermsQueryBuilder other) {
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
