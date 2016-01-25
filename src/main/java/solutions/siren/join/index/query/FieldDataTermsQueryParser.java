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

import com.carrotsearch.hppc.LongHashSet;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.QueryParsingException;

import java.io.IOException;

/**
 * A {@link QueryParser} for {@link FieldDataTermsQuery}.
 */
public class FieldDataTermsQueryParser implements QueryParser {

  public static final String NAME = "fielddata_terms";

  private final ESLogger logger = Loggers.getLogger(getClass());

  public FieldDataTermsQueryParser() {}

  @Override
  public String[] names() {
    return new String[]{NAME};
  }

  @Override
  public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
    XContentParser parser = parseContext.parser();

    XContentParser.Token token = parser.nextToken();
    if (token != XContentParser.Token.FIELD_NAME) {
        throw new QueryParsingException(parseContext, "[fielddata_terms] a field name is required");
    }
    String fieldName = parser.currentName();

    String queryName = null;
    byte[] value = null;
    Integer cacheKey = null;

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
              cacheKey = parser.intValue();
          } else {
            throw new QueryParsingException(parseContext, "[fielddata_terms] filter does not support [" + currentFieldName + "]");
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
      throw new QueryParsingException(parseContext, "[fielddata_terms] a binary value is required");
    }
    if (cacheKey == null) { // cache key is mandatory - see #170
      throw new QueryParsingException(parseContext, "[fielddata_terms] a cache key is required");
    }

    MappedFieldType fieldType = parseContext.fieldMapper(fieldName);
    if (fieldType != null) {
      fieldName = fieldType.names().indexName();
    }

    IndexFieldData fieldData = parseContext.getForField(fieldType);
    Query query = this.toFieldDataTermsQuery(fieldType, fieldData, value, cacheKey);

    if (queryName != null) {
      parseContext.addNamedQuery(queryName, query);
    }

    return query;
  }

  private final Query toFieldDataTermsQuery(MappedFieldType fieldType, IndexFieldData fieldData,
                                            byte[] encodedTerms, int cacheKey) {
    Query query = null;

    if (fieldType instanceof NumberFieldMapper.NumberFieldType) {
      query = FieldDataTermsQuery.newLongs(encodedTerms, (IndexNumericFieldData) fieldData, cacheKey);
    } else if (fieldType instanceof StringFieldMapper.StringFieldType) {
      query = FieldDataTermsQuery.newBytes(encodedTerms, fieldData, cacheKey);
    } else {
      throw new ElasticsearchParseException("[fielddata_terms] query does not support field data type " + fieldType.fieldDataType().getType());
    }

    return query;
  }

}
