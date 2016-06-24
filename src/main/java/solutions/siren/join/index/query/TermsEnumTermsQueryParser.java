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
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.QueryParsingException;

import java.io.IOException;

/**
 * A {@link QueryParser} for {@link FieldDataTermsQuery}.
 */
public class TermsEnumTermsQueryParser implements QueryParser {

  public static final String NAME = "termsenum_terms";

  private static final ESLogger logger = Loggers.getLogger(TermsEnumTermsQueryParser.class);

  public TermsEnumTermsQueryParser() {}

  @Override
  public String[] names() {
    return new String[]{NAME};
  }

  @Override
  public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
    XContentParser parser = parseContext.parser();

    XContentParser.Token token = parser.nextToken();
    if (token != XContentParser.Token.FIELD_NAME) {
        throw new QueryParsingException(parseContext, "[termsenum_terms] a field name is required");
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
            throw new QueryParsingException(parseContext, "[termsenum_terms] filter does not support [" + currentFieldName + "]");
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
      throw new QueryParsingException(parseContext, "[termsenum_terms] a binary value is required");
    }
    if (cacheKey == null) { // cache key is mandatory - see #170
      throw new QueryParsingException(parseContext, "[termsenum_terms] a cache key is required");
    }

    MappedFieldType fieldType = parseContext.fieldMapper(fieldName);
    if (fieldType == null) {
      throw new QueryParsingException(parseContext, "[termsenum_terms] field '" + fieldName +
              "' does not exist in index '" + parseContext.index().getName() +"'.");
    }

    Query query = new TermsEnumTermsQuery(value, fieldName, cacheKey);

    if (queryName != null) {
      parseContext.addNamedQuery(queryName, query);
    }

    return query;
  }

}
