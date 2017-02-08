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
package solutions.siren.join.action.coordinate.execution;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.WrapperQueryParser;
import java.io.IOException;
import java.util.*;

/**
 *  Visitor that will unwrap queries encoded in a {@link WrapperQueryParser#NAME} query.
 */
@SuppressWarnings("unchecked")
public class WrapperQueryVisitor {

  private final Map map;

  public WrapperQueryVisitor(Map map) {
    this.map = map;
  }

  public void traverse() {
    this.visit(map);
  }

  private void visit(Map map) {
    Set<Map.Entry> entries = map.entrySet();

    // if map contains a wrapped query, unwrap it
    if (map.containsKey(WrapperQueryParser.NAME)) {
      Map<String, Object> query = unwrap(map.get(WrapperQueryParser.NAME));
      if (query != null) {
        map.remove(WrapperQueryParser.NAME);
        Map.Entry<String, Object> entry = query.entrySet().iterator().next();
        map.put(entry.getKey(), entry.getValue());
      }
    }

    // traverse map
    for (Map.Entry entry : entries) {
      this.visit(entry.getValue());
    }

  }

  private Map<String, Object> unwrap(Object value) {
    if (value instanceof Map) {
      Map wrapperQuery = (Map) value;
      if (wrapperQuery.containsKey("query")) {
        Object encodedQuery = wrapperQuery.get("query");
        if (encodedQuery instanceof byte[]) {
          return this.parseQuery((byte[]) encodedQuery);
        }
        else if (encodedQuery instanceof String) {
          return this.parseQuery((String) encodedQuery);
        }
      }
    }
    // ignore malformed wrapper query
    return null;
  }

  private void visit(List array) {
    for (Object obj : array) {
      this.visit(obj);
    }
  }

  private void visit(Object value) {
    if (value instanceof Map) {
      this.visit((Map<String, Object>) value);
    }
    else if (value instanceof List) {
      this.visit((List) value);
    }
  }

  protected Map<String, Object> parseQuery(byte[] source) {
    return this.parseQuery(new BytesArray(source, 0, source.length));
  }

  protected Map<String, Object> parseQuery(String source) {
    try {
      byte[] bytes = Base64.decode(source);
      return this.parseQuery(new BytesArray(bytes, 0, bytes.length));
    }
    catch (IOException e) {
      throw new ElasticsearchParseException("Failed to parse source [" + source + "]", e);
    }
  }

  protected Map<String, Object> parseQuery(BytesReference source) {
    // nothing to parse...
    if (source == null || source.length() == 0) {
      return null;
    }

    try {
      Tuple<XContentType, Map<String, Object>> parsedSource = XContentHelper.convertToMap(source, false);
      return parsedSource.v2();
    }
    catch (Throwable e) {
      String sSource = "_na_";
      try {
        sSource = XContentHelper.convertToJson(source, false);
      }
      catch (Throwable e1) { /* ignore  */ }
      throw new ElasticsearchParseException("Failed to parse source [" + sSource + "]", e);
    }
  }

}
