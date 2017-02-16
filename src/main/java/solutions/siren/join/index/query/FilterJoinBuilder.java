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

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;

import solutions.siren.join.action.terms.TermsByQueryRequest;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * A filter for a field based on terms coming from another set of documents.
 */
public class FilterJoinBuilder extends AbstractQueryBuilder<FilterJoinBuilder> {

  private final String name;
  private String[] indices;
  private String[] types;
  private String routing;
  private String path;
  private QueryBuilder query;
  private TermsByQueryRequest.Ordering orderBy;
  private Integer maxTermsPerShard;
  private String filterName;
  private TermsByQueryRequest.TermsEncoding termsEncoding;

  public static final String NAME = "filterjoin";

  public FilterJoinBuilder(String name) {
    this.name = name;
    this.types = new String[0];
    this.indices = new String[0];
  }

  /**
   * Sets the index names to lookup the terms from.
   */
  public FilterJoinBuilder indices(String... indices) {
    this.indices = indices;
    return this;
  }

  /**
   * Sets the index types to lookup the terms from.
   */
  public FilterJoinBuilder types(String... types) {
    this.types = types;
    return this;
  }

  /**
   * Sets the path within the document to lookup the terms from.
   */
  public FilterJoinBuilder path(String path) {
    this.path = path;
    return this;
  }

  /**
   * Sets the node routing used to control the shards the lookup request is executed on
   */
  public FilterJoinBuilder routing(String lookupRouting) {
    this.routing = lookupRouting;
    return this;
  }

  /**
   * Sets the query used to lookup terms with
   */
  public FilterJoinBuilder query(QueryBuilder query) {
    this.query = query;
    return this;
  }

  /**
   * Sets the ordering used to lookup terms
   * @param orderBy
   */
  public FilterJoinBuilder orderBy(TermsByQueryRequest.Ordering orderBy) {
    this.orderBy = orderBy;
    return this;
  }

  /**
   * Sets the maximum number of terms per shard to lookup
   */
  public FilterJoinBuilder maxTermsPerShard(int maxTermsPerShard) {
    this.maxTermsPerShard = maxTermsPerShard;
    return this;
  }

  /**
   * Sets the encoding to use for transferring terms across shards.
   */
  public FilterJoinBuilder termsEncoding(TermsByQueryRequest.TermsEncoding termsEncoding) {
    this.termsEncoding = termsEncoding;
    return this;
  }

  /**
   * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
   */
  public FilterJoinBuilder filterName(String filterName) {
    this.filterName = filterName;
    return this;
  }

  public FilterJoinBuilder(StreamInput in) throws IOException {
    super(in);
    name = in.readString();
    indices = in.readStringArray();
    types = in.readStringArray();
    routing = in.readOptionalString();
    path = in.readString();
    query = in.readNamedWriteable(QueryBuilder.class);

    Integer orderByOrdinal = in.readOptionalVInt();
    if (orderByOrdinal != null) {
      orderBy = TermsByQueryRequest.Ordering.values()[orderByOrdinal];
    }
    maxTermsPerShard = in.readOptionalVInt();
    filterName = in.readOptionalString();

    Integer termsEncodingOrdinal = in.readOptionalVInt();
    if (termsEncodingOrdinal != null) {
      termsEncoding = TermsByQueryRequest.TermsEncoding.values()[termsEncodingOrdinal];
    }
    boost = in.readFloat();
  }

  @Override
  protected void doWriteTo(StreamOutput out) throws IOException {
    out.writeString(name);
    out.writeStringArray(indices);
    out.writeStringArray(types);
    out.writeOptionalString(routing);
    out.writeString(path);
    out.writeNamedWriteable(query);
    out.writeOptionalVInt(orderBy == null ? null : orderBy.ordinal());
    out.writeOptionalVInt(maxTermsPerShard);
    out.writeOptionalString(filterName);
    out.writeOptionalVInt(termsEncoding == null ? null : termsEncoding.ordinal());
    out.writeFloat(boost);
  }

  @Override
  public void doXContent(XContentBuilder builder, Params params) throws IOException {
    builder.startObject(FilterJoinBuilder.NAME);

    builder.startObject(name);
    if (indices != null) {
      builder.field("indices", indices);
    }
    if (types != null) {
      builder.field("types", types);
    }
    if (routing != null) {
      builder.field("routing", routing);
    }
    builder.field("path", path);
    builder.field("query", query);
    if (orderBy != null) {
      builder.field("orderBy", orderBy);
    }
    if (maxTermsPerShard != null) {
      builder.field("maxTermsPerShard", maxTermsPerShard);
    }
    if (termsEncoding != null) {
      builder.field("termsEncoding", termsEncoding);
    }
    builder.endObject();

    if (filterName != null) {
      builder.field("_name", filterName);
    }
    if (boost != DEFAULT_BOOST) {
      builder.field("boost", boost);
    }

    builder.endObject();
  }

  public static Optional<FilterJoinBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
    XContentParser parser = parseContext.parser();

    XContentParser.Token token = parser.nextToken();
    if (token != XContentParser.Token.FIELD_NAME) {
      throw new ParsingException(parser.getTokenLocation(), "[filterjoin] a field name is required");
    }
    String fieldName = parser.currentName();

    String[] indices = null;
    String[] types = null;
    String routing = null;
    String path = null;
    Optional<QueryBuilder> query = null;
    TermsByQueryRequest.Ordering orderBy = null;
    Integer maxTermsPerShard = null;
    TermsByQueryRequest.TermsEncoding termsEncoding = null;
    String filterName = null;
    Float boost = null;

    token = parser.nextToken();
    if (token == XContentParser.Token.START_OBJECT) {
      String currentFieldName = null;
      while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
        if (token == XContentParser.Token.FIELD_NAME) {
          currentFieldName = parser.currentName();
        } else {
          if ("indices".equals(currentFieldName)) {
            indices = parser.list().stream().map(o -> (String) o).toArray(String[]::new);
          } else if ("index".equals(currentFieldName)) {
            indices = new String[]{parser.text()};
          } else if ("type".equals(currentFieldName)) {
            types = new String[] {parser.text()};
          } else if ("types".equals(currentFieldName)) {
            types = parser.list().stream().map(o -> (String)o).toArray(String[]::new);
          } else if ("routing".equals(currentFieldName)) {
            routing = parser.text();
          } else if ("path".equals(currentFieldName)) {
            path = parser.text();
          } else if ("query".equals(currentFieldName)) {
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
              query = Optional.empty();
            } else {
              query = parseContext.parseInnerQueryBuilder();
            }
          } else if ("orderBy".equals(currentFieldName)) {
            orderBy = Enum.valueOf(TermsByQueryRequest.Ordering.class, parser.text());
          } else if ("maxTermsPerShard".equals(currentFieldName)) {
            maxTermsPerShard = parser.intValue();
          } else if ("termsEncoding".equals(currentFieldName)) {
            termsEncoding = Enum.valueOf(TermsByQueryRequest.TermsEncoding.class, parser.text());
          } else if ("_name".equals(currentFieldName)) {
            filterName = parser.text();
          } else if ("boost".equals(currentFieldName)) {
            boost = parser.floatValue();
          } else {
            throw new ParsingException(parser.getTokenLocation(), "[filterjoin] filter does not support [" + currentFieldName + "]");
          }
        }
      }
      parser.nextToken();
    } else {
      // move to the next token
      parser.nextToken();
    }

    if (fieldName == null) {
      throw new ParsingException(parser.getTokenLocation(), "[filterjoin] a field name is required");
    }

    FilterJoinBuilder queryBuilder = new FilterJoinBuilder(fieldName);
    if (query != null && query.isPresent()) {
      queryBuilder.query(query.get());
    }
    if (maxTermsPerShard != null) {
      queryBuilder.maxTermsPerShard(maxTermsPerShard);
    }
    if (boost != null) {
      queryBuilder.boost(boost);
    }
    if (orderBy != null) {
      queryBuilder.orderBy(orderBy);
    }
    if (termsEncoding != null) {
      queryBuilder.termsEncoding(termsEncoding);
    }
    if (indices != null) {
      queryBuilder.indices(indices);
    }
    if (types != null) {
      queryBuilder.types(types);
    }
    if (routing != null) {
      queryBuilder.routing(routing);
    }
    if (filterName != null) {
      queryBuilder.filterName(filterName);
    }
    if (path != null) {
      queryBuilder.path(path);
    }

    return Optional.of(queryBuilder);
  }

  @Override
  protected Query doToQuery(QueryShardContext context) throws IOException {
    return null;
  }

  @Override
  protected boolean doEquals(FilterJoinBuilder other) {
    return Objects.equals(name, other.name)
            && Objects.equals(indices, other.indices)
            && Objects.equals(types, other.types)
            && Objects.equals(routing, other.routing)
            && Objects.equals(path, other.path)
            && Objects.equals(query, other.query)
            && Objects.equals(orderBy, other.orderBy)
            && Objects.equals(maxTermsPerShard, other.maxTermsPerShard)
            && Objects.equals(filterName, other.filterName)
            && Objects.equals(termsEncoding, other.termsEncoding)
            && Objects.equals(boost, other.boost);
  }

  @Override
  protected int doHashCode() {
    return Objects.hash(name, indices, types, routing, path, query, orderBy, maxTermsPerShard, filterName,
            termsEncoding, boost);
  }

  @Override
  public String getWriteableName() {
    return NAME;
  }
}