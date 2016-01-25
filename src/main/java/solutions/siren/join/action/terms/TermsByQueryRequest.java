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
package solutions.siren.join.action.terms;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.query.QueryBuilder;
import solutions.siren.join.action.terms.collector.TermsSet;

import java.io.IOException;
import java.util.Arrays;

/**
 * A request to get the values from a specific field for documents matching a specific query.
 * <p/>
 * The request requires the filter source to be set using {@link #query(QueryBuilder)}.
 *
 * @see TermsByQueryResponse
 */
public class TermsByQueryRequest extends BroadcastRequest<TermsByQueryRequest> {

  @Nullable
  protected String routing;
  private long nowInMillis;
  @Nullable
  private String preference;
  private BytesReference querySource;
  @Nullable
  private String[] types = Strings.EMPTY_ARRAY;
  private String field;
  @Nullable
  private Ordering ordering;
  @Nullable
  private Integer maxTermsPerShard;

  /**
   * The encoding to use for terms. Default to {@link TermsEncoding#LONG}.
   */
  private TermsEncoding termsEncoding = TermsEncoding.LONG;

  TermsByQueryRequest() {}

  /**
   * Constructs a new terms by query request against the provided indices. No indices provided means it will run against all indices.
   */
  public TermsByQueryRequest(String... indices) {
    super(indices);
  }

  /**
   * Validates the request
   *
   * @return null if valid, exception otherwise
   */
  @Override
  public ActionRequestValidationException validate() {
    ActionRequestValidationException validationException = super.validate();
    return validationException;
  }

  /**
   * The field to extract terms from.
   */
  public String field() {
    return field;
  }

  /**
   * The field to extract terms from.
   */
  public TermsByQueryRequest field(String field) {
    this.field = field;
    return this;
  }

  /**
   * The query source to execute.
   */
  public BytesReference querySource() {
    return querySource;
  }

  /**
   * The query source to execute.
   *
   * @see {@link org.elasticsearch.index.query.QueryBuilders}
   */
  public TermsByQueryRequest query(QueryBuilder queryBuilder) {
    this.querySource = queryBuilder == null ? null : queryBuilder.buildAsBytes();
    return this;
  }

  /**
   * The query source to execute.
   */
  public TermsByQueryRequest query(XContentBuilder builder) {
    this.querySource = builder == null ? null : builder.bytes();
    return this;
  }

  /**
   * The types of documents the query will run against. Defaults to all types.
   */
  public String[] types() {
    return this.types;
  }

  /**
   * The types of documents the query will run against. Defaults to all types.
   */
  public TermsByQueryRequest types(String... types) {
    this.types = types;
    return this;
  }

  /**
   * A comma separated list of routing values to control the shards the search will be executed on.
   */
  public String routing() {
    return this.routing;
  }

  /**
   * A comma separated list of routing values to control the shards the search will be executed on.
   */
  public TermsByQueryRequest routing(String routing) {
    this.routing = routing;
    return this;
  }

  /**
   * The current time in milliseconds
   */
  public long nowInMillis() {
    return nowInMillis;
  }

  /**
   * Sets the current time in milliseconds
   */
  public TermsByQueryRequest nowInMillis(long nowInMillis) {
    this.nowInMillis = nowInMillis;
    return this;
  }

  /**
   * The routing values to control the shards that the request will be executed on.
   */
  public TermsByQueryRequest routing(String... routings) {
    this.routing = Strings.arrayToCommaDelimitedString(routings);
    return this;
  }

  /**
   * The preference value to control what node the request will be executed on
   */
  public TermsByQueryRequest preference(String preference) {
    this.preference = preference;
    return this;
  }

  /**
   * The current preference value
   */
  public String preference() {
    return this.preference;
  }

  /**
   * The types of ordering
   */
  public enum Ordering {
    DEFAULT,
    DOC_SCORE
  }

  /**
   * Sets the ordering to use before performing the term cut.
   */
  public TermsByQueryRequest orderBy(Ordering ordering) {
    this.ordering = ordering;
    return this;
  }

  /**
   * Returns the ordering to use before performing the term cut.
   */
  public Ordering getOrderBy() {
    return ordering;
  }

  /**
   * The max number of terms to gather per shard
   */
  public TermsByQueryRequest maxTermsPerShard(Integer maxTermsPerShard) {
    this.maxTermsPerShard = maxTermsPerShard;
    return this;
  }

  /**
   * The max number of terms to gather per shard
   */
  public Integer maxTermsPerShard() {
    return maxTermsPerShard;
  }

  /**
   * The types of terms encoding
   */
  public enum TermsEncoding {
    LONG, INTEGER
  }

  /**
   * Sets the encoding to use for transferring terms across shards.
   */
  public TermsByQueryRequest termsEncoding(TermsEncoding termsEncoding) {
    this.termsEncoding = termsEncoding;
    return this;
  }

  /**
   * Returns the encoding to use for transferring terms across shards.
   */
  public TermsEncoding termsEncoding() {
    return termsEncoding;
  }

  /**
   * Deserialize
   *
   * @param in the input
   * @throws IOException
   */
  @Override
  public void readFrom(StreamInput in) throws IOException {
    super.readFrom(in);

    routing = in.readOptionalString();
    preference = in.readOptionalString();

    if (in.readBoolean()) {
      querySource = in.readBytesReference();
    } else {
      querySource = null;
    }

    if (in.readBoolean()) {
      types = in.readStringArray();
    }

    field = in.readString();
    nowInMillis = in.readVLong();

    if (in.readBoolean()) {
      ordering = Ordering.values()[in.readVInt()];
    }

    if (in.readBoolean()) {
      maxTermsPerShard = in.readVInt();
    }

    termsEncoding = TermsEncoding.values()[in.readVInt()];
  }

  /**
   * Serialize
   *
   * @param out the output
   * @throws IOException
   */
  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);

    out.writeOptionalString(routing);
    out.writeOptionalString(preference);

    if (querySource != null) {
      out.writeBoolean(true);
      out.writeBytesReference(querySource);
    } else {
      out.writeBoolean(false);
    }

    if (types == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeStringArray(types);
    }

    out.writeString(field);
    out.writeVLong(nowInMillis);

    if (ordering == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeVInt(ordering.ordinal());
    }

    if (maxTermsPerShard == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeVInt(maxTermsPerShard);
    }

    out.writeVInt(termsEncoding.ordinal());
  }

  /**
   * String representation of the request
   */
  @Override
  public String toString() {
    String sSource = "_na_";
    try {
      sSource = XContentHelper.convertToJson(querySource, false);
    } catch (Exception e) {
      // ignore
    }
    return Arrays.toString(indices) + Arrays.toString(types) + "[" + field + "], querySource[" + sSource + "]";
  }
}
