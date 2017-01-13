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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import solutions.siren.join.action.terms.TermsByQueryRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Holds various metadata information about the execution of a coordinate search.
 */
public class CoordinateSearchMetadata {

  /**
   * The list of actions
   */
  private List<Action> actions = new ArrayList<>();

  static final class Fields {
    static final XContentBuilderString COORDINATE_SEARCH = new XContentBuilderString("coordinate_search");
    static final XContentBuilderString ACTIONS = new XContentBuilderString("actions");
  }

  public CoordinateSearchMetadata() {}

  Action addAction(Relation from, Relation to) {
    Action action = new Action(from, to);
    this.actions.add(action);
    return action;
  }

  public List<Action> getActions() {
    return this.actions;
  }

  public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
    builder.startObject(Fields.COORDINATE_SEARCH);

    builder.startArray(Fields.ACTIONS);
    for (Action action : this.actions) {
      action.toXContent(builder);
    }
    builder.endArray();

    builder.endObject();
    return builder;
  }

  public void readFrom(StreamInput in) throws IOException {
    int size = in.readVInt();
    for (int i = 0; i < size; i++) {
      Action action = new Action();
      action.readFrom(in);
      this.actions.add(action);
    }
  }

  public void writeTo(StreamOutput out) throws IOException {
    out.writeVInt(this.actions.size());
    for (Action action : this.actions) {
      action.writeTo(out);
    }
  }

  /**
   * Metadata about a filter join action.
   */
  public static class Action {

    Relation[] relations;
    int size;
    long sizeInBytes;
    boolean isPruned;
    boolean cacheHit;
    long tookInMillis;
    TermsByQueryRequest.TermsEncoding termsEncoding;
    TermsByQueryRequest.Ordering ordering;
    int maxTermsPerShard;

    static final class Fields {
      static final XContentBuilderString RELATIONS = new XContentBuilderString("relations");
      static final XContentBuilderString FROM = new XContentBuilderString("from");
      static final XContentBuilderString TO = new XContentBuilderString("to");
      static final XContentBuilderString SIZE = new XContentBuilderString("size");
      static final XContentBuilderString SIZE_IN_BYTES = new XContentBuilderString("size_in_bytes");
      static final XContentBuilderString IS_PRUNED = new XContentBuilderString("is_pruned");
      static final XContentBuilderString CACHE_HIT = new XContentBuilderString("cache_hit");
      static final XContentBuilderString TOOK = new XContentBuilderString("took");
      static final XContentBuilderString TERMS_ENCODING = new XContentBuilderString("terms_encoding");
      static final XContentBuilderString ORDERING = new XContentBuilderString("order_by");
      static final XContentBuilderString MAX_TERMS_PER_SHARD = new XContentBuilderString("max_terms_per_shard");
    }

    Action() {}

    Action(Relation from, Relation to) {
      this.relations = new Relation[] { from, to };
    }

    void setPruned(boolean isPruned) {
      this.isPruned = isPruned;
    }

    public boolean isPruned() {
      return isPruned;
    }

    void setSize(int size) {
      this.size = size;
    }

    public int size() {
      return size;
    }

    void setSizeInBytes(long size) {
      this.sizeInBytes = size;
    }

    public long sizeInBytes() {
      return sizeInBytes;
    }

    void setCacheHit(boolean cacheHit) {
      this.cacheHit = cacheHit;
    }

    public boolean cacheHit() {
      return cacheHit;
    }

    void setTookInMillis(long tookInMillis) {
      this.tookInMillis = tookInMillis;
    }

    public long tookInMillis() {
      return tookInMillis;
    }

    void setTermsEncoding(TermsByQueryRequest.TermsEncoding termsEncoding) {
      this.termsEncoding = termsEncoding;
    }

    public void setMaxTermsPerShard(Integer maxTermsPerShard) {
      this.maxTermsPerShard = maxTermsPerShard == null ? -1 : maxTermsPerShard;
    }

    public void setOrdering(TermsByQueryRequest.Ordering ordering) {
      this.ordering = ordering;
    }

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
      builder.startObject();

      builder.startObject(Fields.RELATIONS);

      builder.startObject(Fields.FROM);
      this.relations[0].toXContent(builder);
      builder.endObject();

      builder.startObject(Fields.TO);
      this.relations[1].toXContent(builder);
      builder.endObject();

      builder.endObject(); // end relations object

      builder.field(Fields.SIZE, size);
      builder.field(Fields.SIZE_IN_BYTES, sizeInBytes);
      builder.field(Fields.IS_PRUNED, isPruned);
      builder.field(Fields.CACHE_HIT, cacheHit);
      builder.field(Fields.TERMS_ENCODING, termsEncoding.name().toLowerCase(Locale.ROOT));
      if (ordering != null) {
        builder.field(Fields.ORDERING, ordering.name().toLowerCase(Locale.ROOT));
      }
      if (maxTermsPerShard != -1) {
        builder.field(Fields.MAX_TERMS_PER_SHARD, maxTermsPerShard);
      }
      builder.field(Fields.TOOK, tookInMillis);

      builder.endObject();
      return builder;
    }

    public void readFrom(StreamInput in) throws IOException {
      Relation left = new Relation();
      left.readFrom(in);
      Relation right = new Relation();
      right.readFrom(in);
      this.relations = new Relation[] { left, right };
      this.size = in.readVInt();
      this.sizeInBytes = in.readVLong();
      this.isPruned = in.readBoolean();
      this.cacheHit = in.readBoolean();
      this.tookInMillis = in.readLong();
      this.termsEncoding = TermsByQueryRequest.TermsEncoding.values()[in.readVInt()];
      if (in.readBoolean()) {
        this.ordering = TermsByQueryRequest.Ordering.values()[in.readVInt()];
      }
      if (in.readBoolean()) {
        this.maxTermsPerShard = in.readVInt();
      }
    }

    public void writeTo(StreamOutput out) throws IOException {
      this.relations[0].writeTo(out);
      this.relations[1].writeTo(out);
      out.writeVInt(size);
      out.writeVLong(sizeInBytes);
      out.writeBoolean(isPruned);
      out.writeBoolean(cacheHit);
      out.writeLong(tookInMillis);
      out.writeVInt(termsEncoding.ordinal());
      if (ordering == null) {
        out.writeBoolean(false);
      } else {
        out.writeBoolean(true);
        out.writeVInt(ordering.ordinal());
      }
      if (maxTermsPerShard == -1) {
        out.writeBoolean(false);
      } else {
        out.writeBoolean(true);
        out.writeVInt(maxTermsPerShard);
      }
    }

  }

  /**
   * Metadata about a relation. A relation is composed of an index, type and field.
   */
  static class Relation {

    String[] indices;
    String[] types;
    String field;

    static final class Fields {
      static final XContentBuilderString INDICES = new XContentBuilderString("indices");
      static final XContentBuilderString TYPES = new XContentBuilderString("types");
      static final XContentBuilderString FIELD = new XContentBuilderString("field");
    }

    Relation() {}

    Relation(String[] indices, String[] types, String field) {
      this.indices = indices;
      this.types = types;
      this.field = field;
    }

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
      if (this.indices != null) {
        builder.field(Fields.INDICES, this.indices);
      }
      else {
        builder.nullField(Fields.INDICES);
      }
      if (this.types != null) {
        builder.field(Fields.TYPES, this.types);
      }
      else {
        builder.nullField(Fields.TYPES);
      }
      builder.field(Fields.FIELD, this.field);
      return builder;
    }

    public void readFrom(StreamInput in) throws IOException {
      this.indices = in.readStringArray();
      this.types = in.readStringArray();
      this.field = in.readOptionalString();
    }

    public void writeTo(StreamOutput out) throws IOException {
      out.writeStringArrayNullable(indices);
      out.writeStringArrayNullable(types);
      out.writeOptionalString(field);
    }

  }

}
