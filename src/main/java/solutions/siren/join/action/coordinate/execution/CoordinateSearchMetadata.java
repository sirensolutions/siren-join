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
    static final String COORDINATE_SEARCH = "coordinate_search";
    static final String ACTIONS = "actions";
  }

  public CoordinateSearchMetadata() {}

  Action addAction(Relation from, Relation to) {
    Action action = new Action(from, to);
    this.actions.add(action);
    return action;
  }

  List<Action> getActions() {
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
  static class Action {

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
      static final String RELATIONS = "relations";
      static final String FROM = "from";
      static final String TO = "to";
      static final String SIZE = "size";
      static final String SIZE_IN_BYTES = "size_in_bytes";
      static final String IS_PRUNED = "is_pruned";
      static final String CACHE_HIT = "cache_hit";
      static final String TOOK = "took";
      static final String TERMS_ENCODING = "terms_encoding";
      static final String ORDERING = "order_by";
      static final String MAX_TERMS_PER_SHARD = "max_terms_per_shard";
    }

    Action() {}

    Action(Relation from, Relation to) {
      this.relations = new Relation[] { from, to };
    }

    void setPruned(boolean isPruned) {
      this.isPruned = isPruned;
    }

    void setSize(int size) {
      this.size = size;
    }

    void setSizeInBytes(long size) {
      this.sizeInBytes = size;
    }

    void setCacheHit(boolean cacheHit) {
      this.cacheHit = cacheHit;
    }

    void setTookInMillis(long tookInMillis) {
      this.tookInMillis = tookInMillis;
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
      static final String INDICES = "indices";
      static final String TYPES = "types";
      static final String FIELD = "field";
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
