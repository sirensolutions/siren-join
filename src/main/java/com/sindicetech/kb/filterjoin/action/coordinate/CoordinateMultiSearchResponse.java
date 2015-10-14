package com.sindicetech.kb.filterjoin.action.coordinate;

import com.google.common.collect.Iterators;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.Iterator;

/**
 * A multi search response from a coordinated action.
 * <br>
 * Hack: This is mainly a copy-paste of {@link org.elasticsearch.action.search.MultiSearchResponse} with a change in
 * {@link com.sindicetech.kb.filterjoin.action.coordinate.CoordinateMultiSearchResponse.Item#readFrom(StreamInput)}
 * to instantiate a {@link CoordinateSearchResponse} instead of a {@link SearchResponse}. We were not able to
 * extend it since {@link org.elasticsearch.action.search.MultiSearchResponse.Item} contains private constructors
 * and variables.
 *
 * todo: push a patch to Elasticsearch to make the variables protected instead of private, and review implementation.
 */
public class CoordinateMultiSearchResponse extends MultiSearchResponse {

  /**
   * A search response item, holding the actual search response, or an error message if it failed.
   * <br>
   * Extends {@link org.elasticsearch.action.search.MultiSearchResponse.Item} just to keep {@link MultiSearchResponse}
   * API compatibility. Since {@link org.elasticsearch.action.search.MultiSearchResponse.Item} has private variables,
   * we can't reuse them and need to re-implement everything with our own set of variables.
   *
   * @see org.elasticsearch.action.search.MultiSearchResponse.Item
   */
  public static class Item extends MultiSearchResponse.Item {

    private SearchResponse response;
    private String failureMessage;

    Item() {
      super(null, null); // hack: empty constructor is private, use this one instead
    }

    public Item(SearchResponse response, String failureMessage) {
      super(null, null); // hack: empty constructor since we can't use parent's private variables.
      this.response = response;
      this.failureMessage = failureMessage;
    }

    /**
     * Is it a failed search?
     */
    public boolean isFailure() {
      return failureMessage != null;
    }

    /**
     * The actual failure message, null if its not a failure.
     */
    @Nullable
    public String getFailureMessage() {
      return failureMessage;
    }

    /**
     * The actual search response, null if its a failure.
     */
    @Nullable
    public SearchResponse getResponse() {
      return this.response;
    }

    public static Item readItem(StreamInput in) throws IOException {
      Item item = new Item();
      item.readFrom(in);
      return item;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
      if (in.readBoolean()) {
        this.response = new CoordinateSearchResponse();
        response.readFrom(in);
      } else {
        failureMessage = in.readString();
      }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
      if (response != null) {
        out.writeBoolean(true);
        response.writeTo(out);
      } else {
        out.writeBoolean(false);
        out.writeString(failureMessage);
      }
    }
  }

  private Item[] items;

  CoordinateMultiSearchResponse() {
    // hack: empty constructor is private, use this one instead
    // we use an empty array to avoid NPE during serialization
    super(new MultiSearchResponse.Item[0]);
  }

  public CoordinateMultiSearchResponse(Item[] items) {
    super(new MultiSearchResponse.Item[0]); // hack: empty constructor is private
    this.items = items;
  }

  @Override
  public Iterator<MultiSearchResponse.Item> iterator() {
    return Iterators.forArray((MultiSearchResponse.Item[]) items);
  }

  /**
   * The list of responses, the order is the same as the one provided in the request.
   */
  public Item[] getResponses() {
    return this.items;
  }

  @Override
  public void readFrom(StreamInput in) throws IOException {
    super.readFrom(in);
    items = new Item[in.readVInt()];
    for (int i = 0; i < items.length; i++) {
      items[i] = Item.readItem(in);
    }
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeVInt(items.length);
    for (Item item : items) {
      item.writeTo(out);
    }
  }

  @Override
  public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
    builder.startArray(Fields.RESPONSES);
    for (Item item : items) {
      if (item.isFailure()) {
        builder.startObject();
        builder.field(Fields.ERROR, item.getFailureMessage());
        builder.endObject();
      } else {
        builder.startObject();
        item.getResponse().toXContent(builder, params);
        builder.endObject();
      }
    }
    builder.endArray();
    return builder;
  }

  static final class Fields {
    static final XContentBuilderString RESPONSES = new XContentBuilderString("responses");
    static final XContentBuilderString ERROR = new XContentBuilderString("error");
  }

}
