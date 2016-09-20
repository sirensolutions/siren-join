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

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.rest.client.RestException;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.junit.Test;
import solutions.siren.join.SirenJoinTestCase;
import solutions.siren.join.action.terms.TermsByQueryRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static solutions.siren.join.index.query.QueryBuilders.filterJoin;

@ESIntegTestCase.ClusterScope(scope= ESIntegTestCase.Scope.SUITE, numDataNodes=1, numClientNodes=0)
public class CoordinateSearchMetadataTest extends SirenJoinTestCase {

  @Override
  protected Settings nodeSettings(int nodeOrdinal) {
    return Settings.builder()
            .put(Node.HTTP_ENABLED, true) // enable http for these tests
            .put(super.nodeSettings(nodeOrdinal)).build();
  }

  private abstract class ExpectedAction {
    protected String fromIndex;
    protected String fromType;
    protected String fromField;
    protected String toIndex;
    protected String toType;
    protected String toField;
    protected boolean isPruned;
    protected int size;
    protected boolean cacheHit;
    protected int sizeInBytes;
    protected int took;
    protected TermsByQueryRequest.TermsEncoding termsEncoding = TermsByQueryRequest.TermsEncoding.LONG;
    protected TermsByQueryRequest.Ordering ordering;
    protected int maxTermsPerShard = -1;
  }

  private void assertAction(Map action, ExpectedAction expectedAction) {
    String key = CoordinateSearchMetadata.Action.Fields.RELATIONS.underscore().getValue();
    assertTrue(action.containsKey(key));
    Map relations = (Map) action.get(key);

    key = CoordinateSearchMetadata.Action.Fields.FROM.underscore().getValue();
    assertTrue(relations.containsKey(key));
    Map to = (Map) relations.get(key);
    assertNotNull(to.get(CoordinateSearchMetadata.Relation.Fields.INDICES.underscore().getValue()));
    assertThat((List<String>) to.get(CoordinateSearchMetadata.Relation.Fields.INDICES.underscore().getValue()), contains(equalTo(expectedAction.toIndex)));
    assertNotNull(to.get(CoordinateSearchMetadata.Relation.Fields.TYPES.underscore().getValue()));
    assertThat((List<String>) to.get(CoordinateSearchMetadata.Relation.Fields.TYPES.underscore().getValue()), contains(equalTo(expectedAction.toType)));
    assertNotNull(to.get(CoordinateSearchMetadata.Relation.Fields.FIELD.underscore().getValue()));
    assertThat((String) to.get(CoordinateSearchMetadata.Relation.Fields.FIELD.underscore().getValue()), equalTo(expectedAction.toField));

    key = CoordinateSearchMetadata.Action.Fields.TO.underscore().getValue();
    assertTrue(relations.containsKey(key));
    Map from = (Map) relations.get(key);
    if (expectedAction.fromIndex == null) {
      assertNull(from.get(CoordinateSearchMetadata.Relation.Fields.INDICES.underscore().getValue()));
    } else {
      assertNotNull(from.get(CoordinateSearchMetadata.Relation.Fields.INDICES.underscore().getValue()));
      assertThat((List<String>) from.get(CoordinateSearchMetadata.Relation.Fields.INDICES.underscore().getValue()), contains(equalTo(expectedAction.fromIndex)));
    }
    if (expectedAction.fromType == null) {
      assertNull(from.get(CoordinateSearchMetadata.Relation.Fields.TYPES.underscore().getValue()));
    } else {
      assertNotNull(from.get(CoordinateSearchMetadata.Relation.Fields.TYPES.underscore().getValue()));
      assertThat((List<String>) to.get(CoordinateSearchMetadata.Relation.Fields.TYPES.underscore().getValue()), contains(equalTo(expectedAction.fromType)));
    }
    assertNotNull(from.get(CoordinateSearchMetadata.Relation.Fields.FIELD.underscore().getValue()));
    assertThat((String) from.get(CoordinateSearchMetadata.Relation.Fields.FIELD.underscore().getValue()), equalTo(expectedAction.fromField));

    key = CoordinateSearchMetadata.Action.Fields.IS_PRUNED.underscore().getValue();
    assertThat((Boolean) action.get(key), equalTo(expectedAction.isPruned));

    key = CoordinateSearchMetadata.Action.Fields.SIZE.underscore().getValue();
    assertThat((Integer) action.get(key), equalTo(expectedAction.size));

    key = CoordinateSearchMetadata.Action.Fields.CACHE_HIT.underscore().getValue();
    assertThat((Boolean) action.get(key), equalTo(expectedAction.cacheHit));

    key = CoordinateSearchMetadata.Action.Fields.SIZE_IN_BYTES.underscore().getValue();
    assertThat((Integer) action.get(key), greaterThan(expectedAction.sizeInBytes));

    key = CoordinateSearchMetadata.Action.Fields.TOOK.underscore().getValue();
    assertThat((Integer) action.get(key), greaterThan(expectedAction.took));

    key = CoordinateSearchMetadata.Action.Fields.TERMS_ENCODING.underscore().getValue();
    assertThat((String) action.get(key), equalTo(expectedAction.termsEncoding.name().toLowerCase(Locale.ROOT)));

    key = CoordinateSearchMetadata.Action.Fields.ORDERING.underscore().getValue();
    if (expectedAction.ordering == null) {
      assertNull(action.get(key));
    } else {
      assertThat((String) action.get(key), equalTo(expectedAction.ordering.name().toLowerCase(Locale.ROOT)));
    }

    key = CoordinateSearchMetadata.Action.Fields.MAX_TERMS_PER_SHARD.underscore().getValue();
    if (expectedAction.maxTermsPerShard == -1) {
      assertNull(action.get(key));
    } else {
      assertThat((Integer) action.get(key), equalTo(expectedAction.maxTermsPerShard));
    }
  }

  @Test
  public void testMaxTermsPerShard() throws IOException, RestException, ExecutionException, InterruptedException {
    ElasticsearchAssertions.assertAcked(prepareCreate("index1").addMapping("type", "id", "type=string", "foreign_key", "type=string"));
    ElasticsearchAssertions.assertAcked(prepareCreate("index2").addMapping("type", "id", "type=string", "tag", "type=string"));

    ensureGreen();

    indexRandom(true,
            client().prepareIndex("index1", "type", "1").setSource("id", "1", "foreign_key", new String[]{"1", "3"}),
            client().prepareIndex("index1", "type", "2").setSource("id", "2"),
            client().prepareIndex("index1", "type", "3").setSource("id", "3", "foreign_key", new String[]{"2"}),
            client().prepareIndex("index1", "type", "4").setSource("id", "4", "foreign_key", new String[]{"1", "4"}),

            client().prepareIndex("index2", "type", "1").setSource("id", "1", "tag", "aaa"),
            client().prepareIndex("index2", "type", "2").setSource("id", "2", "tag", "aaa"),
            client().prepareIndex("index2", "type", "3").setSource("id", "3", "tag", "bbb"),
            client().prepareIndex("index2", "type", "4").setSource("id", "4", "tag", "ccc"));

    // Check body search query with filter join
    String q = boolQuery().filter(
            filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                    boolQuery().filter(termQuery("tag", "aaa"))
            ).maxTermsPerShard(42)).toString();
    String body = "{ \"query\" : " + q + "}";

    HttpResponse response = httpClient().method("GET").path("/_coordinate_search").body(body).execute();
    assertThat(response.getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(response.getBody().getBytes("UTF-8")), false).v2();

    String key = CoordinateSearchMetadata.Fields.COORDINATE_SEARCH.underscore().getValue();
    assertTrue(map.containsKey(key));
    Map coordinateSearch = (Map) map.get(key);

    key = CoordinateSearchMetadata.Fields.ACTIONS.underscore().getValue();
    assertTrue(coordinateSearch.containsKey(key));
    List actions = (List) coordinateSearch.get(key);
    assertThat(actions.size(), equalTo(1));
    assertAction((Map) actions.get(0), new ExpectedAction() {{
      this.fromField = "foreign_key";
      this.toIndex = "index2";
      this.toType = "type";
      this.toField = "id";
      this.isPruned = false;
      this.size = 2;
      this.cacheHit = false;
      this.sizeInBytes = 0;
      this.maxTermsPerShard = 42;
    }});
  }

  @Test
  public void testTermsEncoding() throws IOException, RestException, ExecutionException, InterruptedException {
    ElasticsearchAssertions.assertAcked(prepareCreate("index1").addMapping("type", "id", "type=string", "foreign_key", "type=string"));
    ElasticsearchAssertions.assertAcked(prepareCreate("index2").addMapping("type", "id", "type=string", "tag", "type=string"));

    ensureGreen();

    indexRandom(true,
            client().prepareIndex("index1", "type", "1").setSource("id", "1", "foreign_key", new String[]{"1", "3"}),
            client().prepareIndex("index1", "type", "2").setSource("id", "2"),
            client().prepareIndex("index1", "type", "3").setSource("id", "3", "foreign_key", new String[]{"2"}),
            client().prepareIndex("index1", "type", "4").setSource("id", "4", "foreign_key", new String[]{"1", "4"}),

            client().prepareIndex("index2", "type", "1").setSource("id", "1", "tag", "aaa"),
            client().prepareIndex("index2", "type", "2").setSource("id", "2", "tag", "aaa"),
            client().prepareIndex("index2", "type", "3").setSource("id", "3", "tag", "bbb"),
            client().prepareIndex("index2", "type", "4").setSource("id", "4", "tag", "ccc"));

    // Check body search query with filter join
    String q = boolQuery().filter(
            filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                    boolQuery().filter(termQuery("tag", "aaa"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.INTEGER)).toString();
    String body = "{ \"query\" : " + q + "}";

    HttpResponse response = httpClient().method("GET").path("/_coordinate_search").body(body).execute();
    assertThat(response.getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(response.getBody().getBytes("UTF-8")), false).v2();

    String key = CoordinateSearchMetadata.Fields.COORDINATE_SEARCH.underscore().getValue();
    assertTrue(map.containsKey(key));
    Map coordinateSearch = (Map) map.get(key);

    key = CoordinateSearchMetadata.Fields.ACTIONS.underscore().getValue();
    assertTrue(coordinateSearch.containsKey(key));
    List actions = (List) coordinateSearch.get(key);
    assertThat(actions.size(), equalTo(1));
    assertAction((Map) actions.get(0), new ExpectedAction() {{
      this.fromField = "foreign_key";
      this.toIndex = "index2";
      this.toType = "type";
      this.toField = "id";
      this.isPruned = false;
      this.size = 2;
      this.cacheHit = false;
      this.sizeInBytes = 0;
      this.termsEncoding = TermsByQueryRequest.TermsEncoding.INTEGER;
    }});
  }

  @Test
  public void testOrdering() throws IOException, RestException, ExecutionException, InterruptedException {
    ElasticsearchAssertions.assertAcked(prepareCreate("index1").addMapping("type", "id", "type=string", "foreign_key", "type=string"));
    ElasticsearchAssertions.assertAcked(prepareCreate("index2").addMapping("type", "id", "type=string", "tag", "type=string"));

    ensureGreen();

    indexRandom(true,
            client().prepareIndex("index1", "type", "1").setSource("id", "1", "foreign_key", new String[]{"1", "3"}),
            client().prepareIndex("index1", "type", "2").setSource("id", "2"),
            client().prepareIndex("index1", "type", "3").setSource("id", "3", "foreign_key", new String[]{"2"}),
            client().prepareIndex("index1", "type", "4").setSource("id", "4", "foreign_key", new String[]{"1", "4"}),

            client().prepareIndex("index2", "type", "1").setSource("id", "1", "tag", "aaa"),
            client().prepareIndex("index2", "type", "2").setSource("id", "2", "tag", "aaa"),
            client().prepareIndex("index2", "type", "3").setSource("id", "3", "tag", "bbb"),
            client().prepareIndex("index2", "type", "4").setSource("id", "4", "tag", "ccc"));

    // Check body search query with filter join
    String q = boolQuery().filter(
            filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                    boolQuery().filter(termQuery("tag", "aaa"))
            ).orderBy(TermsByQueryRequest.Ordering.DOC_SCORE).maxTermsPerShard(10)).toString();
    String body = "{ \"query\" : " + q + "}";

    HttpResponse response = httpClient().method("GET").path("/_coordinate_search").body(body).execute();
    assertThat(response.getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(response.getBody().getBytes("UTF-8")), false).v2();

    String key = CoordinateSearchMetadata.Fields.COORDINATE_SEARCH.underscore().getValue();
    assertTrue(map.containsKey(key));
    Map coordinateSearch = (Map) map.get(key);

    key = CoordinateSearchMetadata.Fields.ACTIONS.underscore().getValue();
    assertTrue(coordinateSearch.containsKey(key));
    List actions = (List) coordinateSearch.get(key);
    assertThat(actions.size(), equalTo(1));
    assertAction((Map) actions.get(0), new ExpectedAction() {{
      this.fromField = "foreign_key";
      this.toIndex = "index2";
      this.toType = "type";
      this.toField = "id";
      this.isPruned = false;
      this.size = 2;
      this.cacheHit = false;
      this.sizeInBytes = 0;
      this.maxTermsPerShard = 10;
      this.ordering = TermsByQueryRequest.Ordering.DOC_SCORE;
    }});
  }

  @Test
  public void testCoordinateSearchMetadata() throws IOException, RestException, ExecutionException, InterruptedException {
    ElasticsearchAssertions.assertAcked(prepareCreate("index1").addMapping("type", "id", "type=string", "foreign_key", "type=string"));
    ElasticsearchAssertions.assertAcked(prepareCreate("index2").addMapping("type", "id", "type=string", "tag", "type=string"));

    ensureGreen();

    indexRandom(true,
            client().prepareIndex("index1", "type", "1").setSource("id", "1", "foreign_key", new String[]{"1", "3"}),
            client().prepareIndex("index1", "type", "2").setSource("id", "2"),
            client().prepareIndex("index1", "type", "3").setSource("id", "3", "foreign_key", new String[]{"2"}),
            client().prepareIndex("index1", "type", "4").setSource("id", "4", "foreign_key", new String[]{"1", "4"}),

            client().prepareIndex("index2", "type", "1").setSource("id", "1", "tag", "aaa"),
            client().prepareIndex("index2", "type", "2").setSource("id", "2", "tag", "aaa"),
            client().prepareIndex("index2", "type", "3").setSource("id", "3", "tag", "bbb"),
            client().prepareIndex("index2", "type", "4").setSource("id", "4", "tag", "ccc"));

    // Check body search query with filter join
    String q = boolQuery().filter(
                filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                  boolQuery().filter(termQuery("tag", "aaa"))
                ).termsEncoding(TermsByQueryRequest.TermsEncoding.LONG)).toString();
    String body = "{ \"query\" : " + q + "}";

    HttpResponse response = httpClient().method("GET").path("/_coordinate_search").body(body).execute();
    assertThat(response.getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(response.getBody().getBytes("UTF-8")), false).v2();

    String key = CoordinateSearchMetadata.Fields.COORDINATE_SEARCH.underscore().getValue();
    assertTrue(map.containsKey(key));
    Map coordinateSearch = (Map) map.get(key);

    key = CoordinateSearchMetadata.Fields.ACTIONS.underscore().getValue();
    assertTrue(coordinateSearch.containsKey(key));
    List actions = (List) coordinateSearch.get(key);
    assertThat(actions.size(), equalTo(1));
    assertAction((Map) actions.get(0), new ExpectedAction() {{
      this.fromField = "foreign_key";
      this.toIndex = "index2";
      this.toType = "type";
      this.toField = "id";
      this.isPruned = false;
      this.size = 2;
      this.cacheHit = false;
      this.sizeInBytes = 0;
    }});
  }

  @Test
  public void testCoordinateSearchMetadataWithNestedJoins() throws IOException, RestException, ExecutionException, InterruptedException {
    assertAcked(prepareCreate("index1").addMapping("type", "id", "type=integer", "foreign_key", "type=integer"));
    assertAcked(prepareCreate("index2").addMapping("type", "id", "type=integer", "foreign_key", "type=integer", "tag", "type=string"));
    assertAcked(prepareCreate("index3").addMapping("type", "id", "type=integer", "tag", "type=string"));

    ensureGreen();

    indexRandom(true,
            client().prepareIndex("index1", "type", "1").setSource("id", "1", "foreign_key", new String[]{"1", "3"}),
            client().prepareIndex("index1", "type", "2").setSource("id", "2"),
            client().prepareIndex("index1", "type", "3").setSource("id", "3", "foreign_key", new String[]{"2"}),
            client().prepareIndex("index1", "type", "4").setSource("id", "4", "foreign_key", new String[]{"1", "4"}),

            client().prepareIndex("index2", "type", "1").setSource("id", "1", "tag", "aaa"),
            client().prepareIndex("index2", "type", "2").setSource("id", "2", "tag", "aaa"),
            client().prepareIndex("index2", "type", "3").setSource("id", "3", "foreign_key", new String[]{"2"}, "tag", "bbb"),
            client().prepareIndex("index2", "type", "4").setSource("id", "4", "tag", "ccc"),

            client().prepareIndex("index3", "type", "1").setSource("id", "1", "tag", "aaa"),
            client().prepareIndex("index3", "type", "2").setSource("id", "2", "tag", "aaa"),
            client().prepareIndex("index3", "type", "3").setSource("id", "3", "tag", "bbb"),
            client().prepareIndex("index3", "type", "4").setSource("id", "4", "tag", "ccc"));

    // Check body search query with filter join
    String q = boolQuery().filter(
                    filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                      boolQuery().filter(
                        filterJoin("foreign_key").indices("index3").types("type").path("id").query(
                          boolQuery().filter(termQuery("tag", "aaa"))
                        ).termsEncoding(TermsByQueryRequest.TermsEncoding.LONG)
                      )
                    ).termsEncoding(TermsByQueryRequest.TermsEncoding.LONG)
                  )
                  .filter(
                    termQuery("id", "1")
                  ).toString();
    String body = "{ \"query\" : " + q + "}";

    HttpResponse response = httpClient().method("GET").path("/_coordinate_search").body(body).execute();
    assertThat(response.getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(response.getBody().getBytes("UTF-8")), false).v2();

    String key = CoordinateSearchMetadata.Fields.COORDINATE_SEARCH.underscore().getValue();
    assertTrue(map.containsKey(key));
    Map coordinateSearch = (Map) map.get(key);

    key = CoordinateSearchMetadata.Fields.ACTIONS.underscore().getValue();
    assertTrue(coordinateSearch.containsKey(key));
    List actions = (List) coordinateSearch.get(key);
    assertThat(actions.size(), equalTo(2));
    assertAction((Map) actions.get(0), new ExpectedAction() {{
      this.fromIndex = "index2";
      this.fromType = "type";
      this.fromField = "foreign_key";
      this.toIndex = "index3";
      this.toType = "type";
      this.toField = "id";
      this.isPruned = false;
      this.size = 2;
      this.cacheHit = false;
      this.sizeInBytes = 0;
    }});
    assertAction((Map) actions.get(1), new ExpectedAction() {{
      this.fromField = "foreign_key";
      this.toIndex = "index2";
      this.toType = "type";
      this.toField = "id";
      this.isPruned = false;
      this.size = 1;
      this.cacheHit = false;
      this.sizeInBytes = 0;
    }});
  }

  @Test
  public void testPrunedCachedAction() throws IOException, RestException, ExecutionException, InterruptedException {
    assertAcked(prepareCreate("index1").addMapping("type", "id", "type=string", "foreign_key", "type=string"));

    // Enforce one single shard for index2
    Map<String, Object> indexSettings = new HashMap<>();
    indexSettings.put("number_of_shards", 1);
    assertAcked(prepareCreate("index2").setSettings(indexSettings).addMapping("type", "id", "type=string", "tag", "type=string"));

    ensureGreen();

    indexRandom(true,
            client().prepareIndex("index1", "type", "1").setSource("id", "1", "foreign_key", new String[]{"1", "3"}),
            client().prepareIndex("index1", "type", "2").setSource("id", "2"),
            client().prepareIndex("index1", "type", "3").setSource("id", "3", "foreign_key", new String[]{"2"}),
            client().prepareIndex("index1", "type", "4").setSource("id", "4", "foreign_key", new String[]{"4"}),

            client().prepareIndex("index2", "type", "1").setSource("id", "1", "tag", "aaa"),
            client().prepareIndex("index2", "type", "2").setSource("id", "2", "tag", "aaa aaa"),
            client().prepareIndex("index2", "type", "3").setSource("id", "3", "tag", "aaa"),
            client().prepareIndex("index2", "type", "4").setSource("id", "4", "tag", "aaa aaa") );

    // Order by doc score, and take only the first element of the index2 shard
    // It should therefore pick only odd document ids (with tag:aaa) due to scoring.
    String q = boolQuery().filter(
            filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                    boolQuery().filter(termQuery("tag", "aaa"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.LONG)
             .orderBy(TermsByQueryRequest.Ordering.DOC_SCORE).maxTermsPerShard(1)).toString();
    String body = "{ \"query\" : " + q + "}";

    // Execute a first time to add the action to the cache
    HttpResponse response = httpClient().method("GET").path("/_coordinate_search").body(body).execute();
    assertThat(response.getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(response.getBody().getBytes("UTF-8")), false).v2();
    // Only one document contains a odd document id as foreign key.
    assertThat((Integer) ((Map) map.get("hits")).get("total"), equalTo(1));

    // Execute a second time to hit the cache
    response = httpClient().method("GET").path("/_coordinate_search").body(body).execute();
    assertThat(response.getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    map = XContentHelper.convertToMap(new BytesArray(response.getBody().getBytes("UTF-8")), false).v2();
    // Only one document contains a odd document id as foreign key.
    assertThat((Integer) ((Map) map.get("hits")).get("total"), equalTo(1));

    String key = CoordinateSearchMetadata.Fields.COORDINATE_SEARCH.underscore().getValue();
    Map coordinateSearch = (Map) map.get(key);

    key = CoordinateSearchMetadata.Fields.ACTIONS.underscore().getValue();
    assertTrue(coordinateSearch.containsKey(key));
    List actions = (List) coordinateSearch.get(key);
    assertThat(actions.size(), equalTo(1));
    Map action = (Map) actions.get(0);

    key = CoordinateSearchMetadata.Action.Fields.RELATIONS.underscore().getValue();
    assertTrue(action.containsKey(key));
    Map relations = (Map) action.get(key);

    key = CoordinateSearchMetadata.Action.Fields.FROM.underscore().getValue();
    assertTrue(relations.containsKey(key));
    Map to = (Map) relations.get(key);
    assertNotNull(to.get(CoordinateSearchMetadata.Relation.Fields.INDICES.underscore().getValue()));
    assertThat((List<String>) to.get(CoordinateSearchMetadata.Relation.Fields.INDICES.underscore().getValue()), contains(equalTo("index2")));
    assertNotNull(to.get(CoordinateSearchMetadata.Relation.Fields.TYPES.underscore().getValue()));
    assertThat((List<String>) to.get(CoordinateSearchMetadata.Relation.Fields.TYPES.underscore().getValue()), contains(equalTo("type")));
    assertNotNull(to.get(CoordinateSearchMetadata.Relation.Fields.FIELD.underscore().getValue()));
    assertThat((String) to.get(CoordinateSearchMetadata.Relation.Fields.FIELD.underscore().getValue()), equalTo("id"));

    key = CoordinateSearchMetadata.Action.Fields.TO.underscore().getValue();
    assertTrue(relations.containsKey(key));
    Map from = (Map) relations.get(key);
    assertNull(from.get(CoordinateSearchMetadata.Relation.Fields.INDICES.underscore().getValue()));
    assertNull(from.get(CoordinateSearchMetadata.Relation.Fields.TYPES.underscore().getValue()));
    assertNotNull(from.get(CoordinateSearchMetadata.Relation.Fields.FIELD.underscore().getValue()));
    assertThat((String) from.get(CoordinateSearchMetadata.Relation.Fields.FIELD.underscore().getValue()), equalTo("foreign_key"));

    key = CoordinateSearchMetadata.Action.Fields.IS_PRUNED.underscore().getValue();
    assertThat((Boolean) action.get(key), equalTo(true));

    key = CoordinateSearchMetadata.Action.Fields.SIZE.underscore().getValue();
    assertThat((Integer) action.get(key), equalTo(1));

    key = CoordinateSearchMetadata.Action.Fields.CACHE_HIT.underscore().getValue();
    assertThat((Boolean) action.get(key), equalTo(true));
  }

  @Test
  public void testCoordinateMultiSearchMetadata() throws IOException, RestException, ExecutionException, InterruptedException {
    assertAcked(prepareCreate("index1").addMapping("type", "id", "type=string", "foreign_key", "type=string"));
    assertAcked(prepareCreate("index2").addMapping("type", "id", "type=string", "tag", "type=string"));

    ensureGreen();

    indexRandom(true,
            client().prepareIndex("index1", "type", "1").setSource("id", "1", "foreign_key", new String[]{"1", "3"}),
            client().prepareIndex("index1", "type", "2").setSource("id", "2"),
            client().prepareIndex("index1", "type", "3").setSource("id", "3", "foreign_key", new String[]{"2"}),
            client().prepareIndex("index1", "type", "4").setSource("id", "4", "foreign_key", new String[]{"1", "4"}),

            client().prepareIndex("index2", "type", "1").setSource("id", "1", "tag", "aaa"),
            client().prepareIndex("index2", "type", "2").setSource("id", "2", "tag", "aaa"),
            client().prepareIndex("index2", "type", "3").setSource("id", "3", "tag", "bbb"),
            client().prepareIndex("index2", "type", "4").setSource("id", "4", "tag", "ccc"));

    String q = boolQuery().filter(
            filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                    boolQuery().filter(termQuery("tag", "aaa"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.LONG)).toString().replace('\n', ' ');
    String body = "{\"index\" : \"index1\"}\n";
    body += "{ \"query\" : " + q + "}\n";
    body += "{\"index\" : \"index1\"}\n";
    body += "{ \"query\" : " + q + "}\n";

    HttpResponse response = httpClient().method("GET").path("/_coordinate_msearch").body(body).execute();
    assertThat(response.getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(response.getBody().getBytes("UTF-8")), false).v2();
    ArrayList responses = (ArrayList) map.get("responses");
    assertThat(responses.size(), equalTo(2));
    assertThat((Integer) ((Map) ((Map) responses.get(0)).get("hits")).get("total"), equalTo(3));
    assertThat((Integer) ((Map) ((Map) responses.get(1)).get("hits")).get("total"), equalTo(3));

    // First query
    String key = CoordinateSearchMetadata.Fields.COORDINATE_SEARCH.underscore().getValue();
    Map coordinateSearch = (Map) ((Map) responses.get(0)).get(key);

    key = CoordinateSearchMetadata.Fields.ACTIONS.underscore().getValue();
    assertTrue(coordinateSearch.containsKey(key));
    List actions = (List) coordinateSearch.get(key);
    assertThat(actions.size(), equalTo(1));
    Map action = (Map) actions.get(0);

    key = CoordinateSearchMetadata.Action.Fields.IS_PRUNED.underscore().getValue();
    assertThat((Boolean) action.get(key), equalTo(false));

    key = CoordinateSearchMetadata.Action.Fields.SIZE.underscore().getValue();
    assertThat((Integer) action.get(key), equalTo(2));

    key = CoordinateSearchMetadata.Action.Fields.CACHE_HIT.underscore().getValue();
    assertThat((Boolean) action.get(key), equalTo(false));

    // Second query
    key = CoordinateSearchMetadata.Fields.COORDINATE_SEARCH.underscore().getValue();
    coordinateSearch = (Map) ((Map) responses.get(1)).get(key);

    key = CoordinateSearchMetadata.Fields.ACTIONS.underscore().getValue();
    assertTrue(coordinateSearch.containsKey(key));
    actions = (List) coordinateSearch.get(key);
    assertThat(actions.size(), equalTo(1));
    action = (Map) actions.get(0);

    key = CoordinateSearchMetadata.Action.Fields.IS_PRUNED.underscore().getValue();
    assertThat((Boolean) action.get(key), equalTo(false));

    key = CoordinateSearchMetadata.Action.Fields.SIZE.underscore().getValue();
    assertThat((Integer) action.get(key), equalTo(2));

    key = CoordinateSearchMetadata.Action.Fields.CACHE_HIT.underscore().getValue();
    assertThat((Boolean) action.get(key), equalTo(true));
  }

}
