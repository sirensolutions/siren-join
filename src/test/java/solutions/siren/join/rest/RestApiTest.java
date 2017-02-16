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
package solutions.siren.join.rest;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;

import org.elasticsearch.client.Response;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestStatus;

import org.junit.Test;

import solutions.siren.join.SirenJoinTestCase;
import solutions.siren.join.action.terms.TermsByQueryRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.index.query.QueryBuilders.*;
import static solutions.siren.join.index.query.QueryBuilders.filterJoin;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope= ESIntegTestCase.Scope.SUITE, numDataNodes=1, numClientNodes = 0, supportsDedicatedMasters = false)
public class RestApiTest extends SirenJoinTestCase {

  @Override
  protected Settings nodeSettings(int nodeOrdinal) {
    return Settings.builder()
            .put("force.http.enabled", true) // enable http for these tests
            .put(super.nodeSettings(nodeOrdinal)).build();
  }

  @Test
  public void testCoordinateSearchApi() throws IOException, ExecutionException, InterruptedException {
    assertAcked(prepareCreate("index1").addMapping("type", "id", "type=keyword", "foreign_key", "type=keyword"));
    assertAcked(prepareCreate("index2").addMapping("type", "id", "type=keyword", "tag", "type=string"));

    ensureGreen();

    indexRandom(true,
            client().prepareIndex("index1", "type", "1").setSource("id", "1", "foreign_key", new String[]{"1", "3"}),
            client().prepareIndex("index1", "type", "2").setSource("id", "2"),
            client().prepareIndex("index1", "type", "3").setSource("id", "3", "foreign_key", new String[]{"2"}),
            client().prepareIndex("index1", "type", "4").setSource("id", "4", "foreign_key", new String[]{"1", "4"}),

            client().prepareIndex("index2", "type", "1").setSource("id", "1", "tag", "aaa"),
            client().prepareIndex("index2", "type", "2").setSource("id", "2", "tag", "aaa"),
            client().prepareIndex("index2", "type", "3").setSource("id", "3", "tag", "bbb"),
            client().prepareIndex("index2", "type", "4").setSource("id", "4", "tag", "ccc") );

    // Check body search query with filter join
    String q = boolQuery().filter(
            filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                    boolQuery().filter(termQuery("tag", "aaa"))
            )).toString();
    HttpEntity body = new NStringEntity("{ \"query\" : " + q + "}", ContentType.APPLICATION_JSON);

    Response response = getRestClient().performRequest("GET", "/_coordinate_search", Collections.emptyMap(), body);
    assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(IOUtils.toByteArray(response.getEntity().getContent())), false).v2();
    assertThat(((Map) map.get("hits")).get("total"), equalTo(3));

    // Check uri search
    response = getRestClient().performRequest("GET","/_coordinate_search", ImmutableMap.of("q", "tag:aaa"));
    assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    map = XContentHelper.convertToMap(new BytesArray(IOUtils.toByteArray(response.getEntity().getContent())), false).v2();
    assertThat(((Map) map.get("hits")).get("total"), equalTo(2));
  }

  @Test
  public void testCoordinateSearchAfterUpdates() throws IOException, ExecutionException, InterruptedException {
    assertAcked(prepareCreate("index1").addMapping("type", "id", "type=keyword", "foreign_key", "type=keyword"));
    assertAcked(prepareCreate("index2").addMapping("type", "id", "type=keyword", "tag", "type=keyword"));

    ensureGreen();

    indexRandom(true,
            client().prepareIndex("index1", "type", "1").setSource("id", "1", "foreign_key", new String[]{"1", "3"}),
            client().prepareIndex("index1", "type", "2").setSource("id", "2"),
            client().prepareIndex("index1", "type", "3").setSource("id", "3", "foreign_key", new String[]{"2"}),
            client().prepareIndex("index1", "type", "4").setSource("id", "4", "foreign_key", new String[]{"1", "4"}),

            client().prepareIndex("index2", "type", "1").setSource("id", "1", "tag", "aaa"),
            client().prepareIndex("index2", "type", "2").setSource("id", "2", "tag", "aaa"),
            client().prepareIndex("index2", "type", "3").setSource("id", "3", "tag", "bbb"),
            client().prepareIndex("index2", "type", "4").setSource("id", "4", "tag", "ccc") );

    // Check body search query with filter join
    String q = boolQuery().filter(
            filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                    boolQuery().filter(termQuery("tag", "aaa"))
            )).toString();
    HttpEntity body = new NStringEntity("{ \"query\" : " + q + "}", ContentType.APPLICATION_JSON);

    Response response = getRestClient().performRequest("GET", "/index1/_coordinate_search", Collections.emptyMap(), body);
    assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(IOUtils.toByteArray(response.getEntity().getContent())), false).v2();
    assertThat((Integer) ((Map) map.get("hits")).get("total"), equalTo(3));

    indexRandom(true,
            client().prepareIndex("index1", "type", "5").setSource("id", "5", "foreign_key", new String[]{"5"}),

            client().prepareIndex("index2", "type", "5").setSource("id", "5", "tag", "aaa"));

    response = getRestClient().performRequest("GET", "/index1/_coordinate_search", Collections.emptyMap(), body);
    assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    map = XContentHelper.convertToMap(new BytesArray(IOUtils.toByteArray(response.getEntity().getContent())), false).v2();
    assertThat((Integer) ((Map) map.get("hits")).get("total"), equalTo(4));
  }

  /**
   * Ensures that that the cache id is taking into consideration the index changes. See #73.
   */
  @Test
  public void testCiapiOutput() throws IOException, ExecutionException, InterruptedException {
    assertAcked(prepareCreate("email").addMapping("email", "id", "type=string", "content_md5", "type=keyword"));
    assertAcked(prepareCreate("ciapioutput").addMapping("ciapioutput", "id", "type=string", "content_md5", "type=keyword"));

    ensureGreen();

    indexRandom(true,
            client().prepareIndex("email", "email", "1").setSource("id", "1", "content_md5", "595b6b645b7d9548c8c462e889267477"),
            client().prepareIndex("email", "email", "2").setSource("id", "2", "content_md5", "6aa71033aef3f529926fe3840c6c0a7e"),

            client().prepareIndex("ciapioutput", "ciapioutput", "1").setSource("id", "1", "content_md5", "595b6b645b7d9548c8c462e889267477") );

    // Retrieves all the md5 from ciapioutput that does not appear in email
    String q = boolQuery().mustNot(
            filterJoin("content_md5").indices("ciapioutput").types("ciapioutput").path("content_md5").query(
                    matchAllQuery()
            )
    ).toString();
    HttpEntity body = new NStringEntity("{ \"query\" : " + q + "}", ContentType.APPLICATION_JSON);

    Response response = getRestClient().performRequest("GET", "/email/_coordinate_search", Collections.emptyMap(), body);
    assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(IOUtils.toByteArray(response.getEntity().getContent())), false).v2();
    assertThat((Integer) ((Map) map.get("hits")).get("total"), equalTo(1));

    // Add missing md5 in ciapioutput
    indexRandom(true,
            client().prepareIndex("ciapioutput", "ciapioutput", "2").setSource("id", "2", "content_md5", "6aa71033aef3f529926fe3840c6c0a7e") );

    // It should now return an empty result set
    response = getRestClient().performRequest("GET", "/email/_coordinate_search", Collections.emptyMap(), body);
    assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    map = XContentHelper.convertToMap(new BytesArray(IOUtils.toByteArray(response.getEntity().getContent())), false).v2();
    assertThat((Integer) ((Map) map.get("hits")).get("total"), equalTo(0));
  }


  @Test
  public void testCoordinateMultiSearchApi() throws IOException, ExecutionException, InterruptedException {
    assertAcked(prepareCreate("index1").addMapping("type", "id", "type=keyword", "foreign_key", "type=keyword"));
    assertAcked(prepareCreate("index2").addMapping("type", "id", "type=keyword", "tag", "type=string"));

    ensureGreen();

    indexRandom(true,
            client().prepareIndex("index1", "type", "1").setSource("id", "1", "foreign_key", new String[]{"1", "3"}),
            client().prepareIndex("index1", "type", "2").setSource("id", "2"),
            client().prepareIndex("index1", "type", "3").setSource("id", "3", "foreign_key", new String[]{"2"}),
            client().prepareIndex("index1", "type", "4").setSource("id", "4", "foreign_key", new String[]{"1", "4"}),

            client().prepareIndex("index2", "type", "1").setSource("id", "1", "tag", "aaa"),
            client().prepareIndex("index2", "type", "2").setSource("id", "2", "tag", "aaa"),
            client().prepareIndex("index2", "type", "3").setSource("id", "3", "tag", "bbb"),
            client().prepareIndex("index2", "type", "4").setSource("id", "4", "tag", "ccc") );

    String q = boolQuery().filter(
            filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                    boolQuery().filter(termQuery("tag", "aaa"))
            )).toString().replace('\n', ' ');
    String body = "{\"index\" : \"index1\"}\n";
    body += "{ \"query\" : " + q + "}\n";
    body += "{\"index\" : \"index1\"}\n";
    body += "{ \"query\" : " + q + "}\n";

    HttpEntity entity = new NStringEntity(body, ContentType.APPLICATION_JSON);

    Response response = getRestClient().performRequest("GET", "/_coordinate_msearch", Collections.emptyMap(), entity);
    assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(IOUtils.toByteArray(response.getEntity().getContent())), false).v2();
    ArrayList responses = (ArrayList) map.get("responses");
    assertThat(responses.size(), equalTo(2));
    assertThat((Integer) ((Map) ((Map) responses.get(0)).get("hits")).get("total"), equalTo(3));
    assertThat((Integer) ((Map) ((Map) responses.get(1)).get("hits")).get("total"), equalTo(3));
  }

  /**
   * Check that a null query object in a filter join does not cause unexpected issues - see #122
   */
  @Test
  public void testNullQueryInFilterJoin() throws IOException, ExecutionException, InterruptedException {
    assertAcked(prepareCreate("index1").addMapping("type", "id", "type=keyword", "foreign_key", "type=keyword"));
    assertAcked(prepareCreate("index2").addMapping("type", "id", "type=keyword", "tag", "type=string"));

    ensureGreen();

    indexRandom(true,
            client().prepareIndex("index1", "type", "1").setSource("id", "1", "foreign_key", new String[]{"1", "3"}),
            client().prepareIndex("index1", "type", "2").setSource("id", "2"),
            client().prepareIndex("index1", "type", "3").setSource("id", "3", "foreign_key", new String[]{"2"}),
            client().prepareIndex("index1", "type", "4").setSource("id", "4", "foreign_key", new String[]{"1", "4"}),

            client().prepareIndex("index2", "type", "1").setSource("id", "1", "tag", "aaa"),
            client().prepareIndex("index2", "type", "2").setSource("id", "2", "tag", "aaa"),
            client().prepareIndex("index2", "type", "3").setSource("id", "3", "tag", "bbb"),
            client().prepareIndex("index2", "type", "4").setSource("id", "4", "tag", "ccc") );

    // Check behavior when filterjoin's query is null
    HttpEntity body = new NStringEntity("{\"query\":{\"bool\":{\"filter\":[{\"filterjoin\":{\"foreign_key\":{\"index\":\"index2\",\"type\":\"type\",\"path\":\"id\",\"query\":null}}}]}}}", ContentType.APPLICATION_JSON);

    Response response = getRestClient().performRequest("GET", "/_coordinate_search", Collections.emptyMap(), body);
    assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(IOUtils.toByteArray(response.getEntity().getContent())), false).v2();
    assertThat((Integer) ((Map) map.get("hits")).get("total"), equalTo(3));
  }

  @Test
  public void testOrderByApi() throws IOException, ExecutionException, InterruptedException {
    assertAcked(prepareCreate("index1").addMapping("type", "id", "type=keyword", "foreign_key", "type=keyword"));

    // Enforce one single shard for index2
    Map<String, Object> indexSettings = new HashMap<>();
    indexSettings.put("number_of_shards", 1);
    assertAcked(prepareCreate("index2").setSettings(indexSettings).addMapping("type", "id", "type=keyword", "tag", "type=string"));

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
            ).orderBy(TermsByQueryRequest.Ordering.DOC_SCORE).maxTermsPerShard(1)).toString();
    HttpEntity body = new NStringEntity("{ \"query\" : " + q + "}", ContentType.APPLICATION_JSON);

    Response response = getRestClient().performRequest("GET", "/_coordinate_search", Collections.emptyMap(), body);
    assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(IOUtils.toByteArray(response.getEntity().getContent())), false).v2();
    // Only one document contains a odd document id as foreign key.
    assertThat((Integer) ((Map) map.get("hits")).get("total"), equalTo(1));
  }

}