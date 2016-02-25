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
package solutions.siren.join.action.coordinate;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import solutions.siren.join.SirenJoinTestCase;
import solutions.siren.join.action.terms.TermsByQueryRequest;
import solutions.siren.join.index.query.QueryBuilders;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.*;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes=1, randomDynamicTemplates=false)
public class CircuitBreakerTest extends SirenJoinTestCase {

  @Before
  public void setup() throws ExecutionException, InterruptedException {
    assertAcked(prepareCreate("index1").addMapping("type", "id", "type=integer", "foreign_key", "type=integer"));
    assertAcked(prepareCreate("index2").addMapping("type", "id", "type=integer", "tag", "type=string"));

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
  }

  /** Reset all breaker settings back to their defaults */
  @After
  public void teardown() {
    logger.info("--> resetting breaker settings");
    Settings resetSettings = settingsBuilder()
            .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING,
                    HierarchyCircuitBreakerService.DEFAULT_FIELDDATA_BREAKER_LIMIT)
            .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING,
                    HierarchyCircuitBreakerService.DEFAULT_FIELDDATA_OVERHEAD_CONSTANT)
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING,
                    HierarchyCircuitBreakerService.DEFAULT_REQUEST_BREAKER_LIMIT)
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING, 1.0)
            .build();
    assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(resetSettings));
  }

  @Test
  public void testCircuitBreakerOnShard() throws Exception {
    // Update circuit breaker settings
    Settings settings = settingsBuilder()
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING, "80b")
            .build();
    assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));

    SearchRequestBuilder searchRequest = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
            QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                    boolQuery().filter(termQuery("tag", "aaa"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.LONG)
    );
    assertFailures(searchRequest, RestStatus.INTERNAL_SERVER_ERROR,
            containsString("Data too large, data for [<terms_set>] would be larger than limit of [80/80b]"));

    NodesStatsResponse stats = client().admin().cluster().prepareNodesStats().setBreaker(true).get();
    int breaks = 0;
    for (NodeStats stat : stats.getNodes()) {
      CircuitBreakerStats breakerStats = stat.getBreaker().getStats(CircuitBreaker.REQUEST);
      breaks += breakerStats.getTrippedCount();
    }
    assertThat(breaks, greaterThanOrEqualTo(1));
  }

  @Test
  public void testCircuitBreakerOnCoordinator() throws Exception {
    // Update circuit breaker settings
    Settings settings = settingsBuilder()
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING, "140b")
            .build();
    assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));

    SearchRequestBuilder searchRequest = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
            QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                    boolQuery().filter(termQuery("tag", "aaa"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.LONG)
    );
    assertFailures(searchRequest, RestStatus.INTERNAL_SERVER_ERROR,
            containsString("Data too large, data for [<terms_set>] would be larger than limit of [140/140b]"));

    NodesStatsResponse stats = client().admin().cluster().prepareNodesStats().setBreaker(true).get();
    int breaks = 0;
    for (NodeStats stat : stats.getNodes()) {
      CircuitBreakerStats breakerStats = stat.getBreaker().getStats(CircuitBreaker.REQUEST);
      breaks += breakerStats.getTrippedCount();
    }
    assertThat(breaks, greaterThanOrEqualTo(1));
  }

  public static void assertFailures(SearchRequestBuilder searchRequestBuilder, RestStatus restStatus, Matcher<String> reasonMatcher) {
    //when the number for shards is randomized and we expect failures
    //we can either run into partial or total failures depending on the current number of shards
    try {
      SearchResponse searchResponse = searchRequestBuilder.get();
      assertThat("Expected shard failures, got none", searchResponse.getShardFailures().length, greaterThan(0));
      for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
        assertThat(shardSearchFailure.status(), equalTo(restStatus));
        assertThat(shardSearchFailure.reason(), reasonMatcher);
      }
      assertVersionSerializable(searchResponse);
    } catch (ElasticsearchException e) {
      assertThat(e.status(), equalTo(restStatus));
      assertThat(e.toString(), reasonMatcher);
    } catch (Exception e) {
      fail("ElasticsearchException expected but got " + e.getClass());
    }
  }

}
