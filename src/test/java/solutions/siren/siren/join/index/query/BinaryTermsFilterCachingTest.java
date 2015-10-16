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
package solutions.siren.siren.join.index.query;

import solutions.siren.siren.join.FilterJoinTestCase;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.lucene.search.CachedFilter;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.TestSearchContext;
import org.junit.Test;

import static solutions.siren.siren.join.index.query.FilterBuilders.binaryTermsFilter;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.equalTo;

@ElasticsearchIntegrationTest.ClusterScope(scope=ElasticsearchIntegrationTest.Scope.SUITE, numDataNodes=1)
public class BinaryTermsFilterCachingTest extends FilterJoinTestCase {

  @Override
  protected Settings nodeSettings(int nodeOrdinal) {
    return ImmutableSettings.settingsBuilder()
      .put("index.cache.filter.type", "weighted")   // force filter cache
      .put(super.nodeSettings(nodeOrdinal)).build();
  }

  @Test
  public void testFilterJoinCache() throws Exception {
    assertAcked(prepareCreate("index1").addMapping("type", "id", "type=integer", "foreign_key", "type=integer"));

    ensureGreen();

    indexRandom(true,
      client().prepareIndex("index1", "type", "1").setSource("id", "1", "foreign_key", new String[]{"1", "3"}),
      client().prepareIndex("index1", "type", "2").setSource("id", "2"),
      client().prepareIndex("index1", "type", "3").setSource("id", "3", "foreign_key", new String[]{"2"}),
      client().prepareIndex("index1", "type", "4").setSource("id", "4", "foreign_key", new String[]{"1", "4"}));

    ClusterState clusterState = internalCluster().clusterService().state();
    ShardRouting shardRouting = clusterState.routingTable().index("index1").shard(0).getShards().get(0);
    String nodeName = clusterState.getNodes().get(shardRouting.currentNodeId()).getName();
    IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
    IndexService indexService = indicesService.indexService("index1");

    IndexQueryParserService queryParser = indexService.injector().getInstance(IndexQueryParserService.class);
    SearchContext.setCurrent(new TestSearchContext());

    Query parsedQuery = queryParser.parse(
      filteredQuery(matchAllQuery(),
                    FilterBuilders.binaryTermsFilter("id", new long[]{1, 2, 4, 8, 10, 7, 6, 11, 5}, "cache_key")
      )
    ).query();

    assertThat(parsedQuery, instanceOf(ConstantScoreQuery.class));
    // check that the filter is cached
    assertThat(((ConstantScoreQuery) parsedQuery).getFilter(), instanceOf(CachedFilter.class));
    // check that the byte array is not serialised
    assertThat(parsedQuery.toString(), equalTo("ConstantScore(cache(LateDecodingBinaryTermsFilter:id:[size=76]))"));
  }

}
