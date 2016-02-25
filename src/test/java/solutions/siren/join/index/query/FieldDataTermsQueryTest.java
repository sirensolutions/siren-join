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
package solutions.siren.join.index.query;

import com.google.common.hash.Hashing;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.index.cache.IndexCacheModule;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.test.ESIntegTestCase;
import solutions.siren.join.SirenJoinTestCase;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import solutions.siren.join.action.terms.collector.LongBloomFilter;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static solutions.siren.join.index.query.QueryBuilders.fieldDataTermsQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope= ESIntegTestCase.Scope.SUITE, numDataNodes=1)
public class FieldDataTermsQueryTest extends SirenJoinTestCase {

  private static final Integer CACHE_KEY = 123;

  @Override
  protected int maximumNumberOfShards() {
    return 1;
  }

  @Override
  protected int maximumNumberOfReplicas() {
    return 0;
  }

  @Override
  public Settings indexSettings() {
    Settings.Builder builder = Settings.builder();
    builder.put(super.indexSettings());
    builder.put(IndexCacheModule.QUERY_CACHE_TYPE, IndexCacheModule.INDEX_QUERY_CACHE); // force query cache
    builder.put(IndexCacheModule.QUERY_CACHE_EVERYTHING, true); // force caching even small queries
    return builder.build();
  }

  @Test
  public void testSimpleFilter() throws Exception {
    assertAcked(prepareCreate("index1").addMapping("type", "id", "type=integer"));
    ensureGreen();

    indexRandom(true,
      client().prepareIndex("index1", "type", "1").setSource("id", "1"),
      client().prepareIndex("index1", "type", "3").setSource("id", "3"),
      client().prepareIndex("index1", "type", "7").setSource("id", "7"));

    SearchResponse searchResponse = client().prepareSearch("index1").setQuery(
      boolQuery().filter(fieldDataTermsQuery("id", new long[] { 1, 2, 4, 8, 10, 7, 6, 11, 5 }, CACHE_KEY))
    ).get();
    assertHitCount(searchResponse, 2L);
  }

  @Test
  public void testStringFilter() throws Exception {
    assertAcked(prepareCreate("index1").addMapping("type", "id", "type=string"));
    ensureGreen();

    indexRandom(true,
      client().prepareIndex("index1", "type", "1").setSource("id", "1"),
      client().prepareIndex("index1", "type", "3").setSource("id", "3"),
      client().prepareIndex("index1", "type", "7").setSource("id", "7"));

    long[] ids = new long[] { 1, 2, 4, 8, 10, 7, 6, 11, 5 };
    long[] hashIds = new long[ids.length];
    for (int i = 0; i < ids.length; i++) {
      BytesRef bytesRef = new BytesRef(Long.toString(ids[i]));
      hashIds[i] = LongBloomFilter.hash3_x64_128(bytesRef.bytes, bytesRef.offset, bytesRef.length, 0);
    }

    SearchResponse searchResponse = client().prepareSearch("index1").setQuery(
      boolQuery().filter(fieldDataTermsQuery("id", hashIds, CACHE_KEY))
    ).get();
    assertHitCount(searchResponse, 2L);
  }

  @Test
  public void testCaching() throws Exception {
    assertAcked(prepareCreate("index1").addMapping("type", "id", "type=integer"));
    ensureGreen();

    indexRandom(true,
      client().prepareIndex("index1", "type", "1").setSource("id", "1"),
      client().prepareIndex("index1", "type", "3").setSource("id", "3"),
      client().prepareIndex("index1", "type", "7").setSource("id", "7"));
    forceMerge(); // ensure that we have only one segment - needed for cache stats

    QueryCacheStats queryCacheStats = this.getQueryCacheStats("index1");
    assertThat(queryCacheStats.getCacheSize(), is(equalTo(0L)));
    assertThat(queryCacheStats.getHitCount(), is(equalTo(0L)));

    SearchResponse searchResponse = client().prepareSearch("index1").setQuery(
      boolQuery().filter(fieldDataTermsQuery("id", new long[] { 1, 2, 4, 8, 10, 7, 6, 11, 5 }, CACHE_KEY))
    ).get();
    assertHitCount(searchResponse, 2L);

    queryCacheStats = this.getQueryCacheStats("index1");
    assertThat(queryCacheStats.getCacheSize(), is(equalTo(1L)));
    assertThat(queryCacheStats.getHitCount(), is(equalTo(0L)));

    searchResponse = client().prepareSearch("index1").setQuery(
      boolQuery().filter(fieldDataTermsQuery("id", new long[] { 1, 2, 4, 8, 10, 7, 6, 11, 5 }, CACHE_KEY))
    ).get();
    assertHitCount(searchResponse, 2L);

    queryCacheStats = this.getQueryCacheStats("index1");
    assertThat(queryCacheStats.getCacheSize(), is(equalTo(1L)));
    assertThat(queryCacheStats.getHitCount(), is(equalTo(1L)));
  }

  private QueryCacheStats getQueryCacheStats(String index) {
    IndicesStatsResponse statsResponse = client().admin().indices().prepareStats(index).setQueryCache(true).setRefresh(true).get();
    return statsResponse.getIndex(index).getTotal().getQueryCache();
  }

}
