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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.cache.IndexCacheModule;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;
import solutions.siren.join.SirenJoinTestCase;
import solutions.siren.join.action.terms.collector.LongBloomFilter;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static solutions.siren.join.index.query.QueryBuilders.termsEnumTermsQuery;

@ESIntegTestCase.ClusterScope(scope= ESIntegTestCase.Scope.SUITE, numDataNodes=1)
public class TermsEnumTermsQueryTest extends SirenJoinTestCase {

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
  public void testStringFilter() throws Exception {
    assertAcked(prepareCreate("index1").addMapping("type", "id", "type=string"));
    ensureGreen();

    indexRandom(true,
      client().prepareIndex("index1", "type", "1").setSource("id", "1"),
      client().prepareIndex("index1", "type", "3").setSource("id", "3"),
      client().prepareIndex("index1", "type", "7").setSource("id", "7"));

    SearchResponse searchResponse = client().prepareSearch("index1").setQuery(
      boolQuery().filter(termsEnumTermsQuery("id", new BytesRef[] {
              new BytesRef("1"), new BytesRef("2"), new BytesRef("4"), new BytesRef("8"),
              new BytesRef("10"), new BytesRef("7"), new BytesRef("6"), new BytesRef("11"),
              new BytesRef("5")
      }, CACHE_KEY))
    ).get();
    assertHitCount(searchResponse, 2L);
  }

  @Test
  public void testLongFilter() throws Exception {
    assertAcked(prepareCreate("index1").addMapping("type", "id", "type=long"));
    ensureGreen();

    indexRandom(true,
            client().prepareIndex("index1", "type", "1").setSource("id", "1"),
            client().prepareIndex("index1", "type", "3").setSource("id", "3"),
            client().prepareIndex("index1", "type", "7").setSource("id", "7"));

    SearchResponse searchResponse = client().prepareSearch("index1").setQuery(
            boolQuery().filter(termsEnumTermsQuery("id", toBytesRefs(new long[] {1, 2, 4, 8, 10, 7, 6, 11, 5}), CACHE_KEY))
    ).get();
    assertHitCount(searchResponse, 2L);
  }

  /**
   * Converts a list of long value to their bytes ref representation as performed by
   * {@link org.apache.lucene.analysis.NumericTokenStream}
   */
  private BytesRef[] toBytesRefs(long[] values) {
    BytesRef[] bytesRefs = new BytesRef[values.length];
    for (int i = 0; i < values.length; i++) {
      BytesRefBuilder b = new BytesRefBuilder();
      NumericUtils.longToPrefixCoded(values[i], 0, b);
      bytesRefs[i] = b.toBytesRef();
    }
    return bytesRefs;
  }

}
