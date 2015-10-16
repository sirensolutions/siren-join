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
package solutions.siren.siren.join.action.terms;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import solutions.siren.siren.join.FilterJoinTestCase;
import solutions.siren.siren.join.index.query.BinaryTermsFilterHelper;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;

@ElasticsearchIntegrationTest.ClusterScope(scope=ElasticsearchIntegrationTest.Scope.SUITE, numDataNodes=1)
public class TermsByQueryActionTest extends FilterJoinTestCase {

  /**
   * Tests that the terms by query action returns the correct terms against string fields
   */
  @Test
  public void testTermsByQueryStringField() throws Exception {
    createIndex("test");

    int numDocs = RandomizedTest.randomIntBetween(100, 2000);
    logger.info("--> indexing [" + numDocs + "] docs");
    for (int i = 0; i < numDocs; i++) {
      client().prepareIndex("test", "type", "" + i)
              .setSource(jsonBuilder().startObject()
                                        .field("str", Integer.toString(i))
                                      .endObject())
              .execute().actionGet();
    }

    client().admin().indices().prepareRefresh("test").execute().actionGet();

    logger.info("--> lookup terms in field [str]");
    TermsByQueryResponse resp = new TermsByQueryRequestBuilder(client()).setIndices("test")
                                                                        .setField("str")
                                                                        .setQuery(QueryBuilders.matchAllQuery())
                                                                        .execute()
                                                                        .actionGet();

    ElasticsearchAssertions.assertNoFailures(resp);
    assertThat(resp.getTermsResponse(), notNullValue());
    assertThat(resp.getTermsResponse().size(), is(numDocs));
    assertThat(resp.getTermsResponse().getTerms() instanceof LongHashSet, is(true));
    LongHashSet lTerms = resp.getTermsResponse().getTerms();
    assertThat(lTerms.size(), is(numDocs));
    for (int i = 0; i < numDocs; i++) {
      long termHash = BinaryTermsFilterHelper.hash(new BytesRef(Integer.toString(i)));
      assertThat(lTerms.contains(termHash), is(true));
    }
  }

  /**
   * Tests that the terms by query action returns the correct terms against integer fields
   */
  @Test
  public void testTermsByQueryIntegerField() throws Exception {
    createIndex("test");

    int numDocs = RandomizedTest.randomIntBetween(100, 2000);
    logger.info("--> indexing [" + numDocs + "] docs");
    for (int i = 0; i < numDocs; i++) {
      client().prepareIndex("test", "type", "" + i)
              .setSource(jsonBuilder().startObject()
                                        .field("int", i)
                                      .endObject())
              .execute().actionGet();
    }

    client().admin().indices().prepareRefresh("test").execute().actionGet();

    logger.info("--> lookup terms in field [int]");
    TermsByQueryResponse resp = new TermsByQueryRequestBuilder(client()).setIndices("test")
                                                                        .setField("int")
                                                                        .setQuery(QueryBuilders.matchAllQuery())
                                                                        .execute()
                                                                        .actionGet();

    ElasticsearchAssertions.assertNoFailures(resp);
    assertThat(resp.getTermsResponse(), notNullValue());
    assertThat(resp.getTermsResponse().size(), is(numDocs));
    assertThat(resp.getTermsResponse().getTerms() instanceof LongHashSet, is(true));
    LongHashSet lTerms = resp.getTermsResponse().getTerms();
    assertThat(lTerms.size(), is(numDocs));
    for (int i = 0; i < numDocs; i++) {
      assertThat(lTerms.contains(Long.valueOf(i)), is(true));
    }
  }

  /**
   * Tests that the limit for the number of terms retrieved is properly applied.
   */
  @Test
  public void testTermsByQueryWithLimit() throws Exception {
    createIndex("test");

    int numDocs = RandomizedTest.randomIntBetween(100, 2000);
    logger.info("--> indexing [" + numDocs + "] docs");
    for (int i = 0; i < numDocs; i++) {
      client().prepareIndex("test", "type", "" + i)
              .setSource(jsonBuilder().startObject()
                                        .field("int", i)
                                      .endObject())
              .execute().actionGet();
    }

    client().admin().indices().prepareRefresh("test").execute().actionGet();

    logger.info("--> lookup terms in field [int]");
    TermsByQueryResponse resp = new TermsByQueryRequestBuilder(client()).setIndices("test")
                                                                        .setField("int")
                                                                        .setQuery(QueryBuilders.matchAllQuery())
                                                                        .setOrderBy(TermsByQueryRequest.Ordering.DEFAULT)
                                                                        .setMaxTermsPerShard(50)
                                                                        .execute()
                                                                        .actionGet();

    int expectedMaxResultSize = this.getNumShards("test").totalNumShards * 50;
    ElasticsearchAssertions.assertNoFailures(resp);
    assertThat(resp.getTermsResponse(), notNullValue());
    assertThat(resp.getTermsResponse().size(), lessThanOrEqualTo(expectedMaxResultSize));
    assertThat(resp.getTermsResponse().getTerms() instanceof LongHashSet, is(true));
    LongHashSet lTerms = resp.getTermsResponse().getTerms();
    assertThat(lTerms.size(), lessThanOrEqualTo(expectedMaxResultSize));
  }

  /**
   * Tests the ordering by document score.
   */
  @Test
  public void testTermsByQueryWithLimitOrderByDocScore() throws Exception {
    // Enforce one single shard for index as it is difficult with multiple shards
    // to avoid having one shard with less than 5 even ids (i.e., to avoid the shard
    // returning odd ids.
    Map<String, Object> indexSettings = new HashMap<>();
    indexSettings.put("number_of_shards", 1);
    assertAcked(prepareCreate("test").setSettings(indexSettings));

    int numDocs = RandomizedTest.randomIntBetween(100, 2000);
    logger.info("--> indexing [" + numDocs + "] docs");
    for (int i = 0; i < numDocs / 2; i += 2) {
      client().prepareIndex("test", "type", "" + i)
              .setSource(jsonBuilder().startObject()
              .field("int", i)
              .field("text", "aaa")
              .endObject())
              .execute().actionGet();
    }

    for (int i = 1; i < numDocs / 2; i += 2) {
      client().prepareIndex("test", "type", "" + i)
              .setSource(jsonBuilder().startObject()
              .field("int", i)
              .field("text", "aaa aaa")
              .endObject())
              .execute().actionGet();
    }

    client().admin().indices().prepareRefresh("test").execute().actionGet();

    logger.info("--> lookup terms in field [int]");
    TermsByQueryResponse resp = new TermsByQueryRequestBuilder(client()).setIndices("test")
                                                                        .setField("int")
                                                                        .setQuery(QueryBuilders.termQuery("text", "aaa"))
                                                                        .setOrderBy(TermsByQueryRequest.Ordering.DOC_SCORE)
                                                                        .setMaxTermsPerShard(5)
                                                                        .execute()
                                                                        .actionGet();

    int expectedMaxResultSize = this.getNumShards("test").totalNumShards * 5;
    ElasticsearchAssertions.assertNoFailures(resp);
    assertThat(resp.getTermsResponse(), notNullValue());
    assertThat(resp.getTermsResponse().size(), lessThanOrEqualTo(expectedMaxResultSize));
    assertThat(resp.getTermsResponse().getTerms() instanceof LongHashSet, is(true));
    LongHashSet lTerms = resp.getTermsResponse().getTerms();
    assertThat(lTerms.size(), lessThanOrEqualTo(expectedMaxResultSize));

    // If the ordering by document score worked, we should only have documents with text = aaa (even ids), and no
    // documents with text = aaa aaa (odd ids), as the first one will be ranked higher.

    Iterator<LongCursor> it = lTerms.iterator();
    while (it.hasNext()) {
      long value = it.next().value;
      assertThat(value % 2 == 0, is(true));
    }
  }

}