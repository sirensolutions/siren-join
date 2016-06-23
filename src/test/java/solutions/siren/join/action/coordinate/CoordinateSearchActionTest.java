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

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.test.ESIntegTestCase;
import solutions.siren.join.SirenJoinTestCase;
import solutions.siren.join.action.terms.TermsByQueryRequest;
import solutions.siren.join.index.query.QueryBuilders;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;

@ESIntegTestCase.ClusterScope(scope= ESIntegTestCase.Scope.SUITE, numDataNodes=1)
public class CoordinateSearchActionTest extends SirenJoinTestCase {

  @Test
  public void testSimpleJoinWithStringFields() throws Exception {
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
      client().prepareIndex("index2", "type", "4").setSource("id", "4", "tag", "ccc") );

    // Joining index1.foreign_key with index2.id
    SearchResponse searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
      filteredQuery(matchAllQuery(),
                    QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                      boolQuery().filter(termQuery("tag", "aaa"))
                    ))
    ).get();
    assertHitCount(searchResponse, 3L);
    assertSearchHits(searchResponse, "1", "3", "4");

    // Joining index1.foreign_key with empty index2 relation
    searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
      filteredQuery(matchAllQuery(),
                    QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                      boolQuery().filter(termQuery("tag", "ddd"))
                    ))
    ).get();
    assertHitCount(searchResponse, 0L);

    // Joining index2.id with index1.foreign_key
    searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index2").setQuery(
      filteredQuery(matchAllQuery(),
                    QueryBuilders.filterJoin("id").indices("index1").types("type").path("foreign_key").query(
                      boolQuery().filter(termQuery("id", "1"))
                    ))
    ).get();
    assertHitCount(searchResponse, 2L);
    assertSearchHits(searchResponse, "1", "3");

    // Joining index2.id with empty index1.foreign_key
    searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index2").setQuery(
      filteredQuery(matchAllQuery(),
                    QueryBuilders.filterJoin("id").indices("index1").types("type").path("foreign_key").query(
                      boolQuery().filter(termQuery("id", "2"))
                    ))
    ).get();
    assertHitCount(searchResponse, 0L);
  }

  @Test
  public void testSimpleJoinWithIntegerFields() throws Exception {
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
      client().prepareIndex("index2", "type", "4").setSource("id", "4", "tag", "ccc") );

    // Joining index1.foreign_key with index2.id
    SearchResponse searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
      QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("id").query(
        boolQuery().filter(termQuery("tag", "aaa"))
      )
    ).get();
    assertHitCount(searchResponse, 3L);
    assertSearchHits(searchResponse, "1", "3", "4");

    // Joining index1.foreign_key with empty index2 relation
    searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
      QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("id").query(
        boolQuery().filter(termQuery("tag", "ddd"))
      )
    ).get();
    assertHitCount(searchResponse, 0L);

    // Joining index2.id with index1.foreign_key
    searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index2").setQuery(
      QueryBuilders.filterJoin("id").indices("index1").types("type").path("foreign_key").query(
        boolQuery().filter(termQuery("id", "1"))
      )
    ).get();
    assertHitCount(searchResponse, 2L);
    assertSearchHits(searchResponse, "1", "3");

    // Joining index2.id with empty index1.foreign_key
    searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index2").setQuery(
      QueryBuilders.filterJoin("id").indices("index1").types("type").path("foreign_key").query(
        boolQuery().filter(termQuery("id", "2"))
      )
    ).get();
    assertHitCount(searchResponse, 0L);
  }

  @Test
  public void testNestedJoinWithIntegerFields() throws Exception {
    Settings settings = Settings.settingsBuilder().put("number_of_shards", 1).build();

    assertAcked(prepareCreate("index1").setSettings(settings).addMapping("type", "id", "type=integer", "foreign_key", "type=integer"));
    assertAcked(prepareCreate("index2").setSettings(settings).addMapping("type", "id", "type=integer", "foreign_key", "type=integer", "tag", "type=string"));
    assertAcked(prepareCreate("index3").setSettings(settings).addMapping("type", "id", "type=integer", "tag", "type=string"));

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

    SearchResponse rsp = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
      boolQuery().filter(
                    QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                      boolQuery().filter(
                        QueryBuilders.filterJoin("foreign_key").indices("index3").types("type").path("id").query(
                          boolQuery().filter(termQuery("tag", "aaa"))
                        )
                      )
                    )
                  )
                  .filter(
                    termQuery("id", "1")
                  )
    ).execute().actionGet();

    assertHitCount(rsp, 1L);
    assertSearchHits(rsp, "1");

    // Run the request a second time to hit the filter cache and check that there is
    // no issues with the cached FieldDataTermsQuery
    rsp = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
      boolQuery().filter(
                    QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                      boolQuery().filter(
                        QueryBuilders.filterJoin("foreign_key").indices("index3").types("type").path("id").query(
                          boolQuery().filter(termQuery("tag", "aaa"))
                        )
                      )
                    )
                  )
                  .filter(
                    termQuery("id", "1")
                  )
    ).execute().actionGet();

    assertHitCount(rsp, 1L);
    assertSearchHits(rsp, "1");
  }

  /**
   * Checks that a binary terms filter nested within a filter join can be parsed as a query. See issue #180.
   */
  @Test
  public void testNestedJoinWithOrderByDocScore() throws Exception {
    Settings settings = Settings.settingsBuilder().put("number_of_shards", 1).build();

    assertAcked(prepareCreate("index1").setSettings(settings).addMapping("type", "id", "type=integer", "foreign_key", "type=integer"));
    assertAcked(prepareCreate("index2").setSettings(settings).addMapping("type", "id", "type=integer", "foreign_key", "type=integer", "tag", "type=string"));
    assertAcked(prepareCreate("index3").setSettings(settings).addMapping("type", "id", "type=integer", "tag", "type=string"));

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

    SearchResponse rsp = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
      boolQuery().filter(
                    QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                      boolQuery().filter(
                        QueryBuilders.filterJoin("foreign_key").indices("index3").types("type").path("id").query(
                          boolQuery().filter(termQuery("tag", "aaa"))
                        )
                      )
                    )
                  )
                  .filter(
                    termQuery("id", "1")
                  )
    ).execute().actionGet();

    assertHitCount(rsp, 1L);
    assertSearchHits(rsp, "1");

    // Run the request a second time ordered by doc score
    rsp = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
      boolQuery().filter(
                    QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                      boolQuery().filter(
                        QueryBuilders.filterJoin("foreign_key").indices("index3").types("type").path("id").query(
                          boolQuery().filter(termQuery("tag", "aaa"))
                        )
                      )
                    ).orderBy("doc_score").maxTermsPerShard(1)
                  )
                  .filter(
                    termQuery("id", "1")
                  )
    ).execute().actionGet();

    assertHitCount(rsp, 1L);
    assertSearchHits(rsp, "1");
  }

  /**
   * Checks that the user can omit to specify the types. By default, it should try to search across all types.
   */
  @Test
  public void testSimpleJoinNoTypeSpecified() throws Exception {
    Settings settings = Settings.settingsBuilder().put("number_of_shards", 1).build();

    assertAcked(prepareCreate("index1").setSettings(settings).addMapping("type", "id", "type=integer", "foreign_key", "type=integer"));
    assertAcked(prepareCreate("index2").setSettings(settings).addMapping("type", "id", "type=integer", "foreign_key", "type=integer", "tag", "type=string"));
    assertAcked(prepareCreate("index3").setSettings(settings).addMapping("type", "id", "type=integer", "tag", "type=string"));

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

    SearchResponse rsp = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
      boolQuery().filter(
                    QueryBuilders.filterJoin("foreign_key").indices("index2").path("id").query(
                      boolQuery().filter(
                        QueryBuilders.filterJoin("foreign_key").indices("index3").path("id").query(
                          boolQuery().filter(termQuery("tag", "aaa"))
                        )
                      )
                    )
                  )
                  .filter(
                    termQuery("id", "1")
                  )
    ).execute().actionGet();

    assertHitCount(rsp, 1L);
    assertSearchHits(rsp, "1");
  }

  /**
   * Checks that the user can specify more than one types.
   */
  @Test
  public void testSimpleJoinMoreThanOneTypesSpecified() throws Exception {
    Settings settings = Settings.settingsBuilder().put("number_of_shards", 1).build();

    assertAcked(prepareCreate("index1").setSettings(settings).addMapping("type1", "id", "type=integer", "foreign_key", "type=integer"));
    assertAcked(prepareCreate("index2").setSettings(settings).addMapping("type2", "id", "type=integer", "tag", "type=string")
                                                             .addMapping("type3", "id", "type=integer", "tag", "type=string")
                                                             .addMapping("type4", "id", "type=integer", "tag", "type=string"));

    ensureGreen();

    indexRandom(true,
            client().prepareIndex("index1", "type1", "1").setSource("id", "1", "foreign_key", new String[]{"1", "3"}),
            client().prepareIndex("index1", "type1", "2").setSource("id", "2", "foreign_key", new String[]{"3"}),
            client().prepareIndex("index1", "type1", "3").setSource("id", "3", "foreign_key", new String[]{"4"}),
            client().prepareIndex("index1", "type1", "4").setSource("id", "4", "foreign_key", new String[]{"4", "6"}),

            client().prepareIndex("index2", "type2", "1").setSource("id", "1", "tag", "aaa"),
            client().prepareIndex("index2", "type2", "2").setSource("id", "2", "tag", "bbb"),
            client().prepareIndex("index2", "type3", "1").setSource("id", "3", "tag", "ddd"),
            client().prepareIndex("index2", "type3", "2").setSource("id", "4", "tag", "aaa"),
            client().prepareIndex("index2", "type4", "1").setSource("id", "5", "tag", "ccc"),
            client().prepareIndex("index2", "type4", "2").setSource("id", "6", "tag", "aaa"));

    // In order to query all the indices, we need to call setIndices() without argument to set an empty array,
    // otherwise we will get NPE. This behaviour is identical to Elasticsearch SearchRequestBuilder's behaviour.
    SearchResponse rsp = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setTypes("type1").setQuery(
      boolQuery().filter(
        QueryBuilders.filterJoin("foreign_key").indices("index2").types("type2", "type4").path("id").query(
          termQuery("tag", "aaa")
        )
      )
    ).execute().actionGet();

    assertHitCount(rsp, 2L);
    assertSearchHits(rsp, "1", "4");
  }

  /**
   * Checks that the user can omit to specify the indices. By default, it should try to search across all indices.
   * This test might displays some error log messages as the termsByQuery will try to lookup the 'id' field
   * for a given type across all indices. However, the type will be undefined is some of the indices. For example, it
   * will try to lookup the 'id' field for the type 'type2' on indices 'index1', 'index2' and 'index3', but it is
   * defined only for 'index2'. Such errors will be reported but ignored during the processing, and should not
   * impact the results.
   */
  @Test
  public void testSimpleJoinNoIndexSpecified() throws Exception {
    Settings settings = Settings.settingsBuilder().put("number_of_shards", 1).build();

    assertAcked(prepareCreate("index1").setSettings(settings).addMapping("type1", "id", "type=integer", "foreign_key", "type=integer"));
    assertAcked(prepareCreate("index2").setSettings(settings).addMapping("type2", "id", "type=integer", "foreign_key", "type=integer", "tag", "type=string"));
    assertAcked(prepareCreate("index3").setSettings(settings).addMapping("type3", "id", "type=integer", "tag", "type=string"));

    ensureGreen();

    indexRandom(true,
            client().prepareIndex("index1", "type1", "1").setSource("id", "1", "foreign_key", new String[]{"1", "3"}),
            client().prepareIndex("index1", "type1", "2").setSource("id", "2"),
            client().prepareIndex("index1", "type1", "3").setSource("id", "3", "foreign_key", new String[]{"2"}),
            client().prepareIndex("index1", "type1", "4").setSource("id", "4", "foreign_key", new String[]{"1", "4"}),

            client().prepareIndex("index2", "type2", "1").setSource("id", "1", "tag", "aaa"),
            client().prepareIndex("index2", "type2", "2").setSource("id", "2", "tag", "aaa"),
            client().prepareIndex("index2", "type2", "3").setSource("id", "3", "foreign_key", new String[]{"2"}, "tag", "bbb"),
            client().prepareIndex("index2", "type2", "4").setSource("id", "4", "tag", "ccc"),

            client().prepareIndex("index3", "type3", "1").setSource("id", "1", "tag", "aaa"),
            client().prepareIndex("index3", "type3", "2").setSource("id", "2", "tag", "aaa"),
            client().prepareIndex("index3", "type3", "3").setSource("id", "3", "tag", "bbb"),
            client().prepareIndex("index3", "type3", "4").setSource("id", "4", "tag", "ccc"));

    // In order to query all the indices, we need to call setIndices() without argument to set an empty array,
    // otherwise we will get NPE. This behaviour is identical to Elasticsearch SearchRequestBuilder's behaviour.
    SearchResponse rsp = new CoordinateSearchRequestBuilder(client()).setIndices().setTypes("type1").setQuery(
      boolQuery().filter(
                    QueryBuilders.filterJoin("foreign_key").path("id").query(
                      boolQuery().filter(
                        QueryBuilders.filterJoin("foreign_key").path("id").query(
                          boolQuery().filter(termQuery("tag", "aaa"))
                        )
                      )
                    )
                  )
                  .filter(
                    termQuery("id", "1")
                  )
    ).execute().actionGet();

    assertHitCount(rsp, 1L);
    assertSearchHits(rsp, "1");
  }

  /**
   * Checks that the user can specify more than one indices.
   */
  @Test
  public void testSimpleJoinMoreThanOneIndexSpecified() throws Exception {
    Settings settings = Settings.settingsBuilder().put("number_of_shards", 1).build();

    assertAcked(prepareCreate("index1").setSettings(settings).addMapping("type1", "id", "type=integer", "foreign_key", "type=integer"));
    assertAcked(prepareCreate("index2").setSettings(settings).addMapping("type2", "id", "type=integer", "tag", "type=string"));
    assertAcked(prepareCreate("index3").setSettings(settings).addMapping("type2", "id", "type=integer", "tag", "type=string"));

    ensureGreen();

    indexRandom(true,
            client().prepareIndex("index1", "type1", "1").setSource("id", "1", "foreign_key", new String[]{"1", "3"}),
            client().prepareIndex("index1", "type1", "2").setSource("id", "2", "foreign_key", new String[]{"3"}),
            client().prepareIndex("index1", "type1", "3").setSource("id", "3", "foreign_key", new String[]{"8"}),
            client().prepareIndex("index1", "type1", "4").setSource("id", "4", "foreign_key", new String[]{"4", "6"}),

            client().prepareIndex("index2", "type2", "1").setSource("id", "1", "tag", "aaa"),
            client().prepareIndex("index2", "type2", "2").setSource("id", "2", "tag", "aaa"),
            client().prepareIndex("index2", "type2", "3").setSource("id", "3", "tag", "bbb"),
            client().prepareIndex("index2", "type2", "4").setSource("id", "4", "tag", "ccc"),

            client().prepareIndex("index3", "type2", "1").setSource("id", "5", "tag", "aaa"),
            client().prepareIndex("index3", "type2", "2").setSource("id", "6", "tag", "aaa"),
            client().prepareIndex("index3", "type2", "3").setSource("id", "7", "tag", "bbb"),
            client().prepareIndex("index3", "type2", "4").setSource("id", "8", "tag", "ccc"));

    // In order to query all the indices, we need to call setIndices() without argument to set an empty array,
    // otherwise we will get NPE. This behaviour is identical to Elasticsearch SearchRequestBuilder's behaviour.
    SearchResponse rsp = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setTypes("type1").setQuery(
      boolQuery().filter(
        QueryBuilders.filterJoin("foreign_key").indices("index2", "index3").types("type2").path("id").query(
          termQuery("tag", "aaa")
        )
      )
    ).execute().actionGet();

    assertHitCount(rsp, 2L);
    assertSearchHits(rsp, "1", "4");
  }

  /**
   * Checks indirect self-joins
   */
  @Test
  public void testIndirectSelfJoin() throws Exception {
    Settings settings = Settings.settingsBuilder().put("number_of_shards", 1).build();

    assertAcked(prepareCreate("index1").setSettings(settings).addMapping("type", "id", "type=integer", "foreign_key", "type=integer"));
    assertAcked(prepareCreate("index2").setSettings(settings).addMapping("type", "id", "type=integer", "tag", "type=string"));

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

    SearchResponse rsp = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
      boolQuery().filter(
        QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("id").query(
          boolQuery().filter(
            QueryBuilders.filterJoin("id").indices("index1").types("type").path("foreign_key").query(
              boolQuery().filter(termQuery("id", "1"))
            )
          )
        )
      )
    ).execute().actionGet();

    assertHitCount(rsp, 2L);
    assertSearchHits(rsp, "1", "4");
  }

  @Test
  public void testSimpleJoinWithIntegerEncoding() throws Exception {
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
            client().prepareIndex("index2", "type", "4").setSource("id", "4", "tag", "ccc") );

    // Joining index1.foreign_key with index2.id
    SearchResponse searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
            QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                    boolQuery().filter(termQuery("tag", "aaa"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.INTEGER)
    ).get();
    assertHitCount(searchResponse, 3L);
    assertSearchHits(searchResponse, "1", "3", "4");

    // Joining index1.foreign_key with empty index2 relation
    searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
            QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                    boolQuery().filter(termQuery("tag", "ddd"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.INTEGER)
    ).get();
    assertHitCount(searchResponse, 0L);

    // Joining index2.id with index1.foreign_key
    searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index2").setQuery(
            QueryBuilders.filterJoin("id").indices("index1").types("type").path("foreign_key").query(
                    boolQuery().filter(termQuery("id", "1"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.INTEGER)
    ).get();
    assertHitCount(searchResponse, 2L);
    assertSearchHits(searchResponse, "1", "3");

    // Joining index2.id with empty index1.foreign_key
    searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index2").setQuery(
            QueryBuilders.filterJoin("id").indices("index1").types("type").path("foreign_key").query(
                    boolQuery().filter(termQuery("id", "2"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.INTEGER)
    ).get();
    assertHitCount(searchResponse, 0L);
  }

  @Test(expected=SearchPhaseExecutionException.class)
  public void testInvalidTargetField() throws Exception {
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
            client().prepareIndex("index2", "type", "4").setSource("id", "4", "tag", "ccc") );

    // Invalid target field
    SearchResponse searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
            QueryBuilders.filterJoin("invalid_field").indices("index2").types("type").path("id").query(
                    boolQuery().filter(termQuery("tag", "aaa"))
            )
    ).get();
  }

  @Test
  public void testInvalidSourceField() throws Exception {
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
            client().prepareIndex("index2", "type", "4").setSource("id", "4", "tag", "ccc") );

    // Invalid target field
    SearchResponse searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
            QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("invalid_field").query(
                    boolQuery().filter(termQuery("tag", "aaa"))
            )
    ).get();
    assertHitCount(searchResponse, 0L);
  }

  @Test
  public void testSimpleJoinWithBloomEncoding() throws Exception {
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
            client().prepareIndex("index2", "type", "4").setSource("id", "4", "tag", "ccc") );

    // Joining index1.foreign_key with index2.id
    SearchResponse searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
            QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                    boolQuery().filter(termQuery("tag", "aaa"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.BLOOM)
    ).get();
    assertHitCount(searchResponse, 3L);
    assertSearchHits(searchResponse, "1", "3", "4");

    // Joining index1.foreign_key with empty index2 relation
    searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
            QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                    boolQuery().filter(termQuery("tag", "ddd"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.BLOOM)
    ).get();
    assertHitCount(searchResponse, 0L);

    // Joining index2.id with index1.foreign_key
    searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index2").setQuery(
            QueryBuilders.filterJoin("id").indices("index1").types("type").path("foreign_key").query(
                    boolQuery().filter(termQuery("id", "1"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.BLOOM)
    ).get();
    assertHitCount(searchResponse, 2L);
    assertSearchHits(searchResponse, "1", "3");

    // Joining index2.id with empty index1.foreign_key
    searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index2").setQuery(
            QueryBuilders.filterJoin("id").indices("index1").types("type").path("foreign_key").query(
                    boolQuery().filter(termQuery("id", "2"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.BLOOM)
    ).get();
    assertHitCount(searchResponse, 0L);
  }

  @Test
  public void testSimpleJoinWithBytesEncodingOnIntegerField() throws Exception {
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
            client().prepareIndex("index2", "type", "4").setSource("id", "4", "tag", "ccc") );

    // Joining index1.foreign_key with index2.id
    SearchResponse searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
            QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                    boolQuery().filter(termQuery("tag", "aaa"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.BYTES).maxTermsPerShard(1000)
    ).get();
    assertHitCount(searchResponse, 3L);
    assertSearchHits(searchResponse, "1", "3", "4");

    // Joining index1.foreign_key with empty index2 relation
    searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
            QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                    boolQuery().filter(termQuery("tag", "ddd"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.BYTES).maxTermsPerShard(1000)
    ).get();
    assertHitCount(searchResponse, 0L);

    // Joining index2.id with index1.foreign_key
    searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index2").setQuery(
            QueryBuilders.filterJoin("id").indices("index1").types("type").path("foreign_key").query(
                    boolQuery().filter(termQuery("id", "1"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.BYTES).maxTermsPerShard(1000)
    ).get();
    assertHitCount(searchResponse, 2L);
    assertSearchHits(searchResponse, "1", "3");

    // Joining index2.id with empty index1.foreign_key
    searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index2").setQuery(
            QueryBuilders.filterJoin("id").indices("index1").types("type").path("foreign_key").query(
                    boolQuery().filter(termQuery("id", "2"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.BYTES).maxTermsPerShard(1000)
    ).get();
    assertHitCount(searchResponse, 0L);
  }

  @Test
  public void testSimpleJoinWithBytesEncodingOnStringField() throws Exception {
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
            client().prepareIndex("index2", "type", "4").setSource("id", "4", "tag", "ccc") );

    // Joining index1.foreign_key with index2.id
    SearchResponse searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
            QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                    boolQuery().filter(termQuery("tag", "aaa"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.BYTES).maxTermsPerShard(1000)
    ).get();
    assertHitCount(searchResponse, 3L);
    assertSearchHits(searchResponse, "1", "3", "4");

    // Joining index1.foreign_key with empty index2 relation
    searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
            QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                    boolQuery().filter(termQuery("tag", "ddd"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.BYTES).maxTermsPerShard(1000)
    ).get();
    assertHitCount(searchResponse, 0L);

    // Joining index2.id with index1.foreign_key
    searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index2").setQuery(
            QueryBuilders.filterJoin("id").indices("index1").types("type").path("foreign_key").query(
                    boolQuery().filter(termQuery("id", "1"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.BYTES).maxTermsPerShard(1000)
    ).get();
    assertHitCount(searchResponse, 2L);
    assertSearchHits(searchResponse, "1", "3");

    // Joining index2.id with empty index1.foreign_key
    searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index2").setQuery(
            QueryBuilders.filterJoin("id").indices("index1").types("type").path("foreign_key").query(
                    boolQuery().filter(termQuery("id", "2"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.BYTES).maxTermsPerShard(1000)
    ).get();
    assertHitCount(searchResponse, 0L);
  }

  @Test
  public void testSimpleJoinWithBytesEncodingOnLongField() throws Exception {
    assertAcked(prepareCreate("index1").addMapping("type", "id", "type=long", "foreign_key", "type=long"));
    assertAcked(prepareCreate("index2").addMapping("type", "id", "type=long", "tag", "type=string"));

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

    // Joining index1.foreign_key with index2.id
    SearchResponse searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
            QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                    boolQuery().filter(termQuery("tag", "aaa"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.BYTES).maxTermsPerShard(1000)
    ).get();
    assertHitCount(searchResponse, 3L);
    assertSearchHits(searchResponse, "1", "3", "4");

    // Joining index1.foreign_key with empty index2 relation
    searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
            QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                    boolQuery().filter(termQuery("tag", "ddd"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.BYTES).maxTermsPerShard(1000)
    ).get();
    assertHitCount(searchResponse, 0L);

    // Joining index2.id with index1.foreign_key
    searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index2").setQuery(
            QueryBuilders.filterJoin("id").indices("index1").types("type").path("foreign_key").query(
                    boolQuery().filter(termQuery("id", "1"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.BYTES).maxTermsPerShard(1000)
    ).get();
    assertHitCount(searchResponse, 2L);
    assertSearchHits(searchResponse, "1", "3");

    // Joining index2.id with empty index1.foreign_key
    searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index2").setQuery(
            QueryBuilders.filterJoin("id").indices("index1").types("type").path("foreign_key").query(
                    boolQuery().filter(termQuery("id", "2"))
            ).termsEncoding(TermsByQueryRequest.TermsEncoding.BYTES).maxTermsPerShard(1000)
    ).get();
    assertHitCount(searchResponse, 0L);
  }

}
