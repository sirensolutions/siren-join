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
package solutions.siren.join.action.coordinate;

import org.elasticsearch.test.ESIntegTestCase;
import solutions.siren.join.FilterJoinTestCase;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import solutions.siren.join.index.query.QueryBuilders;

import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;

@ESIntegTestCase.ClusterScope(scope= ESIntegTestCase.Scope.SUITE, numDataNodes=1)
public class CoordinateMultiSearchActionTest extends FilterJoinTestCase {

  @Test
  public void testSimpleJoinWithIntegerFields() throws Exception {
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

    MultiSearchResponse rsp = new CoordinateMultiSearchRequestBuilder(client())
      .add(
        client().prepareSearch("index1").setQuery(
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
        )
      )
      .add(
        client().prepareSearch("index1").setQuery(
          boolQuery().filter(
                        QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                          boolQuery().filter(
                            QueryBuilders.filterJoin("foreign_key").indices("index3").types("type").path("id").query(
                              boolQuery().filter(termQuery("tag", "aaa"))
                            )
                          )
                        )
                      )
        )
      ).execute().actionGet();

    assertEquals(2, rsp.getResponses().length);
    assertHitCount(rsp.getResponses()[0].getResponse(), 1L);
    assertSearchHits(rsp.getResponses()[0].getResponse(), "1");
    assertHitCount(rsp.getResponses()[1].getResponse(), 1L);
    assertSearchHits(rsp.getResponses()[1].getResponse(), "1");
  }

}
