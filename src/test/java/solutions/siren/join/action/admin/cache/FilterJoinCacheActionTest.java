package solutions.siren.join.action.admin.cache;

import com.carrotsearch.randomizedtesting.annotations.Seed;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;
import solutions.siren.join.SirenJoinTestCase;
import solutions.siren.join.action.coordinate.CoordinateSearchRequestBuilder;
import solutions.siren.join.index.query.QueryBuilders;

import java.util.concurrent.ExecutionException;

import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

@ESIntegTestCase.ClusterScope(scope= ESIntegTestCase.Scope.SUITE, numDataNodes=1, numClientNodes=0)
public class FilterJoinCacheActionTest extends SirenJoinTestCase {

  @Test
  public void testCacheStats() throws Exception {
    this.warmCache();
    StatsFilterJoinCacheResponse rsp = new StatsFilterJoinCacheRequestBuilder(client(), StatsFilterJoinCacheAction.INSTANCE).get();
    assertThat(rsp.getNodeResponses().length, equalTo(1));
    for (StatsFilterJoinCacheNodeResponse nodeResponse : rsp.getNodeResponses()) {
      assertThat(nodeResponse.getTimestamp(), not(equalTo(0l)));
      assertThat(nodeResponse.getCacheStats().getSize(), greaterThanOrEqualTo(1l));
      assertThat(nodeResponse.getCacheStats().getCacheStats().hitCount(), greaterThanOrEqualTo(0l));
    }
  }

  private void warmCache() throws ExecutionException, InterruptedException {
    this.loadData();
    for (int i = 0; i < 5; i++) {
      this.runQueries();
    }
  }

  private void runQueries() {
    // Joining index1.foreign_key with index2.id
    SearchResponse searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index1").setQuery(
            boolQuery().filter(
                    QueryBuilders.filterJoin("foreign_key").indices("index2").types("type").path("id").query(
                            boolQuery().filter(termQuery("tag", "aaa"))
                    ))
    ).get();
    assertHitCount(searchResponse, 3L);
    assertSearchHits(searchResponse, "1", "3", "4");

    // Joining index2.id with index1.foreign_key
    searchResponse = new CoordinateSearchRequestBuilder(client()).setIndices("index2").setQuery(
            boolQuery().filter(
                    QueryBuilders.filterJoin("id").indices("index1").types("type").path("foreign_key").query(
                            boolQuery().filter(termQuery("id", "1"))
                    ))
    ).get();
    assertHitCount(searchResponse, 2L);
    assertSearchHits(searchResponse, "1", "3");
  }

  private void loadData() throws ExecutionException, InterruptedException {
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
  }

}