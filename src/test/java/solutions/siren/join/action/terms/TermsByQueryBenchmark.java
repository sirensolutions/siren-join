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
package solutions.siren.join.action.terms;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import solutions.siren.join.SirenJoinPlugin;
import solutions.siren.join.action.coordinate.execution.FilterJoinCache;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;

/**
 * Perform the parent/child search tests using terms query lookup.
 * This should work across multiple shards and not need a special mapping
 */
@SuppressWarnings("unchecked")
public class TermsByQueryBenchmark {

  // index settings
  public static final int NUM_SHARDS = 3;
  public static final int NUM_REPLICAS = 0;
  public static final String PARENT_INDEX = "joinparent";
  public static final String PARENT_TYPE = "p";
  public static final String CHILD_INDEX = "joinchild";
  public static final String CHILD_TYPE = "c";
  // test settings
  public static final int NUM_PARENTS = 1000000;
  public static final int NUM_CHILDREN_PER_PARENT = 5;
  public static final int BATCH_SIZE = 100;
  public static final int NUM_QUERIES = 50;

  public static final int MAX_TERMS_PER_SHARD = -1;
  public static final TermsByQueryRequest.Ordering ORDERING = TermsByQueryRequest.Ordering.DEFAULT;

  private final Node[] nodes;
  private final Client client;
  private final Random random;

    TermsByQueryBenchmark() throws NodeValidationException {
      Settings settings = Settings.builder()
        .put(FilterJoinCache.SIREN_FILTERJOIN_CACHE_ENABLED, false)
        .put("index.engine.robin.refreshInterval", "-1")
        .put("path.home", "./target/elasticsearch-benchmark/home/")
        .put("node.local", true)
        .put(SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
        .put(SETTING_NUMBER_OF_REPLICAS, NUM_REPLICAS)
        .build();

      this.nodes = new MockNode[2];
      this.nodes[0] = new MockNode(Settings.builder().put(settings).put("name", "node1").build(), Collections.singletonList(SirenJoinPlugin.class)).start();
      this.nodes[1] = new MockNode(Settings.builder().put(settings).put("name", "node2").build(), Collections.singletonList(SirenJoinPlugin.class)).start();
      this.client = nodes[0].client();
      this.random = new Random(System.currentTimeMillis());
    }

    public static void main(String[] args) throws Exception {
      TermsByQueryBenchmark bench = new TermsByQueryBenchmark();
      bench.waitForGreen();
      bench.setupIndex();
      bench.memStatus();

      bench.benchHasChildSingleTerm();
      bench.benchHasParentSingleTerm();
      bench.benchHasParentMatchAll();
      bench.benchHasChildMatchAll();

      System.gc();
      bench.memStatus();
      bench.shutdown();
    }

    public void waitForGreen() {
      client.admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("10s").execute().actionGet();
    }

    public void shutdown() throws IOException {
      client.close();
      nodes[0].close();
      nodes[1].close();
    }

    public void log(String msg) {
        System.out.println("--> " + msg);
    }

    public void memStatus() throws IOException {
      List<NodeStats> nodeStats = client.admin().cluster().prepareNodesStats()
        .setJvm(true).setIndices(true).setTransport(true)
        .execute().actionGet().getNodes();

      log("==== MEMORY ====");
      log("Committed heap size: [0]=" + nodeStats.get(0).getJvm().getMem().getHeapCommitted() + ", [1]=" + nodeStats.get(1).getJvm().getMem().getHeapCommitted());
      log("Used heap size: [0]=" + nodeStats.get(0).getJvm().getMem().getHeapUsed() + ", [1]=" + nodeStats.get(1).getJvm().getMem().getHeapUsed());
      log("FieldData cache size: [0]=" + nodeStats.get(0).getIndices().getFieldData().getMemorySize() + ", [1]=" + nodeStats.get(1).getIndices().getFieldData().getMemorySize());
      log("Query cache size: [0]=" + nodeStats.get(0).getIndices().getQueryCache().getMemorySize() + ", [1]=" + nodeStats.get(1).getIndices().getQueryCache().getMemorySize());
      log("");
      log("==== NETWORK ====");
      log("Transport: [0]=" + nodeStats.get(0).getTransport().toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS).string() + ", [1]=" + nodeStats.get(1).getTransport().toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS).string());
      log("");
    }

    public XContentBuilder parentSource(int id, String nameValue) throws IOException {
        return jsonBuilder().startObject().field("id", Integer.toString(id)).field("num", id).field("name", nameValue).endObject();
    }

    public XContentBuilder childSource(String id, int parent, String tag) throws IOException {
        return jsonBuilder().startObject().field("id", id).field("pid", Integer.toString(parent)).field("num", parent)
                .field("tag", tag).endObject();
    }

    public void setupIndex() {
        log("==== INDEX SETUP ====");
        try {
            client.admin().indices().create(createIndexRequest(PARENT_INDEX)).actionGet();
            client.admin().indices().create(createIndexRequest(CHILD_INDEX)).actionGet();
            Thread.sleep(5000);

            StopWatch stopWatch = new StopWatch().start();

            log("Indexing [" + NUM_PARENTS + "] parent documents into [" + PARENT_INDEX + "]");
            log("Indexing [" + (NUM_PARENTS * NUM_CHILDREN_PER_PARENT) + "] child documents into [" + CHILD_INDEX + "]");
            int ITERS = NUM_PARENTS / BATCH_SIZE;
            int i = 1;
            int counter = 0;
            for (; i <= ITERS; i++) {
                BulkRequestBuilder request = client.prepareBulk();
                for (int j = 0; j < BATCH_SIZE; j++) {
                    String parentId = Integer.toString(counter);
                    counter++;
                    request.add(Requests.indexRequest(PARENT_INDEX)
                            .type(PARENT_TYPE)
                            .id(parentId)
                            .source(parentSource(counter, "test" + counter)));

                    for (int k = 0; k < NUM_CHILDREN_PER_PARENT; k++) {
                        String childId = parentId + "_" + k;
                        request.add(Requests.indexRequest(CHILD_INDEX)
                                .type(CHILD_TYPE)
                                .id(childId)
                                .source(childSource(childId, counter, "tag" + k)));
                    }
                }

                BulkResponse response = request.execute().actionGet();
                if (response.hasFailures()) {
                    log("Index Failures...");
                }

                if (((i * BATCH_SIZE) % 10000) == 0) {
                    log("Indexed [" + (i * BATCH_SIZE) * (1 + NUM_CHILDREN_PER_PARENT) + "] took [" + stopWatch.stop().lastTaskTime() + "]");
                    stopWatch.start();
                }
            }

            log("Indexing took [" + stopWatch.totalTime() + "]");
            log("TPS [" + (((double) (NUM_PARENTS * (1 + NUM_CHILDREN_PER_PARENT))) / stopWatch.totalTime().secondsFrac()) + "]");
        } catch (Exception e) {
            log("Indices exist, wait for green");
            waitForGreen();
        }

        client.admin().indices().prepareRefresh().execute().actionGet();
        log("Number of docs in index: " + client.prepareSearch(PARENT_INDEX, CHILD_INDEX).setQuery(matchAllQuery()).setSize(0).execute().actionGet().getHits().getTotalHits());
        log("");
    }

    public void warmFieldData(String parentField, String childField) {
        ListenableActionFuture<SearchResponse> parentSearch = null;
        ListenableActionFuture<SearchResponse> childSearch = null;

        if (parentField != null) {
            parentSearch = client
                    .prepareSearch(PARENT_INDEX)
                    .setQuery(matchAllQuery()).addAggregation(terms("parentfield").field(parentField)).execute();
        }

        if (childField != null) {
            childSearch = client
                    .prepareSearch(CHILD_INDEX)
                    .setQuery(matchAllQuery()).addAggregation(terms("childfield").field(childField)).execute();
        }

        if (parentSearch != null) parentSearch.actionGet();
        if (childSearch != null) childSearch.actionGet();
    }

    public long runQuery(String name, int testNum, long expectedHits, ActionRequestBuilder request) {
      long timestamp = System.nanoTime();
      TermsByQueryResponse response = (TermsByQueryResponse) request.execute().actionGet();
      long timeElapsed = System.nanoTime() - timestamp;

      if (response.getFailedShards() > 0) {
        log("Search Failures " + Arrays.toString(response.getShardFailures()));
      }

      long hits = response.getSize();
      if (MAX_TERMS_PER_SHARD == -1 && hits != expectedHits) {
        log("[" + name + "][#" + testNum + "] Hits Mismatch:  expected [" + expectedHits + "], got [" + hits + "]");
      }

      return timeElapsed / 1000000;
    }

    /**
     * Search for parent documents that have children containing a specified tag.
     * Expect all parents returned since one child from each parent will match the lookup.
     * <p/>
     * Parent string field = "id"
     * Parent long field = "num"
     * Child string field = "pid"
     * Child long field = "num"
     */
    public void benchHasChildSingleTerm() {
      QueryBuilder lookupQuery;

      TermsByQueryRequestBuilder stringFilter = this.newTermsByQueryRequestBuilder();
      stringFilter.setIndices(CHILD_INDEX)
                  .setTypes(CHILD_TYPE)
                  .setField("pid")
                  .setTermsEncoding(TermsByQueryRequest.TermsEncoding.LONG);

      TermsByQueryRequestBuilder longFilter = this.newTermsByQueryRequestBuilder();
      longFilter.setIndices(CHILD_INDEX)
                .setTypes(CHILD_TYPE)
                .setField("num")
                .setTermsEncoding(TermsByQueryRequest.TermsEncoding.LONG);

      long tookString = 0;
      long tookLong = 0;
      long expected = NUM_PARENTS;
      warmFieldData("id", "pid");     // for string fields
      warmFieldData("num", "num");    // for long fields

      log("==== HAS CHILD SINGLE TERM ====");
      for (int i = 0; i < NUM_QUERIES; i++) {
          lookupQuery = boolQuery().filter(termQuery("tag", "tag" + random.nextInt(NUM_CHILDREN_PER_PARENT)));

          stringFilter.setQuery(lookupQuery);
          longFilter.setQuery(lookupQuery);

          tookString += runQuery("string", i, expected, stringFilter);
          tookLong += runQuery("long", i, expected, longFilter);
      }

      log("string: " + (tookString / NUM_QUERIES) + "ms avg");
      log("long  : " + (tookLong / NUM_QUERIES) + "ms avg");
      log("");
    }

    /**
     * Search for parent documents that have any child.
     * Expect all parent documents returned.
     * <p/>
     * Parent string field = "id"
     * Parent long field = "num"
     * Child string field = "pid"
     * Child long field = "num"
     */
    public void benchHasChildMatchAll() {
      TermsByQueryRequestBuilder stringFilter = this.newTermsByQueryRequestBuilder();
      stringFilter.setIndices(CHILD_INDEX)
                  .setTypes(CHILD_TYPE)
                  .setField("pid")
                  .setTermsEncoding(TermsByQueryRequest.TermsEncoding.LONG);

      TermsByQueryRequestBuilder longFilter = this.newTermsByQueryRequestBuilder();
      longFilter.setIndices(CHILD_INDEX)
                .setTypes(CHILD_TYPE)
                .setField("num")
                .setTermsEncoding(TermsByQueryRequest.TermsEncoding.LONG);

      long tookString = 0;
      long tookLong = 0;
      long expected = NUM_PARENTS;
      warmFieldData("id", "pid");     // for string fields
      warmFieldData("num", "num");    // for long fields

      log("==== HAS CHILD MATCH-ALL ====");
      for (int i = 0; i < NUM_QUERIES; i++) {
        tookString += runQuery("string", i, expected, stringFilter);
        tookLong += runQuery("long", i, expected, longFilter);
      }

      log("string: " + (tookString / NUM_QUERIES) + "ms avg");
      log("long  : " + (tookLong / NUM_QUERIES) + "ms avg");
      log("");
    }

    /**
     * Search for children that have a parent with the specified name.
     * Expect NUM_CHILDREN_PER_PARENT since only one parent matching lookup.
     * <p/>
     * Parent string field = "id"
     * Parent numeric field = "num"
     * Child string field = "pid"
     * Child numeric field = "num"
     */
    public void benchHasParentSingleTerm() {
      QueryBuilder lookupQuery;

      TermsByQueryRequestBuilder stringFilter = this.newTermsByQueryRequestBuilder();
      stringFilter.setIndices(PARENT_INDEX)
                  .setTypes(PARENT_TYPE)
                  .setField("id")
                  .setTermsEncoding(TermsByQueryRequest.TermsEncoding.LONG);

      TermsByQueryRequestBuilder longFilter = this.newTermsByQueryRequestBuilder();
      longFilter.setIndices(PARENT_INDEX)
                .setTypes(PARENT_TYPE)
                .setField("num")
                .setTermsEncoding(TermsByQueryRequest.TermsEncoding.LONG);

      long tookString = 0;
      long tookLong = 0;
      long expected = 1;
      warmFieldData("id", "pid");     // for string fields
      warmFieldData("num", "num");    // for long fields

      log("==== HAS PARENT SINGLE TERM ====");
      for (int i = 0; i < NUM_QUERIES; i++) {
        lookupQuery = boolQuery().filter(termQuery("name", "test" + (random.nextInt(NUM_PARENTS) + 1)));

        stringFilter.setQuery(lookupQuery);
        longFilter.setQuery(lookupQuery);

        tookString += runQuery("string", i, expected, stringFilter);
        tookLong += runQuery("long", i, expected, longFilter);
      }

      log("string: " + (tookString / NUM_QUERIES) + "ms avg");
      log("long: " + (tookLong / NUM_QUERIES) + "ms avg");
      log("");
    }

    /**
     * Search for children that have a parent.
     * Expect all children to be returned.
     * <p/>
     * Parent string field = "id"
     * Parent long field = "num"
     * Child string field = "pid"
     * Child long field = "num"
     */
    public void benchHasParentMatchAll() {
      TermsByQueryRequestBuilder stringFilter = this.newTermsByQueryRequestBuilder();
      stringFilter.setIndices(PARENT_INDEX)
                  .setTypes(PARENT_TYPE)
                  .setField("id")
                  .setTermsEncoding(TermsByQueryRequest.TermsEncoding.LONG);

      TermsByQueryRequestBuilder longFilter = this.newTermsByQueryRequestBuilder();
      longFilter.setIndices(PARENT_INDEX)
                .setTypes(PARENT_TYPE)
                .setField("num")
                .setTermsEncoding(TermsByQueryRequest.TermsEncoding.LONG);

        long tookString = 0;
        long tookLong = 0;
        long expected = NUM_PARENTS;
        warmFieldData("id", "pid");     // for string fields
        warmFieldData("num", "num");    // for numeric fields

        log("==== HAS PARENT MATCH-ALL ====");
        for (int i = 0; i < NUM_QUERIES; i++) {
          tookString += runQuery("string", i, expected, stringFilter);
          tookLong += runQuery("long", i, expected, longFilter);
        }

        log("string: " + (tookString / NUM_QUERIES) + "ms avg");
        log("long  : " + (tookLong / NUM_QUERIES) + "ms avg");
        log("");
    }

  private TermsByQueryRequestBuilder newTermsByQueryRequestBuilder() {
    TermsByQueryRequestBuilder builder = new TermsByQueryRequestBuilder(client, TermsByQueryAction.INSTANCE);
    builder.setOrderBy(ORDERING);
    if (MAX_TERMS_PER_SHARD != -1) builder.setMaxTermsPerShard(MAX_TERMS_PER_SHARD);
    return builder;
  }

}
