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
package solutions.siren.join;

import solutions.siren.join.action.coordinate.CoordinateSearchRequestBuilder;
import solutions.siren.join.index.query.FilterJoinBuilder;
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
import org.elasticsearch.node.Node;
import solutions.siren.join.index.query.FilterBuilders;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;

/**
 * Perform the parent/child search tests using terms query lookup.
 * This should work across multiple shards and not need a special mapping
 */
@SuppressWarnings("unchecked")
public class FilterJoinBenchmark {

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
    private final Node[] nodes;
    private final Client client;
    private final Random random;

    FilterJoinBenchmark() {
      Settings settings = settingsBuilder()
        .put("index.engine.robin.refreshInterval", "-1")
        .put("path.data", "./target/elasticsearch-benchmark/data/")
        .put("gateway.type", "local")
//        .put("transport.tcp.compress", "true")
        .put(SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
        .put(SETTING_NUMBER_OF_REPLICAS, NUM_REPLICAS)
        .build();

      this.nodes = new Node[2];
      this.nodes[0] = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "node1")).node();
      this.nodes[1] = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "node2")).node();
      this.client = nodes[0].client();
      this.random = new Random(System.currentTimeMillis());
    }

    public static void main(String[] args) throws Exception {
      FilterJoinBenchmark bench = new FilterJoinBenchmark();
      bench.waitForGreen();
      bench.setupIndex();
      bench.memStatus();

      bench.benchHasChildSingleTerm();
      bench.benchHasParentSingleTerm();
      bench.benchHasParentMatchAll();
      bench.benchHasChildMatchAll();
//        bench.benchHasParentRandomTerms();

      System.gc();
      bench.memStatus();
      bench.shutdown();
    }

    public void waitForGreen() {
      client.admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("10s").execute().actionGet();
    }

    public void shutdown() {
      client.close();
      nodes[0].close();
      nodes[1].close();
    }

    public void log(String msg) {
        System.out.println("--> " + msg);
    }

    public void memStatus() throws IOException {
      NodeStats[] nodeStats = client.admin().cluster().prepareNodesStats()
        .setJvm(true).setIndices(true).setNetwork(true).setTransport(true)
        .execute().actionGet().getNodes();

      log("==== MEMORY ====");
      log("Committed heap size: [0]=" + nodeStats[0].getJvm().getMem().getHeapCommitted() + ", [1]=" + nodeStats[1].getJvm().getMem().getHeapCommitted());
      log("Used heap size: [0]=" + nodeStats[0].getJvm().getMem().getHeapUsed() + ", [1]=" + nodeStats[1].getJvm().getMem().getHeapUsed());
      log("FieldData cache size: [0]=" + nodeStats[0].getIndices().getFieldData().getMemorySize() + ", [1]=" + nodeStats[1].getIndices().getFieldData().getMemorySize());
      log("Filter cache size: [0]=" + nodeStats[0].getIndices().getFilterCache().getMemorySize() + ", [1]=" + nodeStats[1].getIndices().getFilterCache().getMemorySize());
      log("");
      log("==== NETWORK ====");
      log("Transport: [0]=" + nodeStats[0].getTransport().toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS).string() + ", [1]=" + nodeStats[1].getTransport().toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS).string());
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
        log("Number of docs in index: " + client.prepareCount(PARENT_INDEX, CHILD_INDEX).setQuery(matchAllQuery()).execute().actionGet().getCount());
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

    public long runQuery(String name, int testNum, String index, long expectedHits, QueryBuilder query) {
        SearchResponse searchResponse = new CoordinateSearchRequestBuilder(client)
        .setIndices(index)
                .setQuery(query)
                .execute().actionGet();

        if (searchResponse.getFailedShards() > 0) {
            log("Search Failures " + Arrays.toString(searchResponse.getShardFailures()));
        }

        long hits = searchResponse.getHits().totalHits();
        if (hits != expectedHits) {
            log("[" + name + "][#" + testNum + "] Hits Mismatch:  expected [" + expectedHits + "], got [" + hits + "]");
        }

        return searchResponse.getTookInMillis();
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
        QueryBuilder mainQuery = matchAllQuery();

        FilterJoinBuilder stringFilter = FilterBuilders.filterJoin("id")
                .indices(CHILD_INDEX)
                .types(CHILD_TYPE)
                .path("pid");

        FilterJoinBuilder longFilter = FilterBuilders.filterJoin("num")
                .indices(CHILD_INDEX)
                .types(CHILD_TYPE)
                .path("num");

        long tookString = 0;
        long tookLong = 0;
        long expected = NUM_PARENTS;
        warmFieldData("id", "pid");     // for string fields
        warmFieldData("num", "num");    // for long fields

        log("==== HAS CHILD SINGLE TERM ====");
        for (int i = 0; i < NUM_QUERIES; i++) {
            lookupQuery = filteredQuery(matchAllQuery(), termFilter("tag", "tag" + random.nextInt(NUM_CHILDREN_PER_PARENT)));

            stringFilter.query(lookupQuery);
            longFilter.query(lookupQuery);

            tookString += runQuery("string", i, PARENT_INDEX, expected, filteredQuery(mainQuery, stringFilter));
            tookLong += runQuery("long", i, PARENT_INDEX, expected, filteredQuery(mainQuery, longFilter));
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
        QueryBuilder lookupQuery = matchAllQuery();
        QueryBuilder mainQuery = matchAllQuery();

        FilterJoinBuilder stringFilter = FilterBuilders.filterJoin("id")
                .indices(CHILD_INDEX)
                .types(CHILD_TYPE)
                .path("pid")
                .query(lookupQuery);

        FilterJoinBuilder longFilter = FilterBuilders.filterJoin("num")
                .indices(CHILD_INDEX)
                .types(CHILD_TYPE)
                .path("num")
                .query(lookupQuery);

        long tookString = 0;
        long tookLong = 0;
        long expected = NUM_PARENTS;
        warmFieldData("id", "pid");     // for string fields
        warmFieldData("num", "num");    // for long fields

        log("==== HAS CHILD MATCH-ALL ====");
        for (int i = 0; i < NUM_QUERIES; i++) {
            tookString += runQuery("string", i, PARENT_INDEX, expected, filteredQuery(mainQuery, stringFilter));
            tookLong += runQuery("long", i, PARENT_INDEX, expected, filteredQuery(mainQuery, longFilter));
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
        QueryBuilder mainQuery = matchAllQuery();

        FilterJoinBuilder stringFilter = FilterBuilders.filterJoin("pid")
                .indices(PARENT_INDEX)
                .types(PARENT_TYPE)
                .path("id");

        FilterJoinBuilder longFilter = FilterBuilders.filterJoin("num")
                .indices(PARENT_INDEX)
                .types(PARENT_TYPE)
                .path("num");

        long tookString = 0;
        long tookLong = 0;
        long expected = NUM_CHILDREN_PER_PARENT;
        warmFieldData("id", "pid");     // for string fields
        warmFieldData("num", "num");    // for long fields

        log("==== HAS PARENT SINGLE TERM ====");
        for (int i = 0; i < NUM_QUERIES; i++) {
            lookupQuery = filteredQuery(matchAllQuery(), termFilter("name", "test" + (random.nextInt(NUM_PARENTS) + 1)));

            stringFilter.query(lookupQuery);
            longFilter.query(lookupQuery);

            tookString += runQuery("string", i, CHILD_INDEX, expected, filteredQuery(mainQuery, stringFilter));
            tookLong += runQuery("long", i, CHILD_INDEX, expected, filteredQuery(mainQuery, longFilter));
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
        QueryBuilder lookupQuery = matchAllQuery();
        QueryBuilder mainQuery = matchAllQuery();

        FilterJoinBuilder stringFilter = FilterBuilders.filterJoin("pid")
                .indices(PARENT_INDEX)
                .types(PARENT_TYPE)
                .path("id")
                .query(lookupQuery);

        FilterJoinBuilder longFilter = FilterBuilders.filterJoin("num")
                .indices(PARENT_INDEX)
                .types(PARENT_TYPE)
                .path("num")
                .query(lookupQuery);

        long tookString = 0;
        long tookLong = 0;
        long expected = NUM_CHILDREN_PER_PARENT * NUM_PARENTS;
        warmFieldData("id", "pid");     // for string fields
        warmFieldData("num", "num");    // for numeric fields

        log("==== HAS PARENT MATCH-ALL ====");
        for (int i = 0; i < NUM_QUERIES; i++) {
            tookString += runQuery("string", i, CHILD_INDEX, expected, filteredQuery(mainQuery, stringFilter));
            tookLong += runQuery("long", i, CHILD_INDEX, expected, filteredQuery(mainQuery, longFilter));
        }

        log("string: " + (tookString / NUM_QUERIES) + "ms avg");
        log("long  : " + (tookLong / NUM_QUERIES) + "ms avg");
        log("");
    }

    /**
     * Search for children that have a parent with any of the specified names.
     * Expect NUM_CHILDREN_PER_PARENT * # of names.
     * <p/>
     * Parent string field = "id"
     * Parent long field = "num"
     * Child string field = "pid"
     * Child long field = "num"
     */
    public void benchHasParentRandomTerms() {
        QueryBuilder lookupQuery;
        QueryBuilder mainQuery = matchAllQuery();
        Set<String> names = new HashSet<>(NUM_PARENTS);

        FilterJoinBuilder stringFilter = FilterBuilders.filterJoin("pid")
                .indices(PARENT_INDEX)
                .types(PARENT_TYPE)
                .path("id");

        FilterJoinBuilder longFilter = FilterBuilders.filterJoin("num")
                .indices(PARENT_INDEX)
                .types(PARENT_TYPE)
                .path("num");

        long tookString = 0;
        long tookLong = 0;
        int expected = 0;
        warmFieldData("id", "pid");     // for string fields
        warmFieldData("num", "num");    // for long fields
        warmFieldData("name", null);    // for field data terms filter

        log("==== HAS PARENT RANDOM TERMS ====");
        for (int i = 0; i < NUM_QUERIES; i++) {

            // add a random number of terms to the set on each iteration
            int randNum = random.nextInt(NUM_PARENTS / NUM_QUERIES) + 1;
            for (int j = 0; j < randNum; j++) {
                names.add("test" + (random.nextInt(NUM_PARENTS) + 1));
            }

            lookupQuery = filteredQuery(matchAllQuery(), termsFilter("name", names).execution("fielddata"));
            expected = NUM_CHILDREN_PER_PARENT * names.size();
            stringFilter.query(lookupQuery);
            longFilter.query(lookupQuery);

            tookString += runQuery("string", i, CHILD_INDEX, expected, filteredQuery(mainQuery, stringFilter));
            tookLong += runQuery("long", i, CHILD_INDEX, expected, filteredQuery(mainQuery, longFilter));
        }

        log("string: " + (tookString / NUM_QUERIES) + "ms avg");
        log("long  : " + (tookLong / NUM_QUERIES) + "ms avg");
        log("");
    }
}
