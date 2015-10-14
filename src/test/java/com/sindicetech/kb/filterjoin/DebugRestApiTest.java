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
package com.sindicetech.kb.filterjoin;

import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.test.rest.client.RestException;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;

@Ignore
public class DebugRestApiTest {

  protected static final ESLogger logger = Loggers.getLogger(DebugRestApiTest.class);

  @Test
  public void testCoordinateMultiSearchApi() throws IOException, RestException, ExecutionException, InterruptedException {

    Settings settings = ImmutableSettings.settingsBuilder()
            .put("path.home", "/home/renaud/Work/cerved/elasticsearch-1.6.0/")
            .put("http.enabled", "true")
            .put("path.data", "/home/renaud/Work/cerved/elasticsearch-1.6.0/data")
            .build();

    Node node = NodeBuilder.nodeBuilder().settings(settings).node();

    Thread.sleep(2000);

    String q = "{\"index\":\"company\",\"search_type\":\"count\",\"ignore_unavailable\":true}\n" +
            "{\"size\":0,\"aggs\":{\"2\":{\"filters\":{\"filters\":{}}}},\"query\":{\"filtered\":{\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"*\"}},\"filter\":{\"bool\":{\"must\":[{\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"lince\"}}},{\"filterjoin\":{\"mentioned.id\":{\"index\":\"news\",\"type\":\"News\",\"path\":\"id\",\"query\":{\"filtered\":{\"query\":{\"bool\":{\"must\":[{\"match_all\":{}}]}},\"filter\":{\"bool\":{\"must\":[{\"range\":{\"n_dataPubblicazioneArticolo\":{\"gte\":963739376862,\"lte\":1437038576862}}},{\"filterjoin\":{\"c_sourceID\":{\"index\":\"companies-csv\",\"type\":\"companies\",\"path\":\"ID_SOGGETTO\",\"query\":{\"filtered\":{\"query\":{\"bool\":{\"must\":[{\"match_all\":{}}]}},\"filter\":{\"bool\":{\"must\":[]}}}}}}}]}}}}}}}],\"must_not\":[]}}}},\"highlight\":{\"pre_tags\":[\"@kibana-highlighted-field@\"],\"post_tags\":[\"@/kibana-highlighted-field@\"],\"fields\":{\"*\":{}},\"fragment_size\":2147483647}}\n" +
            "{\"index\":\"company\",\"search_type\":\"count\",\"ignore_unavailable\":true}\n" +
            "{\"query\":{\"filtered\":{\"query\":{\"query_string\":{\"query\":\"*\",\"analyze_wildcard\":true}},\"filter\":{\"bool\":{\"must\":[{\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"lince\"}}},{\"filterjoin\":{\"mentioned.id\":{\"index\":\"news\",\"type\":\"News\",\"path\":\"id\",\"query\":{\"filtered\":{\"query\":{\"bool\":{\"must\":[{\"match_all\":{}}]}},\"filter\":{\"bool\":{\"must\":[{\"range\":{\"n_dataPubblicazioneArticolo\":{\"gte\":963739376862,\"lte\":1437038576862}}},{\"filterjoin\":{\"c_sourceID\":{\"index\":\"companies-csv\",\"type\":\"companies\",\"path\":\"ID_SOGGETTO\",\"query\":{\"filtered\":{\"query\":{\"bool\":{\"must\":[{\"match_all\":{}}]}},\"filter\":{\"bool\":{\"must\":[]}}}}}}}]}}}}}}}],\"must_not\":[]}}}},\"size\":0,\"aggs\":{}}\n" +
            "{\"index\":\"company\",\"search_type\":\"count\",\"ignore_unavailable\":true}\n" +
            "{\"size\":0,\"aggs\":{},\"query\":{\"filtered\":{\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"*\"}},\"filter\":{\"bool\":{\"must\":[{\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"lince\"}}},{\"filterjoin\":{\"mentioned.id\":{\"index\":\"news\",\"type\":\"News\",\"path\":\"id\",\"query\":{\"filtered\":{\"query\":{\"bool\":{\"must\":[{\"match_all\":{}}]}},\"filter\":{\"bool\":{\"must\":[{\"range\":{\"n_dataPubblicazioneArticolo\":{\"gte\":963739376862,\"lte\":1437038576862}}},{\"filterjoin\":{\"c_sourceID\":{\"index\":\"companies-csv\",\"type\":\"companies\",\"path\":\"ID_SOGGETTO\",\"query\":{\"filtered\":{\"query\":{\"bool\":{\"must\":[{\"match_all\":{}}]}},\"filter\":{\"bool\":{\"must\":[]}}}}}}}]}}}}}}}],\"must_not\":[]}}}},\"highlight\":{\"pre_tags\":[\"@kibana-highlighted-field@\"],\"post_tags\":[\"@/kibana-highlighted-field@\"],\"fields\":{\"*\":{}},\"fragment_size\":2147483647}}\n" +
            "{\"index\":\"company\",\"search_type\":\"count\",\"ignore_unavailable\":true}\n";

    HttpResponse response = this.httpClient(node.client()).method("GET").path("/_coordinate_msearch").body(q).execute();
    Map<String, Object> map = XContentHelper.convertToMap(response.getBody().getBytes("UTF-8"), false).v2();
    ArrayList responses = (ArrayList) map.get("responses");
    System.out.println(responses.get(0));
    System.out.println(responses.get(1));
    System.out.println(responses.get(2));

    response = this.httpClient(node.client()).method("GET").path("/_coordinate_msearch").body(q).execute();
    map = XContentHelper.convertToMap(response.getBody().getBytes("UTF-8"), false).v2();
    responses = (ArrayList) map.get("responses");
    System.out.println(responses.get(0));
    System.out.println(responses.get(1));
    System.out.println(responses.get(2));

    node.close();
  }

  @Test
  public void testCaching() throws IOException, RestException, ExecutionException, InterruptedException {

    Settings settings = ImmutableSettings.settingsBuilder()
            .put("path.home", "/home/renaud/Work/cerved/elasticsearch-1.6.0/")
            .put("http.enabled", "true")
            .put("path.data", "/home/renaud/Work/cerved/elasticsearch-1.6.0/data")
            .build();

    Node node = NodeBuilder.nodeBuilder().settings(settings).node();

    Thread.sleep(2000);

    String q = "{\"index\":\"news\",\"search_type\":\"count\",\"ignore_unavailable\":true}\n" +
            "{\"size\":0,\"aggs\":{},\"query\":{\"filtered\":{\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"*\"}},\"filter\":{\"bool\":{\"must\":[{\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"*\"}}},{\"range\":{\"n_dataPubblicazioneArticolo\":{\"gte\":963844643677,\"lte\":1437143843677}}},{\"filterjoin\":{\"c_sourceID\":{\"index\":\"companies-csv\",\"type\":\"companies\",\"path\":\"ID_SOGGETTO\",\"query\":{\"filtered\":{\"query\":{\"bool\":{\"must\":[{\"match_all\":{}}]}},\"filter\":{\"bool\":{\"must\":[]}}}}}}},{\"filterjoin\":{\"id\":{\"index\":\"company\",\"type\":\"Company\",\"path\":\"mentioned.id\",\"query\":{\"filtered\":{\"query\":{\"bool\":{\"must\":[{\"match_all\":{}}]}},\"filter\":{\"bool\":{\"must\":[]}}}}}}}],\"must_not\":[]}}}},\"highlight\":{\"pre_tags\":[\"@kibana-highlighted-field@\"],\"post_tags\":[\"@/kibana-highlighted-field@\"],\"fields\":{\"*\":{}},\"fragment_size\":2147483647}}\n" +
            "{\"index\":\"news\",\"search_type\":\"count\",\"ignore_unavailable\":true}\n" +
            "{\"size\":0,\"aggs\":{\"2\":{\"terms\":{\"field\":\"n_fonte\",\"size\":20,\"order\":{\"_count\":\"desc\"}}}},\"query\":{\"filtered\":{\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"*\"}},\"filter\":{\"bool\":{\"must\":[{\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"*\"}}},{\"range\":{\"n_dataPubblicazioneArticolo\":{\"gte\":963844643677,\"lte\":1437143843677}}},{\"filterjoin\":{\"c_sourceID\":{\"index\":\"companies-csv\",\"type\":\"companies\",\"path\":\"ID_SOGGETTO\",\"query\":{\"filtered\":{\"query\":{\"bool\":{\"must\":[{\"match_all\":{}}]}},\"filter\":{\"bool\":{\"must\":[]}}}}}}},{\"filterjoin\":{\"id\":{\"index\":\"company\",\"type\":\"Company\",\"path\":\"mentioned.id\",\"query\":{\"filtered\":{\"query\":{\"bool\":{\"must\":[{\"match_all\":{}}]}},\"filter\":{\"bool\":{\"must\":[]}}}}}}}],\"must_not\":[]}}}},\"highlight\":{\"pre_tags\":[\"@kibana-highlighted-field@\"],\"post_tags\":[\"@/kibana-highlighted-field@\"],\"fields\":{\"*\":{}},\"fragment_size\":2147483647}}\n" +
            "{\"index\":\"news\",\"search_type\":\"count\",\"ignore_unavailable\":true}\n" +
            "{\"size\":0,\"aggs\":{\"2\":{\"terms\":{\"field\":\"mentions.ls_label\",\"size\":5,\"order\":{\"_count\":\"desc\"}}}},\"query\":{\"filtered\":{\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"*\"}},\"filter\":{\"bool\":{\"must\":[{\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"*\"}}},{\"range\":{\"n_dataPubblicazioneArticolo\":{\"gte\":963844643677,\"lte\":1437143843677}}},{\"filterjoin\":{\"c_sourceID\":{\"index\":\"companies-csv\",\"type\":\"companies\",\"path\":\"ID_SOGGETTO\",\"query\":{\"filtered\":{\"query\":{\"bool\":{\"must\":[{\"match_all\":{}}]}},\"filter\":{\"bool\":{\"must\":[]}}}}}}},{\"filterjoin\":{\"id\":{\"index\":\"company\",\"type\":\"Company\",\"path\":\"mentioned.id\",\"query\":{\"filtered\":{\"query\":{\"bool\":{\"must\":[{\"match_all\":{}}]}},\"filter\":{\"bool\":{\"must\":[]}}}}}}}],\"must_not\":[]}}}},\"highlight\":{\"pre_tags\":[\"@kibana-highlighted-field@\"],\"post_tags\":[\"@/kibana-highlighted-field@\"],\"fields\":{\"*\":{}},\"fragment_size\":2147483647}}\n" +
            "{\"index\":\"news\",\"search_type\":\"count\",\"ignore_unavailable\":true}\n" +
            "{\"size\":0,\"aggs\":{\"2\":{\"date_histogram\":{\"field\":\"n_dataPubblicazioneArticolo\",\"interval\":\"1M\",\"pre_zone\":\"+01:00\",\"pre_zone_adjust_large_interval\":true,\"min_doc_count\":1,\"extended_bounds\":{\"min\":963844643660,\"max\":1437143843660}}}},\"query\":{\"filtered\":{\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"*\"}},\"filter\":{\"bool\":{\"must\":[{\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"*\"}}},{\"range\":{\"n_dataPubblicazioneArticolo\":{\"gte\":963844643677,\"lte\":1437143843678}}},{\"filterjoin\":{\"c_sourceID\":{\"index\":\"companies-csv\",\"type\":\"companies\",\"path\":\"ID_SOGGETTO\",\"query\":{\"filtered\":{\"query\":{\"bool\":{\"must\":[{\"match_all\":{}}]}},\"filter\":{\"bool\":{\"must\":[]}}}}}}},{\"filterjoin\":{\"id\":{\"index\":\"company\",\"type\":\"Company\",\"path\":\"mentioned.id\",\"query\":{\"filtered\":{\"query\":{\"bool\":{\"must\":[{\"match_all\":{}}]}},\"filter\":{\"bool\":{\"must\":[]}}}}}}}],\"must_not\":[]}}}},\"highlight\":{\"pre_tags\":[\"@kibana-highlighted-field@\"],\"post_tags\":[\"@/kibana-highlighted-field@\"],\"fields\":{\"*\":{}},\"fragment_size\":2147483647}}\n" +
            "{\"index\":\"news\",\"search_type\":\"count\",\"ignore_unavailable\":true}\n" +
            "{\"size\":0,\"aggs\":{\"2\":{\"range\":{\"field\":\"mentions.c_numberOfEmployees\",\"ranges\":[{\"from\":0,\"to\":10},{\"from\":11,\"to\":100},{\"to\":500,\"from\":101},{\"to\":20000,\"from\":501},{\"from\":20001,\"to\":1000000}],\"keyed\":true}}},\"query\":{\"filtered\":{\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"*\"}},\"filter\":{\"bool\":{\"must\":[{\"query\":{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"*\"}}},{\"range\":{\"n_dataPubblicazioneArticolo\":{\"gte\":963844643678,\"lte\":1437143843678}}},{\"filterjoin\":{\"c_sourceID\":{\"index\":\"companies-csv\",\"type\":\"companies\",\"path\":\"ID_SOGGETTO\",\"query\":{\"filtered\":{\"query\":{\"bool\":{\"must\":[{\"match_all\":{}}]}},\"filter\":{\"bool\":{\"must\":[]}}}}}}},{\"filterjoin\":{\"id\":{\"index\":\"company\",\"type\":\"Company\",\"path\":\"mentioned.id\",\"query\":{\"filtered\":{\"query\":{\"bool\":{\"must\":[{\"match_all\":{}}]}},\"filter\":{\"bool\":{\"must\":[]}}}}}}}],\"must_not\":[]}}}},\"highlight\":{\"pre_tags\":[\"@kibana-highlighted-field@\"],\"post_tags\":[\"@/kibana-highlighted-field@\"],\"fields\":{\"*\":{}},\"fragment_size\":2147483647}}\n";

    HttpResponse response = this.httpClient(node.client()).method("GET").path("/_coordinate_msearch").body(q).execute();
    Map<String, Object> map = XContentHelper.convertToMap(response.getBody().getBytes("UTF-8"), false).v2();
    ArrayList responses = (ArrayList) map.get("responses");
    for (Object r : responses) {
      System.out.println(r);
    }

    for (int i = 0; i < 5; i++) {
      response = this.httpClient(node.client()).method("GET").path("/_coordinate_msearch").body(q).execute();
      map = XContentHelper.convertToMap(response.getBody().getBytes("UTF-8"), false).v2();
      responses = (ArrayList) map.get("responses");
      for (Object r : responses) {
        System.out.println(r);
      }
    }

    node.close();
  }

  protected HttpRequestBuilder httpClient(Client client) {
    final NodesInfoResponse nodeInfos = client.admin().cluster().prepareNodesInfo().get();
    final NodeInfo[] nodes = nodeInfos.getNodes();
    TransportAddress publishAddress = nodes[0].getHttp().address().publishAddress();
    InetSocketAddress address = ((InetSocketTransportAddress) publishAddress).address();
    return new HttpRequestBuilder(HttpClients.createDefault()).host(address.getHostName()).port(address.getPort());
  }


  protected HttpRequestBuilder httpClient() {
    return new HttpRequestBuilder(HttpClients.createDefault()).host("localhost").port(9200);
  }

  public ClusterHealthStatus ensureGreen(Client client, String... indices) {
    ClusterHealthResponse actionGet = client.admin().cluster()
            .health(Requests.clusterHealthRequest(indices).timeout(TimeValue.timeValueSeconds(30)).waitForYellowStatus().waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)).actionGet();
    if (actionGet.isTimedOut()) {
      logger.info("ensureGreen timed out, cluster state:\n{}\n{}", client.admin().cluster().prepareState().get().getState().prettyPrint(), client.admin().cluster().preparePendingClusterTasks().get().prettyPrint());
      throw new AssertionError("timed out waiting for green state");
    }
    Assert.assertThat(actionGet.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
    logger.debug("indices {} are green", indices.length == 0 ? "[_all]" : indices);
    return actionGet.getStatus();
  }  
  
}
