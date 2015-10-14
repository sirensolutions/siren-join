/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.PluginInfo;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.notNullValue;

@ElasticsearchIntegrationTest.ClusterScope(scope=ElasticsearchIntegrationTest.Scope.SUITE, numDataNodes=1)
public class FilterJoinPluginTest extends FilterJoinTestCase {

  @Test
  public void testPluginLoaded() {
    NodesInfoResponse nodesInfoResponse = client().admin().cluster().prepareNodesInfo().clear().setPlugins(true).get();
    assertTrue(nodesInfoResponse.getNodes().length != 0);
    assertThat(nodesInfoResponse.getNodes()[0].getPlugins().getInfos(), notNullValue());
    assertThat(nodesInfoResponse.getNodes()[0].getPlugins().getInfos().size(), not(0));

    boolean pluginFound = false;

    for (PluginInfo pluginInfo : nodesInfoResponse.getNodes()[0].getPlugins().getInfos()) {
      if (pluginInfo.getName().equals("FilterJoinPlugin")) {
        pluginFound = true;
        break;
      }
    }

    assertThat(pluginFound, is(true));
  }

}
