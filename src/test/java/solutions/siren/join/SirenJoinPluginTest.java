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

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope= ESIntegTestCase.Scope.SUITE, numDataNodes=1)
public class SirenJoinPluginTest extends SirenJoinTestCase {

  @Test
  public void testPluginLoaded() {
    NodesInfoResponse nodesInfoResponse = client().admin().cluster().prepareNodesInfo().clear().setPlugins(true).get();
    assertTrue(nodesInfoResponse.getNodes().length != 0);
    assertThat(nodesInfoResponse.getNodes()[0].getPlugins().getPluginInfos(), notNullValue());
    assertThat(nodesInfoResponse.getNodes()[0].getPlugins().getPluginInfos().size(), not(0));

    boolean pluginFound = false;

    for (PluginInfo pluginInfo : nodesInfoResponse.getNodes()[0].getPlugins().getPluginInfos()) {
      if (pluginInfo.getName().equals("SirenJoinPlugin")) {
        pluginFound = true;
        break;
      }
    }

    assertThat(pluginFound, is(true));
  }

}
