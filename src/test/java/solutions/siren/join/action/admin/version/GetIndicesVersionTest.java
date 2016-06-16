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
package solutions.siren.join.action.admin.version;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;
import solutions.siren.join.SirenJoinTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;

@ESIntegTestCase.ClusterScope(scope=ESIntegTestCase.Scope.SUITE, numDataNodes=2)
public class GetIndicesVersionTest extends SirenJoinTestCase {

  @Override
  public Settings indexSettings() {
    Settings settings = super.indexSettings();
    return Settings.builder()
            .put(settings)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
  }

  @Test
  public void testUpdateDelete() throws Exception {
    assertAcked(prepareCreate("index1").addMapping("type", "id", "type=integer"));

    GetIndicesVersionResponse rsp = new GetIndicesVersionRequestBuilder(client(), GetIndicesVersionAction.INSTANCE).get();
    long version = rsp.getVersion();

    indexRandom(true,
            client().prepareIndex("index1", "type", "1").setSource("id", "1"));

    rsp = new GetIndicesVersionRequestBuilder(client(), GetIndicesVersionAction.INSTANCE).get();
    assertThat(rsp.getVersion(), is(not(equalTo(version))));

    version = rsp.getVersion();

    client().prepareDelete("index1", "type", "1").execute().get();

    rsp = new GetIndicesVersionRequestBuilder(client(), GetIndicesVersionAction.INSTANCE).get();
    assertThat(rsp.getVersion(), is(not(equalTo(version))));

    version = rsp.getVersion();

    rsp = new GetIndicesVersionRequestBuilder(client(), GetIndicesVersionAction.INSTANCE).get();
    assertThat(rsp.getVersion(), is(equalTo(version)));
  }

}
