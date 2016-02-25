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

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Test;
import solutions.siren.join.action.terms.collector.IntegerTermsSet;
import solutions.siren.join.action.terms.collector.LongTermsSet;

import static org.hamcrest.Matchers.*;

public class TermsSetTest extends ESSingleNodeTestCase {

  @Test(expected=CircuitBreakingException.class)
  public void testCircuitBreakerOnNewLongTermsSet() {
    final int size = 42;
    HierarchyCircuitBreakerService hcbs = new HierarchyCircuitBreakerService(
            Settings.builder()
                    .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING, size - 1, ByteSizeUnit.BYTES)
                    .build(),
            new NodeSettingsService(Settings.EMPTY));

    LongTermsSet termsSet = new LongTermsSet(size, hcbs.getBreaker(CircuitBreaker.REQUEST));
  }

  @Test(expected=CircuitBreakingException.class)
  public void testCircuitBreakerOnNewIntTermsSet() {
    final int size = 42;
    HierarchyCircuitBreakerService hcbs = new HierarchyCircuitBreakerService(
            Settings.builder()
                    .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING, size - 1, ByteSizeUnit.BYTES)
                    .build(),
            new NodeSettingsService(Settings.EMPTY));

    IntegerTermsSet termsSet = new IntegerTermsSet(size, hcbs.getBreaker(CircuitBreaker.REQUEST));
  }

  @Test
  public void testCircuitBreakerAdjustmentOnLongTermsSet() {
    HierarchyCircuitBreakerService hcbs = new HierarchyCircuitBreakerService(
            Settings.builder().build(),
            new NodeSettingsService(Settings.EMPTY));

    CircuitBreaker breaker = hcbs.getBreaker(CircuitBreaker.REQUEST);
    assertThat(breaker.getUsed(), is(equalTo(0L)));

    LongTermsSet termsSet = new LongTermsSet(8, hcbs.getBreaker(CircuitBreaker.REQUEST));
    long usedMem = breaker.getUsed();
    assertThat(usedMem, greaterThan(0L));

    for (int i = 0; i < 16; i++) {
      termsSet.add(i);
    }

    assertThat(breaker.getUsed(), greaterThan(usedMem));

    termsSet.release();

    assertThat(breaker.getUsed(), is(equalTo(0L)));
  }

  @Test
  public void testCircuitBreakerAdjustmentOnIntTermsSet() {
    HierarchyCircuitBreakerService hcbs = new HierarchyCircuitBreakerService(
            Settings.builder().build(),
            new NodeSettingsService(Settings.EMPTY));

    CircuitBreaker breaker = hcbs.getBreaker(CircuitBreaker.REQUEST);
    assertThat(breaker.getUsed(), is(equalTo(0L)));

    IntegerTermsSet termsSet = new IntegerTermsSet(8, hcbs.getBreaker(CircuitBreaker.REQUEST));
    long usedMem = breaker.getUsed();
    assertThat(usedMem, greaterThan(0L));

    for (int i = 0; i < 16; i++) {
      termsSet.add(i);
    }

    assertThat(breaker.getUsed(), greaterThan(usedMem));

    termsSet.release();

    assertThat(breaker.getUsed(), is(equalTo(0L)));
  }

}
