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

import org.elasticsearch.test.ESSingleNodeTestCase;

import org.junit.Test;

import solutions.siren.join.action.terms.collector.LongBloomFilter;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.hamcrest.Matchers.closeTo;

public class BloomFilterTest extends ESSingleNodeTestCase {

  @Test
  public void testCardinalityEstimation() {
    Random r = new Random();

    LongBloomFilter instance = new LongBloomFilter(100000, 0.01);
    for (int i = 0; i < 10000; i++) {
      long value = r.nextLong();
      instance.put(value);
    }

    assertThat((double) instance.estimateCardinality(), closeTo(10000, 10000 * 0.01));
  }

  @Test
  public void testBloomFilter() {
    // Numbers are from // http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html

    Random r = new Random();

    for (int j = 10; j < 21; j++) {
      Set<Long> values = new HashSet<>();
      LongBloomFilter instance = new LongBloomFilter(1000, 0.01);

      for (int i = 0; i < 1000; i++) {
        long value = r.nextLong();
        values.add(value);
        instance.put(value);
      }

      long f = 0;
      double tests = 3000000;
      for (int i = 0; i < tests; i++) {
        long value = r.nextLong();
        if (instance.mightContain(value)) {
          if (!values.contains(value)) {
            f++;
          }
        }
      }

      double ratio = f / tests;
      assertThat(ratio, closeTo(0.01, 0.002));
    }
  }

}