package solutions.siren.join.action.terms;

import org.apache.lucene.util.BytesRef;
import org.junit.Assert;
import org.junit.Test;
import solutions.siren.join.action.terms.collector.LongBloomFilter;

import java.util.*;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

public class BloomFilterTest {

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
      assertThat(ratio, closeTo(0.01, 0.001));
    }
  }

}
