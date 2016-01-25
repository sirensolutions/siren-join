package solutions.siren.join.action.terms;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

public class BloomFilterTest {

  @Test
  public void testBloomFilter() throws Exception {
    int size = 10000000;
    double fpp = 0.001;

    BloomFilter<Long> bloomFilter = BloomFilter.create(Funnels.longFunnel(), size, fpp);
    for (long i  = 0; i < size; i++) {
      bloomFilter.put(i);
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    bloomFilter.writeTo(baos);
    System.out.println(baos.size());
  }

}
