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
package solutions.siren.join.action.terms.collector;

import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import org.apache.lucene.util.RamUsageEstimator;

import java.math.RoundingMode;
import java.util.Arrays;

/**
 * A bloom filter. Inspired by Guava bloom filter implementation though with some optimizations.
 */
public class LongBloomFilter {

  /**
   * The bit set of the BloomFilter (not necessarily power of 2!)
   */
  final BitArray bits;
  /**
   * Number of hashes per element
   */
  final int numHashFunctions;

  final Hashing hashing;

  /**
   * Creates a bloom filter based on the expected number
   * of insertions and expected false positive probability.
   *
   * @param expectedInsertions the number of expected insertions to the constructed
   * @param fpp                the desired false positive probability (must be positive and less than 1.0)
   */
  public LongBloomFilter(int expectedInsertions, double fpp) {
    this(expectedInsertions, fpp, -1);
  }

  /**
   * Creates a bloom filter based on the expected number of insertions, expected false positive probability,
   * and number of hash functions.
   *
   * @param expectedInsertions the number of expected insertions to the constructed
   * @param fpp                the desired false positive probability (must be positive and less than 1.0)
   * @param numHashFunctions   the number of hash functions to use (must be less than or equal to 255)
   */
  LongBloomFilter(int expectedInsertions, double fpp, int numHashFunctions) {
    if (expectedInsertions == 0) {
      expectedInsertions = 1;
    }
      /*
       * TODO(user): Put a warning in the javadoc about tiny fpp values,
       * since the resulting size is proportional to -log(p), but there is not
       * much of a point after all, e.g. optimalM(1000, 0.0000000000000001) = 76680
       * which is less that 10kb. Who cares!
       */
    long numBits = optimalNumOfBits(expectedInsertions, fpp);
    this.bits = this.createBitArray(numBits);

    // calculate the optimal number of hash functions
    if (numHashFunctions == -1) {
      numHashFunctions = optimalNumOfHashFunctions(expectedInsertions, numBits);
    }

    this.numHashFunctions = numHashFunctions;
    this.hashing = Hashing.DEFAULT;

    /*
     * This only exists to forbid BFs that cannot use the compact persistent representation.
     * If it ever throws, at a user who was not intending to use that representation, we should
     * reconsider
     */
    if (numHashFunctions > 255) {
      throw new IllegalArgumentException("Currently we don't allow BloomFilters that would use more than 255 hash functions");
    }
  }

  /**
   * Constructor used during serialisation
   */
  LongBloomFilter(BitArray bits, int numHashFunctions, Hashing hashing) {
    this.bits = bits;
    this.numHashFunctions = numHashFunctions;
    this.hashing = hashing;
  }

  protected BitArray createBitArray(long numBits) {
    return new BitArray(numBits);
  }

  /**
   * Removes all elements from the set and additionally releases any internal buffers.
   */
  protected void release() {
    bits.data = new long[0];
    bits.bitSize = 0;
    bits.bitCount = 0;
  }

  public boolean put(long hash64) {
    return hashing.put(hash64, numHashFunctions, bits);
  }

  /**
   * Merge with another {@link LongBloomFilter}. The other bloom filter must have the same size, number of hash
   * functions, and hashing type.
   *
   * @param other the other {@link LongBloomFilter} to merge with.
   */
  public void merge(LongBloomFilter other) {
    if (bits.data.length != other.bits.data.length ||
        numHashFunctions != other.getNumHashFunctions() ||
        hashing.type() != other.hashing.type()) {
      throw new IllegalArgumentException("BloomFilters must have same size, number of hash functions, and hash type");
    }

    bits.putAll(other.bits);
  }

  public boolean mightContain(long hash64) {
    return hashing.mightContain(hash64, numHashFunctions, bits);
  }

  public int getNumHashFunctions() {
    return this.numHashFunctions;
  }

  public int getSizeInBytes() {
    return bits.ramBytesUsed();
  }

  /**
   * Estimates the cardinality of the bloom filter based on the bloom filter length (m), number of bits set to true (t)
   * and number of hash functions (k). The estimation is derived from the maximum likelihood value for the number of
   * hashed elements as defined in "Cardinality estimation and dynamic length adaptation for Bloom filters" from
   * O. Papapetrou, W. Silberski, W. Nedjl.
   */
  public int estimateCardinality() {
    double t = bits.bitCount();
    double m = bits.bitSize();
    double k = numHashFunctions;

    double x = Math.log(1 - (t / m));
    double y = k * Math.log(1 - (1 / m));

    return (int) Math.ceil(x / y);
  }

  @Override
  public int hashCode() {
    return bits.hashCode() + numHashFunctions;
  }

  /*
   * Cheat sheet:
   *
   * m: total bits
   * n: expected insertions
   * b: m/n, bits per insertion

   * p: expected false positive probability
   *
   * 1) Optimal k = b * ln2
   * 2) p = (1 - e ^ (-kn/m))^k
   * 3) For optimal k: p = 2 ^ (-k) ~= 0.6185^b
   * 4) For optimal k: m = -nlnp / ((ln2) ^ 2)
   */

  /**
   * Computes the optimal k (number of hashes per element inserted in Bloom filter), given the
   * expected insertions and total number of bits in the Bloom filter.
   * <p/>
   * See http://en.wikipedia.org/wiki/File:Bloom_filter_fp_probability.svg for the formula.
   *
   * @param n expected insertions (must be positive)
   * @param m total number of bits in Bloom filter (must be positive)
   */
  static int optimalNumOfHashFunctions(long n, long m) {
    return Math.max(1, (int) Math.round(m / n * Math.log(2)));
  }

  /**
   * Computes m (total bits of Bloom filter) which is expected to achieve, for the specified
   * expected insertions, the required false positive probability.
   * <p/>
   * See http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives for the formula.
   *
   * @param n expected insertions (must be positive)
   * @param p false positive rate (must be 0 < p < 1)
   */
  static long optimalNumOfBits(long n, double p) {
    if (p == 0) {
      p = Double.MIN_VALUE;
    }
    return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
  }

  // Note: We use this instead of java.util.BitSet because we need access to the long[] data field
  static final class BitArray {

    long[] data;
    long bitSize;
    long bitCount;

    BitArray(long bits) {
      this(new long[Ints.checkedCast(LongMath.divide(bits, 64, RoundingMode.CEILING))]);
    }

    // Used by serialization
    BitArray(long[] data) {
      this.data = data;
      long bitCount = 0;
      for (long value : data) {
        bitCount += Long.bitCount(value);
      }
      this.bitCount = bitCount;
      this.bitSize = data.length * Long.SIZE;
    }

    /** Returns true if the bit changed value. */
    boolean set(long index) {
      if (!get(index)) {
        data[(int) (index >>> 6)] |= (1L << index);
        bitCount++;
        return true;
      }
      return false;
    }

    boolean get(long index) {
      return (data[(int) (index >>> 6)] & (1L << index)) != 0;
    }

    /** Number of bits */
    long bitSize() {
      return bitSize;
    }

    /** Number of set bits (1s) */
    long bitCount() {
      return bitCount;
    }

    BitArray copy() {
      return new BitArray(data.clone());
    }

    /** Combines the two BitArrays using bitwise OR. */
    void putAll(BitArray array) {
      bitCount = 0;
      for (int i = 0; i < data.length; i++) {
        data[i] |= array.data[i];
        bitCount += Long.bitCount(data[i]);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof BitArray) {
        BitArray bitArray = (BitArray) o;
        return Arrays.equals(data, bitArray.data);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(data);
    }

    public int ramBytesUsed() {
      return RamUsageEstimator.NUM_BYTES_LONG * data.length + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + 16;
    }

  }

  enum Hashing {

    V0() {
      @Override
      protected boolean put(long hash64, int numHashFunctions, BitArray bits) {
        long bitSize = bits.bitSize();
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        boolean bitsChanged = false;
        long combinedHash = hash1;
        for (int i = 1; i <= numHashFunctions; i++) {
          // Make the combined hash positive and indexable
          bitsChanged |= bits.set((combinedHash & Long.MAX_VALUE) % bitSize);
          combinedHash += hash2;
        }
        return bitsChanged;
      }

      @Override
      protected boolean mightContain(long hash64, int numHashFunctions, BitArray bits) {
        long bitSize = bits.bitSize();
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        long combinedHash = hash1;
        for (int i = 1; i <= numHashFunctions; i++) {
          // Make the combined hash positive and indexable
          if (!bits.get((combinedHash & Long.MAX_VALUE) % bitSize)) {
            return false;
          }
          combinedHash += hash2;
        }
        return true;
      }

      @Override
      protected int type() {
        return 0;
      }
    };

    protected abstract boolean put(long hash64, int numHashFunctions, BitArray bits);

    protected abstract boolean mightContain(long hash64, int numHashFunctions, BitArray bits);

    protected abstract int type();

    public static final Hashing DEFAULT = Hashing.V0;

    public static Hashing fromType(int type) {
      if (type == 0) {
        return Hashing.V0;
      } else {
        throw new IllegalArgumentException("no hashing type matching " + type);
      }
    }
  }

  // START : MURMUR 3_128 USED FOR Hashing.V0
  // NOTE: don't replace this code with the o.e.common.hashing.MurmurHash3 method which returns a different hash

  protected static long getblock(byte[] key, int offset, int index) {
    int i_8 = index << 3;
    int blockOffset = offset + i_8;
    return ((long) key[blockOffset + 0] & 0xff) + (((long) key[blockOffset + 1] & 0xff) << 8) +
            (((long) key[blockOffset + 2] & 0xff) << 16) + (((long) key[blockOffset + 3] & 0xff) << 24) +
            (((long) key[blockOffset + 4] & 0xff) << 32) + (((long) key[blockOffset + 5] & 0xff) << 40) +
            (((long) key[blockOffset + 6] & 0xff) << 48) + (((long) key[blockOffset + 7] & 0xff) << 56);
  }

  protected static long rotl64(long v, int n) {
    return ((v << n) | (v >>> (64 - n)));
  }

  protected static long fmix(long k) {
    k ^= k >>> 33;
    k *= 0xff51afd7ed558ccdL;
    k ^= k >>> 33;
    k *= 0xc4ceb9fe1a85ec53L;
    k ^= k >>> 33;

    return k;
  }

  public static long hash3_x64_128(byte[] key, int offset, int length, long seed) {
    final int nblocks = length >> 4; // Process as 128-bit blocks.

    long h1 = seed;
    long h2 = seed;

    long c1 = 0x87c37b91114253d5L;
    long c2 = 0x4cf5ad432745937fL;

    //----------
    // body

    for (int i = 0; i < nblocks; i++) {
      long k1 = getblock(key, offset, i * 2 + 0);
      long k2 = getblock(key, offset, i * 2 + 1);

      k1 *= c1;
      k1 = rotl64(k1, 31);
      k1 *= c2;
      h1 ^= k1;

      h1 = rotl64(h1, 27);
      h1 += h2;
      h1 = h1 * 5 + 0x52dce729;

      k2 *= c2;
      k2 = rotl64(k2, 33);
      k2 *= c1;
      h2 ^= k2;

      h2 = rotl64(h2, 31);
      h2 += h1;
      h2 = h2 * 5 + 0x38495ab5;
    }

    //----------
    // tail

    // Advance offset to the unprocessed tail of the data.
    offset += nblocks * 16;

    long k1 = 0;
    long k2 = 0;

    switch (length & 15) {
      case 15:
        k2 ^= ((long) key[offset + 14]) << 48;
      case 14:
        k2 ^= ((long) key[offset + 13]) << 40;
      case 13:
        k2 ^= ((long) key[offset + 12]) << 32;
      case 12:
        k2 ^= ((long) key[offset + 11]) << 24;
      case 11:
        k2 ^= ((long) key[offset + 10]) << 16;
      case 10:
        k2 ^= ((long) key[offset + 9]) << 8;
      case 9:
        k2 ^= ((long) key[offset + 8]) << 0;
        k2 *= c2;
        k2 = rotl64(k2, 33);
        k2 *= c1;
        h2 ^= k2;

      case 8:
        k1 ^= ((long) key[offset + 7]) << 56;
      case 7:
        k1 ^= ((long) key[offset + 6]) << 48;
      case 6:
        k1 ^= ((long) key[offset + 5]) << 40;
      case 5:
        k1 ^= ((long) key[offset + 4]) << 32;
      case 4:
        k1 ^= ((long) key[offset + 3]) << 24;
      case 3:
        k1 ^= ((long) key[offset + 2]) << 16;
      case 2:
        k1 ^= ((long) key[offset + 1]) << 8;
      case 1:
        k1 ^= ((long) key[offset]);
        k1 *= c1;
        k1 = rotl64(k1, 31);
        k1 *= c2;
        h1 ^= k1;
    }

    //----------
    // finalization

    h1 ^= length;
    h2 ^= length;

    h1 += h2;
    h2 += h1;

    h1 = fmix(h1);
    h2 = fmix(h2);

    h1 += h2;
    h2 += h1;

    // SAME AS GUAVA, they take the first long out of the 128bit
    return h1;
  }

  // END: MURMUR 3_128
}