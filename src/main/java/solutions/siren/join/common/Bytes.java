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
package solutions.siren.join.common;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import solutions.siren.join.action.terms.collector.BytesRefTermsSet;
import solutions.siren.join.action.terms.collector.LongTermsSet;
import solutions.siren.join.index.query.FieldDataTermsQueryParser;

import java.io.IOException;

/**
 * Helper class for byte encoding and decoding of values.
 */
public class Bytes {

  /**
   * Encodes a list of longs into a serialised {@link LongTermsSet}.
   */
  public final static byte[] encode(long[] values) throws IOException {
    LongTermsSet termsSet = new LongTermsSet(values.length, new NoopCircuitBreaker("<term_set>"));
    for (int i = 0; i < values.length; i++) {
      termsSet.add(values[i]);
    }
    return termsSet.writeToBytes().bytes;
  }

  /**
   * Encodes a list of {@link BytesRef} into a serialised {@link solutions.siren.join.action.terms.collector.BytesRefTermsSet}.
   */
  public final static byte[] encode(BytesRef[] values) throws IOException {
    BytesRefTermsSet termsSet = new BytesRefTermsSet(new NoopCircuitBreaker("<term_set>"));
    for (int i = 0; i < values.length; i++) {
      termsSet.add(values[i]);
    }
    return termsSet.writeToBytes().bytes;
  }

  /**
   * Encodes a long into the byte array dst at the given offset.
   */
  public final static void writeInt(BytesRef dst, int i) {
    dst.bytes[dst.offset] = ((byte) (i >> 24));
    dst.bytes[dst.offset + 1] = ((byte) (i >> 16));
    dst.bytes[dst.offset + 2] = ((byte) (i >> 8));
    dst.bytes[dst.offset + 3] = ((byte) i);
    dst.offset += 4;
  }

  /**
   * Writes an int in a variable-length format.
   * Writes between one and five bytes. Smaller values take fewer bytes. Negative numbers
   * will always use all 5 bytes and are therefore better serialized using {@link #writeInt}.
   */
  public final static void writeVInt(BytesRef dst, int i) {
    while ((i & ~0x7F) != 0) {
      dst.bytes[dst.offset++] = (byte) ((i & 0x7f) | 0x80);
      i >>>= 7;
    }
    dst.bytes[dst.offset++] = (byte) i;
  }

  /**
   * Encodes a long into the byte array dst at the given offset.
   */
  public final static void writeLong(BytesRef dst, long i) {
    writeInt(dst, (int) (i >> 32));
    writeInt(dst, (int) i);
  }

  public final static int readInt(BytesRef src) {
    return ((src.bytes[src.offset++] & 0xFF) << 24) | ((src.bytes[src.offset++] & 0xFF) << 16)
            | ((src.bytes[src.offset++] & 0xFF) << 8) | (src.bytes[src.offset++] & 0xFF);
  }

  /**
   * Reads an int stored in variable-length format.  Reads between one and
   * five bytes.  Smaller values take fewer bytes.  Negative numbers
   * will always use all 5 bytes and are therefore better serialized
   * using {@link #readInt}
   */
  public final static int readVInt(BytesRef src) {
    byte b = src.bytes[src.offset++];
    int i = b & 0x7F;
    if ((b & 0x80) == 0) {
      return i;
    }
    b = src.bytes[src.offset++];
    i |= (b & 0x7F) << 7;
    if ((b & 0x80) == 0) {
      return i;
    }
    b = src.bytes[src.offset++];
    i |= (b & 0x7F) << 14;
    if ((b & 0x80) == 0) {
      return i;
    }
    b = src.bytes[src.offset++];
    i |= (b & 0x7F) << 21;
    if ((b & 0x80) == 0) {
      return i;
    }
    b = src.bytes[src.offset++];
    assert (b & 0x80) == 0;
    return i | ((b & 0x7F) << 28);
  }

  public final static long readLong(BytesRef src) {
    return (((long) readInt(src)) << 32) | (readInt(src) & 0xFFFFFFFFL);
  }

  public final static void writeBytesRef(BytesRef src, BytesRef dst) {
    if (src == null) {
      Bytes.writeVInt(dst, 0);
      return;
    }
    Bytes.writeVInt(dst, src.length);
    System.arraycopy(src.bytes, src.offset, dst.bytes, dst.offset, src.length);
    dst.offset += src.length;
  }

  public final static void readBytesRef(BytesRef src, BytesRef dst) {
    int length = Bytes.readVInt(src);

    if (length == 0) {
      dst.offset = dst.length = 0;
      return;
    }

    if (dst.bytes.length < length) {
      dst.bytes = new byte[length];
    }

    System.arraycopy(src.bytes, src.offset, dst.bytes, 0, length);

    src.offset += length;
    dst.offset = 0;
    dst.length = length;
  }


}
