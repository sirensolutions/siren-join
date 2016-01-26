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
package solutions.siren.join.index.query;

import com.google.common.hash.Hashing;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import solutions.siren.join.action.terms.collector.LongTermsSet;

import java.io.IOException;

/**
 * Helper class for {@link FieldDataTermsQueryParser} to encode, decode and hash terms.
 */
public class FieldDataTermsQueryHelper {

  /**
   * Encodes the list of longs into a serialised {@link LongTermsSet}.
   */
  public final static byte[] encode(long[] values) throws IOException {
    LongTermsSet termsSet = new LongTermsSet(values.length);
    for (int i = 0; i < values.length; i++) {
      termsSet.add(values[i]);
    }
    return termsSet.writeToBytes().bytes;
  }

  /**
   * Hash the given terms using Sip hash.
   * @see Hashing#sipHash24()
   */
  public final static long hash(BytesRef term) {
    return Hashing.sipHash24().hashBytes(term.bytes, term.offset, term.length).asLong();
  }

  /**
   * Encodes a long into the byte array dst at the given offset.
   */
  public final static void writeInt(BytesRef dst, int i) {
    dst.bytes[dst.offset++] = ((byte) (i >> 24));
    dst.bytes[dst.offset++] = ((byte) (i >> 16));
    dst.bytes[dst.offset++] = ((byte) (i >> 8));
    dst.bytes[dst.offset++] = ((byte) i);
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

}
