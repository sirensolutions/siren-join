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
    return termsSet.writeToBytes();
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
  public final static void writeInt(byte[] dst, int offset, int i) {
    dst[offset] = ((byte) (i >> 24));
    dst[offset + 1] = ((byte) (i >> 16));
    dst[offset + 2] = ((byte) (i >> 8));
    dst[offset + 3] = ((byte) i);
  }

  /**
   * Encodes a long into the byte array dst at the given offset.
   */
  public final static void writeLong(byte[] dst, int offset, long i) {
    writeInt(dst, offset, (int) (i >> 32));
    writeInt(dst, offset + 4, (int) i);
  }

  public final static int readInt(byte[] src, int offset) {
    return ((src[offset] & 0xFF) << 24) | ((src[offset + 1] & 0xFF) << 16)
            | ((src[offset + 2] & 0xFF) << 8) | (src[offset + 3] & 0xFF);
  }

  public final static long readLong(byte[] src, int offset) {
    return (((long) readInt(src, offset)) << 32) | (readInt(src, offset + 4) & 0xFFFFFFFFL);
  }

}
