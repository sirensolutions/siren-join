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

import org.apache.lucene.index.IndexReader;

/**
 * A stream of terms coming for a given document and field. A {@link TermStream} is a reusable object
 * used in combination with {@link HitStream#getTermStream(TermStream)}.
 */
abstract class TermStream {

  protected final IndexReader reader;

  protected TermStream(IndexReader reader) {
    this.reader = reader;
  }

  /**
   * Set the stream to the given document.
   * @see HitStream#getTermStream(TermStream)
   */
  protected abstract void set(int atomicReaderId, int atomicDocId);

}