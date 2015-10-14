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
package com.sirensolutions.siren.join.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.QueryParsingException;

import java.io.IOException;

/**
 * A query based on a byte encoded list of long terms. Deleguates parsing to a {@link BinaryTermsFilterParser}
 * instance and wraps the resulting filter in a {@link XConstantScoreQuery}.
 */
public class BinaryTermsQueryParser implements QueryParser {

  private final BinaryTermsFilterParser filterParser;

  public static final String NAME = "binary_terms";

  public BinaryTermsQueryParser() {
    this.filterParser = new BinaryTermsFilterParser();
  }

  @Override
  public String[] names() {
    return new String[]{NAME};
  }

  @Override
  public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
    return new XConstantScoreQuery(filterParser.parse(parseContext));
  }

}
