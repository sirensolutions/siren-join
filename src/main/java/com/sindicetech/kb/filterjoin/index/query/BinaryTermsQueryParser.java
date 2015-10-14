/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.index.query;

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
