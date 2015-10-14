/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.index.query;

import com.carrotsearch.hppc.LongHashSet;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.hppc.LongOpenHashSet;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.ResolvableFilter;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.IntegerFieldMapper;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.query.FilterParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParsingException;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.index.query.support.QueryParsers.wrapSmartNameFilter;

/**
 * A filter based on a byte encoded list of long terms.
 */
public class BinaryTermsFilterParser implements FilterParser {

  public static final String NAME = "binary_terms";

  private final ESLogger logger = Loggers.getLogger(getClass());

  public BinaryTermsFilterParser() {}

  @Override
  public String[] names() {
    return new String[]{NAME};
  }

  @Override
  public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
    XContentParser parser = parseContext.parser();

    XContentParser.Token token = parser.nextToken();
    if (token != XContentParser.Token.FIELD_NAME) {
        throw new QueryParsingException(parseContext.index(), "[binary_terms] filter malformed, no field");
    }
    String fieldName = parser.currentName();

    boolean cache = true; // since usually term filter is on repeating terms, cache it by default
    CacheKeyFilter.Key cacheKey = null;
    String filterName = null;
    byte[] value = null;

    token = parser.nextToken();
    if (token == XContentParser.Token.START_OBJECT) {
      String currentFieldName = null;
      while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
        if (token == XContentParser.Token.FIELD_NAME) {
          currentFieldName = parser.currentName();
        } else {
          if ("value".equals(currentFieldName)) {
              value = parser.binaryValue();
          } else if ("_name".equals(currentFieldName)) {
              filterName = parser.text();
          } else if ("_cache".equals(currentFieldName)) {
              cache = parser.booleanValue();
          } else if ("_cache_key".equals(currentFieldName) || "_cacheKey".equals(currentFieldName)) {
              cacheKey = new CacheKeyFilter.Key(parser.text());
          } else {
            throw new QueryParsingException(parseContext.index(), "[binary_terms] filter does not support [" + currentFieldName + "]");
          }
        }
      }
      parser.nextToken();
    } else {
      value = parser.binaryValue();
      // move to the next token
      parser.nextToken();
    }

    if (value == null) {
      throw new QueryParsingException(parseContext.index(), "[binary_terms] no value specified");
    }
    if (cacheKey == null) { // cache key is mandatory - see #170
      throw new QueryParsingException(parseContext.index(), "[binary_terms] no cache key specified");
    }

    FieldMapper fieldMapper = null;
    MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
    if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
      if (smartNameFieldMappers.explicitTypeInNameWithDocMapper()) {
        String[] previousTypes = QueryParseContext.setTypesWithPrevious(new String[]{smartNameFieldMappers.docMapper().type()});
        try {
          fieldMapper = smartNameFieldMappers.mapper();
        } finally {
          QueryParseContext.setTypes(previousTypes);
        }
      } else {
        fieldMapper = smartNameFieldMappers.mapper();
      }
    }

    // if there are no mappings, then nothing has been indexing yet against this shard, so we can return
    // no match (but not cached!), since the Filter Join relies on the fact that there are mappings...
    if (fieldMapper == null) {
      return Queries.MATCH_NO_FILTER;
    }

    // Create a late decoding filter that will decode the binary value
    // at the last possible moment, and if not already cached. See #170.
    IndexFieldData fieldData = parseContext.getForField(fieldMapper);
    Filter filter = new LateDecodingFilter(fieldMapper, fieldData, value);

    // cache the whole filter by default, or if explicitly told to
    if (cache) {
      filter = parseContext.cacheFilter(filter, cacheKey);
    }

    filter = wrapSmartNameFilter(filter, smartNameFieldMappers, parseContext);
    if (filterName != null) {
      parseContext.addNamedFilter(filterName, filter);
    }

    return filter;
  }

  /**
   * A filter implementation that resolves details at the last possible moment between filter parsing and execution.
   * Similar to {@link ResolvableFilter}, but not detected by
   * {@link QueryParseContext#cacheFilter(Filter, CacheKeyFilter.Key)} as one, so that it is itself cached with the
   * associated cache key. This filter must be used in conjunction with a cache key.
   * <br>
   * The filter returned by {@link #resolve()} will be cached, and not the
   * {@link com.sindicetech.kb.filterjoin.index.query.BinaryTermsFilterParser.LateDecodingFilter} itself. We
   * therefore avoid to cache the byte array and long hash set, which is not memory friendly and not very
   * efficient. See {@link QueryParseContext#cacheFilter(Filter, CacheKeyFilter.Key)}.
   * The late decoding enables to decode the byte array only when it is really necessary, and not at
   * construction time. This saves unnecessary decoding time, e.g., no need to decode the values if the filter is
   * already in the cache.
   *
   * @see ResolvableFilter
   */
  private final class LateDecodingFilter extends Filter {

    private byte[] value;
    private LongHashSet longHashSet;

    private IndexFieldData fieldData;

    private FieldMapper fieldMapper;

    private LateDecodingFilter(FieldMapper fieldMapper, IndexFieldData fieldData, byte[] value) {
      this.fieldMapper = fieldMapper;
      this.fieldData = fieldData;
      this.value = value;
    }

    public Filter resolve() {
      // Decodes the values and creates the long hash set
      try {
        long start = System.nanoTime();
        // keep a reference to the decoded set of terms, to avoid having to decode it for every index segment
        longHashSet = longHashSet == null ? BinaryTermsFilterHelper.decode(value) : longHashSet;
        value = null; // release reference to the byte array to be able to reclaim memory
        logger.debug("Deserialized {} terms - took {} ms", longHashSet.size(), (System.nanoTime() - start) / 1000000);
      }
      catch (IOException e) {
        throw new ElasticsearchParseException("[binary_terms] error while decoding filter binary value", e);
      }

      int size = longHashSet.size();
      logger.debug("{}: Filter is composed of {} terms", Thread.currentThread().getName(), size);

      if (size == 0) {
        return Queries.MATCH_NO_FILTER;
      }

      Filter filter = null;
      if (fieldMapper instanceof LongFieldMapper || fieldMapper instanceof IntegerFieldMapper) {
        filter = FieldDataTermsFilter.newLongs((IndexNumericFieldData) fieldData, longHashSet);
      } else if (fieldMapper instanceof StringFieldMapper) {
        filter = FieldDataTermsFilter.newBytes(fieldData, longHashSet);
      } else {
        throw new ElasticsearchParseException("[binary_terms] filter does not support field data type " + fieldMapper.fieldDataType().getType());
      }

      return filter;
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
      Filter resolvedFilter = resolve();
      if (resolvedFilter != null) {
        return resolvedFilter.getDocIdSet(context, acceptDocs);
      } else {
        return null;
      }
    }

    @Override
    public String toString() {
      return new StringBuilder("LateDecodingBinaryTermsFilter:")
        .append(fieldMapper.name())
        .append(":[")
        .append("size=")
        .append(value != null ? value.length : (longHashSet.size() * 8) + 4)
        .append(']')
        .toString();
    }

  }

}
