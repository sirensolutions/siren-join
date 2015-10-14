/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
package com.sindicetech.kb.filterjoin.index.query;

import java.io.IOException;

/**
 * A static factory for simple "import static" usage.
 */
public class FilterBuilders {

  /**
   * A filter join for the provided field name. A filter join can
   * extract the terms to filter by querying another index.
   */
  public static FilterJoinBuilder filterJoin(String name) {
    return new FilterJoinBuilder(name);
  }

  /**
   * A binary terms filter for the provided field name.
   */
  public static BinaryTermsFilterBuilder binaryTermsFilter(String name, byte[] value, String cacheKey) {
    return new BinaryTermsFilterBuilder(name, value, cacheKey);
  }

  /**
   * A binary terms filter for the provided field name.
   */
  public static BinaryTermsFilterBuilder binaryTermsFilter(String name, long[] values, String cacheKey) throws IOException {
    return new BinaryTermsFilterBuilder(name, values, cacheKey);
  }

}
