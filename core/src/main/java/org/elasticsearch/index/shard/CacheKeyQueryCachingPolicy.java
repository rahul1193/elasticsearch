package org.elasticsearch.index.shard;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;

import java.io.IOException;

/**
 * @author Utkarsh
 */
public class CacheKeyQueryCachingPolicy implements QueryCachingPolicy {
  @Override
  public void onUse(Query query) {
    // noop
  }

  @Override
  public boolean shouldCache(Query query) throws IOException {
    if (query instanceof BooleanQuery) {
      String cacheKey = ((BooleanQuery) query).getCacheKey();
      if (cacheKey != null && cacheKey.length() > 0) {
        return true;
      }
    }
    return false;
  }
}
