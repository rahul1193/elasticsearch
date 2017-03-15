package org.elasticsearch.index.cache.query;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.Strings;

import java.io.IOException;
import java.util.Objects;

/**
 * @author rahulanishetty
 * @since 15/03/17.
 */
public class CacheableQueryWrapper extends Query {

    private final Query query;
    private final String cacheKey;

    public CacheableQueryWrapper(Query query, String cacheKey) {
        this.query = Objects.requireNonNull(query, "query to cache cannot be null");
        if (Strings.isEmpty(cacheKey)) {
            throw new IllegalArgumentException("cacheKey cannot be blank for cacheable wrapper query");
        }
        this.cacheKey = cacheKey;
    }

    public String getCacheKey() {
        return cacheKey;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        return query.createWeight(searcher, needsScores);
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query rewritten = query.rewrite(reader);
        if (this == rewritten) {
            return this;
        } else if (rewritten instanceof CacheableQueryWrapper) {
            return rewritten;
        }
        return new CacheableQueryWrapper(rewritten, cacheKey);
    }

    @Override
    public String toString(String field) {
        return query.toString(field);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CacheableQueryWrapper that = (CacheableQueryWrapper) o;

        return query.equals(that.query);
    }

    @Override
    public int hashCode() {
        return query.hashCode();
    }
}
