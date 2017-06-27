package com.spr.elasticsearch.index.query;

import org.apache.lucene.search.DocIdSet;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * @author rahulanishetty
 * @since 19/06/17.
 */
public class QueryBuilderRewriteCacheStats implements Writeable, ToXContent {
    private long hitCount;
    private long missCount;
    private long cacheCount;

    public QueryBuilderRewriteCacheStats() {
    }

    public QueryBuilderRewriteCacheStats(StreamInput in) throws IOException {
        hitCount = in.readLong();
        missCount = in.readLong();
        cacheCount = in.readLong();
    }

    public QueryBuilderRewriteCacheStats(long hitCount, long missCount, long cacheCount) {
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.cacheCount = cacheCount;
    }

    public void add(QueryBuilderRewriteCacheStats stats) {
        hitCount += stats.hitCount;
        missCount += stats.missCount;
        cacheCount += stats.cacheCount;
    }

    /**
     * The total number of lookups in the cache.
     */
    public long getTotalCount() {
        return hitCount + missCount;
    }

    /**
     * The number of successful lookups in the cache.
     */
    public long getHitCount() {
        return hitCount;
    }

    /**
     * The number of lookups in the cache that failed to retrieve a {@link DocIdSet}.
     */
    public long getMissCount() {
        return missCount;
    }

    /**
     * The number of {@link org.apache.lucene.search.Query}s that have been cached.
     */
    public long getCacheCount() {
        return cacheCount;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(hitCount);
        out.writeLong(missCount);
        out.writeLong(cacheCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(QueryBuilderRewriteCacheStats.Fields.QUERY_BUILDER_CACHE);
        builder.field(QueryBuilderRewriteCacheStats.Fields.TOTAL_COUNT, getTotalCount());
        builder.field(QueryBuilderRewriteCacheStats.Fields.HIT_COUNT, getHitCount());
        builder.field(QueryBuilderRewriteCacheStats.Fields.MISS_COUNT, getMissCount());
        builder.field(QueryBuilderRewriteCacheStats.Fields.CACHE_COUNT, getCacheCount());
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String QUERY_BUILDER_CACHE = "query_builder_cache";
        static final String TOTAL_COUNT = "total_count";
        static final String HIT_COUNT = "hit_count";
        static final String MISS_COUNT = "miss_count";
        static final String CACHE_COUNT = "cache_count";
    }
}
