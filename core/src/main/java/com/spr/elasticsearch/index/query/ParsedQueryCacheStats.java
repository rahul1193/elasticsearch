/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spr.elasticsearch.index.query;

import org.apache.lucene.search.DocIdSet;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 */
public class ParsedQueryCacheStats implements Streamable, ToXContent {

    private long hitCount;
    private long missCount;
    private long cacheCount;

    public ParsedQueryCacheStats() {
    }

    public ParsedQueryCacheStats(long hitCount, long missCount, long cacheCount) {
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.cacheCount = cacheCount;
    }

    public void add(ParsedQueryCacheStats stats) {
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

    public static ParsedQueryCacheStats readQueryCacheStats(StreamInput in) throws IOException {
        ParsedQueryCacheStats stats = new ParsedQueryCacheStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        hitCount = in.readLong();
        missCount = in.readLong();
        cacheCount = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(hitCount);
        out.writeLong(missCount);
        out.writeLong(cacheCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.QUERY_CACHE);
        builder.field(Fields.TOTAL_COUNT, getTotalCount());
        builder.field(Fields.HIT_COUNT, getHitCount());
        builder.field(Fields.MISS_COUNT, getMissCount());
        builder.field(Fields.CACHE_COUNT, getCacheCount());
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String QUERY_CACHE = "query_cache";
        static final String TOTAL_COUNT = "total_count";
        static final String HIT_COUNT = "hit_count";
        static final String MISS_COUNT = "miss_count";
        static final String CACHE_COUNT = "cache_count";
    }

}
