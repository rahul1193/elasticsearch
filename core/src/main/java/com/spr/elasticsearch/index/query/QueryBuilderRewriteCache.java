package com.spr.elasticsearch.index.query;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author rahulanishetty
 * @since 19/06/17.
 */
public class QueryBuilderRewriteCache {

    public static final Setting<Boolean> QUERY_BUILDER_REWRITE_CACHE_ENABLED =
        Setting.boolSetting("query.builder.rewrite.cache.enabled", true, Setting.Property.NodeScope);

    public static final Setting<Integer> QUERY_BUILDER_REWRITE_CACHE_SIZE =
        Setting.intSetting("query.builder.rewrite.cache.size", 10000, Setting.Property.NodeScope);

    public static final Setting<Long> QUERY_BUILDER_REWRITE_EXPIRE_AFTER_ACCESS =
        Setting.longSetting("query.builder.rewrite.expire.after.access", TimeUnit.HOURS.toMillis(2), TimeUnit.MINUTES.toMillis(1), Setting.Property.NodeScope);

    private static final long HASHTABLE_RAM_BYTES_PER_ENTRY =
        2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF // key + value
            * 2; // hash tables need to be oversized to avoid collisions, assume 2x capacity

    private final Logger logger;
    private final Cache<String, QueryBuilder> cache;
    private final QueryBuilderRewriteCache.Stats stats;

    public QueryBuilderRewriteCache(Settings settings) {
        int maxSize = QUERY_BUILDER_REWRITE_CACHE_SIZE.get(settings);
        assert maxSize > 0;
        logger = Loggers.getLogger(getClass(), settings);
        stats = new QueryBuilderRewriteCache.Stats();
        cache = Caffeine.newBuilder()
            .maximumSize(maxSize)
            .expireAfterAccess(QUERY_BUILDER_REWRITE_EXPIRE_AFTER_ACCESS.get(settings), TimeUnit.MILLISECONDS)
            .removalListener(new QueryBuilderRewriteCache.CacheRemovalListener(settings, stats))
            .executor(Runnable::run)
            .build();
    }

    public QueryBuilderRewriteCacheStats getStats() {
        return stats.toQueryCacheStats();
    }

    public QueryBuilder get(String cacheKey) {
        assert cacheKey != null;
        assert cacheKey.length() > 0;
        final QueryBuilder cached = cache.getIfPresent(cacheKey);
        if (cached == null) {
            onMiss(cacheKey);
        } else {
            onHit(cacheKey);
        }
        return cached;
    }

    private void onMiss(String cacheKey) {
        stats.missCount.incrementAndGet();
    }

    private void onHit(String cacheKey) {
        stats.hitCount.incrementAndGet();
    }

    private void onPut(String cacheKey) {
        // nop
    }

    public QueryBuilder put(String cacheKey, QueryBuilder queryBuilder) {
        assert cacheKey != null;
        assert cacheKey.length() > 0;
        cache.put(cacheKey, queryBuilder);
        onPut(cacheKey);
        return queryBuilder;
    }

    public void clear() {
        cache.invalidateAll();
        stats.missCount.set(0);
        stats.hitCount.set(0);
    }

    public class Stats implements Cloneable {

        private AtomicLong hitCount = new AtomicLong();
        private AtomicLong missCount = new AtomicLong();

        QueryBuilderRewriteCacheStats toQueryCacheStats() {
            return new QueryBuilderRewriteCacheStats(hitCount.get(), missCount.get(), cache.estimatedSize());
        }
    }

    private static final class CacheRemovalListener implements RemovalListener<String, QueryBuilder> {

        private final QueryBuilderRewriteCache.Stats stats;
        private final Logger logger;

        private CacheRemovalListener(Settings settings, QueryBuilderRewriteCache.Stats stats) {
            this.logger = Loggers.getLogger(getClass(), settings);
            this.stats = stats;
        }

        @Override
        public void onRemoval(String key, QueryBuilder removed, RemovalCause cause) {
            if (key == null) {
                return;
            }
            if (stats == null) {
                logger.error("stats is null for key  {}", key);
                return;
            } else {
                logger.debug("cache is removed for key {}", key);
            }
            if (cause != null) {
                switch (cause) {
                    case COLLECTED:
                    case EXPIRED:
                    case SIZE:
                    case EXPLICIT:
                    case REPLACED:
                        // nop
                        break;
                }
            }
        }
    }
}
