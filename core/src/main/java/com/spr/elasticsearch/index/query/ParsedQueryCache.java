package com.spr.elasticsearch.index.query;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Utkarsh
 */
public class ParsedQueryCache {

    public static final Setting<Boolean> PARSED_QUERY_CACHE_ENABLED =
        Setting.boolSetting("parsed.query.cache.enabled", true, Setting.Property.NodeScope);

    public static final Setting<Integer> PARSED_QUERY_CACHE_SIZE =
        Setting.intSetting("parsed.query.cache.size", 10000, Setting.Property.NodeScope);

    private static final long HASHTABLE_RAM_BYTES_PER_ENTRY =
        2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF // key + value
            * 2; // hash tables need to be oversized to avoid collisions, assume 2x capacity

    private final Logger logger;
    private final Cache<String, Query> cache;
    private final Stats stats;

    public ParsedQueryCache(Settings settings) {
        int maxSize = PARSED_QUERY_CACHE_SIZE.get(settings);
        assert maxSize > 0;
        logger = Loggers.getLogger(getClass(), settings);
        stats = new Stats();
        cache = Caffeine.newBuilder()
            .maximumSize(maxSize)
            .removalListener(new CacheRemovalListener(settings, stats))
            .executor(Runnable::run)
            .build();
    }

    public ParsedQueryCacheStats getStats() {
        return stats.toQueryCacheStats();
    }

    public Query get(String cacheKey) {
        assert cacheKey != null;
        assert cacheKey.length() > 0;
        final Query cached = cache.getIfPresent(cacheKey);
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

    public Query put(String cacheKey, Query query) {
        assert cacheKey != null;
        assert cacheKey.length() > 0;
        cache.put(cacheKey, query);
        onPut(cacheKey);
        return query;
    }

    public void clear() {
        cache.invalidateAll();
        stats.missCount.set(0);
        stats.hitCount.set(0);
    }

    public class Stats implements Cloneable {

        private AtomicLong hitCount = new AtomicLong();
        private AtomicLong missCount = new AtomicLong();

        ParsedQueryCacheStats toQueryCacheStats() {
            return new ParsedQueryCacheStats(hitCount.get(), missCount.get(), cache.estimatedSize());
        }
    }

    private static final class CacheRemovalListener implements RemovalListener<String, Query> {

        private final Stats stats;
        private final Logger logger;

        private CacheRemovalListener(Settings settings, Stats stats) {
            this.logger = Loggers.getLogger(getClass(), settings);
            this.stats = stats;
        }

        @Override
        public void onRemoval(String key, Query removed, RemovalCause cause) {
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
