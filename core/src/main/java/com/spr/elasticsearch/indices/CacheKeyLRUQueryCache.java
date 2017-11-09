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

package com.spr.elasticsearch.indices;

import com.github.benmanes.caffeine.cache.*;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesQueryCache;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * @author Utkarsh
 */
public class CacheKeyLRUQueryCache extends XLRUQueryCache {

    public static final Setting<Long> LRU_QUERY_CACHE_EXPIRE_AFTER_ACCESS_SECONDS =
        Setting.longSetting("lru.query.cache.expire.after.access.seconds", TimeUnit.HOURS.toSeconds(2), 1, Setting.Property.NodeScope);

    private static final int HASHTABLE_RAM_BYTES_PER_ENTRY =
        2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF // key + value
            * 2; // hash tables need to be oversized to avoid collisions, assume 2x capacity

    private final Logger logger;
    private final Cache<LeafCacheKey, DocIdSet> cache;
    private final ConcurrentMap<Object, Set<LeafCacheKey>> segmentVsCacheKeys;
    private final Function<Object, IndicesQueryCache.Stats> shardStatsSupplier;

    public CacheKeyLRUQueryCache(Settings settings, long maxBytes, Function<Object, IndicesQueryCache.Stats> shardStatsSupplier) {
        super(Integer.MAX_VALUE, maxBytes, leafReaderContext -> true);
        assert maxBytes > 0;
        logger = Loggers.getLogger(getClass(), settings);
        this.shardStatsSupplier = shardStatsSupplier;
        this.segmentVsCacheKeys = new ConcurrentHashMap<>();
        Caffeine<LeafCacheKey, DocIdSet> cacheBuilder = Caffeine.newBuilder()
            .removalListener(new CacheRemovalListener(settings, shardStatsSupplier, segmentVsCacheKeys))
            .executor(Runnable::run)
            .maximumWeight(maxBytes)
            .weigher(new CacheKeyQueryCacheWeigher());

        long expireAfterAccess = LRU_QUERY_CACHE_EXPIRE_AFTER_ACCESS_SECONDS.get(settings);
        if (expireAfterAccess > 0) {
            cacheBuilder.expireAfterAccess(expireAfterAccess, TimeUnit.SECONDS);
        }

        cache = cacheBuilder.build();
    }

    @Override
    protected void onHit(Object readerCoreKey, Query query) {
        IndicesQueryCache.Stats shardStats = this.shardStatsSupplier.apply(readerCoreKey);
        if (shardStats == null) {
            logger.debug("shard stats is null for key {}", readerCoreKey);
            return;
        }
        shardStats.hitCount.incrementAndGet();
    }

    @Override
    protected void onMiss(Object readerCoreKey, Query query) {
        final IndicesQueryCache.Stats shardStats = this.shardStatsSupplier.apply(readerCoreKey);
        if (shardStats == null) {
            logger.debug("shard stats is null for key {}", readerCoreKey);
            return;
        }
        shardStats.missCount.incrementAndGet();
    }

    @Override
    protected void onQueryCache(Query query, long ramBytesUsed) {
        // noop
    }

    @Override
    protected void onQueryEviction(Query query, long ramBytesUsed) {
        // noop
    }

    @Override
    protected void onDocIdSetCache(Object readerCoreKey, long ramBytesUsed) {
        IndicesQueryCache.Stats shardStats = this.shardStatsSupplier.apply(readerCoreKey);
        if (shardStats != null) {
            shardStats.cacheSize.incrementAndGet();
            shardStats.cacheCount.incrementAndGet();
            shardStats.ramBytesUsed.addAndGet(ramBytesUsed);
        }
    }

    @Override
    protected void onDocIdSetEviction(Object readerCoreKey, int numEntries, long sumRamBytesUsed) {
        // noop
    }

    @Override
    protected void onClear() {
        // noop
    }

    @Override
    public void clearCoreCacheKey(Object coreKey) {
        Set<LeafCacheKey> cacheKeys = segmentVsCacheKeys.remove(coreKey);
        if (cacheKeys != null && !cacheKeys.isEmpty()) {
            cache.invalidateAll(cacheKeys);
        }
    }

    @Override
    public void clearQuery(Query query) {
        if (query instanceof BooleanQuery) {
            Set<LeafCacheKey> leafCacheKeys = cache.asMap().keySet();
            List<LeafCacheKey> keysToRemove = new ArrayList<>();
            for (LeafCacheKey leafCacheKey : leafCacheKeys) {
                if (leafCacheKey.cacheKey.equals(((BooleanQuery) query).getCacheKey())) {
                    keysToRemove.add(leafCacheKey);
                    Set<LeafCacheKey> cacheKeys = segmentVsCacheKeys.get(leafCacheKey.leaf);
                    if (cacheKeys != null) {
                        cacheKeys.remove(leafCacheKey);
                    }
                }
            }
            cache.invalidateAll(keysToRemove);
        }
    }

    @Override
    public void clear() {
        segmentVsCacheKeys.clear();
        cache.invalidateAll();
    }

    @Override
    public Weight doCache(Weight weight, QueryCachingPolicy policy) {
        while (weight instanceof CachingWrapperWeight) {
            weight = ((CachingWrapperWeight) weight).in;
        }

        return new CachingWrapperWeight(weight, policy);
    }

    @Override
    public long ramBytesUsed() {
        return -1;
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }

    @Override
    protected long ramBytesUsed(Query query) {
        return -1;
    }

    @Override
    protected DocIdSet cacheImpl(BulkScorer scorer, int maxDoc) throws IOException {
        return super.cacheImpl(scorer, maxDoc);
    }

    DocIdSet get(Query query, LeafReaderContext context) {
        assert query instanceof BooleanQuery;
        assert ((BooleanQuery) query).getCacheKey() != null;
        final Object leaf = context.reader().getCoreCacheKey();
        final String key = ((BooleanQuery) query).getCacheKey();
        final DocIdSet cached = cache.getIfPresent(LeafCacheKey.of(leaf, key));
        if (cached == null) {
            onMiss(leaf, query);
        } else {
            onHit(leaf, query);
        }
        return cached;
    }

    void put(Query query, LeafReaderContext context, DocIdSet set) {
        assert query instanceof BooleanQuery;
        assert ((BooleanQuery) query).getCacheKey() != null;
        final Object leaf = context.reader().getCoreCacheKey();
        final String key = ((BooleanQuery) query).getCacheKey();
        LeafCacheKey cacheKey = LeafCacheKey.of(leaf, key);
        cache.put(cacheKey, set);
        addToSegmentVsCacheKeys(leaf, cacheKey);
        context.reader().addCoreClosedListener(this::clearCoreCacheKey);
        onDocIdSetCache(leaf, HASHTABLE_RAM_BYTES_PER_ENTRY + set.ramBytesUsed());
    }

    private void addToSegmentVsCacheKeys(Object leaf, LeafCacheKey cacheKey) {
        Set<LeafCacheKey> cacheKeys = segmentVsCacheKeys.get(leaf);
        if (cacheKeys == null) {
            cacheKeys = Collections.synchronizedSet(new HashSet<>());
            Set<LeafCacheKey> existing = segmentVsCacheKeys.putIfAbsent(leaf, cacheKeys);
            if (existing != null) {
                cacheKeys = existing;
            }
        }
        cacheKeys.add(cacheKey);
    }

    private static class LeafCacheKey {

        private final Object leaf;
        private final String cacheKey;
        private int hashCode;


        private LeafCacheKey(Object leaf, String cacheKey) {
            this.leaf = leaf;
            this.cacheKey = cacheKey;
            this.hashCode = calculateHashCode();
        }

        private int calculateHashCode() {
            int h = System.identityHashCode(leaf);
            h = 31 * h + cacheKey.hashCode();
            return h;
        }

        private static LeafCacheKey of(Object leaf, String cacheKey) {
            return new LeafCacheKey(leaf, cacheKey);
        }

        @Override
        public int hashCode() {
            return this.hashCode;
        }

        @Override
        public boolean equals(Object obj) {
            boolean isEquals = false;
            if (this == obj) {
                isEquals = true;
            } else if (obj instanceof LeafCacheKey) {
                LeafCacheKey other = (LeafCacheKey) obj;
                isEquals = (leaf == other.leaf) && (cacheKey.equals(other.cacheKey));
            }
            return isEquals;
        }
    }

    private static final class CacheKeyQueryCacheWeigher implements Weigher<LeafCacheKey, DocIdSet> {

        @Override
        public int weigh(LeafCacheKey key, DocIdSet value) {
            int weight = HASHTABLE_RAM_BYTES_PER_ENTRY;
            long byteValue = value.ramBytesUsed();
            if (byteValue + weight > Integer.MAX_VALUE) {
                weight = Integer.MAX_VALUE;
            } else {
                weight += byteValue;
            }
            return weight;
        }
    }

    private class CachingWrapperWeight extends ConstantScoreWeight {

        private final Weight in;
        private final QueryCachingPolicy policy;
        // we use an AtomicBoolean because Weight.scorer may be called from multiple
        // threads when IndexSearcher is created with threads
        private final AtomicBoolean used;

        CachingWrapperWeight(Weight in, QueryCachingPolicy policy) {
            super(in.getQuery());
            this.in = in;
            this.policy = policy;
            used = new AtomicBoolean(false);
        }

        @Override
        public void extractTerms(Set<Term> terms) {
            in.extractTerms(terms);
        }

        private DocIdSet cache(LeafReaderContext context) throws IOException {
            final BulkScorer scorer = in.bulkScorer(context);
            if (scorer == null) {
                return DocIdSet.EMPTY;
            } else {
                return cacheImpl(scorer, context.reader().maxDoc());
            }
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            if (used.compareAndSet(false, true)) {
                policy.onUse(getQuery());
            }

            if (policy.shouldCache(in.getQuery()) == false) {
                return in.scorer(context);
            }

            DocIdSet docIdSet = get(in.getQuery(), context);

            if (docIdSet == null) {
                docIdSet = cache(context);
                put(in.getQuery(), context, docIdSet);
            }

            assert docIdSet != null;
            if (docIdSet == DocIdSet.EMPTY) {
                return null;
            }
            final DocIdSetIterator disi = docIdSet.iterator();
            if (disi == null) {
                return null;
            }

            return new ConstantScoreScorer(this, 0f, disi);
        }

        @Override
        public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
            if (used.compareAndSet(false, true)) {
                policy.onUse(getQuery());
            }

            if (policy.shouldCache(in.getQuery()) == false) {
                return in.bulkScorer(context);
            }

            DocIdSet docIdSet = get(in.getQuery(), context);
            if (docIdSet == null) {
                docIdSet = cache(context);
                put(in.getQuery(), context, docIdSet);
            }

            assert docIdSet != null;
            if (docIdSet == DocIdSet.EMPTY) {
                return null;
            }
            final DocIdSetIterator disi = docIdSet.iterator();
            if (disi == null) {
                return null;
            }

            return new DefaultBulkScorer(new ConstantScoreScorer(this, 0f, disi));
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            if (used.compareAndSet(false, true)) {
                policy.onUse(getQuery());
            }

            if (policy.shouldCache(in.getQuery()) == false) {
                return in.scorerSupplier(context);
            }

            DocIdSet docIdSet = get(in.getQuery(), context);

            if (docIdSet == null) {
                docIdSet = cache(context);
                put(in.getQuery(), context, docIdSet);
            }

            assert docIdSet != null;
            if (docIdSet == DocIdSet.EMPTY) {
                return null;
            }
            final DocIdSetIterator disi = docIdSet.iterator();
            if (disi == null) {
                return null;
            }

            return new ScorerSupplier() {
                @Override
                public Scorer get(boolean randomAccess) throws IOException {
                    return new ConstantScoreScorer(CachingWrapperWeight.this, 0f, disi);
                }

                @Override
                public long cost() {
                    return disi.cost();
                }
            };

        }
    }

    private static final class CacheRemovalListener implements RemovalListener<LeafCacheKey, DocIdSet> {

        private final Function<Object, IndicesQueryCache.Stats> statsSupplier;
        private final Logger logger;
        private final ConcurrentMap<Object, Set<LeafCacheKey>> segmentVsCacheKeys;

        CacheRemovalListener(Settings settings, Function<Object, IndicesQueryCache.Stats> statsSupplier, ConcurrentMap<Object, Set<LeafCacheKey>> segmentVsCacheKeys) {
            logger = Loggers.getLogger(getClass(), settings);
            this.statsSupplier = statsSupplier;
            this.segmentVsCacheKeys = segmentVsCacheKeys;
        }

        @Override
        public void onRemoval(LeafCacheKey key, DocIdSet removed, RemovalCause cause) {
            if (key == null) {
                return;
            }

            if (!RemovalCause.EXPLICIT.equals(cause)) {
                Set<LeafCacheKey> cacheKeys = segmentVsCacheKeys.get(key.cacheKey);
                if (cacheKeys != null && !cacheKeys.isEmpty()) {
                    cacheKeys.remove(key);
                }
            }
            IndicesQueryCache.Stats stats = this.statsSupplier.apply(key.leaf);
            if (stats == null) {
                logger.error("stats is null for key  {}", key);
                return;
            } else {
                logger.debug("cache is removed for key {}", key);
            }
            stats.cacheSize.decrementAndGet();
            if (removed != null) {
                stats.ramBytesUsed.addAndGet(-HASHTABLE_RAM_BYTES_PER_ENTRY - removed.ramBytesUsed());
            }
        }
    }


}
