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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesQueryCache;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * @author Utkarsh
 */
public class CacheKeyLRUQueryCache extends LRUQueryCache {

    private static final long HASHTABLE_RAM_BYTES_PER_ENTRY =
        2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF // key + value
            * 2; // hash tables need to be oversized to avoid collisions, assume 2x capacity

    private final Logger logger;
    private final Cache<LeafCacheKey, DocIdSet> cache;
    private final Function<Object, IndicesQueryCache.Stats> shardStatsSupplier;

    public CacheKeyLRUQueryCache(Settings settings, int maxSize, Function<Object, IndicesQueryCache.Stats> shardStatsSupplier) {
        super(maxSize, Long.MAX_VALUE, leafReaderContext -> true);
        assert maxSize > 0;
        logger = Loggers.getLogger(getClass(), settings);
        this.shardStatsSupplier = shardStatsSupplier;
        cache = Caffeine.newBuilder()
            .maximumSize(maxSize)
            .removalListener(new CacheRemovalListener(settings, shardStatsSupplier))
            .executor(Runnable::run)
            .build();
    }

    @Override
    protected void onHit(Object readerCoreKey, Query query) {
        IndicesQueryCache.Stats shardStats = this.shardStatsSupplier.apply(readerCoreKey);
        if (shardStats == null) {
            logger.debug("shard stats is null for key {}", readerCoreKey);
            return;
        }
        shardStats.hitCount++;
    }

    @Override
    protected void onMiss(Object readerCoreKey, Query query) {
        final IndicesQueryCache.Stats shardStats = this.shardStatsSupplier.apply(readerCoreKey);
        if (shardStats == null) {
            logger.debug("shard stats is null for key {}", readerCoreKey);
            return;
        }
        shardStats.missCount++;
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
            shardStats.cacheSize++;
            shardStats.cacheCount++;
            shardStats.ramBytesUsed += ramBytesUsed;
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
        Set<LeafCacheKey> leafCacheKeys = cache.asMap().keySet();
        List<LeafCacheKey> keysToRemove = new ArrayList<>();
        for (LeafCacheKey leafCacheKey : leafCacheKeys) {
            if (leafCacheKey.leaf.equals(coreKey)) {
                keysToRemove.add(leafCacheKey);
            }
        }
        cache.invalidateAll(keysToRemove);
    }

    @Override
    public void clearQuery(Query query) {
        Set<LeafCacheKey> leafCacheKeys = cache.asMap().keySet();
        List<LeafCacheKey> keysToRemove = new ArrayList<>();
        for (LeafCacheKey leafCacheKey : leafCacheKeys) {
            if (leafCacheKey.cacheKey.equals(query)) {
                keysToRemove.add(leafCacheKey);
            }
        }
        cache.invalidateAll(keysToRemove);
    }

    @Override
    public void clear() {
        ConcurrentMap<LeafCacheKey, DocIdSet> map = cache.asMap();
        cache.invalidateAll();
        for (LeafCacheKey leafCacheKey : map.keySet()) {
            IndicesQueryCache.Stats shardStats = this.shardStatsSupplier.apply(leafCacheKey);
            if (shardStats != null) {
                shardStats.ramBytesUsed = 0;
                shardStats.cacheSize = 0;
            }
        }
    }

    @Override
    public Weight doCache(Weight weight, QueryCachingPolicy policy) {
        try {
            if (!policy.shouldCache(weight.getQuery())) {
                return weight;
            }
        } catch (IOException e) {
            throw ExceptionsHelper.convertToElastic(e);
        }

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
        cache.put(LeafCacheKey.of(leaf, key), set);
        onDocIdSetCache(leaf, HASHTABLE_RAM_BYTES_PER_ENTRY + set.ramBytesUsed());
    }

    private static class LeafCacheKey {

        private final Object leaf;
        private final String cacheKey;


        private LeafCacheKey(Object leaf, String cacheKey) {
            this.leaf = leaf;
            this.cacheKey = cacheKey;
        }

        private static LeafCacheKey of(Object leaf, String cacheKey) {
            return new LeafCacheKey(leaf, cacheKey);
        }

        @Override
        public int hashCode() {
            int h = System.identityHashCode(leaf);
            h = 31 * h + cacheKey.hashCode();
            return h;
        }

        @Override
        public boolean equals(Object obj) {
            return obj == this || (obj instanceof LeafCacheKey && leaf == ((LeafCacheKey) obj).leaf && cacheKey.equals(((LeafCacheKey) obj).cacheKey));
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

            if (!policy.shouldCache(in.getQuery())) {
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

            DocIdSet docIdSet = get(in.getQuery(), context);
            if (docIdSet == null) {
                if (policy.shouldCache(in.getQuery())) {
                    docIdSet = cache(context);
                    put(in.getQuery(), context, docIdSet);
                } else {
                    return in.bulkScorer(context);
                }
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

    }

    private static final class CacheRemovalListener implements RemovalListener<LeafCacheKey, DocIdSet> {

        private final Function<Object, IndicesQueryCache.Stats> statsSupplier;
        private final Logger logger;

        CacheRemovalListener(Settings settings, Function<Object, IndicesQueryCache.Stats> statsSupplier) {
            logger = Loggers.getLogger(getClass(), settings);
            this.statsSupplier = statsSupplier;
        }

        @Override
        public void onRemoval(LeafCacheKey key, DocIdSet removed, RemovalCause cause) {
            if (key == null) {
                return;
            }
            IndicesQueryCache.Stats stats = this.statsSupplier.apply(key.leaf);
            if (stats == null) {
                logger.error("stats is null for key  {}", key);
                return;
            } else {
                logger.debug("cache is removed for key {}", key);
            }
            switch (cause) {
                case COLLECTED:
                case EXPIRED:
                case SIZE:
                    stats.cacheCount--;
                    break;
                case EXPLICIT:
                case REPLACED:
                    stats.cacheSize--;
                    stats.cacheCount--;
                    break;
            }
            if (removed != null) {
                stats.ramBytesUsed -= HASHTABLE_RAM_BYTES_PER_ENTRY + removed.ramBytesUsed();
            }
        }
    }
}
