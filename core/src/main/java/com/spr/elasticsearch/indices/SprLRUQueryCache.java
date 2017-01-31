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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Accountable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Utkarsh
 */
public class SprLRUQueryCache extends LRUQueryCache {

    private final Cache<LeafCacheKey, DocIdSet> cache;

    // these variables are volatile so that we do not need to sync reads
    // but increments need to be performed under the lock
    private volatile long hitCount;
    private volatile long missCount;
    private volatile long cacheCount;

    public SprLRUQueryCache(int maxSize) {
        super(maxSize, Long.MAX_VALUE, leafReaderContext -> true);
        assert maxSize > 0;
        cache = Caffeine.newBuilder().maximumSize(maxSize).build();
    }

    @Override
    protected void onHit(Object readerCoreKey, Query query) {
        // noop
    }

    @Override
    protected void onMiss(Object readerCoreKey, Query query) {
        // noop
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
        // noop
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
        // noop
    }

    @Override
    public void clearQuery(Query query) {
        // noop
    }

    @Override
    public void clear() {
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

    DocIdSet get(Query key, LeafReaderContext context) {
        assert key instanceof BoostQuery == false;
        assert key instanceof ConstantScoreQuery == false;
        final Object readerKey = context.reader().getCoreCacheKey();
        final DocIdSet cached = cache.getIfPresent(LeafCacheKey.of(readerKey, key));
        if (cached == null) {
            onMiss(readerKey, key);
        } else {
            onHit(readerKey, key);
        }
        return cached;
    }

    void put(Query query, LeafReaderContext context, DocIdSet set) {
        assert query instanceof BoostQuery == false;
        assert query instanceof ConstantScoreQuery == false;
        final Object key = context.reader().getCoreCacheKey();
        cache.put(LeafCacheKey.of(key, query), set);
    }

    private static class LeafCacheKey {

        private final Object leaf;
        private final Query query;


        private LeafCacheKey(Object leaf, Query query) {
            this.leaf = leaf;
            this.query = query;
        }

        private static LeafCacheKey of(Object leaf, Query query) {
            return new LeafCacheKey(leaf, query);
        }

        @Override
        public int hashCode() {
            int h = System.identityHashCode(leaf);
            h = 31 * h + query.hashCode();
            return h;
        }

        @Override
        public boolean equals(Object obj) {
            return obj == this || (obj instanceof LeafCacheKey && leaf == ((LeafCacheKey) obj).leaf && query.equals(((LeafCacheKey) obj).query));
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

            DocIdSet docIdSet = get(in.getQuery(), context);

            if (docIdSet == null) {
                if (policy.shouldCache(in.getQuery())) {
                    docIdSet = cache(context);
                    put(in.getQuery(), context, docIdSet);
                } else {
                    return in.scorer(context);
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
}
