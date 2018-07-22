package com.spr.elasticsearch.redis;

import com.spr.elasticsearch.utils.LazyInitializer;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import redis.clients.jedis.HostAndPort;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author rahulanishetty
 * @since 28/07/18.
 */
public class RedisIndicesService implements Closeable {

    private static final SetOnce<RedisIndicesService> instance = new SetOnce<>();

    /**
     * A list of initial seed nodes to discover eligible nodes from the remote cluster
     */
    public static final Setting.AffixSetting<String> REDIS_CLUSTERS_SEEDS = Setting.affixKeySetting("index.redis.cluster.",
        "seeds", s -> Setting.simpleString(s, Setting.Property.Dynamic, Setting.Property.IndexScope));


    private final AtomicReference<ConcurrentMap<String, RedisClientLookup>> clientCache = new AtomicReference<>(new ConcurrentHashMap<>());
    private final ConcurrentMap<String, RedisIndexService> indicesRegistry = new ConcurrentHashMap<>();

    public RedisIndicesService() {
        instance.set(this);
    }

    public static RedisIndicesService getInstance() {
        RedisIndicesService redisIndicesClientProvider = instance.get();
        if (redisIndicesClientProvider == null) {
            throw new IllegalStateException("Instance is not set yet");
        }
        return redisIndicesClientProvider;
    }

    public RedisClientLookup getOrCreateRedisClient(IndexSettings indexSettings, String clusterName) {
        String key = indexSettings.getIndex().getName() + "_" + clusterName;
        RedisClientLookup redisClient = clientCache.get().get(key);
        if (redisClient == null) {
            Setting<String> redisSetting = REDIS_CLUSTERS_SEEDS.getConcreteSettingForNamespace(clusterName);
            if (redisSetting == null) {
                throw new IllegalStateException("No Redis Cluster Settings found for cluster : " + clusterName);
            }
            String hostAndPortStr = indexSettings.getScopedSettings().get(redisSetting);
            if (Strings.isEmpty(hostAndPortStr)) {
                throw new IllegalStateException("No Redis Cluster Settings found for cluster : " + clusterName);
            }
            redisClient = new RedisClientLookup(HostAndPort.parseString(hostAndPortStr));
            RedisClientLookup existing = clientCache.get().putIfAbsent(key, redisClient);
            if (existing != null) {
                IOUtils.closeWhileHandlingException(redisClient);
                redisClient = existing;
            }
        }
        return redisClient;
    }

    public void register(Index index, RedisIndexService redisIndexService) {
        RedisIndexService existing = this.indicesRegistry.put(index.getName(), redisIndexService);
        if (existing != null) {
            throw new IllegalStateException("duplicate service for index : " + index);
        }
    }

    public RedisIndexService getIndexService(Index index) {
        return this.indicesRegistry.get(index.getName());
    }

    @Override
    public void close() throws IOException {
        ConcurrentMap<String, RedisClientLookup> cache = clientCache.getAndSet(null);
        cache.forEach((s, redisClient) -> IOUtils.closeWhileHandlingException(redisClient));
    }

    public void incRef(RedisClientLookup lookup) {
        lookup.incRef();
    }

    public void decRef(RedisClientLookup lookup) {
        lookup.decRef();
    }

    public void tryRemoveUnused(String clusterName) {
        RedisClientLookup redisClientLookup = clientCache.get().get(clusterName);
        if (redisClientLookup != null) {
            if (redisClientLookup.refCount.get() == 0) {
                redisClientLookup = clientCache.get().remove(clusterName);
                IOUtils.closeWhileHandlingException(redisClientLookup);
            }
        }
    }

    public void unregisterIndexService(Index index) {
        this.indicesRegistry.remove(index.getName());
    }

    public static class RedisClientLookup extends LazyInitializer<RedisClient, Void> implements Closeable {

        public AtomicBoolean closed = new AtomicBoolean(false);
        private final AtomicInteger refCount = new AtomicInteger(0);

        RedisClientLookup(HostAndPort hostAndPort) {
            super(param -> new RedisClientImpl(hostAndPort));
        }

        @Override
        public RedisClient getOrCreate(Void param) {
            if (closed.get()) {
                throw new AlreadyClosedException("Close is invoked");
            }
            return super.getOrCreate(param);
        }

        public RedisClient getOrCreate() {
            return super.getOrCreate(null);
        }

        public int incRef() {
            return refCount.incrementAndGet();
        }

        public int decRef() {
            return refCount.decrementAndGet();
        }

        @Override
        public synchronized void close() throws IOException {
            closed.compareAndSet(false, true);
            RedisClient redisClient = this.getIfExist();
            if (redisClient != null) {
                redisClient.close();
            }
        }
    }
}
