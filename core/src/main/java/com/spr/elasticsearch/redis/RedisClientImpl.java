package com.spr.elasticsearch.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author rahulanishetty
 * @since 28/07/18.
 */
public class RedisClientImpl implements RedisClient {

    private final JedisPool jedis;

    public RedisClientImpl(HostAndPort hostAndPort, JedisPoolConfig jedisPoolConfig) {
        this.jedis = new JedisPool(jedisPoolConfig, hostAndPort.getHost(), hostAndPort.getPort());
    }

    @Override
    public void set(String key, String value) {
        consumeResource(jedis -> jedis.set(key, value));
    }

    @Override
    public String get(String key) {
        return doWithResource(jedis -> jedis.get(key));
    }

    @Override
    public void zAdd(String key, int... docs) {
        Map<String, Double> docsWithScore = new HashMap<>();
        for (int doc : docs) {
            docsWithScore.put(String.valueOf(doc), (double) doc);
        }
        consumeResource(jedis -> jedis.zadd(key, docsWithScore));
    }

    @Override
    public void zAdd(String key, String... terms) {
        consumeResource(jedis -> jedis.zadd(key, Arrays.stream(terms).collect(Collectors.toMap(Function.identity(), score -> 0D))));
    }

    @Override
    public void sAdd(String key, long... values) {
        String[] terms = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            terms[i] = String.valueOf(values[i]);
        }
        consumeResource(jedis -> {
            jedis.sadd(key, terms);
        });
    }

    @Override
    public void sAdd(String key, String... terms) {
        consumeResource(jedis -> {
            jedis.sadd(key, terms);
        });
    }

    @Override
    public long zCount(String key) {
        return doWithResource(jedis -> jedis.zcount(key, -1d, Double.MAX_VALUE));
    }

    @Override
    public Set<String> zRange(String key, long min, long max) {
        return doWithResource(jedis -> jedis.zrange(key, min, max));
    }

    @Override
    public Set<String> zRangeByLex(String key, String min, String max, int offset, int count) {
        return doWithResource(jedis -> jedis.zrangeByLex(key, min, max, offset, count));
    }

    @Override
    public List<String> sMembers(String key) {
        return doWithResource(jedis -> {
            Set<String> smembers = jedis.smembers(key);
            if (smembers == null || smembers.isEmpty()) {
                return null;
            }
            return new ArrayList<>(smembers);
        });
    }

    @Override
    public void close() throws IOException {
        jedis.close();
    }

    private <Return> Return doWithResource(Function<Jedis, Return> function) {
        Jedis resource = this.jedis.getResource();
        try {
            return function.apply(resource);
        } finally {
            resource.close();
        }
    }

    private void consumeResource(Consumer<Jedis> consumer) {
        Jedis resource = this.jedis.getResource();
        try {
            consumer.accept(resource);
        } finally {
            resource.close();
        }
    }
}
