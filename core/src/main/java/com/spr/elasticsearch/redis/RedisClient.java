package com.spr.elasticsearch.redis;

import java.io.Closeable;
import java.util.List;
import java.util.Set;

/**
 * @author rahulanishetty
 * @since 28/07/18.
 */
public interface RedisClient extends Closeable {

    void zAdd(String key, int... docs);

    void zAdd(String key, String... terms);

    void sAdd(String key, long... values);

    void sAdd(String key, String... terms);

    long zCount(String key);

    Set<String> zRange(String key, long min, long max);

    Set<String> zRangeByLex(String key, String min, String max, int offset, int count);

    List<String> sMembers(String key);

    void set(String key, String value);

    String get(String key);
}
