package com.spr.elasticsearch.redis;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.shard.ShardId;

import java.util.Objects;

/**
 * @author rahulanishetty
 * @since 18/08/18.
 */
public final class RedisPrefix {
    private final String shardId;
    private final String id;

    public RedisPrefix(ShardId shardId, String id) {
        this.shardId = Objects.requireNonNull(shardId, "shardId cannot be null").toString();
        this.id = Objects.requireNonNull(id, "id cannot be null");
    }

    public ShardId getShardId() {
        return ShardId.fromString(shardId);
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RedisPrefix that = (RedisPrefix) o;

        if (!shardId.equals(that.shardId)) return false;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        int result = shardId.hashCode();
        result = 31 * result + id.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return shardId + "/" + id;
    }

    public static RedisPrefix fromString(String redisPrefixStr) {
        if (!Strings.hasLength(redisPrefixStr)) {
            return null;
        }
        int index = redisPrefixStr.indexOf("/");
        if (index == -1) {
            throw new IllegalArgumentException("Invalid sytax for redis prefix");
        }
        ShardId shardId = ShardId.fromString(redisPrefixStr.substring(0, index));
        String id = redisPrefixStr.substring(index + 1);
        return new RedisPrefix(shardId, id);
    }
}
