package com.spr.elasticsearch.redis;

import org.elasticsearch.index.shard.ShardId;

/**
 * @author rahulanishetty
 * @since 28/07/18.
 */
public class RedisUtils {

    public static String createName(String segmentName, String field) {
        return segmentName + "/" + field;
    }

    public static String createDocValuesKey(ShardId shardId, String name, int docId) {
        return "dV/" + shardId + "/" + name + "/" + docId;
    }

    public static String createTermsKey(ShardId shardId, String name) {
        return "T/" + shardId + "/" + name;
    }

    public static String createIndexKey(ShardId shardId, String name, String value) {
        return "iD/" + shardId + "/" + name + "/" + value;
    }

    public static String createSegmentKey(ShardId shardId, String name) {
        return "S/" + shardId + "/" + name;
    }

}
