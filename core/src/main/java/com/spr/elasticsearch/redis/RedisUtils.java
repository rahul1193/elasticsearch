package com.spr.elasticsearch.redis;

/**
 * @author rahulanishetty
 * @since 28/07/18.
 */
public class RedisUtils {

    public static String createDocValuesKey(RedisPrefix redisPrefix, String segmentName, String field, int docId) {
        return "dV/" + redisPrefix + "/" + segmentName + "/" + field + "/" + docId;
    }

    public static String createTermsKey(RedisPrefix redisPrefix, String segmentName, String field) {
        return "T/" + redisPrefix + "/" + segmentName + "/" + field;
    }

    public static String createIndexKey(RedisPrefix redisPrefix, String segmentName, String field, String value) {
        return "iD/" + redisPrefix + "/" + segmentName + "/" + field + "/" + value;
    }

    public static String createSegmentKey(RedisPrefix redisPrefix, String segmentName, String field) {
        return "S/" + redisPrefix + "/" + segmentName + "/" + field;
    }

}
