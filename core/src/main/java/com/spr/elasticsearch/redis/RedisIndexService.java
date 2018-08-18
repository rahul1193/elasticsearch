package com.spr.elasticsearch.redis;

import com.spr.elasticsearch.fields.UpdatableFieldHandler;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.shard.ShardId;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author rahulanishetty
 * @since 28/07/18.
 */
public class RedisIndexService implements UpdatableFieldHandler, Closeable {

    private final MapperService mapperService;
    private final IndexFieldDataService indexFieldData;

    private final ConcurrentMap<String, RedisPrefix> segmentsRegistry = new ConcurrentHashMap<>();
    private final AtomicReference<ConcurrentMap<String, String>> fieldToClusterCache = new AtomicReference<>(new ConcurrentHashMap<>());
    private final IndexSettings indexSettings;
    private final RedisIndicesService redisIndicesService;

    public RedisIndexService(IndexSettings indexSettings, RedisIndicesService redisIndicesService, MapperService mapperService, IndexFieldDataService indexFieldData) {
        this.mapperService = mapperService;
        this.indexSettings = indexSettings;
        this.redisIndicesService = redisIndicesService;
        this.indexFieldData = indexFieldData;
        this.mapperService.registerUpdatableFieldHandler(this);
        redisIndicesService.register(indexSettings.getIndex(), this);
    }

    @Override
    public void updateSource(Map<String, Object> source, LeafReader leafReader, int docId) throws Exception {
        for (String field : mapperService.updatableFields()) {
            MappedFieldType fieldType = mapperService.fullName(field);
            if (fieldType.hasDocValues() && fieldType instanceof NumberFieldMapper.NumberFieldType) {
                if (NumberFieldMapper.NumberType.CUSTOM_LONG.name().toLowerCase(Locale.ROOT).equals(fieldType.typeName())) {
                    List<?> values = fetchDocValues(fieldType, leafReader, docId);
                    if (values == null) {
                        continue;
                    }
                    UpdatableFieldHandler.setValueAtField(source, field, values);
                }
            }
        }
    }

    private List<?> fetchDocValues(MappedFieldType fieldType, LeafReader leafReader, int docId) throws Exception {
        IndexFieldData<?> indexFieldData = this.indexFieldData.getForField(fieldType);
        AtomicFieldData atomicFieldData = indexFieldData.loadDirect(leafReader.getContext());
        ScriptDocValues scriptValues = atomicFieldData.getScriptValues();
        scriptValues.setNextDocId(docId);
        return scriptValues.getValues();
    }

    public void consumeSegment(RedisPrefix redisPrefix, String segmentName, String field, Map<String, List<Integer>> index) {
        RedisIndicesService.RedisClientLookup client = getRedisClient(field);
        client.incRef();
        try {
            for (Map.Entry<String, List<Integer>> entry : index.entrySet()) {
                String indexKey = RedisUtils.createIndexKey(redisPrefix, segmentName, field, entry.getKey());
                if (entry.getValue().isEmpty()) {
                    continue;
                }
                client.getOrCreate().zAdd(indexKey, CollectionUtils.toArray(entry.getValue()));
            }
            String termsKey = RedisUtils.createTermsKey(redisPrefix, segmentName, field);
            client.getOrCreate().zAdd(termsKey, index.keySet().toArray(new String[0]));
            Set<Integer> docs = index.values().stream().flatMap(List::stream).collect(Collectors.toSet());
            client.getOrCreate().set(RedisUtils.createSegmentKey(redisPrefix, segmentName, field), String.valueOf(docs.size()));
        } finally {
            client.decRef();
        }
    }

    public int getDocCount(RedisPrefix redisPrefix, String segmentName, String field) {
        RedisIndicesService.RedisClientLookup client = getRedisClient(field);
        client.incRef();
        try {
            String value = client.getOrCreate().get(RedisUtils.createSegmentKey(redisPrefix, segmentName, field));
            if (value == null) {
                return -1;
            }
            return Integer.valueOf(value);
        } finally {
            client.decRef();
        }
    }

    public int getDocCountForTerm(RedisPrefix redisPrefix, String segmentName, String field, String term) {
        RedisIndicesService.RedisClientLookup client = getRedisClient(field);
        client.incRef();
        try {
            return (int) client.getOrCreate().zCount(RedisUtils.createIndexKey(redisPrefix, segmentName, field, term));
        } finally {
            client.decRef();
        }
    }

    public void consumeDocValues(RedisPrefix redisPrefix, String segmentName, String field, Map<Integer, List<String>> docVsValues) {
        if (docVsValues.isEmpty()) {
            return;
        }
        RedisIndicesService.RedisClientLookup client = getRedisClient(field);
        client.incRef();
        try {
            for (Map.Entry<Integer, List<String>> entry : docVsValues.entrySet()) {
                String docValuesKey = RedisUtils.createDocValuesKey(redisPrefix, segmentName, field, entry.getKey());
                client.getOrCreate().sAdd(docValuesKey, entry.getValue().toArray(new String[0]));
            }
        } finally {
            client.decRef();
        }
    }

    public void consumeNumericDocValues(RedisPrefix redisPrefix, String segmentName, String field, Map<Integer, List<Long>> docVsValues) {
        if (docVsValues.isEmpty()) {
            return;
        }
        RedisIndicesService.RedisClientLookup client = getRedisClient(field);
        client.incRef();
        try {
            for (Map.Entry<Integer, List<Long>> entry : docVsValues.entrySet()) {
                String docValuesKey = RedisUtils.createDocValuesKey(redisPrefix, segmentName, field, entry.getKey());
                client.getOrCreate().sAdd(docValuesKey, toLongArray(entry.getValue()));
            }
        } finally {
            client.decRef();
        }
    }

    public List<String> fetchDocValues(RedisPrefix redisPrefix, String segmentName, String field, int docId) {
        RedisIndicesService.RedisClientLookup client = getRedisClient(field);
        client.incRef();
        try {
            String docValuesKey = RedisUtils.createDocValuesKey(redisPrefix, segmentName, field, docId);
            return client.getOrCreate().sMembers(docValuesKey);
        } finally {
            client.decRef();
        }
    }

    public long termsSize(RedisPrefix redisPrefix, String segmentName, String field) {
        RedisIndicesService.RedisClientLookup redisClient = getRedisClient(field);
        redisClient.incRef();
        try {
            return redisClient.getOrCreate().zCount(RedisUtils.createTermsKey(redisPrefix, segmentName, field));
        } finally {
            redisClient.decRef();
        }
    }

    public String seekCeil(RedisPrefix redisPrefix, String segmentName, String field, String term) {
        return fetchTermFrom(redisPrefix, segmentName, field, term, true);
    }

    public String next(RedisPrefix redisPrefix, String segmentName, String field, String term) {
        return fetchTermFrom(redisPrefix, segmentName, field, term, false);
    }

    public int[] getDocAfter(RedisPrefix redisPrefix, String segmentName, String field, String term, Integer docId, int batchSize) {
        long min = 0;
        if (docId != null) {
            min = docId + 1;
        }
        RedisIndicesService.RedisClientLookup redisClient = getRedisClient(field);
        redisClient.incRef();
        try {
            String key = RedisUtils.createIndexKey(redisPrefix, segmentName, field, term);
            Set<String> values = redisClient.getOrCreate().zRange(key, min, Long.MAX_VALUE);
            if (values == null || values.isEmpty()) {
                return new int[0];
            }
            return values.stream().mapToInt(Integer::valueOf).toArray();
        } finally {
            redisClient.decRef();
        }
    }

    private String fetchTermFrom(RedisPrefix redisPrefix, String segmentName, String field, String term, boolean inclusive) {
        RedisIndicesService.RedisClientLookup client = getRedisClient(field);
        client.incRef();
        try {
            String key = RedisUtils.createTermsKey(redisPrefix, segmentName, field);
            String min = "-";
            if (term != null) {
                min = (inclusive ? "[" : "(") + term;
            }
            Set<String> values = client.getOrCreate().zRangeByLex(key, min, "+", 0, 1);
            if (values == null || values.isEmpty()) {
                return null;
            }
            return values.iterator().next();
        } finally {
            client.decRef();
        }
    }

    private RedisIndicesService.RedisClientLookup getRedisClient(String field) {
        String redisClusterSeeds = fieldToClusterCache.get().computeIfAbsent(field, new Function<String, String>() {
            @Override
            public String apply(String field) {
                final MappedFieldType fieldType = mapperService.fullName(field);
                assert fieldType instanceof NumberFieldMapper.NumberFieldType;
                assert NumberFieldMapper.NumberType.CUSTOM_LONG.name().toLowerCase(Locale.ROOT).equals(fieldType.typeName());
                Map<String, Object> additional = ((NumberFieldMapper.NumberFieldType) fieldType).getAdditional();
                if (additional == null || additional.isEmpty()) {
                    throw new IllegalArgumentException("no cluster configured for custom_long");
                }
                Object redisClusterSeeds = additional.get("redis_cluster");
                if (redisClusterSeeds == null) {
                    throw new IllegalArgumentException("no cluster configured for custom_long");
                }
                return String.valueOf(redisClusterSeeds);
            }
        });
        return redisIndicesService.getOrCreateRedisClient(indexSettings, String.valueOf(redisClusterSeeds));
    }

    @Override
    public void close() throws IOException {
        ConcurrentMap<String, String> cache = fieldToClusterCache.getAndSet(null);
        for (String clusterName : cache.values()) {
            redisIndicesService.tryRemoveUnused(clusterName);
        }
        redisIndicesService.unregisterIndexService(indexSettings.getIndex());
    }

    private static long[] toLongArray(Collection<Long> doubles) {
        Objects.requireNonNull(doubles);
        return doubles.stream().mapToLong(s -> s).toArray();
    }

    public RedisPrefix getOrCreatePrefix(ShardId shardId, byte[] segmentId) {
        Objects.requireNonNull(shardId, "shardId cannot be null");
        String key = createSegmentKey(shardId, segmentId);
        RedisPrefix redisPrefix = segmentsRegistry.get(key);
        if (redisPrefix == null) {
            redisPrefix = new RedisPrefix(shardId, UUIDs.base64UUID());
            RedisPrefix existing = segmentsRegistry.putIfAbsent(key, redisPrefix);
            if (existing != null) {
                redisPrefix = existing;
            }
        }
        return redisPrefix;
    }

    public void registerSegment(RedisPrefix redisPrefix, byte[] segmentId) {
        RedisPrefix existing = segmentsRegistry.putIfAbsent(createSegmentKey(redisPrefix.getShardId(), segmentId), redisPrefix);
        if (existing != null) {
            assert existing.equals(redisPrefix);
            if (!existing.equals(redisPrefix)) {
                throw new IllegalStateException("Rahul You Screwed up!");
            }
        }
    }

    private String createSegmentKey(ShardId shardId, byte[] segmentId) {
        return shardId + "_" + new String(segmentId);
    }
}
