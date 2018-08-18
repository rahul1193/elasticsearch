package com.spr.elasticsearch.redis.codec;

import com.spr.elasticsearch.redis.RedisIndexService;
import com.spr.elasticsearch.redis.RedisPrefix;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.*;

import static com.spr.elasticsearch.redis.codec.RedisBackedDocValuesFormat.EXTENSION;
import static com.spr.elasticsearch.redis.codec.RedisBackedDocValuesFormat.createUnsupportedOperationException;
import static com.spr.elasticsearch.redis.codec.RedisBackedPostingsFormat.CODEC_NAME;
import static com.spr.elasticsearch.redis.codec.RedisBackedPostingsFormat.VERSION_CURRENT;

/**
 * @author rahulanishetty
 * @since 23/07/18.
 */
public class RedisBackedDocValuesConsumer extends DocValuesConsumer {

    private final RedisPrefix redisPrefix;
    private final String field;
    private final RedisIndexService redisIndexService;
    private final SegmentWriteState segmentWriteState;

    public RedisBackedDocValuesConsumer(RedisPrefix redisPrefix, String field, RedisIndexService redisIndexService, SegmentWriteState segmentWriteState) {
        this.redisPrefix = redisPrefix;
        this.field = field;
        this.redisIndexService = redisIndexService;
        this.segmentWriteState = segmentWriteState;
    }

    @Override
    public void addNumericField(FieldInfo fieldInfo, Iterable<Number> values) {
        int docId = 0;
        Map<Integer, List<Long>> docVsValues = new HashMap<>();
        for (Number value : values) {
            if (value != null) {
                docVsValues.put(docId, Collections.singletonList(value.longValue()));
            }
            docId++;
        }
        this.redisIndexService.consumeNumericDocValues(redisPrefix, segmentWriteState.segmentInfo.name, field, docVsValues);
    }

    @Override
    public void addSortedNumericField(FieldInfo fieldInfo, Iterable<Number> docToValueCount, Iterable<Number> values) {
        int docId = 0;
        Map<Integer, List<Long>> docVsValues = new HashMap<>();
        Iterator<Number> iterator = values.iterator();
        for (Number number : docToValueCount) {
            int count = number.intValue();
            if (count != 0) {
                List<Long> docValues = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    Number value = iterator.next();
                    docValues.add(value.longValue());
                }
                docVsValues.put(docId, docValues);
            }
            docId++;
        }
        this.redisIndexService.consumeNumericDocValues(redisPrefix, segmentWriteState.segmentInfo.name, field, docVsValues);
    }

    @Override
    public void addBinaryField(FieldInfo fieldInfo, Iterable<BytesRef> values) {
        int docId = 0;
        Map<Integer, List<String>> docVsValues = new HashMap<>();
        for (BytesRef value : values) {
            if (value != null) {
                docVsValues.put(docId, Collections.singletonList(value.utf8ToString()));
            }
            docId++;
        }
        this.redisIndexService.consumeDocValues(redisPrefix, segmentWriteState.segmentInfo.name, field, docVsValues);
    }

    @Override
    public void addSortedField(FieldInfo fieldInfo, Iterable<BytesRef> values, Iterable<Number> docToOrd) {
        throw createUnsupportedOperationException();
    }

    @Override
    public void addSortedSetField(FieldInfo fieldInfo, Iterable<BytesRef> values, Iterable<Number> docToOrdCount, Iterable<Number> ords) {
        throw createUnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        String fileName = IndexFileNames.segmentFileName(segmentWriteState.segmentInfo.name, segmentWriteState.segmentSuffix, EXTENSION);
        IndexOutput output = segmentWriteState.directory.createOutput(fileName, segmentWriteState.context);
        CodecUtil.writeIndexHeader(output, CODEC_NAME, VERSION_CURRENT, segmentWriteState.segmentInfo.getId(), segmentWriteState.segmentSuffix);
        output.writeString(this.redisPrefix.toString());
        output.writeString(this.field);
        CodecUtil.writeFooter(output);
        output.close();
    }
}
