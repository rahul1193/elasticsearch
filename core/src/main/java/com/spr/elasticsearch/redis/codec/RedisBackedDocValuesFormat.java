package com.spr.elasticsearch.redis.codec;

import com.spr.elasticsearch.redis.RedisIndexService;
import com.spr.elasticsearch.redis.RedisIndicesService;
import com.spr.elasticsearch.redis.RedisPrefix;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

import static com.spr.elasticsearch.redis.codec.RedisBackedPostingsFormat.CODEC_NAME;
import static com.spr.elasticsearch.redis.codec.RedisBackedPostingsFormat.VERSION_CURRENT;

/**
 * @author rahulanishetty
 * @since 23/07/18.
 */
public class RedisBackedDocValuesFormat extends DocValuesFormat {

    public static final String EXTENSION = "rbldvd";

    private ShardId shardId;
    private String field;

    public RedisBackedDocValuesFormat() {
        super("RedisBackedLong");
        this.shardId = null;
        this.field = null;
    }

    public RedisBackedDocValuesFormat(ShardId shardId, String field) {
        super("RedisBackedLong");
        this.shardId = shardId;
        this.field = field;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState segmentWriteState) throws IOException {
        assert shardId != null;
        RedisIndexService redisIndexService = RedisIndicesService.getInstance().getIndexService(shardId.getIndex());
        RedisPrefix redisPrefix = redisIndexService.getOrCreatePrefix(shardId, segmentWriteState.segmentInfo.getId());
        return new RedisBackedDocValuesConsumer(redisPrefix, field, redisIndexService, segmentWriteState);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState segmentReadState) throws IOException {
        String fileName = IndexFileNames.segmentFileName(segmentReadState.segmentInfo.name, segmentReadState.segmentSuffix, EXTENSION);
        IndexInput indexInput = segmentReadState.directory.openInput(fileName, segmentReadState.context);
        try {
            CodecUtil.checkIndexHeader(indexInput, CODEC_NAME, VERSION_CURRENT, VERSION_CURRENT, segmentReadState.segmentInfo.getId(), segmentReadState.segmentSuffix);
            String redisPrefixStr = indexInput.readString();
            String field = indexInput.readString();
            RedisPrefix redisPrefix = RedisPrefix.fromString(redisPrefixStr);
            assert redisPrefix != null;
            RedisIndexService indexService = RedisIndicesService.getInstance().getIndexService(redisPrefix.getShardId().getIndex());
            indexService.registerSegment(redisPrefix, segmentReadState.segmentInfo.getId());
            return new RedisBackedDocValuesProducer(redisPrefix, field, indexService, segmentReadState);
        } finally {
            IOUtils.closeWhileHandlingException(indexInput);
        }
    }


    public static UnsupportedOperationException createUnsupportedOperationException() {
        return new UnsupportedOperationException("ordinal based doc values is not supported");
    }
}
