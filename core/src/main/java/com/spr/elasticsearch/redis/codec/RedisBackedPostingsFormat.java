package com.spr.elasticsearch.redis.codec;

import com.spr.elasticsearch.redis.RedisIndexService;
import com.spr.elasticsearch.redis.RedisIndicesService;
import com.spr.elasticsearch.redis.RedisPrefix;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * @author rahulanishetty
 * @since 21/07/18.
 */
public class RedisBackedPostingsFormat extends PostingsFormat {

    public static final String CODEC_NAME = "RedisBackedLong";
    public static final int CODEC_VERSION = 1;
    public static final int VERSION_CURRENT = CODEC_VERSION;
    public static final String EXTENSION = "rbl";

    private ShardId shardId;
    private String field;

    public RedisBackedPostingsFormat() {
        super(CODEC_NAME);
    }

    public RedisBackedPostingsFormat(ShardId shardId, String field) {
        super(CODEC_NAME);
        this.shardId = shardId;
        this.field = field;
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState segmentWriteState) throws IOException {
        assert shardId != null;
        RedisIndexService indexService = RedisIndicesService.getInstance().getIndexService(shardId.getIndex());
        RedisPrefix redisKey = indexService.getOrCreatePrefix(shardId, segmentWriteState.segmentInfo.getId());
        return new RedisBackedFieldsConsumer(redisKey, field, indexService, segmentWriteState);
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState segmentReadState) throws IOException {
        String fileName = IndexFileNames.segmentFileName(segmentReadState.segmentInfo.name, segmentReadState.segmentSuffix, EXTENSION);
        IndexInput indexInput = segmentReadState.directory.openInput(fileName, segmentReadState.context);
        CodecUtil.checkIndexHeader(indexInput, CODEC_NAME, VERSION_CURRENT, VERSION_CURRENT, segmentReadState.segmentInfo.getId(), segmentReadState.segmentSuffix);
        String redisPrefixKey = indexInput.readString();
        String field = indexInput.readString();
        RedisPrefix redisPrefix = RedisPrefix.fromString(redisPrefixKey);
        assert redisPrefix != null;
        RedisIndexService indexService = RedisIndicesService.getInstance().getIndexService(redisPrefix.getShardId().getIndex());
        indexService.registerSegment(redisPrefix, segmentReadState.segmentInfo.getId());
        return new RedisBackedFieldsProducer(redisPrefix, field, indexService, segmentReadState);
    }
}
