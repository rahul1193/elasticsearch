package com.spr.elasticsearch.redis.codec;

import com.spr.elasticsearch.redis.RedisIndexService;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.index.*;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.spr.elasticsearch.redis.codec.RedisBackedPostingsFormat.*;

/**
 * @author rahulanishetty
 * @since 21/07/18.
 */
public class RedisBackedFieldsConsumer extends FieldsConsumer {

    private final ShardId shardId;
    private final String field;
    private final SegmentWriteState segmentWriteState;

    private final Map<String, List<Integer>> index = new HashMap<>();
    private final RedisIndexService indexService;

    public RedisBackedFieldsConsumer(ShardId shardId, String field, RedisIndexService indexService, SegmentWriteState segmentWriteState) {
        this.shardId = shardId;
        this.field = field;
        this.indexService = indexService;
        this.segmentWriteState = segmentWriteState;
    }

    @Override
    public void write(Fields fields) throws IOException {
        Terms terms = fields.terms(field);
        if (terms == null) {
            return;
        }
        TermsEnum termsEnum = terms.iterator();
        PostingsEnum postingsEnum = null;
        while (true) {
            BytesRef term = termsEnum.next();
            if (term == null) {
                break;
            }
            postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
            while (true) {
                int doc = postingsEnum.nextDoc();
                if (doc == PostingsEnum.NO_MORE_DOCS) {
                    break;
                }
                index.computeIfAbsent(term.utf8ToString(), key -> new ArrayList<>()).add(doc);
            }
        }
    }

    @Override
    public void close() throws IOException {
        indexService.consumeSegment(shardId, segmentWriteState.segmentInfo.name, field, index);
        String fileName = IndexFileNames.segmentFileName(segmentWriteState.segmentInfo.name, segmentWriteState.segmentSuffix, EXTENSION);
        IndexOutput output = segmentWriteState.directory.createOutput(fileName, segmentWriteState.context);
        CodecUtil.writeIndexHeader(output, CODEC_NAME, VERSION_CURRENT, segmentWriteState.segmentInfo.getId(), segmentWriteState.segmentSuffix);
        output.writeString(shardId.toString());
        output.writeString(field);
        CodecUtil.writeFooter(output);
        output.close();
    }
}
