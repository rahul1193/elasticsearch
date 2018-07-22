package com.spr.elasticsearch.redis.codec;

import com.spr.elasticsearch.redis.RedisIndexService;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.spr.elasticsearch.redis.codec.RedisBackedDocValuesFormat.createUnsupportedOperationException;

/**
 * @author rahulanishetty
 * @since 23/07/18.
 */
public class RedisBackedDocValuesProducer extends DocValuesProducer {

    private final ShardId shardId;
    private final String field;
    private final RedisIndexService redisIndexService;
    private final SegmentReadState segmentReadState;

    public RedisBackedDocValuesProducer(ShardId shardId, String field, RedisIndexService redisIndexService, SegmentReadState segmentReadState) {
        this.shardId = shardId;
        this.field = field;
        this.redisIndexService = redisIndexService;
        this.segmentReadState = segmentReadState;
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo fieldInfo) throws IOException {
        return new NumericDocValues() {
            private int currentDoc = -1;
            private long value;

            @Override
            public long get(int docId) {
                if (docId == currentDoc) {
                    return value;
                }
                currentDoc = docId;
                List<String> values = redisIndexService.fetchDocValues(shardId, segmentReadState.segmentInfo.name, field, docId);
                if (values == null || values.isEmpty()) {
                    return 0;
                }
                this.value = Double.valueOf(values.iterator().next()).longValue();
                return value;
            }
        };
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo fieldInfo) throws IOException {
        return new SortedNumericDocValues() {
            int currentDoc = -1;
            boolean valuesFetched = false;
            private List<Long> values = null;

            @Override
            public void setDocument(int docId) {
                if (currentDoc != docId) {
                    currentDoc = docId;
                    valuesFetched = false;
                }
            }

            @Override
            public long valueAt(int i) {
                ensureInitialized();
                return this.values.get(i);
            }

            @Override
            public int count() {
                ensureInitialized();
                return values.size();
            }

            private void ensureInitialized() {
                if (!valuesFetched) {
                    initValues();
                }
                valuesFetched = true;
            }

            private void initValues() {
                List<String> values = redisIndexService.fetchDocValues(shardId, segmentReadState.segmentInfo.name, field, currentDoc);
                if (values == null || values.isEmpty()) {
                    this.values = Collections.emptyList();
                    return;
                }
                this.values = values.stream().map(Double::valueOf).map(Double::longValue).collect(Collectors.toList());
                Collections.sort(this.values);
            }
        };
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo fieldInfo) {
        return new BinaryDocValues() {
            private int currentDoc = -1;
            private BytesRef value;

            @Override
            public BytesRef get(int docId) {
                if (docId == currentDoc) {
                    return value;
                }
                currentDoc = docId;
                List<String> values = redisIndexService.fetchDocValues(shardId, segmentReadState.segmentInfo.name, field, docId);
                if (values == null || values.isEmpty()) {
                    return null;
                }
                this.value = new BytesRef(values.iterator().next());
                return value;
            }
        };
    }

    @Override
    public SortedDocValues getSorted(FieldInfo fieldInfo) throws IOException {
        throw createUnsupportedOperationException();
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo fieldInfo) throws IOException {
        throw createUnsupportedOperationException();
    }

    @Override
    public Bits getDocsWithField(FieldInfo fieldInfo) throws IOException {
        DocValuesType docValuesType = fieldInfo.getDocValuesType();
        switch (docValuesType) {
            case BINARY:
            case SORTED_NUMERIC:
            case NUMERIC:
                return new Bits() {
                    @Override
                    public boolean get(int docId) {
                        List<String> docValues = redisIndexService.fetchDocValues(shardId, segmentReadState.segmentInfo.name, field, docId);
                        return docValues != null && !docValues.isEmpty();
                    }

                    @Override
                    public int length() {
                        return segmentReadState.segmentInfo.maxDoc();
                    }
                };
            case NONE:
                return new Bits.MatchNoBits(segmentReadState.segmentInfo.maxDoc());
            default:
                throw new UnsupportedOperationException("unsupported doc values type : " + docValuesType);
        }
    }

    @Override
    public void checkIntegrity() throws IOException {
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public long ramBytesUsed() {
        return RamUsageEstimator.shallowSizeOfInstance(getClass());
    }
}
