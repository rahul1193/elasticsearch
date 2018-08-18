package com.spr.elasticsearch.redis.codec;

import com.spr.elasticsearch.redis.RedisIndexService;
import com.spr.elasticsearch.redis.RedisPrefix;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

/**
 * @author rahulanishetty
 * @since 21/07/18.
 */
public class RedisBackedFieldsProducer extends FieldsProducer {

    private final RedisPrefix redisPrefix;
    private final String field;
    private final RedisIndexService redisIndexService;
    private final SegmentReadState segmentReadState;
    private final Terms terms;

    public RedisBackedFieldsProducer(RedisPrefix redisPrefix, String field, RedisIndexService redisIndexService, SegmentReadState segmentReadState) {
        this.redisPrefix = redisPrefix;
        this.field = field;
        this.redisIndexService = redisIndexService;
        this.segmentReadState = segmentReadState;
        this.terms = new RedisBackedTerms();
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void checkIntegrity() {
    }

    @Override
    public Iterator<String> iterator() {
        return Collections.singletonList(field).iterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
        return terms;
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public long ramBytesUsed() {
        return RamUsageEstimator.shallowSizeOfInstance(this.getClass());
    }


    public class RedisBackedTerms extends Terms {

        @Override
        public TermsEnum iterator() {
            return new Iterator();
        }

        @Override
        public long size() {
            return redisIndexService.termsSize(redisPrefix, segmentReadState.segmentInfo.name, field);
        }

        @Override
        public int getDocCount() {
            return redisIndexService.getDocCount(redisPrefix, segmentReadState.segmentInfo.name, field);
        }

        @Override
        public long getSumTotalTermFreq() {
            return -1;
        }

        @Override
        public long getSumDocFreq() {
            return -1;
        }

        @Override
        public boolean hasFreqs() {
            return false;
        }

        @Override
        public boolean hasOffsets() {
            return false;
        }

        @Override
        public boolean hasPositions() {
            return false;
        }

        @Override
        public boolean hasPayloads() {
            return false;
        }

        private class Iterator extends TermsEnum {
            private String currentTerm;
            boolean ended = false;

            Iterator() {
            }

            @Override
            public SeekStatus seekCeil(BytesRef bytesRef) {
                String ceil = redisIndexService.seekCeil(redisPrefix, segmentReadState.segmentInfo.name, field, bytesRef.utf8ToString());
                if (ceil == null) {
                    ended = true;
                    currentTerm = null;
                    return SeekStatus.END;
                }
                currentTerm = ceil;
                return currentTerm.equals(bytesRef.utf8ToString()) ? SeekStatus.FOUND : SeekStatus.NOT_FOUND;
            }

            @Override
            public BytesRef term() {
                initIfRequired();
                return new BytesRef(currentTerm);
            }


            @Override
            public int docFreq() {
                initIfRequired();
                return redisIndexService.getDocCountForTerm(redisPrefix, segmentReadState.segmentInfo.name, field, currentTerm);
            }

            @Override
            public PostingsEnum postings(PostingsEnum postingsEnum, int flags) {
                initIfRequired();
                if (ended) {
                    return null;
                }
                return new RedisBackedPostingsEnum(currentTerm);
            }

            @Override
            public BytesRef next() {
                boolean initialized = initIfRequired();
                if (initialized) {
                    return new BytesRef(currentTerm);
                }
                String next = redisIndexService.next(redisPrefix, segmentReadState.segmentInfo.name, field, currentTerm);
                if (next == null) {
                    ended = true;
                    currentTerm = null;
                    return null;
                }
                currentTerm = next;
                return new BytesRef(currentTerm);
            }

            private boolean initIfRequired() {
                if (ended) {
                    return false;
                }
                if (currentTerm != null) {
                    return false;
                }
                currentTerm = redisIndexService.seekCeil(redisPrefix, segmentReadState.segmentInfo.name, field, null);
                return true;
            }


            @Override
            public long totalTermFreq() {
                return -1;
            }

            @Override
            public void seekExact(long l) {
                throw new UnsupportedOperationException("ordinals is not supported");
            }

            @Override
            public long ord() {
                throw new UnsupportedOperationException("ordinals is not supported");
            }
        }

        private class RedisBackedPostingsEnum extends PostingsEnum {

            private static final int BATCH_SIZE = 10000;

            private final String term;
            private boolean initialized;
            private boolean lastBatch = false;

            private int[] docs;

            int index;
            private int cost = -1;

            RedisBackedPostingsEnum(String term) {
                this.term = term;
            }

            @Override
            public int docID() {
                if (!initialized) {
                    return -1;
                }
                if (index == docs.length) {
                    return NO_MORE_DOCS;
                }
                return docs[index];
            }

            @Override
            public int nextDoc() {
                int docID = docID();
                if (docID == NO_MORE_DOCS) {
                    return NO_MORE_DOCS;
                }
                return advance(docID + 1);
            }

            @Override
            public int advance(int docId) {
                initIfRequired();
                int index = Arrays.binarySearch(docs, docId);
                if (index < 0) {
                    index = -index - 1;
                }
                while (index == docs.length) {
                    if (lastBatch) {
                        break;
                    } else {
                        initNextDocsBatch();
                        if (docs.length == 0) {
                            break;
                        }
                        index = Arrays.binarySearch(docs, docId);
                        if (index < 0) {
                            index = -index - 1;
                        }
                    }
                }
                if (index == docs.length) {
                    return NO_MORE_DOCS;
                }
                this.index = index;
                return docs[this.index];
            }

            @Override
            public long cost() {
                if (cost == -1) {
                    cost = redisIndexService.getDocCountForTerm(redisPrefix, segmentReadState.segmentInfo.name, field, term);
                }
                return cost;
            }

            private void initIfRequired() {
                if (initialized) {
                    return;
                }
                initialized = true;
                fetchDocs(null);
            }

            private void initNextDocsBatch() {
                fetchDocs(docs[docs.length - 1]);
            }

            private void fetchDocs(Integer currentDocId) {
                int[] docIds = redisIndexService.getDocAfter(redisPrefix, segmentReadState.segmentInfo.name, field, term, currentDocId, BATCH_SIZE);
                if (docIds == null) {
                    docs = new int[0];
                    return;
                }
                resetDocs(docIds);
            }

            private void resetDocs(int[] docIds) {
                Arrays.sort(docIds);
                this.docs = docIds;
                if (docs.length < BATCH_SIZE) {
                    lastBatch = true;
                }
                this.index = 0;
            }

            @Override
            public int freq() {
                return 1;
            }

            @Override
            public int nextPosition() {
                return -1;
            }

            @Override
            public int startOffset() {
                return -1;
            }

            @Override
            public int endOffset() {
                return -1;
            }

            @Override
            public BytesRef getPayload() {
                return null;
            }
        }
    }

}
