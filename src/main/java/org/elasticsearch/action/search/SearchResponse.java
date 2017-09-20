/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.facet.Facets;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.suggest.Suggest;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.search.ShardSearchFailure.readShardSearchFailure;
import static org.elasticsearch.search.internal.InternalSearchResponse.newInternalSearchResponse;

/**
 * A response of a search request.
 */
public class SearchResponse extends ActionResponse implements StatusToXContent, WithRestHeaders {

    private InternalSearchResponse internalResponse;

    private String scrollId;

    private int totalShards;

    private int successfulShards;

    private ShardSearchFailure[] shardFailures = ShardSearchFailure.EMPTY_ARRAY;

    private long tookInMillis;

    private Long requestReceivedTime; // epoch millis

    private Long totalTimeTaken; // total time spent in ES side (handleSearch method)

    private Long responseRecvdTimestamp; // epoch millis

    private Long responseReadTime; // time spent in reading response from socket

    private Long responseParseTime; // total time spent in client side while parsing response

    private Integer responseContentLength;

    public SearchResponse() {
    }

    public SearchResponse(InternalSearchResponse internalResponse, String scrollId, int totalShards, int successfulShards, long tookInMillis, ShardSearchFailure[] shardFailures) {
        this.internalResponse = internalResponse;
        this.scrollId = scrollId;
        this.totalShards = totalShards;
        this.successfulShards = successfulShards;
        this.tookInMillis = tookInMillis;
        this.shardFailures = shardFailures;
    }

    public RestStatus status() {
        if (shardFailures.length == 0) {
            if (successfulShards == 0 && totalShards > 0) {
                return RestStatus.SERVICE_UNAVAILABLE;
            }
            return RestStatus.OK;
        }
        // if total failure, bubble up the status code to the response level
        if (successfulShards == 0 && totalShards > 0) {
            RestStatus status = RestStatus.OK;
            for (int i = 0; i < shardFailures.length; i++) {
                RestStatus shardStatus = shardFailures[i].status();
                if (shardStatus.getStatus() >= status.getStatus()) {
                    status = shardFailures[i].status();
                }
            }
            return status;
        }
        return RestStatus.OK;
    }

    /**
     * The search hits.
     */
    public SearchHits getHits() {
        return internalResponse.hits();
    }

    /**
     * The search facets.
     */
    public Facets getFacets() {
        return internalResponse.facets();
    }

    public Aggregations getAggregations() {
        return internalResponse.aggregations();
    }


    public Suggest getSuggest() {
        return internalResponse.suggest();
    }

    /**
     * Has the search operation timed out.
     */
    public boolean isTimedOut() {
        return internalResponse.timedOut();
    }

    /**
     * Has the search operation terminated early due to reaching
     * <code>terminateAfter</code>
     */
    public Boolean isTerminatedEarly() {
        return internalResponse.terminatedEarly();
    }

    /**
     * How long the search took.
     */
    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    /**
     * How long the search took in milliseconds.
     */
    public long getTookInMillis() {
        return tookInMillis;
    }

    /**
     * The total number of shards the search was executed on.
     */
    public int getTotalShards() {
        return totalShards;
    }

    /**
     * The successful number of shards the search was executed on.
     */
    public int getSuccessfulShards() {
        return successfulShards;
    }

    /**
     * The failed number of shards the search was executed on.
     */
    public int getFailedShards() {
        // we don't return totalShards - successfulShards, we don't count "no shards available" as a failed shard, just don't
        // count it in the successful counter
        return shardFailures.length;
    }

    /**
     * The failures that occurred during the search.
     */
    public ShardSearchFailure[] getShardFailures() {
        return this.shardFailures;
    }

    /**
     * If scrolling was enabled ({@link SearchRequest#scroll(org.elasticsearch.search.Scroll)}, the
     * scroll id that can be used to continue scrolling.
     */
    public String getScrollId() {
        return scrollId;
    }

    public void scrollId(String scrollId) {
        this.scrollId = scrollId;
    }

    static final class Fields {
        static final XContentBuilderString _SCROLL_ID = new XContentBuilderString("_scroll_id");
        static final XContentBuilderString _SHARDS = new XContentBuilderString("_shards");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString SUCCESSFUL = new XContentBuilderString("successful");
        static final XContentBuilderString FAILED = new XContentBuilderString("failed");
        static final XContentBuilderString FAILURES = new XContentBuilderString("failures");
        static final XContentBuilderString STATUS = new XContentBuilderString("status");
        static final XContentBuilderString INDEX = new XContentBuilderString("index");
        static final XContentBuilderString SHARD = new XContentBuilderString("shard");
        static final XContentBuilderString REASON = new XContentBuilderString("reason");
        static final XContentBuilderString TOOK = new XContentBuilderString("took");
        static final XContentBuilderString TIMED_OUT = new XContentBuilderString("timed_out");
        static final XContentBuilderString TERMINATED_EARLY = new XContentBuilderString("terminated_early");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (scrollId != null) {
            builder.field(Fields._SCROLL_ID, scrollId);
        }
        builder.field(Fields.TOOK, tookInMillis);
        builder.field(Fields.TIMED_OUT, isTimedOut());
        if (isTerminatedEarly() != null) {
            builder.field(Fields.TERMINATED_EARLY, isTerminatedEarly());
        }
        builder.startObject(Fields._SHARDS);
        builder.field(Fields.TOTAL, getTotalShards());
        builder.field(Fields.SUCCESSFUL, getSuccessfulShards());
        builder.field(Fields.FAILED, getFailedShards());

        if (shardFailures.length > 0) {
            builder.startArray(Fields.FAILURES);
            for (ShardSearchFailure shardFailure : shardFailures) {
                builder.startObject();
                if (shardFailure.shard() != null) {
                    builder.field(Fields.INDEX, shardFailure.shard().index());
                    builder.field(Fields.SHARD, shardFailure.shard().shardId());
                }
                builder.field(Fields.STATUS, shardFailure.status().getStatus());
                builder.field(Fields.REASON, shardFailure.reason());
                builder.endObject();
            }
            builder.endArray();
        }

        builder.endObject();
        internalResponse.toXContent(builder, params);
        return builder;
    }

    public static SearchResponse readSearchResponse(StreamInput in) throws IOException {
        SearchResponse response = new SearchResponse();
        response.readFrom(in);
        return response;
    }

    public Long getRequestReceivedTime() {
        return requestReceivedTime;
    }

    public Long getTotalTimeTaken() {
        return totalTimeTaken;
    }

    public Long getResponseRecvdTimestamp() {
        return responseRecvdTimestamp;
    }

    public Long getResponseParseTime() {
        return responseParseTime;
    }

    public Integer getResponseContentLength() {
        return responseContentLength;
    }

    public Long getResponseReadTime() {
        return responseReadTime;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        internalResponse = newInternalSearchResponse(in);
        totalShards = in.readVInt();
        successfulShards = in.readVInt();
        int size = in.readVInt();
        if (size == 0) {
            shardFailures = ShardSearchFailure.EMPTY_ARRAY;
        } else {
            shardFailures = new ShardSearchFailure[size];
            for (int i = 0; i < shardFailures.length; i++) {
                shardFailures[i] = readShardSearchFailure(in);
            }
        }
        scrollId = in.readOptionalString();
        tookInMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        internalResponse.writeTo(out);
        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);

        out.writeVInt(shardFailures.length);
        for (ShardSearchFailure shardSearchFailure : shardFailures) {
            shardSearchFailure.writeTo(out);
        }

        out.writeOptionalString(scrollId);
        out.writeVLong(tookInMillis);
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }

    enum JsonField implements XContentObjectParseable<SearchResponse> {
        error {
            @Override
            public void apply(XContentObject in, SearchResponse response) throws IOException {
                throw new RuntimeException(in.get(this));
            }

        },
        _scroll_id {
            @Override
            public void apply(XContentObject in, SearchResponse response) throws IOException {
                response.scrollId = in.get(this);
            }

        },
        took {
            @Override
            public void apply(XContentObject in, SearchResponse response) throws IOException {
                response.tookInMillis = in.getAsLong(this);
            }

        },
        timed_out {
            @Override
            public void apply(XContentObject in, SearchResponse response) throws IOException {
                response.internalResponse = newInternalSearchResponse(in.getAsBoolean(this));
            }

        },
        hits {
            @Override
            public void apply(XContentObject in, SearchResponse response) throws IOException {
                response.internalResponse.readHits(in.getAsXContentObject(this));
            }

        },
        aggregations {
            @Override
            public void apply(XContentObject in, SearchResponse response) throws IOException {
                response.internalResponse.readAggregations(in.getAsXContentObject(this));
            }

        },
        _shards {
            @Override
            public void apply(XContentObject in, SearchResponse response) throws IOException {
                XContentObject shardInfo = in.getAsXContentObject(this);
                response.totalShards = shardInfo.getAsInt("total");
                response.successfulShards = shardInfo.getAsInt("successful");
            }
        }
    }

    @Override
    public void readFrom(XContentObject in) throws IOException {
        XContentHelper.populate(in, JsonField.values(), this);
    }

    @Override
    public void readHeaders(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return;
        }
        String requestReceivedTime = headers.get("Request-Received-Time");
        if (Strings.hasLength(requestReceivedTime)) {
            this.requestReceivedTime = Long.valueOf(requestReceivedTime);
        }

        String totalTime = headers.get("Total-Time-Taken");
        if (Strings.hasLength(totalTime)) {
            this.totalTimeTaken = Long.valueOf(totalTime);
        }

        String responseParseTime = headers.get("esResponseParseTime");
        if (Strings.hasLength(responseParseTime)) {
            this.responseParseTime = Long.valueOf(responseParseTime);
        }

        String responseRecvdTimestamp = headers.get("responseRecvdTimestamp");
        if (Strings.hasLength(responseRecvdTimestamp)) {
            this.responseRecvdTimestamp = Long.valueOf(responseRecvdTimestamp);
        }

        String contentLength = headers.get("Content-Length");
        if (Strings.hasLength(contentLength)) {
            this.responseContentLength = Integer.valueOf(contentLength);
        }

        String responseReadTime = headers.get("Response-Read-Time");
        if (Strings.hasLength(contentLength)) {
            this.responseReadTime = Long.valueOf(responseReadTime);
        }
    }
}
