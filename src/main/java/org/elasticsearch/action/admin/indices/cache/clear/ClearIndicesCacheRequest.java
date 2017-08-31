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

package org.elasticsearch.action.admin.indices.cache.clear;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.UriBuilder;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 *
 */
public class ClearIndicesCacheRequest extends BroadcastOperationRequest<ClearIndicesCacheRequest> {

    private boolean filterCache = false;
    private boolean fieldDataCache = false;
    private boolean idCache = false;
    private boolean recycler = false;
    private boolean queryCache = false;
    private boolean queryBuilder = false;
    private boolean parsedQuery = false;
    private String[] fields = null;
    private String[] filterKeys = null;


    ClearIndicesCacheRequest() {
    }

    public ClearIndicesCacheRequest(String... indices) {
        super(indices);
    }

    public boolean filterCache() {
        return filterCache;
    }

    public ClearIndicesCacheRequest filterCache(boolean filterCache) {
        this.filterCache = filterCache;
        return this;
    }

    public boolean queryCache() {
        return this.queryCache;
    }

    public ClearIndicesCacheRequest queryCache(boolean queryCache) {
        this.queryCache = queryCache;
        return this;
    }

    public boolean fieldDataCache() {
        return this.fieldDataCache;
    }

    public ClearIndicesCacheRequest fieldDataCache(boolean fieldDataCache) {
        this.fieldDataCache = fieldDataCache;
        return this;
    }

    public ClearIndicesCacheRequest fields(String... fields) {
        this.fields = fields;
        return this;
    }

    public String[] fields() {
        return this.fields;
    }

    public ClearIndicesCacheRequest filterKeys(String... filterKeys) {
        this.filterKeys = filterKeys;
        return this;
    }

    public ClearIndicesCacheRequest queryBuilderCache(boolean queryBuilderCache) {
        this.queryBuilder = queryBuilderCache;
        return this;
    }

    public ClearIndicesCacheRequest parsedQuery(boolean parsedQuery) {
        this.parsedQuery = parsedQuery;
        return this;
    }

    public String[] filterKeys() {
        return this.filterKeys;
    }

    public boolean idCache() {
        return this.idCache;
    }

    public ClearIndicesCacheRequest recycler(boolean recycler) {
        this.recycler = recycler;
        return this;
    }

    public boolean recycler() {
        return this.recycler;
    }

    public ClearIndicesCacheRequest idCache(boolean idCache) {
        this.idCache = idCache;
        return this;
    }

    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        filterCache = in.readBoolean();
        fieldDataCache = in.readBoolean();
        idCache = in.readBoolean();
        recycler = in.readBoolean();
        fields = in.readStringArray();
        filterKeys = in.readStringArray();
        if (in.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            queryCache = in.readBoolean();
        }
    }

    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(filterCache);
        out.writeBoolean(fieldDataCache);
        out.writeBoolean(idCache);
        out.writeBoolean(recycler);
        out.writeStringArrayNullable(fields);
        out.writeStringArrayNullable(filterKeys);
        if (out.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            out.writeBoolean(queryCache);
        }
    }

    @Override
    public String getEndPoint() {
        return UriBuilder.newBuilder()
                .csvOrDefault("_all", this.indices())
                .slash("_cache", "clear").build();
    }

    @Override
    public Map<String, String> getParams() {
        ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<>();
        builder.put("query", String.valueOf(filterCache))
                .put("request", String.valueOf(queryCache))
                .put("field_data", String.valueOf(fieldDataCache))
                .put("parsed_query", String.valueOf(parsedQuery))
                .put("query_builder", String.valueOf(queryBuilder))
                .put("recycler", String.valueOf(recycler));
        if (fields != null && fields.length > 0) {
            builder.put("fields", Arrays.toString(fields));
        }
        return builder.build();
    }

    @Override
    public RestRequest.Method getMethod() {
        return RestRequest.Method.POST;
    }
}
