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

package org.elasticsearch.index.query;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.ToXContentUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A filter that matches documents matching boolean combinations of other filters.
 */
public class BoolFilterBuilder extends BaseFilterBuilder implements NamedFilterBuilder<BoolFilterBuilder> {

    private ArrayList<FilterBuilder> mustClauses = new ArrayList<>();

    private ArrayList<FilterBuilder> mustNotClauses = new ArrayList<>();

    private ArrayList<FilterBuilder> shouldClauses = new ArrayList<>();

    private Boolean cache;
    private String cacheKey;

    private String filterName;

    /**
     * Adds a filter that <b>must</b> appear in the matching documents.
     */
    public BoolFilterBuilder must(FilterBuilder filterBuilder) {
        mustClauses.add(filterBuilder);
        return this;
    }

    /**
     * Adds a filter that <b>must not</b> appear in the matching documents.
     */
    public BoolFilterBuilder mustNot(FilterBuilder filterBuilder) {
        mustNotClauses.add(filterBuilder);
        return this;
    }


    /**
     * Adds a filter that <i>should</i> appear in the matching documents. For a boolean filter
     * with no <tt>MUST</tt> clauses one or more <code>SHOULD</code> clauses must match a document
     * for the BooleanQuery to match.
     */
    public BoolFilterBuilder should(FilterBuilder filterBuilder) {
        shouldClauses.add(filterBuilder);
        return this;
    }

    /**
     * Adds multiple <i>must</i> filters.
     */
    public BoolFilterBuilder must(FilterBuilder... filterBuilders) {
        for (FilterBuilder fb : filterBuilders) {
            mustClauses.add(fb);
        }
        return this;
    }

    /**
     * Adds multiple <i>must not</i> filters.
     */
    public BoolFilterBuilder mustNot(FilterBuilder... filterBuilders) {
        for (FilterBuilder fb : filterBuilders) {
            mustNotClauses.add(fb);
        }
        return this;
    }

    /**
     * Adds multiple <i>should</i> filters.
     */
    public BoolFilterBuilder should(FilterBuilder... filterBuilders) {
        for (FilterBuilder fb : filterBuilders) {
            shouldClauses.add(fb);
        }
        return this;
    }

    /**
     * Returns <code>true</code> iff this filter builder has at least one should, must or mustNot clause.
     * Otherwise <code>false</code>.
     */
    public boolean hasClauses() {
        return !(mustClauses.isEmpty() && shouldClauses.isEmpty() && mustNotClauses.isEmpty());
    }

    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public BoolFilterBuilder filterName(String filterName) {
        this.filterName = filterName;
        return this;
    }

    /**
     * Should the filter be cached or not. Defaults to <tt>false</tt>.
     */
    public BoolFilterBuilder cache(boolean cache) {
        this.cache = cache;
        return this;
    }

    public BoolFilterBuilder cacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("bool");
        doXContentInternal(builder, params);
        addCacheToQuery(cacheKey, cache, builder, params);
        builder.endObject();
    }

    private void doXContentInternal(XContentBuilder builder, Params params) throws IOException {
        doXArrayContent("must", mustClauses, builder, params);
        doXArrayContent("must_not", mustNotClauses, builder, params);
        doXArrayContent("should", shouldClauses, builder, params);

        if (filterName != null) {
            builder.field("_name", filterName);
        }
    }

    private void doXArrayContent(String field, List<FilterBuilder> clauses, XContentBuilder builder, Params params) throws IOException {
        if (clauses.isEmpty()) {
            return;
        }
        if (clauses.size() == 1) {
            builder.field(field);
            clauses.get(0).toXContent(builder, params);
        } else {
            builder.startArray(field);
            for (FilterBuilder clause : clauses) {
                clause.toXContent(builder, params);
            }
            builder.endArray();
        }
    }

    private String generateCacheKey() throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.SMILE);
        builder.startObject("bool");
        doXContentInternal(builder, EMPTY_PARAMS);
        builder.endObject();
        return DigestUtils.sha512Hex(builder.bytes().streamInput());
    }

    @Override
    protected void addCacheToQuery(String cacheKey, Boolean cache, XContentBuilder builder, Params params) throws IOException {
        if (ToXContentUtils.getVersionFromParams(params).onOrAfter(Version.V_5_0_0)) {
            if (BooleanUtils.isTrue(cache)) {
                if (cacheKey != null) {
                    builder.field("_cache_key", cacheKey);
                } else {
                    builder.field("_cache_key", generateCacheKey());
                }
            }
            return;
        }

        if (cache != null) {
            builder.field("_cache", cache);
        }
        if (cacheKey != null) {
            builder.field("_cache_key", cacheKey);
        }
    }
}