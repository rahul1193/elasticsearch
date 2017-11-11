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

package org.elasticsearch.search.aggregations.metrics;

import com.google.common.collect.Maps;
import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.ToXContentUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public abstract class ValuesSourceMetricsAggregationBuilder<B extends ValuesSourceMetricsAggregationBuilder<B>> extends MetricsAggregationBuilder<B> {

    private String field;
    private String script;
    private String lang;
    private Map<String, Object> params;

    /**
     * request cache for script
     */
    private Boolean requestCache;
    private Object missing;

    protected ValuesSourceMetricsAggregationBuilder(String name, String type) {
        super(name, type);
    }

    @SuppressWarnings("unchecked")
    public B field(String field) {
        this.field = field;
        return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B script(String script) {
        this.script = script;
        return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B lang(String lang) {
        this.lang = lang;
        return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B params(Map<String, Object> params) {
        if (this.params == null) {
            this.params = params;
        } else {
            this.params.putAll(params);
        }
        return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B param(String name, Object value) {
        if (this.params == null) {
            this.params = Maps.newHashMap();
        }
        this.params.put(name, value);
        return (B) this;
    }

    public B requestCache(boolean requestCache) {
        this.requestCache = requestCache;
        return (B) this;
    }

    /**
     * Sets the value to use when the aggregation finds a missing value in a
     * document
     */
    @SuppressWarnings("unchecked")
    public B missing(Object missing) {
        this.missing = missing;
        return (B) this;
    }

    /**
     * Gets the value to use when the aggregation finds a missing value in a
     * document
     */
    public Object missing() {
        return missing;
    }

    @Override
    protected void internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (field != null) {
            builder.field("field", field);
        }

        if (ToXContentUtils.getVersionFromParams(params).onOrAfter(Version.V_5_0_0)) {
            if (this.missing != null) {
                builder.field("missing", missing);
            }
            if (this.script != null) {
                builder.startObject("script");
                builder.field("inline", script);
                if (lang != null) {
                    builder.field("lang", lang);
                }
                if (this.params != null && !this.params.isEmpty()) {
                    builder.field("params").map(this.params);
                }
                if (this.requestCache != null) {
                    builder.field("_cache", this.requestCache);
                }
                builder.endObject();
            }

        } else {

            if (missing != null) {
//                throw new IllegalArgumentException("missing is supported only for elasticsearch version 5+");
            }

            if (script != null) {
                builder.field("script", script);
            }

            if (lang != null) {
                builder.field("lang", lang);
            }

            if (this.params != null && !this.params.isEmpty()) {
                builder.field("params").map(this.params);
            }
        }
    }
}
