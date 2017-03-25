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

package org.elasticsearch.action.support;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;

import java.io.IOException;

public class QuerySourceBuilder implements ToXContent {

    private QueryBuilder queryBuilder;

    private BytesReference queryBinary;

    public QuerySourceBuilder setQuery(QueryBuilder query) {
        this.queryBuilder = query;
        return this;
    }

    public QuerySourceBuilder setQuery(BytesReference queryBinary) {
        this.queryBinary = queryBinary;
        return this;
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (queryBuilder != null) {
            builder.field("query");
            queryBuilder.toXContent(builder, params);
        }

        if (queryBinary != null) {
            if (XContentFactory.xContentType(queryBinary) == builder.contentType()) {
                builder.rawField("query", queryBinary);
            } else {
                builder.field("query_binary", queryBinary);
            }
        }

        builder.endObject();
        return builder;
    }

    public BytesReference buildAsBytes(XContentType contentType) throws SearchSourceBuilderException{
        return buildAsBytes(contentType, Version.CURRENT);
    }

    public BytesReference buildAsBytes(XContentType contentType, Version version) throws SearchSourceBuilderException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType, version);
            toXContent(builder, ToXContentUtils.createParamsWithTargetClusterVersion(version));
            return builder.bytes();
        } catch (Exception e) {
            throw new SearchSourceBuilderException("Failed to build search source", e);
        }
    }
}
