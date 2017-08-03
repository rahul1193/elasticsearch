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

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A gap policy determines how "holes" in a set of buckets should be handled.  For example,
 * a date_histogram might have empty buckets due to no data existing for that time interval.
 * This can cause problems for operations like a derivative, which relies on a continuous
 * function.
 * <p/>
 * "insert_zeros": empty buckets will be filled with zeros for all metrics
 * "ignore": empty buckets will simply be ignored
 *
 * @author sudeep
 * @since 15/07/17
 */
public enum GapPolicy implements ToXContent {
    INSERT_ZEROS((byte) 0, "insert_zeros"), SKIP((byte) 1, "skip");

    private final byte id;
    private final String name;

    GapPolicy(byte id, String name) {
        this.id = id;
        this.name = name;
    }

    public byte getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.value(name);
        return builder;
    }
}
