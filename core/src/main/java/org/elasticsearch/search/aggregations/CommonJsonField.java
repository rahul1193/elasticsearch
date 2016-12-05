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

package org.elasticsearch.search.aggregations;

import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public enum CommonJsonField {
    _name,
    _type,
    value,
    values,
    buckets,
    doc_count,
    key,
    key_as_string,
    from,
    from_as_string,
    to,
    to_as_string,
    score;

    static Set<String> names = new HashSet<>(values().length);

    static {
        for (CommonJsonField jsonField : values()) {
            names.add(jsonField.name());
        }
    }

    public static boolean contains(String key) {
        return names.contains(key);
    }
}
