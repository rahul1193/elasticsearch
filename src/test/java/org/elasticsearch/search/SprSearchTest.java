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

package org.elasticsearch.search;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.rest.RestClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

/**
 * author : Sandeep Maheshwari
 * timestamp : 20/09/17
 */
public class SprSearchTest {

    public static void main(String[] args) {
        final HttpHost host = new HttpHost("172.16.0.12", 9206);
        final String indexName = "ps_msg_p5_v_migration_1_20170520_0000";
        RestClient restClient = RestClient.builder(host).setMaxResponseSize(new ByteSizeValue(2, ByteSizeUnit.GB)).build();
        SearchResponse response = restClient.prepareSearch(indexName).execute().actionGet();
        System.out.println("Time Taken:" + response.getTookInMillis());
    }
}
