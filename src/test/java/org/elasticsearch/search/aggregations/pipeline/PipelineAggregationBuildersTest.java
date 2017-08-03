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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.*;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.support.AbstractClusterAdminClient;
import org.elasticsearch.client.support.AbstractIndicesAdminClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketselector.BucketSelectorPipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import java.io.IOException;

/**
 * @author sudeep
 * @since 16/07/17
 */
public class PipelineAggregationBuildersTest {

    private static Version VERSION = Version.V_5_0_0;

    @Test
    public void print_bucket_selector() {
        print(getXContentBuilder(bucket_selector(), VERSION));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void fullQuery() throws IOException {
        TermsBuilder account = AggregationBuilders.terms("account").field("dimensions.ACCOUNT_ID");
        SumBuilder sum_fans_online = AggregationBuilders.sum("sum_fans_online").field("measurements.FACEBOOK_PAGE_FANS_ONLINE");
        BucketSelectorPipelineAggregationBuilder sum_fans_filter = bucket_selector();
        account.subAggregation(sum_fans_online);
        account.subAggregation(sum_fans_filter);
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(new NoOpClientWithVersion(VERSION)).setSize(0).addAggregation(account);
        SearchSourceBuilder source = searchRequestBuilder.internalBuilder();

        //XContentBuilder builder = getJsonBuilder();
        //source.toXContent(builder, ToXContentUtils.createParamsWithTargetClusterVersion(VERSION));
        System.out.println(source.toString());
    }

    public BucketSelectorPipelineAggregationBuilder bucket_selector() {
        Script script = new Script("sumFansOnline > limit", ImmutableMap.of("limit", (Object) 20000));
        return PipelineAggregationBuilders.bucketSelector("sum_fans_filter", script, ImmutableMap.of("sumFansOnline", "sum_fans_online"));
    }

    private XContentBuilder getXContentBuilder(ToXContent toXContent, Version esVersion) {
        try {
            XContentBuilder xContentBuilder = getJsonBuilder();
            xContentBuilder.startObject();
            toXContent.toXContent(xContentBuilder, ToXContentUtils.createParamsWithTargetClusterVersion(esVersion));
            xContentBuilder.endObject();
            return xContentBuilder;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void print(XContentBuilder builder) {
        System.out.println(builder.toString());
    }

    private XContentBuilder getJsonBuilder() throws IOException {
        return XContentFactory.jsonBuilder().prettyPrint();
    }


    private static class NoOpClientWithVersion extends NoOpClient {
        private Version version;

        public NoOpClientWithVersion(Version version) {
            this.version = version;
        }

        @Override
        public Version getClusterVersion() {
            return version;
        }
    }

    private static class NoOpClient extends AbstractClient implements AdminClient {

        @Override
        public AdminClient admin() {
            return this;
        }

        @Override
        public Settings settings() {
            return null;
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder, Client> action, Request request) {
            return null;
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> void execute(Action<Request, Response, RequestBuilder, Client> action, Request request, ActionListener<Response> listener) {
            listener.onResponse(null);
        }

        @Override
        public ThreadPool threadPool() {
            return null;
        }

        @Override
        public void close() throws ElasticsearchException {

        }

        @Override
        public ClusterAdminClient cluster() {
            return new AbstractClusterAdminClient() {
                @Override
                public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, ClusterAdminClient>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder, ClusterAdminClient> action, Request request) {
                    return null;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, ClusterAdminClient>> void execute(Action<Request, Response, RequestBuilder, ClusterAdminClient> action, Request request, ActionListener<Response> listener) {
                    listener.onResponse(null);
                }

                @Override
                public ThreadPool threadPool() {
                    return null;
                }
            };
        }

        @Override
        public IndicesAdminClient indices() {
            return new AbstractIndicesAdminClient() {
                @Override
                public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, IndicesAdminClient>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder, IndicesAdminClient> action, Request request) {
                    return null;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, IndicesAdminClient>> void execute(Action<Request, Response, RequestBuilder, IndicesAdminClient> action, Request request, ActionListener<Response> listener) {
                    listener.onResponse(null);
                }

                @Override
                public ThreadPool threadPool() {
                    return null;
                }
            };
        }
    }
}