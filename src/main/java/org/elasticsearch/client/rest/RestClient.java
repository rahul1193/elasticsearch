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

package org.elasticsearch.client.rest;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.*;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.rest.support.InternalRestClient;
import org.elasticsearch.client.rest.support.InternalRestClientBuilder;
import org.elasticsearch.client.rest.support.RestExecuteUtil;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.TransportSearchModule;
import org.elasticsearch.threadpool.ThreadPool;

import javax.net.ssl.SSLContext;
import java.io.IOException;

/**
 */
public class RestClient extends AbstractClient implements Client {

    private static final int DEFAULT_PORT = 9200;

    private InternalRestClient internalRestClient;
    private RestAdminClient restAdminClient;

    private RestClient(InternalRestClient internalRestClient) {
        this.internalRestClient = internalRestClient;
        restAdminClient = new RestAdminClient(this.internalRestClient);

        ModulesBuilder modules = new ModulesBuilder();
        modules.add(new TransportSearchModule());
        modules.createInjector();
    }

    public static Builder builder(HttpHost... hosts) {
        return new RestClient.Builder(hosts);
    }

    public static Builder builder(String hostname) {
        return new RestClient.Builder(new HttpHost(hostname, DEFAULT_PORT));
    }

    public String getClusterName() {
        if (this.internalRestClient.getClusterName() == null) {
            this.internalRestClient.readVersionAndClusterName();
        }
        return this.internalRestClient.getClusterName();
    }

    public Version getClusterVersion() {
        if (this.internalRestClient.getVersion() == null) {
            this.internalRestClient.readVersionAndClusterName();
        }
        return this.internalRestClient.getVersion();
    }


    @Override
    public void close() throws ElasticsearchException {
        try {
            internalRestClient.close();
        } catch (IOException e) {
            throw new ElasticsearchException(e.getMessage(), e);
        }
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse,
            RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>>
    ActionFuture<Response> execute(Action<Request, Response, RequestBuilder, Client> action, Request request) {
        PlainActionFuture<Response> actionFuture = PlainActionFuture.newFuture();
        execute(action, request, actionFuture);
        return actionFuture;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> void execute(Action<Request, Response, RequestBuilder, Client> action, Request request, ActionListener<Response> listener) {
        RestExecuteUtil.execute(internalRestClient, action, request, listener);
    }

    @Override
    public ThreadPool threadPool() {
        return null;
    }

    @Override
    public AdminClient admin() {
        return restAdminClient;
    }

    @Override
    public Settings settings() {
        return null;
    }


    public static class Builder {
        private InternalRestClientBuilder internalRestClientBuilder;

        public Builder(HttpHost... hosts) {
            internalRestClientBuilder = InternalRestClient.builder(hosts);
        }

        public Builder setDefaultHeaders(Header[] defaultHeaders) {
            internalRestClientBuilder.setDefaultHeaders(defaultHeaders);
            return this;
        }

        public Builder setFailureListener(FailureListener failureListener) {
            internalRestClientBuilder.setFailureListener(failureListener);
            return this;
        }

        public Builder setMaxRetryTimeoutMillis(int maxRetryTimeoutMillis) {
            internalRestClientBuilder.setMaxRetryTimeoutMillis(maxRetryTimeoutMillis);
            return this;
        }

        public Builder setHttpClientConfigCallback(HttpClientConfigCallback httpClientConfigCallback) {
            internalRestClientBuilder.setHttpClientConfigCallback(httpClientConfigCallback);
            return this;
        }

        public Builder setRequestConfigCallback(RequestConfigCallback requestConfigCallback) {
            internalRestClientBuilder.setRequestConfigCallback(requestConfigCallback);
            return this;
        }

        public Builder setConnectionRequestTimeout(TimeValue connectionRequestTimeout) {
            internalRestClientBuilder.setConnectionRequestTimeout(connectionRequestTimeout);
            return this;
        }

        public Builder setConnectTimeout(TimeValue connectTimeout) {
            internalRestClientBuilder.setConnectTimeout(connectTimeout);
            return this;
        }

        public Builder setSocketTimeout(TimeValue socketTimeout) {
            internalRestClientBuilder.setSocketTimeout(socketTimeout);
            return this;
        }

        public Builder setMaxConnectionsTotal(int maxConnectionsTotal) {
            internalRestClientBuilder.setMaxConnectionsTotal(maxConnectionsTotal);
            return this;
        }

        public Builder setMaxConnectionsPerRoute(int maxConnectionsPerRoute) {
            internalRestClientBuilder.setMaxConnectionsPerRoute(maxConnectionsPerRoute);
            return this;
        }

        public Builder setMaxRetryTimeoutMillis(long maxRetryTimeoutMillis) {
            internalRestClientBuilder.setMaxRetryTimeoutMillis(maxRetryTimeoutMillis);
            return this;
        }

        public Builder setPathPrefix(String pathPrefix) {
            internalRestClientBuilder.setPathPrefix(pathPrefix);
            return this;
        }

        public Builder setSSLContext(SSLContext sslContext) {
            internalRestClientBuilder.setSSLContext(sslContext);
            return this;
        }

        public RestClient build() {
            InternalRestClient internalRestClient = internalRestClientBuilder.build();
            return new RestClient(internalRestClient);
        }

        public Builder setMaxRetryTimeout(TimeValue maxRetryTimeout) {
            internalRestClientBuilder.setMaxRetryTimeoutMillis(maxRetryTimeout.millis());
            return this;
        }

        public Builder setMaxResponseSize(ByteSizeValue size) {
            internalRestClientBuilder.setMaxResponseSize(size);
            return this;
        }

        /**
         * Sets the content compression enabled or not, current default is true
         * <p>
         */
        public Builder setContentCompressionEnabled(boolean contentCompressionEnabled) {
            internalRestClientBuilder.setContentCompressionEnabled(contentCompressionEnabled);
            return this;
        }

        public Builder setProxy(HttpHost proxy) {
            internalRestClientBuilder.setProxy(proxy);
            return this;
        }

        public Builder setProxyPreferredAuthSchemes(String... proxyPreferredAuthSchemes) {
            internalRestClientBuilder.setProxyPreferredAuthSchemes(proxyPreferredAuthSchemes);
            return this;
        }

        public Builder setTargetPreferredAuthSchemes(String... targetPreferredAuthSchemes) {
            internalRestClientBuilder.setTargetPreferredAuthSchemes(targetPreferredAuthSchemes);
            return this;
        }

        public Builder async(boolean async) {
            internalRestClientBuilder.async(async);
            return this;
        }

        public Builder connectionLiveTimeInPool(TimeValue timeValue) {
            internalRestClientBuilder.connectionLiveTimeInPool(timeValue);
            return this;
        }
    }
}
