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
package org.elasticsearch.client.rest.support;

import org.apache.http.HttpEntity;
import org.elasticsearch.Version;
import org.elasticsearch.action.*;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentObject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentObjectImpl;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.util.Map;

/**
 */
public class RestExecuteUtil {

    public static final int STATUS_OK = 200;

    public static <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, ?>>
    void execute(InternalRestClient internalRestClient,
                 final Action<Request, Response, RequestBuilder, ?> action, Request request, final ActionListener<Response> listener) {
        try {
            if (internalRestClient.getVersion() == null) {
                internalRestClient.readVersionAndClusterName();
            }
            final Version version = internalRestClient.getVersion();
            assert version != null;

            ActionRequestValidationException validationException = request.validate();
            if (validationException != null) {
                listener.onFailure(validationException);
                return;
            }
            final ActionRestRequest actionRestRequest = request.getActionRestRequest(version);

            if (listener instanceof AsyncActionListener) {
                performAsyncRequest(internalRestClient, action, listener, version, actionRestRequest);
            } else {
                RestResponse restResponse = internalRestClient.performRequest(
                        actionRestRequest.getMethod().name(),
                        actionRestRequest.getEndPoint(),
                        actionRestRequest.getParams(),
                        actionRestRequest.getEntity());
                Response response = parseResponse(action, actionRestRequest, version, restResponse);
                listener.onResponse(response);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private static <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, ?>> void performAsyncRequest(InternalRestClient internalRestClient, final Action<Request, Response, RequestBuilder, ?> action, final ActionListener<Response> listener, final Version version, final ActionRestRequest actionRestRequest) throws IOException {
        ResponseListener responseListener = new ResponseListener() {
            @Override
            public void onSuccess(RestResponse restResponse) {
                Response response = null;
                try {
                    response = parseResponse(action, actionRestRequest, version, restResponse);
                    listener.onResponse(response);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception exception) {
                listener.onFailure(exception);
            }
        };
        internalRestClient.performRequestAsync(actionRestRequest.getMethod().name(),
                actionRestRequest.getEndPoint(),
                actionRestRequest.getParams(),
                actionRestRequest.getEntity(), responseListener);
    }

    private static <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, ?>> Response parseResponse(Action<Request, Response, RequestBuilder, ?> action, ActionRestRequest actionRestRequest, Version version, RestResponse restResponse) throws Exception {
        Response response = action.newResponse();
        if (actionRestRequest.getMethod() == RestRequest.Method.HEAD) {
            response.exists(restResponse.getHttpResponse().getStatusLine().getStatusCode() == STATUS_OK);
        } else {
            HttpEntity entity = restResponse.getEntity();
            assert entity != null;
            String content = HttpUtils.readUtf8(entity);
            XContentParser parser = XContentHelper.createParser(new BytesArray(content));
            XContentObject source = new XContentObjectImpl(parser.mapOrderedAndClose(), version);
            validate(source);
            response.readFrom(source);
        }
        return response;
    }

    private static void validate(XContentObject source) throws Exception {
        if (!source.containsKey("error")) {
            return;
        }
        if (source.getAsObject("error") instanceof Map) {
            XContentObject error = source.getAsXContentObject("error");
            if (error.containsKey("root_cause")) {
                error.getAsXContentObjects("root_cause").get(0);
                String type = error.get("type");
                ElasticsearchExceptionHandler handler = ElasticsearchExceptionHandler.safeValueOf(type);
                throw handler.newException(source);
            } else {
                throw new UncategorizedExecutionException(source.toJson());
            }
        } else {
            String message = source.get("error");
            throw new UncategorizedExecutionException(message);
        }
    }
}
