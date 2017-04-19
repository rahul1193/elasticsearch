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

package org.elasticsearch.action.shrink;

import org.apache.http.HttpEntity;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionRestRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.UriBuilder;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * @author rahulanishetty
 * @since 19/04/17.
 */
public class ShrinkRequest extends AcknowledgedRequest<ShrinkRequest> implements IndicesRequest {

    private CreateIndexRequest shrinkIndexRequest;
    private String sourceIndex;


    ShrinkRequest() {
    }

    public ShrinkRequest(String targetIndex, String sourceindex) {
        this.shrinkIndexRequest = new CreateIndexRequest(targetIndex);
        this.sourceIndex = sourceindex;
    }


    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = shrinkIndexRequest == null ? null : shrinkIndexRequest.validate();
        if (sourceIndex == null) {
            validationException = addValidationError("source index is missing", validationException);
        }
        if (shrinkIndexRequest == null) {
            validationException = addValidationError("shrink index request is missing", validationException);
        }
        return validationException;
    }

    public void setSourceIndex(String index) {
        this.sourceIndex = index;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shrinkIndexRequest = new CreateIndexRequest();
        shrinkIndexRequest.readFrom(in);
        sourceIndex = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shrinkIndexRequest.writeTo(out);
        out.writeString(sourceIndex);
    }

    @Override
    public String[] indices() {
        return new String[]{sourceIndex};
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.lenientExpandOpen();
    }

    public void setShrinkIndex(CreateIndexRequest shrinkIndexRequest) {
        this.shrinkIndexRequest = Objects.requireNonNull(shrinkIndexRequest, "shrink index request must not be null");
    }

    /**
     * Returns the {@link CreateIndexRequest} for the shrink index
     */
    public CreateIndexRequest getShrinkIndexRequest() {
        return shrinkIndexRequest;
    }

    /**
     * Returns the source index name
     */
    public String getSourceIndex() {
        return sourceIndex;
    }

    @Override
    public RestRequest.Method getMethod() {
        return RestRequest.Method.POST;
    }

    @Override
    public HttpEntity getEntity() throws IOException {
        return this.shrinkIndexRequest.getEntity();
    }

    @Override
    public String getEndPoint() {
        return UriBuilder.newBuilder().slash(this.sourceIndex).slash("_shrink").slash(this.shrinkIndexRequest.index()).build();
    }

    @Override
    public ActionRestRequest getActionRestRequest(Version version) {
        if (Version.V_5_0_0.after(version)) {
            throw new UnsupportedOperationException("shrink is supported only after version 5");
        }
        return super.getActionRestRequest(version);
    }
}
