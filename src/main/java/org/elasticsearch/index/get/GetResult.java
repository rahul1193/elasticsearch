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

package org.elasticsearch.index.get;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.text.StringAndBytesText;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.collect.Iterators.emptyIterator;
import static com.google.common.collect.Maps.newHashMapWithExpectedSize;
import static org.elasticsearch.index.get.GetField.readGetField;

/**
 */
public class GetResult implements Streamable, Iterable<GetField>, ToXContent, FromXContentObject {

    private String index;
    private String type;
    private String id;
    private long version;
    private boolean exists;
    private Map<String, GetField> fields;
    private Map<String, Object> sourceAsMap;
    private BytesReference source;
    private byte[] sourceAsBytes;
    private String sourceAsString;

    GetResult() {
    }

    public GetResult(String index, String type, String id, long version, boolean exists, BytesReference source, Map<String, GetField> fields) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.version = version;
        this.exists = exists;
        this.source = source;
        this.fields = fields;
        if (this.fields == null) {
            this.fields = ImmutableMap.of();
        }
    }

    /**
     * Does the document exists.
     */
    public boolean isExists() {
        return exists;
    }

    /**
     * The index the document was fetched from.
     */
    public String getIndex() {
        return index;
    }

    /**
     * The type of the document.
     */
    public String getType() {
        return type;
    }

    /**
     * The id of the document.
     */
    public String getId() {
        return id;
    }

    /**
     * The version of the doc.
     */
    public long getVersion() {
        return version;
    }

    /**
     * The source of the document if exists.
     */
    public byte[] source() {
        if (source == null) {
            return null;
        }
        if (sourceAsBytes != null) {
            return sourceAsBytes;
        }
        this.sourceAsBytes = sourceRef().toBytes();
        return this.sourceAsBytes;
    }

    /**
     * Returns bytes reference, also un compress the source if needed.
     */
    public BytesReference sourceRef() {
        try {
            this.source = CompressorFactory.uncompressIfNeeded(this.source);
            return this.source;
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to decompress source", e);
        }
    }

    /**
     * Internal source representation, might be compressed....
     */
    public BytesReference internalSourceRef() {
        return source;
    }

    /**
     * Is the source empty (not available) or not.
     */
    public boolean isSourceEmpty() {
        return source == null;
    }

    /**
     * The source of the document (as a string).
     */
    public String sourceAsString() {
        if (sourceAsString != null) {
            return sourceAsString;
        }

        if (source == null) {
            return null;
        }
        try {
            sourceAsString = XContentHelper.convertToJson(sourceRef(), false);
            return sourceAsString;
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to convert source to a json string");
        }
    }

    /**
     * The source of the document (As a map).
     */
    @SuppressWarnings({"unchecked"})
    public Map<String, Object> sourceAsMap() throws ElasticsearchParseException {
        if (source == null) {
            return null;
        }
        if (sourceAsMap != null) {
            return sourceAsMap;
        }

        sourceAsMap = SourceLookup.sourceAsMap(source);
        return sourceAsMap;
    }

    public Map<String, Object> getSource() {
        return sourceAsMap();
    }

    public Map<String, GetField> getFields() {
        return fields;
    }

    public GetField field(String name) {
        return fields.get(name);
    }

    @Override
    public Iterator<GetField> iterator() {
        if (fields == null) {
            return emptyIterator();
        }
        return fields.values().iterator();
    }


    static final class Fields {
        static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString _VERSION = new XContentBuilderString("_version");
        static final XContentBuilderString FOUND = new XContentBuilderString("found");
        static final XContentBuilderString FIELDS = new XContentBuilderString("fields");

        static final Map<String, XContentBuilderString> ALL = new HashMap<>();

        static {
            add(_INDEX, _TYPE, _ID, _VERSION, FOUND, FIELDS);
        }

        private static void add(XContentBuilderString... fields) {
            for (XContentBuilderString field : fields) {
                ALL.put(field.underscore().getValue(), field);
            }
        }
    }

    public XContentBuilder toXContentEmbedded(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.FOUND, exists);

        if (source != null) {
            XContentHelper.writeRawField("_source", source, builder, params);
        }

        if (fields != null && !fields.isEmpty()) {
            builder.startObject(Fields.FIELDS);
            for (GetField field : fields.values()) {
                if (field.getValues().isEmpty()) {
                    continue;
                }
                String fieldName = field.getName();
                if (field.isMetadataField()) {
                    builder.field(fieldName, field.getValue());
                } else {
                    builder.startArray(field.getName());
                    for (Object value : field.getValues()) {
                        builder.value(value);
                    }
                    builder.endArray();
                }
            }
            builder.endObject();
        }
        return builder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (!isExists()) {
            builder.field(Fields._INDEX, index);
            builder.field(Fields._TYPE, type);
            builder.field(Fields._ID, id);
            builder.field(Fields.FOUND, false);
        } else {
            builder.field(Fields._INDEX, index);
            builder.field(Fields._TYPE, type);
            builder.field(Fields._ID, id);
            if (version != -1) {
                builder.field(Fields._VERSION, version);
            }
            toXContentEmbedded(builder, params);
        }
        return builder;
    }

    public static GetResult readGetResult(StreamInput in) throws IOException {
        GetResult result = new GetResult();
        result.readFrom(in);
        return result;
    }

    public static GetResult readGetResult(XContentObject parser) throws IOException {
        GetResult result = new GetResult();
        result.readFrom(parser);
        return result;
    }


    enum JsonField implements XContentObjectParseable<GetResult> {
        _index {
            @Override
            public void apply(XContentObject in, GetResult response) throws IOException {
                response.index = in.get(this);
            }

        },
        _type {
            @Override
            public void apply(XContentObject in, GetResult response) throws IOException {
                response.type = in.get(this);
            }

        },
        _id {
            @Override
            public void apply(XContentObject in, GetResult response) throws IOException {
                response.id = in.get(this);
            }

        },
        _version {
            @Override
            public void apply(XContentObject in, GetResult response) throws IOException {
                response.version = in.getAsInt(this);
            }

        },
        found {
            @Override
            public void apply(XContentObject in, GetResult response) throws IOException {
                response.exists = in.getAsBoolean(this);
            }

        },
        _source {
            @Override
            public void apply(XContentObject in, GetResult response) throws IOException {
                response.sourceAsMap = in.getAsMap(this);
                response.sourceAsString = XContentHelper.convertToJson(response.sourceAsMap, false);
                response.source = new StringAndBytesText(response.sourceAsString).bytes();
            }
        },
        script {
            @Override
            public void apply(XContentObject in, GetResult response) throws IOException {
                _source.apply(in, response);
                if (response.source != null && response.source.length() > 0) {
                    response.exists = true;
                }
            }

        }
    }

    public void readFrom(XContentObject in) throws IOException {
        XContentHelper.populate(in, JsonField.values(), this);
    }



    @Override
    public void readFrom(StreamInput in) throws IOException {
        index = in.readSharedString();
        type = in.readOptionalSharedString();
        id = in.readString();
        version = in.readLong();
        exists = in.readBoolean();
        if (exists) {
            source = in.readBytesReference();
            if (source.length() == 0) {
                source = null;
            }
            int size = in.readVInt();
            if (size == 0) {
                fields = ImmutableMap.of();
            } else {
                fields = newHashMapWithExpectedSize(size);
                for (int i = 0; i < size; i++) {
                    GetField field = readGetField(in);
                    fields.put(field.getName(), field);
                }
            }
        }
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeSharedString(index);
        out.writeOptionalSharedString(type);
        out.writeString(id);
        out.writeLong(version);
        out.writeBoolean(exists);
        if (exists) {
            out.writeBytesReference(source);
            if (fields == null) {
                out.writeVInt(0);
            } else {
                out.writeVInt(fields.size());
                for (GetField field : fields.values()) {
                    field.writeTo(out);
                }
            }
        }
    }
}

