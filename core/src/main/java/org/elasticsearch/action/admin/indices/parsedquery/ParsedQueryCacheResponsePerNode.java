package org.elasticsearch.action.admin.indices.parsedquery;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author rahulanishetty
 * @since 23/11/17.
 */
public class ParsedQueryCacheResponsePerNode extends BaseNodeResponse implements ToXContent {

    private final Map<String, String> cacheKeyVsQuery = new HashMap<>();

    public ParsedQueryCacheResponsePerNode() {
    }

    public ParsedQueryCacheResponsePerNode put(String cacheKey, String query) {
        if (Strings.hasLength(cacheKey)) {
            cacheKeyVsQuery.put(cacheKey, query);
        }
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        cacheKeyVsQuery.putAll(in.readMap(StreamInput::readString, StreamInput::readOptionalString));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(cacheKeyVsQuery, StreamOutput::writeString, StreamOutput::writeOptionalString);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (getNode() != null) {
            builder.field("node_id", getNode().getId());
            builder.field("node_ip", getNode().getHostAddress());
        }
        for (Map.Entry<String, String> entry : cacheKeyVsQuery.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
        return null;
    }
}
