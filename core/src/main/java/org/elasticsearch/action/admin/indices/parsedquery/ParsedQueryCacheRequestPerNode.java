package org.elasticsearch.action.admin.indices.parsedquery;

import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author rahulanishetty
 * @since 23/11/17.
 */
public class ParsedQueryCacheRequestPerNode extends BaseNodeRequest {

    private final List<String> cacheKeys = new ArrayList<>();

    public ParsedQueryCacheRequestPerNode() {
    }

    public ParsedQueryCacheRequestPerNode(String nodeId) {
        super(nodeId);
    }

    public List<String> getCacheKeys() {
        return cacheKeys;
    }

    public ParsedQueryCacheRequestPerNode cacheKeys(List<String> cacheKeys) {
        if (cacheKeys != null && cacheKeys.size() != 0) {
            this.cacheKeys.addAll(cacheKeys);
        }
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.cacheKeys.addAll(in.readList(StreamInput::readString));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringList(cacheKeys);
    }
}
