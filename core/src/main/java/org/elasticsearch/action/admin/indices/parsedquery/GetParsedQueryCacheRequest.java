package org.elasticsearch.action.admin.indices.parsedquery;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author rahulanishetty
 * @since 22/11/17.
 */
public class GetParsedQueryCacheRequest extends BaseNodesRequest<GetParsedQueryCacheRequest> {

    private final List<String> cacheKeys = new ArrayList<>();

    public GetParsedQueryCacheRequest() {
        super();
    }

    public GetParsedQueryCacheRequest(String... nodesIds) {
        super(nodesIds);
    }

    public GetParsedQueryCacheRequest cacheKey(String cacheKey) {
        if (Strings.hasLength(cacheKey)) {
            this.cacheKeys.add(cacheKey);
        }
        return this;
    }

    public GetParsedQueryCacheRequest cacheKeys(String... cacheKeys) {
        if (cacheKeys != null && cacheKeys.length != 0) {
            this.cacheKeys.addAll(Arrays.asList(cacheKeys));
        }
        return this;
    }


    public List<String> getCacheKeys() {
        return cacheKeys;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        cacheKeys.addAll(in.readList(StreamInput::readString));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringList(cacheKeys);
    }
}
