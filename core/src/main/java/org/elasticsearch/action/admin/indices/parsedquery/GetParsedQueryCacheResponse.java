package org.elasticsearch.action.admin.indices.parsedquery;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.List;

/**
 * @author rahulanishetty
 * @since 22/11/17.
 */
public class GetParsedQueryCacheResponse extends BaseNodesResponse<ParsedQueryCacheResponsePerNode> implements StatusToXContentObject {

    GetParsedQueryCacheResponse() {
    }

    public GetParsedQueryCacheResponse(ClusterName clusterName, List<ParsedQueryCacheResponsePerNode> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<ParsedQueryCacheResponsePerNode> readNodesFrom(StreamInput in) throws IOException {
        return in.readStreamableList(ParsedQueryCacheResponsePerNode::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<ParsedQueryCacheResponsePerNode> nodes) throws IOException {
        out.writeStreamableList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray();
        for (ParsedQueryCacheResponsePerNode response : getNodes()) {
            response.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public RestStatus status() {
        return RestStatus.OK;
    }
}
