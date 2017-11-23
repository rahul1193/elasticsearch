package org.elasticsearch.action.admin.indices.parsedquery;

import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * @author rahulanishetty
 * @since 22/11/17.
 */
public class GetParsedQueryCacheRequestBuilder extends NodesOperationRequestBuilder<GetParsedQueryCacheRequest, GetParsedQueryCacheResponse, GetParsedQueryCacheRequestBuilder> {

    public GetParsedQueryCacheRequestBuilder(ElasticsearchClient client) {
        super(client, GetParsedQueryAction.INSTANCE, new GetParsedQueryCacheRequest());
    }

    public GetParsedQueryCacheRequestBuilder cacheKey(String cacheKey) {
        request().cacheKey(cacheKey);
        return this;
    }
}
