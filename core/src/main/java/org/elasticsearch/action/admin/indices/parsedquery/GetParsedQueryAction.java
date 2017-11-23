package org.elasticsearch.action.admin.indices.parsedquery;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * @author rahulanishetty
 * @since 23/11/17.
 */
public class GetParsedQueryAction extends Action<GetParsedQueryCacheRequest, GetParsedQueryCacheResponse, GetParsedQueryCacheRequestBuilder> {

    public static final String NAME = "node:get/parsedQuery/cache";
    public static final GetParsedQueryAction INSTANCE = new GetParsedQueryAction();

    public GetParsedQueryAction() {
        super(NAME);
    }

    @Override
    public GetParsedQueryCacheRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new GetParsedQueryCacheRequestBuilder(client);
    }

    @Override
    public GetParsedQueryCacheResponse newResponse() {
        return new GetParsedQueryCacheResponse();
    }
}
