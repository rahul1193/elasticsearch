package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.parsedquery.GetParsedQueryCacheRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * @author rahulanishetty
 * @since 22/11/17.
 */
public class RestGetParsedQueryAction extends BaseRestHandler {

    public RestGetParsedQueryAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "_parsedCache/{cache_keys}", this);
        controller.registerHandler(GET, "_parsedCache/{cache_keys}/{node_id}", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] cacheKeys = Strings.splitStringByCommaToArray(request.param("cache_keys"));
        String[] nodeIds = Strings.splitStringByCommaToArray(request.param("node_id"));
        GetParsedQueryCacheRequest getParsedQueryCacheRequest = new GetParsedQueryCacheRequest(nodeIds);
        getParsedQueryCacheRequest.cacheKeys(cacheKeys);
        return restChannel -> client.admin().cluster().getParsedQuery(getParsedQueryCacheRequest, new RestStatusToXContentListener<>(restChannel));
    }
}
