package org.elasticsearch.action.admin.indices.parsedquery;

import com.spr.elasticsearch.index.query.ParsedQueryCache;
import org.apache.lucene.search.Query;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

/**
 * @author rahulanishetty
 * @since 23/11/17.
 */
public class TransportGetParsedQueryCacheAction extends TransportNodesAction<GetParsedQueryCacheRequest, GetParsedQueryCacheResponse, ParsedQueryCacheRequestPerNode, ParsedQueryCacheResponsePerNode> {

    private final IndicesService indicesService;

    @Inject
    public TransportGetParsedQueryCacheAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, IndicesService indicesService) {
        super(settings, GetParsedQueryAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver, GetParsedQueryCacheRequest::new, ParsedQueryCacheRequestPerNode::new, ThreadPool.Names.MANAGEMENT, ParsedQueryCacheResponsePerNode.class);
        this.indicesService = indicesService;
    }

    @Override
    protected GetParsedQueryCacheResponse newResponse(GetParsedQueryCacheRequest request, List<ParsedQueryCacheResponsePerNode> parsedQueryCacheResponsePerNodes, List<FailedNodeException> failures) {
        return new GetParsedQueryCacheResponse(clusterService.getClusterName(), parsedQueryCacheResponsePerNodes, failures);
    }

    @Override
    protected ParsedQueryCacheRequestPerNode newNodeRequest(String nodeId, GetParsedQueryCacheRequest request) {
        return new ParsedQueryCacheRequestPerNode(nodeId).cacheKeys(request.getCacheKeys());
    }

    @Override
    protected ParsedQueryCacheResponsePerNode newNodeResponse() {
        return new ParsedQueryCacheResponsePerNode();
    }

    @Override
    protected ParsedQueryCacheResponsePerNode nodeOperation(ParsedQueryCacheRequestPerNode request) {
        List<String> cacheKeys = request.getCacheKeys();
        ParsedQueryCacheResponsePerNode response = new ParsedQueryCacheResponsePerNode(transportService.getLocalNode());
        if (cacheKeys != null && cacheKeys.size() != 0) {
            ParsedQueryCache parsedQueryCache = indicesService.getParsedQueryCache();
            if (parsedQueryCache != null) {
                for (String cacheKey : cacheKeys) {
                    Query query = parsedQueryCache.get(cacheKey);
                    if (query != null) {
                        response.put(cacheKey, query.toString());
                    } else {
                        response.put(cacheKey, null);
                    }
                }
            }
        }
        return response;
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }
}
