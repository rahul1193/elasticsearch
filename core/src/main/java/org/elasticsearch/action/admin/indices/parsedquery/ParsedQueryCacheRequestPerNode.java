package org.elasticsearch.action.admin.indices.parsedquery;

import org.elasticsearch.action.support.nodes.BaseNodeRequest;

import java.util.List;

/**
 * @author rahulanishetty
 * @since 23/11/17.
 */
public class ParsedQueryCacheRequestPerNode extends BaseNodeRequest {

    private List<String> cacheKeys;

    public ParsedQueryCacheRequestPerNode() {
    }

    public ParsedQueryCacheRequestPerNode(String nodeId) {
        super(nodeId);
    }

    public List<String> getCacheKeys() {
        return cacheKeys;
    }

    public void setCacheKeys(List<String> cacheKeys) {
        this.cacheKeys = cacheKeys;
    }

    public ParsedQueryCacheRequestPerNode cacheKeys(List<String> cacheKeys) {
        this.cacheKeys = cacheKeys;
        return this;
    }
}
