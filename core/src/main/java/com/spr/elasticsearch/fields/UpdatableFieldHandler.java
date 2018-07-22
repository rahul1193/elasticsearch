package com.spr.elasticsearch.fields;

import org.apache.lucene.index.LeafReader;

import java.io.IOException;
import java.util.Map;

/**
 * @author rahulanishetty
 * @since 30/07/18.
 */
public interface UpdatableFieldHandler {

    void update(Map<String, Object> source, LeafReader leafReader, int docId) throws Exception;
}
