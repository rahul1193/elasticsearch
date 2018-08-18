package com.spr.elasticsearch.fields;

import org.apache.lucene.index.LeafReader;

import java.util.HashMap;
import java.util.Map;

/**
 * @author rahulanishetty
 * @since 30/07/18.
 */
public interface UpdatableFieldHandler {

    void updateSource(Map<String, Object> source, LeafReader leafReader, int docId) throws Exception;

    public static void setValueAtField(Map<String, Object> source, String field, Object value) {
        String[] keys = field.split("\\.");
        Map<String, Object> lastObj = source;
        for (int i = 0; i < keys.length - 1; i++) {
            Object o = source.computeIfAbsent(keys[i], k -> new HashMap<>());
            assert o instanceof Map;
            //noinspection unchecked
            lastObj = (Map<String, Object>) o;
        }
        lastObj.put(keys[keys.length - 1], value);
    }
}
