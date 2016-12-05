/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.xcontent.support;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentObject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public class XContentObjectImpl implements XContentObject {

    private final Map<String, Object> internalMap;
    private Version version;
    private XContentObjectImpl parent;

    XContentObjectImpl(Map<String, Object> map) {
        this.internalMap = map;
    }

    public XContentObjectImpl(Map<String, Object> map, Version version) {
        this(null, map, version);
    }

    private XContentObjectImpl(XContentObjectImpl parent, Map<String, Object> map, Version version) {
        this.internalMap = map;
        this.parent = parent;
        this.version = version;
    }

    @Override
    public Version getVersion() {
        return version;
    }

    @Override
    public String get(Enum key) {
        return get(key.name());
    }

    @Override
    public String get(String key) {
        Object value = this.internalMap.get(key);
        if (value == null) {
            return null;
        }
        return value.toString();
    }

    @Override
    public String get(String key, String defaultValue) {
        String value = get(key);
        if (Strings.isNotEmpty(value)) {
            return value;
        }
        return defaultValue;
    }

    @Override
    public Double getAsDouble(String key) {
        Object value = this.internalMap.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number)value).doubleValue();
        }
        try {
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
            throw new XContentObjectValueException(Double.class, key, value, e);
        }
    }

    @Override
    public Double getAsDouble(Enum key) {
        return getAsDouble(key.name());
    }

    @Override
    public Double getAsDouble(String key, Double defaultValue) {
        Double value = getAsDouble(key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    @Override
    public Double getAsDouble(Enum key, Double defaultValue) {
        return getAsDouble(key.name(), defaultValue);
    }

    @Override
    public Float getAsFloat(String key) {
        Object value = this.internalMap.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number)value).floatValue();
        }
        try {
            return Float.parseFloat(value.toString());
        } catch (NumberFormatException e) {
            throw new XContentObjectValueException(Float.class, key, value, e);
        }
    }

    @Override
    public Float getAsFloat(String key, Float defaultValue) {
        Float value = getAsFloat(key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    @Override
    public Float getAsFloat(Enum key, Float defaultValue) {
        return getAsFloat(key.name(), defaultValue);
    }

    @Override
    public Long getAsLong(String key) {
        Object value = this.internalMap.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number)value).longValue();
        }
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            throw new XContentObjectValueException(Long.class, key, value, e);
        }
    }

    @Override
    public Long getAsLong(String key, Long defaultValue) {
        Long value = getAsLong(key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    @Override
    public Integer getAsInt(String key) {
        Object value = this.internalMap.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number)value).intValue();
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            throw new XContentObjectValueException(Integer.class, key, value, e);
        }
    }

    @Override
    public Integer getAsInt(String key, Integer defaultValue) {
        Integer value = getAsInt(key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    @Override
    public Boolean getAsBoolean(String key) {
        Object value = this.internalMap.get(key);
        if (value == null) {
            return null;
        }
        return Boolean.valueOf(value.toString());
    }

    @Override
    public Boolean getAsBoolean(String key, Boolean defaultValue) {
        Boolean value = getAsBoolean(key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    @Override
    public Boolean getAsBoolean(Enum key) {
        return getAsBoolean(key.name());
    }

    @Override
    public XContentObject getAsXContentObject(String key) {
        Object value = internalMap.get(key);
        try {
            //noinspection unchecked
            Map<String, Object> map = (Map<String, Object>) value;
            if (map == null) {
                return null;
            }
            return new XContentObjectImpl(this, map, version);
        } catch (ClassCastException e) {
            throw new XContentObjectValueException(Map.class, key, value, e);
        }
    }

    @Override
    public XContentObject getAsXContentObject(Enum key) {
        return getAsXContentObject(key.name());
    }


    @Override
    public int size() {
        return internalMap.size();
    }

    @Override
    public boolean isEmpty() {
        return internalMap.isEmpty();
    }

    @Override
    public boolean containsKey(String key) {
        return internalMap.containsKey(key);
    }

    @Override
    public boolean containsKey(Enum key) {
        return containsKey(key.name());
    }

    @Override
    public Float getAsFloat(Enum key) {
        return getAsFloat(key.name());
    }

    @Override
    public Object getAsObject(String key) {
        return internalMap.get(key);
    }

    @Override
    public Object getAsObject(Enum key) {
        return getAsObject(key.name());
    }

    @Override
    public Text getAsText(String key) {
        return new Text(get(key));
    }

    @Override
    public Map<String, Object> getInternalMap() {
        return internalMap;
    }

    @Override
    public String getAsJson(String key) throws IOException {
        return XContentFactory.jsonBuilder().map(getAsMap(key)).string();
    }

    @Override
    public String getAsJson(Enum key) throws IOException {
        return getAsJson(key.name());
    }

    @Override
    public XContentParser getAsXContentParser(String key) throws IOException {
        return XContentHelper.createParser(new Text(getAsJson(key)).bytes());
    }

    @Override
    public DateTime getAsDate(String key) {
        Object value = internalMap.get(key);
        if (!this.containsKey(key)) {
            return null;
        }
        if (this.internalMap.get(key) instanceof String) {
            return new DateTime(get(key));
        }
        else if (this.internalMap.get(key) instanceof Number) {
            return new DateTime(getAsLong(key));
        }
        else {
            throw new XContentObjectValueException(DateTime.class, key, value);
        }

    }

    @Override
    public List<String> getAsStrings(String key) {
        Object value = internalMap.get(key);
        try {
            if (value instanceof String[]) {
                String[] valueAsArray = (String[]) value;
                return Arrays.asList(valueAsArray);
            }
            //noinspection unchecked
            return (List<String>) value;
        } catch (ClassCastException e) {
            throw new XContentObjectValueException(String[].class, key, value);
        }
    }
    @Override
    public List<Object> getAsObjects(String key) {
        Object value = internalMap.get(key);
        try {
            if (value instanceof Object[]) {
                Object[] valueAsArray = (Object[]) value;
                return Arrays.asList(valueAsArray);
            }
            //noinspection unchecked
            return (List<Object>) value;
        }
        catch (ClassCastException e) {
            throw new XContentObjectValueException(Object[].class, key, value);
        }
    }


    @Override
    public String toJson()  {
        try {
            return XContentHelper.convertToJson(internalMap, true);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public boolean isNull(String key) {
        return !internalMap.containsKey(key) ||
                internalMap.get(key) == null ||
                internalMap.get(key).equals("null");
    }

    @Override
    public Set<String> keySet() {
        return internalMap.keySet();
    }

    @Override
    public void put(String key, Object value) {
        internalMap.put(key, value);
    }

    @Override
    public void put(Enum key, Object value) {
        put(key.name(), value);
    }

    @Override
    public boolean isXContentObject(String key) {
        return internalMap.get(key) instanceof Map || internalMap.get(key) instanceof XContentObject;
    }

    @Override
    public List<XContentObject> getAsXContentObjectsOrEmpty(Enum key) {
        return getAsXContentObjectsOrEmpty(key.name());
    }

    @Override
    public BytesReference getAsBytesReference(String key) {
        return new BytesArray(get(key));
    }

    @Override
    public List<BytesReference> getAsBytesReferences(Enum key) {
        return getAsBytesReferences(key.name());
    }

    @Override
    public List<BytesReference> getAsBytesReferences(String key) {
        List<String> refs = getAsStrings(key);
        List<BytesReference> transformed = new ArrayList<>(refs.size());
        for (String ref : refs) {
            transformed.add(new BytesArray(ref));
        }

        return transformed;
    }

    @Override
    public BytesReference getAsBytesReference(Enum key) {
        return getAsBytesReference(key.name());
    }

    @Override
    public <T> Map<T, XContentObject> getAsXContentObjectsMap(String key) {
        Object value = internalMap.get(key);
        Map<T, XContentObject> results = Collections.emptyMap();
        try {
            //noinspection unchecked
            Map<T, Map> map = (Map<T, Map>) value;
            if (map == null || map.isEmpty()) {
                return results;
            }
            results = new LinkedHashMap<>(map.size());
            for (Map.Entry<T, Map> entry : map.entrySet()) {
                //noinspection unchecked
                results.put(entry.getKey(), new XContentObjectImpl(this, entry.getValue(), version));
            }
        }
        catch (ClassCastException e) {
            throw new XContentObjectValueException(Map.class, key,  value, e);
        }
        return results;
    }

    @Override
    public <T> Map<T, XContentObject> getAsXContentObjectsMap(Enum key) {
        return getAsXContentObjectsMap(key.name());
    }

    @Override
    public <V> Map<String, V> getAsMap(String key) {
        Object value = internalMap.get(key);
        if (value == null) {
            return Collections.emptyMap();
        }
        try {
            @SuppressWarnings("unchecked")
            Map<String, V> map = (Map<String, V>) value;
            return map;
        }
        catch (ClassCastException e) {
            throw new XContentObjectValueException(List.class, key,  value, e);
        }
    }

    @Override
    public <V> Map<String, V> getAsMap(Enum key) {
        return getAsMap(key.name());
    }


    @Override
    public List<XContentObject> getAsXContentObjectsOrEmpty(String key) {
        List<XContentObject> asXContentObjects = getAsXContentObjects(key);
        if (asXContentObjects == null) {
            return Collections.emptyList();
        }
        return asXContentObjects;
    }

    @Override
    public List<XContentObject> getAsXContentObjects(String key) {
        Object value = internalMap.get(key);
        if (value == null) {
            return null;
        }
        try {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> list = (List<Map<String, Object>>) value;
            List<XContentObject> transformed = new ArrayList<>(list.size());
            for (Map<String, Object> map : list) {
                transformed.add(new XContentObjectImpl(parent, map, version));
            }
            return transformed;
        }
        catch (ClassCastException e) {
            throw new XContentObjectValueException(List.class, key,  value, e);
        }
    }

    @Override
    public List<XContentObject> getAsXContentObjects(Enum key) {
        return getAsXContentObjects(key.name());
    }

    @Override
    public Long getAsLong(Enum key) {
        return getAsLong(key.name());
    }

    @Override
    public Long getAsLong(Enum key, Long defaultValue) {
        return getAsLong(key.name(), defaultValue);
    }

    @Override
    public Integer getAsInt(Enum key) {
        return getAsInt(key.name());
    }

    @Override
    public Integer getAsInt(Enum key, Integer defaultValue) {
        return getAsInt(key.name(), defaultValue);
    }

    @Override
    public String toString() {
        return internalMap.toString();
    }

    @Override
    public XContentObject getParent() {
        return parent;
    }
}
