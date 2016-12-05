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

package org.elasticsearch.common.xcontent;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.text.Text;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Brandon Kearby
 *         September 14, 2016
 */
public interface XContentObject {

    /**
     * Returns the server's version for this content
     * @return the server's version
     */
    Version getVersion();

    /**
     * Returns the value associated with the key.
     *
     * @return The value, <tt>null</tt> if it does not exists.
     */
    String get(Enum key);

    /**
     * Returns the value associated with the key.
     *
     * @return The value, <tt>null</tt> if it does not exists.
     */
    String get(String key);

    /**
     * Returns the value associated with the key.
     *
     * @return The value for the given key. If it does not exists,
     * returns the default value provided.
     */
    String get(String key, String defaultValue);

    /**
     * Returns the Double value associated with the key.
     *
     * @return The value, <tt>null</tt> if it does not exists.
     */
    Double getAsDouble(String key);

    /**
     * Returns the Double value associated with the key.
     *
     * @return The value, <tt>null</tt> if it does not exists.
     */
    Double getAsDouble(Enum key);

    /**
     * Returns the Double value associated with the key.
     *
     * @return The value for the given key. If it does not exists,
     * returns the default value provided.
     */
    Double getAsDouble(String key, Double defaultValue);

    /**
     * Returns the Double value associated with the key.
     *
     * @return The value for the given key. If it does not exists,
     * returns the default value provided.
     */
    Double getAsDouble(Enum key, Double defaultValue);

    /**
     * Returns the Boolean value associated with the key.
     *
     * @return The value, <tt>null</tt> if it does not exists.
     */
    Boolean getAsBoolean(String key);

    /**
     * Returns the Boolean value associated with the key.
     *
     * @return The value, <tt>null</tt> if it does not exists.
     */
    Boolean getAsBoolean(Enum key);

    /**
     * Returns the Boolean value associated with the key.
     *
     * @return The value for the given key. If it does not exists,
     * returns the default value provided.
     */
    Boolean getAsBoolean(String key, Boolean defaultValue);

    /**
     * Returns the Float value associated with the key.
     *
     * @return The value, <tt>null</tt> if it does not exists.
     */
    Float getAsFloat(String key);

    /**
     * Returns the Float value associated with the key.
     *
     * @return The value, <tt>null</tt> if it does not exists.
     */
    Float getAsFloat(Enum key);


    /**
     * Returns the Float value associated with the key.
     *
     * @return The value for the given key. If it does not exists,
     * returns the default value provided.
     */
    Float getAsFloat(String key, Float defaultValue);

    /**
     * Returns the Float value associated with the key.
     *
     * @return The value for the given key. If it does not exists,
     * returns the default value provided.
     */
    Float getAsFloat(Enum key, Float defaultValue);

    /**
     * Returns the Long value associated with the key.
     *
     * @return The value, <tt>null</tt> if it does not exists.
     */
    Long getAsLong(String key);

    /**
     * Returns the Long value associated with the key.
     *
     * @return The value for the given key. If it does not exists,
     * returns the default value provided.
     */
    Long getAsLong(String key, Long defaultValue);

    /**
     * Returns the Long value associated with the key.
     *
     * @return The value, <tt>null</tt> if it does not exists.
     */
    Long getAsLong(Enum key);

    /**
     * Returns the Long value associated with the key.
     *
     * @return The value for the given key. If it does not exists,
     * returns the default value provided.
     */
    Long getAsLong(Enum key, Long defaultValue);

    /**
     * Returns the Integer value associated with the key.
     *
     * @return The value, <tt>null</tt> if it does not exists.
     */
    Integer getAsInt(String key);

    /**
     * Returns the Integer value associated with the key.
     *
     * @return The value for the given key. If it does not exists,
     * returns the default value provided.
     */
    Integer getAsInt(String key, Integer defaultValue);

    /**
     * Returns the Integer value associated with the key.
     *
     * @return The value, <tt>null</tt> if it does not exists.
     */
    Integer getAsInt(Enum key);

    /**
     * Returns the Integer value associated with the key.
     *
     * @return The value for the given key. If it does not exists,
     * returns the default value provided.
     */
    Integer getAsInt(Enum key, Integer defaultValue);


    /**
     * Returns the XContentObject value associated with the key.
     *
     * @return The value, <tt>null</tt> if it does not exists.
     */
    XContentObject getAsXContentObject(String key);

    /**
     * Returns the XContentObject value associated with the key.
     *
     * @return The value, <tt>null</tt> if it does not exists.
     */
    XContentObject getAsXContentObject(Enum key);

    /**
     * Returns the number of key-value mappings in this map.  If the
     * map contains more than <tt>Integer.MAX_VALUE</tt> elements, returns
     * <tt>Integer.MAX_VALUE</tt>.
     *
     * @return the number of key-value mappings in this map
     */
    int size();

    /**
     * Returns <tt>true</tt> if this map contains no key-value mappings.
     *
     * @return <tt>true</tt> if this map contains no key-value mappings
     */
    boolean isEmpty();

    /**
     * Returns <tt>true</tt> if this map contains a mapping for the specified
     * key.  More formally, returns <tt>true</tt> if and only if
     * this map contains a mapping for a key <tt>k</tt> such that
     * <tt>(key==null ? k==null : key.equals(k))</tt>.  (There can be
     * at most one such mapping.)
     *
     * @param key key whose presence in this map is to be tested
     * @return <tt>true</tt> if this map contains a mapping for the specified
     *         key
     * @throws ClassCastException if the key is of an inappropriate type for
     *         this map
     * (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException if the specified key is null and this map
     *         does not permit null keys
     * (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
     */
    boolean containsKey(String key);

    /**
     * Returns <tt>true</tt> if this map contains a mapping for the specified
     * key.  More formally, returns <tt>true</tt> if and only if
     * this map contains a mapping for a key <tt>k</tt> such that
     * <tt>(key==null ? k==null : key.equals(k))</tt>.  (There can be
     * at most one such mapping.)
     *
     * @param key key whose presence in this map is to be tested
     * @return <tt>true</tt> if this map contains a mapping for the specified
     *         key
     * @throws ClassCastException if the key is of an inappropriate type for
     *         this map
     * (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException if the specified key is null and this map
     *         does not permit null keys
     * (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
     */
    boolean containsKey(Enum key);


    /**
     * Returns a {@link Set} view of the keys contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  If the map is modified
     * while an iteration over the set is in progress (except through
     * the iterator's own <tt>remove</tt> operation), the results of
     * the iteration are undefined.  The set supports element removal,
     * which removes the corresponding mapping from the map, via the
     * <tt>Iterator.remove</tt>, <tt>Set.remove</tt>,
     * <tt>removeAll</tt>, <tt>retainAll</tt>, and <tt>clear</tt>
     * operations.  It does not support the <tt>add</tt> or <tt>addAll</tt>
     * operations.
     *
     * @return a set view of the keys contained in this map
     */
    Set<String> keySet();

    /**
     * Associates the specified value with the specified key in this map
     * (optional operation).  If the map previously contained a mapping for
     * the key, the old value is replaced by the specified value.  (A map
     * <tt>m</tt> is said to contain a mapping for a key <tt>k</tt> if and only
     * if {@link #containsKey(String) m.containsKey(k)} would return
     * <tt>true</tt>.)
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @throws UnsupportedOperationException if the <tt>put</tt> operation
     *         is not supported by this map
     * @throws ClassCastException if the class of the specified key or value
     *         prevents it from being stored in this map
     * @throws NullPointerException if the specified key or value is null
     *         and this map does not permit null keys or values
     * @throws IllegalArgumentException if some property of the specified key
     *         or value prevents it from being stored in this map
     */
    void put(String key, Object value);

    /**
     * Associates the specified value with the specified key in this map
     * (optional operation).  If the map previously contained a mapping for
     * the key, the old value is replaced by the specified value.  (A map
     * <tt>m</tt> is said to contain a mapping for a key <tt>k</tt> if and only
     * if {@link #containsKey(String) m.containsKey(k)} would return
     * <tt>true</tt>.)
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @throws UnsupportedOperationException if the <tt>put</tt> operation
     *         is not supported by this map
     * @throws ClassCastException if the class of the specified key or value
     *         prevents it from being stored in this map
     * @throws NullPointerException if the specified key or value is null
     *         and this map does not permit null keys or values
     * @throws IllegalArgumentException if some property of the specified key
     *         or value prevents it from being stored in this map
     */
    void put(Enum key, Object value);

    /**
     * Tests if the current value is an xContentObject
     * @param key the key to test
     * @return true if a the key exists otherwise false
     */
    boolean isXContentObject(String key);

    /**
     * Returns the given key as a list of XContentObject(s) or null if missing
     * @return The list of values, <tt>null</tt> if it does not exists.
     */
    List<XContentObject> getAsXContentObjects(String key);

    /**
     * Returns the given key as a list of <tt>XContentObject</tt>s or null if missing
     * @return The list of values, <tt>null</tt> if it does not exists.
     */
    List<XContentObject> getAsXContentObjects(Enum key);

    /**
     * Returns the given key as a list of <tt>XContentObject</tt>s or as an empty list if null
     */
    List<XContentObject> getAsXContentObjectsOrEmpty(String key);

    /**
     * Returns the given key as a list of <tt>XContentObject</tt>s or as an empty list if null
     */
    List<XContentObject> getAsXContentObjectsOrEmpty(Enum key);

    BytesReference getAsBytesReference(String key);

    List<BytesReference> getAsBytesReferences(Enum key);
    List<BytesReference> getAsBytesReferences(String key);

    BytesReference getAsBytesReference(Enum key);

    <T> Map<T, XContentObject>  getAsXContentObjectsMap(String key);
    <T> Map<T,XContentObject> getAsXContentObjectsMap(Enum key);

    <V> Map<String, V> getAsMap(String key);
    <V> Map<String, V> getAsMap(Enum key);


    Object getAsObject(String key);

    Object getAsObject(Enum key);


    Text getAsText(String key);

    Map<String,Object> getInternalMap();

    String getAsJson(String key) throws IOException;
    String getAsJson(Enum key) throws IOException;

    XContentParser getAsXContentParser(String key) throws IOException;

    DateTime getAsDate(String key);

    List<String> getAsStrings(String key);

    String toJson();

    boolean isNull(String key);

    List<Object> getAsObjects(String key);

    XContentObject getParent();
}
