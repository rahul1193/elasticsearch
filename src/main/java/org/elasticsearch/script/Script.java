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

package org.elasticsearch.script;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.ScriptService.ScriptType;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.util.CollectionUtils.emptyIfNull;

/**
 * @author sudeep
 * @since 15/07/17
 */
public class Script implements ToXContent {

    public static final String DEFAULT_SCRIPT_LANG = "mvel";
    public static final ScriptType DEFAULT_SCRIPT_TYPE = ScriptType.INLINE;

    /**
     * Standard {@link ParseField} for outer level of script queries.
     */
    public static final ParseField SCRIPT_PARSE_FIELD = new ParseField("script");

    /**
     * Standard {@link ParseField} for lang on the inner level.
     */
    public static final ParseField LANG_PARSE_FIELD = new ParseField("lang");

    /**
     * Standard {@link ParseField} for options on the inner level.
     */
    public static final ParseField OPTIONS_PARSE_FIELD = new ParseField("options");

    /**
     * Standard {@link ParseField} for params on the inner level.
     */
    public static final ParseField PARAMS_PARSE_FIELD = new ParseField("params");


    private final ScriptType type;
    private final String lang;
    private final Map<String, Object> params;
    private final String idOrCode;
    private final Map<String, String> options;

    /**
     * Constructor for simple script using the default language and default type.
     *
     * @param idOrCode The id or code to use dependent on the default script type.
     */
    public Script(String idOrCode) {
        this(DEFAULT_SCRIPT_TYPE, DEFAULT_SCRIPT_LANG, idOrCode, Collections.<String, String>emptyMap(), Collections.<String, Object>emptyMap());
    }

    public Script(String idOrCode, Map<String, Object> params) {
        this(DEFAULT_SCRIPT_TYPE, DEFAULT_SCRIPT_LANG, idOrCode, params);
    }

    public Script(ScriptType type, String lang, String idOrCode, Map<String, Object> params) {
        this(type, lang, idOrCode, Collections.<String, String>emptyMap(), params);
    }

    public Script(ScriptType type, String lang, String idOrCode, Map<String, String> options, Map<String, Object> params) {
        this.type = Objects.requireNonNull(type);
        this.idOrCode = Objects.requireNonNull(idOrCode);
        this.params = Collections.unmodifiableMap(emptyIfNull(params));
        this.lang = lang;
        this.options = Collections.unmodifiableMap(emptyIfNull(options));
    }

    public ScriptType getType() {
        return type;
    }

    public String getLang() {
        return lang;
    }

    public String getIdOrCode() {
        return idOrCode;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (ToXContentUtils.getVersionFromParams(params).onOrAfter(Version.V_5_0_0)) {
            builder.field(type.lowerCaseName(), idOrCode);
        } else if (ScriptType.INDEXED.equals(type)) {
            builder.field(ScriptService.SCRIPT_ID.getPreferredName(), idOrCode);
        } else if (ScriptType.FILE.equals(type)) {
            builder.field(ScriptService.SCRIPT_FILE.getPreferredName(), idOrCode);
        } else if (ScriptType.INLINE.equals(type)) {
            builder.field(ScriptService.SCRIPT_INLINE.getPreferredName(), idOrCode);
        }

        builder.field(LANG_PARSE_FIELD.getPreferredName(), lang);
        if (CollectionUtils.isNotEmpty(this.params)) {
            builder.field(PARAMS_PARSE_FIELD.getPreferredName(), this.params);
        }
        if (CollectionUtils.isNotEmpty(this.options)) {
            builder.field(OPTIONS_PARSE_FIELD.getPreferredName(), this.options);
        }

        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Script script = (Script) o;

        if (type != script.type) return false;
        if (lang != null ? !lang.equals(script.lang) : script.lang != null) return false;
        if (!idOrCode.equals(script.idOrCode)) return false;
        if (options != null ? !options.equals(script.options) : script.options != null) return false;
        return params.equals(script.params);

    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + (lang != null ? lang.hashCode() : 0);
        result = 31 * result + idOrCode.hashCode();
        result = 31 * result + (options != null ? options.hashCode() : 0);
        result = 31 * result + params.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Script{" +
                "type=" + type +
                ", lang='" + lang + '\'' +
                ", idOrCode='" + idOrCode + '\'' +
                ", options=" + options +
                ", params=" + params +
                '}';
    }
}
