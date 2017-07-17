package org.elasticsearch.search.aggregations.pipeline;

import com.google.common.collect.Maps;
import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * @author sudeep
 * @since 15/07/17
 */
public abstract class AbstractPipelineAggregationBuilder<PAB extends AbstractPipelineAggregationBuilder<PAB>> implements ToXContent {

    protected final String name;
    protected final String[] bucketsPaths;
    protected final String type;
    protected Map<String, Object> metaData;

    protected AbstractPipelineAggregationBuilder(String name, String type, String[] bucketsPaths) {
        if (type == null) {
            throw new IllegalArgumentException("[type] must not be null: [" + name + "]");
        }
        if (bucketsPaths == null) {
            throw new IllegalArgumentException("[bucketsPaths] must not be null: [" + name + "]");
        }
        this.type = type;
        this.name = name;
        this.bucketsPaths = bucketsPaths;
    }

    public String type() {
        return type;
    }

    /**
     * Return this aggregation's name.
     */
    public String getName() {
        return name;
    }

    @SuppressWarnings("unchecked")
    public PAB setMetaData(Map<String, Object> metaData) {
        this.metaData = metaData;
        return (PAB) this;
    }

    @SuppressWarnings("unchecked")
    public PAB addMetadata(String key, Object value) {
        if (this.metaData == null) {
            this.metaData = Maps.newHashMap();
        }
        this.metaData.put(key, value);
        return (PAB) this;
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (ToXContentUtils.getVersionFromParams(params).onOrAfter(Version.V_5_0_0)) {
            builder.startObject(getName());

            if (this.metaData != null) {
                builder.field("meta", this.metaData);
            }

            builder.startObject(type);
            _toXContent(builder, params);
            builder.endObject();
            builder.endObject();
        }

        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(bucketsPaths), metaData, name, type, _hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        @SuppressWarnings("unchecked")
        AbstractPipelineAggregationBuilder<PAB> other = (AbstractPipelineAggregationBuilder<PAB>) obj;
        if (!Objects.equals(name, other.name))
            return false;
        if (!Objects.equals(type, other.type))
            return false;
        if (!Objects.deepEquals(bucketsPaths, other.bucketsPaths))
            return false;
        if (!Objects.equals(metaData, other.metaData))
            return false;
        return _equals(obj);
    }

    protected abstract int _hashCode();

    protected abstract boolean _equals(Object obj);

    protected abstract XContentBuilder _toXContent(XContentBuilder builder, Params params) throws IOException;

}
