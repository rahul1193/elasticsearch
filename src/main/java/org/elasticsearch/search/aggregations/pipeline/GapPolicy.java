package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A gap policy determines how "holes" in a set of buckets should be handled.  For example,
 * a date_histogram might have empty buckets due to no data existing for that time interval.
 * This can cause problems for operations like a derivative, which relies on a continuous
 * function.
 * <p/>
 * "insert_zeros": empty buckets will be filled with zeros for all metrics
 * "ignore": empty buckets will simply be ignored
 *
 * @author sudeep
 * @since 15/07/17
 */
public enum GapPolicy implements ToXContent {
    INSERT_ZEROS((byte) 0, "insert_zeros"), SKIP((byte) 1, "skip");

    private final byte id;
    private final String name;

    GapPolicy(byte id, String name) {
        this.id = id;
        this.name = name;
    }

    public byte getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.value(name);
        return builder;
    }
}
