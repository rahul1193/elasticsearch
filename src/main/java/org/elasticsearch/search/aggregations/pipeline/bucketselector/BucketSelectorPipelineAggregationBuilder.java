package org.elasticsearch.search.aggregations.pipeline.bucketselector;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.GapPolicy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * @author sudeep
 * @since 15/07/17
 */
public class BucketSelectorPipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<BucketSelectorPipelineAggregationBuilder> {

    public static final String TYPE = "bucket_selector";

    public static ParseField BUCKETS_PATH = new ParseField("buckets_path");
    public static ParseField GAP_POLICY = new ParseField("gap_policy");


    private final Map<String, String> bucketsPathsMap;
    private Script script;
    private GapPolicy gapPolicy = GapPolicy.SKIP;

    public BucketSelectorPipelineAggregationBuilder(String name, Script script, Map<String, String> bucketsPathsMap) {
        super(name, TYPE, new TreeMap<>(bucketsPathsMap).values().toArray(new String[bucketsPathsMap.size()]));
        this.bucketsPathsMap = bucketsPathsMap;
        this.script = script;
    }

    public BucketSelectorPipelineAggregationBuilder(String name, Script script, String... bucketsPaths) {
        this(name, script, convertToBucketsPathMap(bucketsPaths));
    }

    @Override
    protected int _hashCode() {
        return Objects.hash(bucketsPathsMap, script, gapPolicy);
    }

    @Override
    protected boolean _equals(Object obj) {
        BucketSelectorPipelineAggregationBuilder other = (BucketSelectorPipelineAggregationBuilder) obj;
        return Objects.equals(bucketsPathsMap, other.bucketsPathsMap) && Objects.equals(script, other.script)
                && Objects.equals(gapPolicy, other.gapPolicy);
    }

    @Override
    protected XContentBuilder _toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(GAP_POLICY.getPreferredName(), gapPolicy);
        builder.field(BUCKETS_PATH.getPreferredName(), bucketsPathsMap);
        builder.field(Script.SCRIPT_PARSE_FIELD.getPreferredName(), script, params);
        return builder;
    }

    private static Map<String, String> convertToBucketsPathMap(String[] bucketsPaths) {
        Map<String, String> bucketsPathsMap = new HashMap<>();
        for (int i = 0; i < bucketsPaths.length; i++) {
            bucketsPathsMap.put("_value" + i, bucketsPaths[i]);
        }
        return bucketsPathsMap;
    }
}
