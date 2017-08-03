package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.pipeline.bucketselector.BucketSelectorPipelineAggregationBuilder;

import java.util.Map;

/**
 * @author sudeep
 * @since 15/07/17
 */
public class PipelineAggregationBuilders {

    public static BucketSelectorPipelineAggregationBuilder bucketSelector(String name, Script script,
                                                                          Map<String, String> bucketsPathsMap) {
        return new BucketSelectorPipelineAggregationBuilder(name, script, bucketsPathsMap);
    }

    public static BucketSelectorPipelineAggregationBuilder bucketSelector(String name, Script script, String... bucketPaths) {
        return new BucketSelectorPipelineAggregationBuilder(name, script, bucketPaths);
    }
}