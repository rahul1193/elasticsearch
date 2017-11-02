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
package org.elasticsearch.client.rest;

import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.IndexDeleteByQueryResponse;
import org.elasticsearch.action.exists.ExistsResponse;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.shrink.ShrinkRequestBuilder;
import org.elasticsearch.action.shrink.ShrinkResponse;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentObject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.*;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.TransportSearchModule;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.children.Children;
import org.elasticsearch.search.aggregations.bucket.children.ChildrenBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.nested.NestedBuilder;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.GeoDistance;
import org.elasticsearch.search.aggregations.bucket.range.ipv4.IPv4Range;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 */
public class RestClientTest extends AbstractRestClientTest {

    @Before
    public void setUp() {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testResponse() throws IOException {
        ModulesBuilder modules = new ModulesBuilder();
        modules.add(new TransportSearchModule());
        modules.createInjector();
        XContentParser parser = XContentHelper.createParser(new BytesArray(
                "{\"took\":7,\"timed_out\":false,\"_shards\":{\"total\":4,\"successful\":4," +
                        "\"failed\":0},\"hits\":{\"total\":5672879,\"max_score\":0.0,\"hits\":[]}," +
                        "\"aggregations\":{\"global\":{\"buckets\":[{\"doc_count\":0," +
                        "\"Topics_dimension_daily_terms\":{\"buckets\":{\"57eb671fe4b0086b1d8a966d\":{\"doc_count\":0,\"Topics_dimension_minute_histogram\":{\"buckets\":[],\"_type\":\"date_histogram\"}}},\"_type\":\"filters\"}}],\"_type\":\"filters\"},\"groupBy\":{\"buckets\":[{\"doc_count\":0,\"Topics\":{\"buckets\":{\"57eb671fe4b0086b1d8a966d\":{\"doc_count\":0,\"Topics_dimension_minute_histogram\":{\"buckets\":[],\"_type\":\"date_histogram\"}}},\"_type\":\"filters\"}}],\"_type\":\"filters\"}}}"));
        XContentObject xContentObject = parser.xContentObject();
        SearchResponse searchResponse = new SearchResponse();
        searchResponse.readFrom(xContentObject);
        System.out.println(searchResponse);
    }

    @Test
    public void testSortSettings() {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        searchRequestBuilder.addSort(new ScriptSortBuilder("doc['t'].value", "number").order(SortOrder.DESC));
        searchRequestBuilder.setIndices("tweet_p999999_v7");
        searchRequestBuilder.setSize(2);
        searchRequestBuilder.setFrom(0);
        searchRequestBuilder.setFetchSource(false);
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        SearchHit[] hits = searchResponse.getHits().getHits();
        SearchHit hit1 = hits[0];
        SearchHit hit2 = hits[1];
        assert (Double) hit2.sortValues()[0] < (Double) hit1.sortValues()[0];
    }

    @Test
    public void testSortSettings1_4() {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        searchRequestBuilder.addSort(new ScriptSortBuilder("doc['id'].value", "number").order(SortOrder.DESC));
        searchRequestBuilder.setIndices("rahul_test_index");
        searchRequestBuilder.setSize(1);
        searchRequestBuilder.setFrom(0);
        searchRequestBuilder.setFetchSource(false);
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        SearchHit[] hits = searchResponse.getHits().getHits();
        SearchHit hit1 = hits[0];
        assert 4 < (Double) hit1.sortValues()[0];
    }

    @Test
    public void testMultiGet() {
        MultiGetRequestBuilder requestBuilder = new MultiGetRequestBuilder(client);
        requestBuilder.add(new MultiGetRequest.Item("tweet_p999999_v4", "tweet", "678818515042742272")
                .fetchSourceContext(new FetchSourceContext(false)));
        MultiGetResponse multiGetItemResponses = requestBuilder.execute().actionGet();
        for (MultiGetItemResponse multiGetItemResponse : multiGetItemResponses.getResponses()) {
            assert multiGetItemResponse.getResponse().getSource() == null;
        }
    }


    @Test
    public void testUpdateSettings() {
        UpdateSettingsRequestBuilder updateSettingsRequestBuilder = client.admin().indices().prepareUpdateSettings(index);
        updateSettingsRequestBuilder.setSettings("{\"index.number_of_replicas\":1}");
        UpdateSettingsResponse updateSettingsResponse = updateSettingsRequestBuilder.execute().actionGet();
        System.out.println(updateSettingsResponse);
    }

    @Test
    public void testIndexSettings() {
        ImmutableOpenMap<String, Settings> settings = client.admin().indices().getIndex(new GetIndexRequest().addFeatures(GetIndexRequest.Feature.SETTINGS)).actionGet()
                .getSettings();
        assert settings != null;
    }

    @Test
    public void postFilterTest() {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
        searchRequestBuilder.setIndices("tweet_p999999_v7");
        searchRequestBuilder.setPostFilter(FilterBuilders.boolFilter().must(FilterBuilders.termFilter("ln", "en")));
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        assert searchResponse.getHits().getMaxScore() == 1.0;
    }

    @Test
    public void testIndexStats() {
        IndicesAdminClient indices = client.admin().indices();
        IndicesStatsResponse indicesStatsResponse = indices.stats(new IndicesStatsRequest()).actionGet();
        System.out.println(indicesStatsResponse);
    }

    @Test
    public void testGet() throws ExecutionException, InterruptedException {
        IndexResponse indexResponse = indexDocument();
        String id = indexResponse.getId();

        GetResponse getResponse = getDocument(id);
        assertEquals(id, getResponse.getId());
    }

    private GetResponse getDocument(String id) throws InterruptedException, ExecutionException {
        GetRequest getRequest = new GetRequest(index, STATS_TYPE, id);
        ActionFuture<GetResponse> getResponseActionFuture = this.client.get(getRequest);
        return getResponseActionFuture.get();
    }

    @Test
    public void testShrink() {
        ShrinkRequestBuilder shrinkRequestBuilder = new ShrinkRequestBuilder(client.admin().indices());
        shrinkRequestBuilder.setSourceIndex("tweet_p999999_v3_new");
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("tweet_p999999_v3_new_shrinked_test");
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        builder.put("index.number_of_replicas", "0");
        builder.put("index.number_of_shards", "1");
        builder.put("index.codec", "best_compression");
        createIndexRequest.settings(builder.build());
        shrinkRequestBuilder.setTargetIndex(createIndexRequest);
        ShrinkResponse shrinkResponse = shrinkRequestBuilder.execute().actionGet();
        assert shrinkResponse.isAcknowledged();
    }

    @Test
    public void testClusterHealth() {
        ClusterHealthRequestBuilder clusterHealthRequestBuilder = new ClusterHealthRequestBuilder(client.admin().cluster());
        ClusterHealthResponse clusterIndexHealths = clusterHealthRequestBuilder.execute().actionGet();
        assert clusterIndexHealths.getStatus() == ClusterHealthStatus.GREEN;
    }


    @Test
    public void testAliasAction() {
        IndicesAliasesRequestBuilder builder = new IndicesAliasesRequestBuilder(client.admin().indices());
        builder.addAlias("tweet_p999999_v3_new_shrinked_test", "tweet_p999999_v3_new_shrinked");
        builder.removeIndex("tweet_p999999_v3_new_shrinked");
        IndicesAliasesResponse indicesAliasesResponse = builder.execute().actionGet();
        assert indicesAliasesResponse.isAcknowledged();
    }

    @Test
    public void testIndex() throws ExecutionException, InterruptedException {
        String id = UUID.randomUUID().toString();
        IndexRequest request = new IndexRequest(index, STATS_TYPE, id);
        Map<String, Object> source = Maps.newHashMap();
        source.put("datePretty", "2016-02-28T05:30:00+05:30");
        request.source(source);
        IndexResponse indexResponse = this.client.index(request).get();
        assertEquals(id, indexResponse.getId());
        assertEquals(index, indexResponse.getIndex());
        assertEquals(STATS_TYPE, indexResponse.getType());

        GetResponse getResponse = getDocument(id);
        assertEquals(id, getResponse.getId());
        assertEquals(source.get("datePretty"), getResponse.getSourceAsMap().get("datePretty"));
    }

    @Test
    public void testIndexWithoutId() throws ExecutionException, InterruptedException {
        IndexRequest request = new IndexRequest(index, STATS_TYPE);
        Map<String, Object> source = Maps.newHashMap();
        source.put("datePretty", "2016-02-28T05:30:00+05:30");
        request.source(source);
        IndexResponse indexResponse = this.client.index(request).get();
        GetResponse getResponse = getDocument(indexResponse.getId());
        assertEquals(source.get("datePretty"), getResponse.getSourceAsMap().get("datePretty"));
    }

    @Test(expected = VersionConflictEngineException.class)
    public void testIndexDocExistsOpTypeCreate() throws Throwable {
        String id = UUID.randomUUID().toString();
        IndexRequest request = new IndexRequest(index, STATS_TYPE, id);
        Map<String, Object> source = Maps.newHashMap();
        source.put("datePretty", "2016-02-28T05:30:00+05:30");
        request.source(source);
        IndexResponse indexResponse = this.client.index(request).get();
        assertEquals(id, indexResponse.getId());
        assertEquals(index, indexResponse.getIndex());
        assertEquals(STATS_TYPE, indexResponse.getType());

        request.create(true);
        try {
            IndexResponse indexResponse1 = this.client.index(request).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw cause;
        }
    }

    @Test
    public void testSearchIndex() throws ExecutionException, InterruptedException {
        String id = UUID.randomUUID().toString();
        IndexRequest request = new IndexRequest(index, STATS_TYPE, id);
        Map<String, Object> source = Maps.newHashMap();
        source.put("datePretty", "2016-02-28T05:30:00+05:30");
        request.source(source);
        IndexResponse indexResponse = this.client.index(request).get();
        assertEquals(id, indexResponse.getId());
        assertEquals(index, indexResponse.getIndex());
        assertEquals(STATS_TYPE, indexResponse.getType());

        GetResponse getResponse = getDocument(id);
        assertEquals(id, getResponse.getId());
        assertEquals(source.get("datePretty"), getResponse.getSourceAsMap().get("datePretty"));
    }

    @Test
    public void testDelete() throws ExecutionException, InterruptedException {
        // add test doc
        IndexResponse indexResponse = indexDocument();

        // delete the test doc
        DeleteRequest deleteRequest = new DeleteRequest(indexResponse.getIndex(), indexResponse.getType(), indexResponse.getId());
        DeleteResponse deleteResponse = client.delete(deleteRequest).get();

        assertEquals(indexResponse.getId(), deleteResponse.getId());
        assertEquals(index, deleteResponse.getIndex());
        assertEquals(STATS_TYPE, deleteResponse.getType());
        assertTrue("Document should be found", deleteResponse.isFound());

        GetResponse getResponse = getDocument(indexResponse.getId());
        assertFalse("Document should not exist", getResponse.isExists());
    }

    @Test
    public void testUpdate() throws ExecutionException, InterruptedException {
        IndexResponse indexResponse = indexDocument();
        GetResponse document = getDocument(indexResponse.getId());
        UpdateRequest updateRequest = new UpdateRequest(document.getIndex(), document.getType(), document.getId());
        Map<String, Object> source = document.getSourceAsMap();
        source.put("datePretty", "2017-02-28T05:30:00+05:30");
        updateRequest.doc(source);
        client.update(updateRequest);
        GetResponse updatedDocument = getDocument(document.getId());
        assertEquals(source.get("datePretty"), updatedDocument.getSourceAsMap().get("datePretty"));
    }


    @Test
    public void testSearchScriptOn1_4_1() throws ExecutionException, InterruptedException {
        List<IndexResponse> indexResponses = indexDocument(5);
        ConstantScoreQueryBuilder constantScoreQueryBuilder = QueryBuilders.constantScoreQuery(FilterBuilders.scriptFilter("doc['sentiment'].value>=0"));
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(this.client);
        searchRequestBuilder.setQuery(constantScoreQueryBuilder);
        searchRequestBuilder.setIndices(this.index);
        SearchResponse searchResponse = searchRequestBuilder.execute().get();
        assert searchResponse.getHits().getTotalHits() == 5;
    }

    @Test
    public void testCountScriptOn1_4_1() throws ExecutionException, InterruptedException {
        List<IndexResponse> indexResponses = indexDocument(5);
        ConstantScoreQueryBuilder constantScoreQueryBuilder = QueryBuilders.constantScoreQuery(FilterBuilders.scriptFilter("doc['sentiment'].value>=0"));
        CountRequestBuilder countRequestBuilder = new CountRequestBuilder(client);
        countRequestBuilder.setQuery(constantScoreQueryBuilder);
        CountResponse countResponse = countRequestBuilder.execute().get();
        assert countResponse.getCount() == 5;
    }

    @Test
    public void testDeleteByQueryScript1_4_1() throws ExecutionException, InterruptedException {
        List<IndexResponse> indexResponses = indexDocument(5);
        ConstantScoreQueryBuilder constantScoreQueryBuilder = QueryBuilders.constantScoreQuery(FilterBuilders.scriptFilter("doc['sentiment'].value>=0"));
        DeleteByQueryRequestBuilder deleteByQueryRequestBuilder = new DeleteByQueryRequestBuilder(client);
        deleteByQueryRequestBuilder.setQuery(constantScoreQueryBuilder);
        DeleteByQueryResponse indexDeleteByQueryResponses = deleteByQueryRequestBuilder.execute().get();
        assert new CountRequestBuilder(client).setQuery(constantScoreQueryBuilder).execute().get().getCount() == 0;
    }

    @Test
    public void testSearchScriptOn5_1_1() throws ExecutionException, InterruptedException {
        List<IndexResponse> indexResponses = indexDocument(5);
        ConstantScoreQueryBuilder constantScoreQueryBuilder = QueryBuilders.constantScoreQuery(FilterBuilders.scriptFilter("doc['sentiment'].value>=0"));
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(this.client);
        searchRequestBuilder.setQuery(constantScoreQueryBuilder);
        searchRequestBuilder.setIndices(this.index);
        SearchResponse searchResponse = searchRequestBuilder.execute().get();
        assert searchResponse.getHits().getTotalHits() == 5;
    }

    @Test
    public void testCountScriptOn5_1_1() throws ExecutionException, InterruptedException {
        List<IndexResponse> indexResponses = indexDocument(5);
        ConstantScoreQueryBuilder constantScoreQueryBuilder = QueryBuilders.constantScoreQuery(FilterBuilders.scriptFilter("doc['sentiment'].value>=0"));
        CountRequestBuilder countRequestBuilder = new CountRequestBuilder(client);
        countRequestBuilder.setQuery(constantScoreQueryBuilder);
        CountResponse countResponse = countRequestBuilder.execute().get();
        assert countResponse.getCount() == 5;
    }

    @Test
    public void testDeleteByQueryScript5_1_1() throws ExecutionException, InterruptedException {
        List<IndexResponse> indexResponses = indexDocument(5);
        ConstantScoreQueryBuilder constantScoreQueryBuilder = QueryBuilders.constantScoreQuery(FilterBuilders.scriptFilter("doc['sentiment'].value>=0"));
        DeleteByQueryRequestBuilder deleteByQueryRequestBuilder = new DeleteByQueryRequestBuilder(client);
        deleteByQueryRequestBuilder.setQuery(constantScoreQueryBuilder);
        deleteByQueryRequestBuilder.setIndices(index);
        DeleteByQueryResponse indexDeleteByQueryResponses = deleteByQueryRequestBuilder.execute().get();
        assert new CountRequestBuilder(client).setQuery(constantScoreQueryBuilder).execute().get().getCount() == 0;
    }

    @Test
    public void testQueryFilterBuilder5_1_1() throws ExecutionException, InterruptedException {
        List<IndexResponse> indexResponses = indexDocument(5);
        ConstantScoreQueryBuilder constantScoreQueryBuilder = QueryBuilders.constantScoreQuery(FilterBuilders.queryFilter(QueryBuilders.matchQuery("color", "red")));
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(this.client);
        searchRequestBuilder.setQuery(constantScoreQueryBuilder);
        searchRequestBuilder.setIndices(this.index);
        SearchResponse searchResponse = searchRequestBuilder.execute().get();
        assert searchResponse.getHits().getTotalHits() != -1;
    }

    @Test
    public void testNestedQueryBuilder5_1_1() throws ExecutionException, InterruptedException {
        List<IndexResponse> indexResponses = indexDocument(5);
        ConstantScoreQueryBuilder constantScoreQueryBuilder = QueryBuilders.constantScoreQuery(QueryBuilders.nestedQuery("author.books", QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("author.books.price").from(0))));
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(this.client);
        searchRequestBuilder.setQuery(constantScoreQueryBuilder);
        searchRequestBuilder.setIndices(this.index);
        SearchResponse searchResponse = searchRequestBuilder.execute().get();
        assert searchResponse.getHits().getTotalHits() == 5;
    }

    @Test
    public void testNestedQueryBuilderFilter5_1_1() throws ExecutionException, InterruptedException {
        List<IndexResponse> indexResponses = indexDocument(5);
        ConstantScoreQueryBuilder constantScoreQueryBuilder = QueryBuilders.constantScoreQuery(QueryBuilders.nestedQuery("author.books", FilterBuilders.boolFilter().must(FilterBuilders.rangeFilter("author.books.price").from(0))));
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(this.client);
        searchRequestBuilder.setQuery(constantScoreQueryBuilder);
        searchRequestBuilder.setIndices(this.index);
        SearchResponse searchResponse = searchRequestBuilder.execute().get();
        assert searchResponse.getHits().getTotalHits() == 5;
    }

    @Test
    public void testNestedFilterBuilder5_1_1() throws ExecutionException, InterruptedException {
        List<IndexResponse> indexResponses = indexDocument(5);
        ConstantScoreQueryBuilder constantScoreQueryBuilder = QueryBuilders.constantScoreQuery(FilterBuilders.nestedFilter("author.books", FilterBuilders.boolFilter().must(FilterBuilders.rangeFilter("author.books.price").from(0))));
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(this.client);
        searchRequestBuilder.setQuery(constantScoreQueryBuilder);
        searchRequestBuilder.setIndices(this.index);
        SearchResponse searchResponse = searchRequestBuilder.execute().get();
        assert searchResponse.getHits().getTotalHits() == 5;
    }

    @Test
    public void testNestedFilterBuilderQuery5_1_1() throws ExecutionException, InterruptedException {
        List<IndexResponse> indexResponses = indexDocument(5);
        ConstantScoreQueryBuilder constantScoreQueryBuilder = QueryBuilders.constantScoreQuery(FilterBuilders.nestedFilter("author.books", QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("author.books.price").from(0))));
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(this.client);
        searchRequestBuilder.setQuery(constantScoreQueryBuilder);
        searchRequestBuilder.setIndices(this.index);
        SearchResponse searchResponse = searchRequestBuilder.execute().get();
        assert searchResponse.getHits().getTotalHits() == 5;
    }

    @Test
    public void testMvelPlugin() throws ExecutionException, InterruptedException {
        List<IndexResponse> indexResponses = indexDocument(5);
        ConstantScoreQueryBuilder constantScoreQueryBuilder = QueryBuilders.constantScoreQuery(FilterBuilders.scriptFilter("field='sentiment';if(doc[field].value!=null) return doc[field].value; else return 0").lang("mvel"));
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(this.client);
        searchRequestBuilder.setQuery(constantScoreQueryBuilder);
        searchRequestBuilder.setIndices(this.index);
        SearchResponse searchResponse = searchRequestBuilder.execute().get();
        assert searchResponse.getHits().getTotalHits() == 5;
    }

    @Test
    public void testBulkIndex() throws ExecutionException, InterruptedException {
        BulkRequest request = new BulkRequest();
        int count = 10000;
        for (int i = 0; i < count; i++) {
            request.add(newIndexRequest());
        }
        BulkResponse bulkItemResponse = client.bulk(request).get();
        assertEquals(count, bulkItemResponse.getItems().length);
        for (BulkItemResponse itemResponse : bulkItemResponse.getItems()) {
            assertFalse("Item failed to index", itemResponse.isFailed());
        }
    }

    @Test
    public void testDateHistogram() throws ExecutionException, InterruptedException {
        TransportClient transportClient = createTransportClient();
        try {
            for (int i = 0; i < 5; i++) {
                indexDocument(client, transportClient);
            }
            ConstantScoreQueryBuilder constantScoreQuery = QueryBuilders.constantScoreQuery(QueryBuilders.matchAllQuery());
            DateHistogramBuilder dateHistogramBuilder = AggregationBuilders.dateHistogram("rahul")
                    .interval(DateHistogram.Interval.MONTH)
                    .field("datePretty")
                    .preZone("Asia/Kolkata")
                    .postZone("Asia/Kolkata");
            SearchRequestBuilder restSearchRequest = new SearchRequestBuilder(client).setIndices(index);
            restSearchRequest.setQuery(constantScoreQuery);
            restSearchRequest.addAggregation(dateHistogramBuilder);
            restSearchRequest.setSize(0);
            SearchResponse restSearchResponse = restSearchRequest.execute().get();
            SearchRequestBuilder transportSearchRequest = new SearchRequestBuilder(transportClient).setIndices(index);
            transportSearchRequest.setQuery(constantScoreQuery);
            transportSearchRequest.addAggregation(dateHistogramBuilder);
            transportSearchRequest.setSize(0);
            SearchResponse transportSearchResponse = transportSearchRequest.execute().get();
            Aggregations aggregations = transportSearchResponse.getAggregations();
            DateHistogram rahul = aggregations.get("rahul");
            DateHistogram rest_rahul = restSearchResponse.getAggregations().get("rahul");
            Set<String> keys = new HashSet<>();
            for (DateHistogram.Bucket bucket : rahul.getBuckets()) {
                keys.add(bucket.getKey());
                assert bucket.getDocCount() == rest_rahul.getBucketByKey(bucket.getKey()).getDocCount();
            }
            for (DateHistogram.Bucket bucket : rest_rahul.getBuckets()) {
                if (keys.contains(bucket.getKey())) {
                    continue;
                }
                assert bucket.getDocCount() == 0;
            }
        } finally {
            try {
                deleteIndex(index, transportClient.admin().indices());
                transportClient.close();
            } catch (Exception e) {

            }
        }
    }


    @Test
    public void testDateHistogramWithOffset() throws ExecutionException, InterruptedException {
        TransportClient transportClient = createTransportClient();
        try {
            for (int i = 0; i < 5; i++) {
                indexDocument(client, transportClient);
            }
            ConstantScoreQueryBuilder constantScoreQuery = QueryBuilders.constantScoreQuery(QueryBuilders.matchAllQuery());
            DateHistogramBuilder dateHistogramBuilder = AggregationBuilders.dateHistogram("rahul")
                    .interval(DateHistogram.Interval.MONTH)
                    .field("datePretty").preOffset("5h").postOffset("5h");
            SearchRequestBuilder restSearchRequest = new SearchRequestBuilder(client).setIndices(index);
            restSearchRequest.setQuery(constantScoreQuery);
            restSearchRequest.addAggregation(dateHistogramBuilder);
            restSearchRequest.setSize(0);
            SearchResponse restSearchResponse = restSearchRequest.execute().get();
            SearchRequestBuilder transportSearchRequest = new SearchRequestBuilder(transportClient).setIndices(index);
            transportSearchRequest.setQuery(constantScoreQuery);
            transportSearchRequest.addAggregation(dateHistogramBuilder);
            transportSearchRequest.setSize(0);
            SearchResponse transportSearchResponse = transportSearchRequest.execute().get();
            Aggregations aggregations = transportSearchResponse.getAggregations();
            DateHistogram rahul = aggregations.get("rahul");
            DateHistogram rest_rahul = restSearchResponse.getAggregations().get("rahul");
            Set<String> keys = new HashSet<>();
            for (DateHistogram.Bucket bucket : rahul.getBuckets()) {
                keys.add(bucket.getKey());
                assert bucket.getDocCount() == rest_rahul.getBucketByKey(bucket.getKey()).getDocCount();
            }
            for (DateHistogram.Bucket bucket : rest_rahul.getBuckets()) {
                if (keys.contains(bucket.getKey())) {
                    continue;
                }
                assert bucket.getDocCount() == 0;
            }
        } finally {
            try {
                deleteIndex(index, transportClient.admin().indices());
                transportClient.close();
            } catch (Exception e) {

            }
        }
    }

    @Test
    public void testBulkIndexWithOptTypeCreate() throws ExecutionException, InterruptedException {
        BulkRequest request = new BulkRequest();
        IndexRequest request1 = newIndexRequest();
        request.add(request1);
        request1.create(true);
        request.add(request1);

        BulkResponse bulkItemResponse = client.bulk(request).get();
        BulkItemResponse[] items = bulkItemResponse.getItems();
        assertEquals(2, items.length);
        assertFalse("Item failed to index", items[0].isFailed());
        assertTrue("Item failed to index", items[1].isFailed());
    }

    @Test
    public void testBulkProcessorIndex() throws ExecutionException, InterruptedException {
        BulkProcessorListener listener = new BulkProcessorListener();
        BulkProcessor.Builder builder = BulkProcessor.builder(client, listener).setBulkActions(100);
        BulkProcessor esBulkProcessor = builder.setConcurrentRequests(0).build();
        int count = 1000;
        for (int i = 0; i < count; i++) {
            esBulkProcessor.add(newIndexRequest());
        }
        esBulkProcessor.flush();
        esBulkProcessor.close();
        refresh();
        assertEquals(count, getCount());
    }

    @Test(expected = IndexAlreadyExistsException.class)
    public void testIndexAlreadyExistsException() throws ExecutionException, InterruptedException {
        client.admin().indices().prepareCreate(index).get();
        fail("Expecting IndexAlreadyExistsException");
    }

    @Test(expected = DocumentMissingException.class)
    public void testDocumentMissingException() throws ExecutionException, InterruptedException {
        client.prepareUpdate(index, STATS_TYPE, UUID.randomUUID().toString()).setDoc(Maps.newHashMap()).get();
        fail("Expecting DocumentMissingException");
    }

    @Test(expected = VersionConflictEngineException.class)
    public void testVersionConflictEngineException() throws ExecutionException, InterruptedException {
        IndexRequest indexRequest = newIndexRequest();
        IndexResponse indexResponse = index(indexRequest);

        client.prepareGet(indexRequest.index(), indexRequest.type(),
                indexRequest.id())
                .setVersion(indexResponse.getVersion() + 1).get();

        fail("Expecting VersionConflictEngineException");
    }

    @Test
    public void testBulkProcessorDelete() throws ExecutionException, InterruptedException {
        BulkProcessorListener listener = new BulkProcessorListener();
        BulkProcessor.Builder builder = BulkProcessor.builder(client, listener).setBulkActions(100);
        BulkProcessor esBulkProcessor = builder.setConcurrentRequests(0).build();
        List<IndexRequest> indexRequests = new ArrayList<>();
        int count = 1000;
        for (int i = 0; i < count; i++) {
            IndexRequest request = newIndexRequest();
            indexRequests.add(request);
            esBulkProcessor.add(request);
        }
        esBulkProcessor.flush();

        for (IndexRequest indexRequest : indexRequests) {
            esBulkProcessor.add(new DeleteRequest(indexRequest.index(), indexRequest.type(), indexRequest.id()));
        }
        esBulkProcessor.flush();
        esBulkProcessor.close();

        refresh();
        assertEquals(0, getCount());
    }

    @Test
    public void testBulkProcessorUpdate() throws Exception {
        BulkProcessorListener listener = new BulkProcessorListener();
        BulkProcessor.Builder builder = BulkProcessor.builder(client, listener).setBulkActions(100);
        BulkProcessor esBulkProcessor = builder.setConcurrentRequests(0).build();
        List<IndexRequest> indexRequests = new ArrayList<>();
        int count = 1000;
        for (int i = 0; i < count; i++) {
            IndexRequest request = newIndexRequest();
            indexRequests.add(request);
            esBulkProcessor.add(request);
        }
        esBulkProcessor.flush();

        for (IndexRequest indexRequest : indexRequests) {
            UpdateRequest request = new UpdateRequest(indexRequest.index(), indexRequest.type(), indexRequest.id());
            request.doc(indexRequest);
            esBulkProcessor.add(request);
        }
        esBulkProcessor.flush();
        esBulkProcessor.close();

        refresh();
        assertEquals(count, getCount());
    }

    public static class BulkProcessorListener implements BulkProcessor.Listener {

        private final Map<String, ActionResponse> responseMap = Maps.newHashMap();

        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            request.replicationType(ReplicationType.ASYNC);
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            for (BulkItemResponse itemResponse : response.getItems()) {
                String id = itemResponse.getId();
                BulkItemResponse.Failure failure = itemResponse.getFailure();

                if (failure == null) {
                    responseMap.put(id, itemResponse.getResponse());
                }
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            if (failure != null) {
                throw new RuntimeException(failure);
            }
        }

        public Map<String, ActionResponse> getResponseMap() {
            return responseMap;
        }
    }

    @Test
    public void testBulkWithErrors() throws ExecutionException, InterruptedException {
        BulkRequest request = new BulkRequest();
        int count = 10000;
        for (int i = 0; i < count; i++) {
            IndexRequest indexRequest = newIndexRequest();
            request.add(indexRequest);
            DeleteRequest deleteRequest = new DeleteRequest(indexRequest.index(), indexRequest.type(), indexRequest.id());
            request.add(deleteRequest);
            request.add(deleteRequest);
        }
        BulkResponse bulkItemResponse = client.bulk(request).get();
        for (BulkItemResponse itemResponse : bulkItemResponse.getItems()) {
            assertFalse("Item failed to index", itemResponse.isFailed());
        }
    }

    public long getCount() throws ExecutionException, InterruptedException {
        CountRequest request;
        request = new CountRequest(index);
        request.types(STATS_TYPE);
        CountResponse countResponse = client.count(request).get();
        return countResponse.getCount();

    }

    @Test
    public void testCount() throws ExecutionException, InterruptedException {
        IndexResponse indexResponse = indexDocument();
        CountRequest request;
        request = new CountRequest(index);
        request.types(STATS_TYPE);
        CountResponse countResponse = client.count(request).get();
        assertTrue(countResponse.getCount() > 0);

        request = new CountRequest();
        request.types();
        countResponse = client.count(request).get();
        assertTrue(countResponse.getCount() > 0);

        request = new CountRequest();
        request.source(new QuerySourceBuilder().setQuery(new IdsQueryBuilder().addIds(indexResponse.getId())));

        request.types();
        countResponse = client.count(request).get();
        assertEquals(1, countResponse.getCount());
    }

    @Test
    public void testSearch() throws ExecutionException, InterruptedException {
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(index).addSort("datePretty", SortOrder.DESC);
        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertTrue(hits1.length > 0);
    }

    @Test
    public void testSearchWithFields() throws ExecutionException, InterruptedException {
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(index).addField("datePretty");
        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertTrue(hits1.length > 0);
        assertNotNull(hits1[0].field("datePretty"));
    }

    @Test
    public void testSearchWithAggregationValueCount() throws ExecutionException, InterruptedException {
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(index);
        String colorsAgg = "colors";
        search.addAggregation(AggregationBuilders.count(colorsAgg).field("color"));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Aggregation aggregation = aggregations.get(colorsAgg);
        assertNotNull(aggregation);
        assertTrue(aggregation instanceof ValueCount);
        ValueCount valueCount = (ValueCount) aggregation;
        assertTrue(valueCount.getValue() > 0);


    }

    @Test
    public void testSearchWithAggregationSum() throws ExecutionException, InterruptedException {
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "amount_sum";
        search.addAggregation(AggregationBuilders.sum(name).field("amount"));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Aggregation aggregation = aggregations.get(name);
        assertNotNull(aggregation);
        assertTrue(aggregation instanceof Sum);
        Sum valueCount = (Sum) aggregation;
        assertTrue(valueCount.getValue() > 0);

    }

    @Test
    public void testSearchWithAggregationAvg() throws ExecutionException, InterruptedException {
        indexDocument();
        indexDocument();
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "amount_value";
        search.addAggregation(AggregationBuilders.avg(name).field("amount"));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Aggregation aggregation = aggregations.get(name);
        assertNotNull(aggregation);
        assertTrue(aggregation instanceof Avg);
        Avg valueCount = (Avg) aggregation;
        assertTrue(valueCount.getValue() > 0);

    }

    @Test
    public void testSearchWithAggregationCardinality() throws ExecutionException, InterruptedException {
        indexDocument();
        indexDocument();
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "amount_value";
        search.addAggregation(AggregationBuilders.cardinality(name).field("amount"));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Aggregation aggregation = aggregations.get(name);
        assertNotNull(aggregation);
        assertTrue(aggregation instanceof Cardinality);
        Cardinality valueCount = (Cardinality) aggregation;
        assertTrue(valueCount.getValue() > 0);

    }

    @Test
    public void testSearchWithAggregationGlobal() throws ExecutionException, InterruptedException {
        indexDocument();
        indexDocument();
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";
        search.addAggregation(AggregationBuilders.global(name).subAggregation(
                AggregationBuilders.terms("colors").field("color").size(0)
        ));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Global global = aggregations.get(name);
        assertTrue(global.getDocCount() > 0);
    }

    @Test
    public void testSearchWithTermFilterAggregation() throws ExecutionException, InterruptedException {
        indexDocument();
        indexDocument();
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";
        search.addAggregation(AggregationBuilders.filter(name).filter(FilterBuilders.termFilter("color", "red")));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Filter aggregation = aggregations.get(name);
        Aggregations buckets = aggregation.getAggregations();
        System.out.println("buckets = " + buckets);
    }

    @Test
    public void testSearchWithTermFilterBucketAggregation() throws ExecutionException, InterruptedException {
        indexDocument();
        indexDocument();
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";
        AggregationBuilder aggregation =
                AggregationBuilders
                        .filters(name)
                        .filter("red", FilterBuilders.termFilter("color", "red"))
                        .filter("blue", FilterBuilders.termFilter("color", "blue"));

        search.addAggregation(aggregation);
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Filters agg = aggregations.get(name);
        Collection<? extends Filters.Bucket> buckets = agg.getBuckets();
        assertEquals(2, buckets.size());
        // For each entry
        for (Filters.Bucket entry : agg.getBuckets()) {
            String key = entry.getKey();                    // bucket key
            long docCount = entry.getDocCount();            // Doc count
            System.out.println("key = " + key + " docCount = " + docCount);
        }
    }

    @Test
    public void testSearchWithMissingAggregation() throws ExecutionException, InterruptedException {
        indexDocument();
        indexDocument();
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";


        search.addAggregation(AggregationBuilders.missing(name).field("MyFakeField"));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Missing agg = aggregations.get(name);
        assertTrue(agg.getDocCount() > 0);
    }

    @Test
    public void testSearchWithNestedAggregation() throws ExecutionException, InterruptedException {
        indexDocument();
        indexDocument();
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";


        NestedBuilder nestedBuilder = AggregationBuilders.nested(name).path("author.books");
        String genere_count_agg = "genere_count";
        String avg_price_agg = "avg_price";
        nestedBuilder.subAggregation(AggregationBuilders.terms(genere_count_agg).field("author.books.genre"))
                .subAggregation(AggregationBuilders.avg(avg_price_agg).field("author.books.price"));
        search.addAggregation(nestedBuilder);
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        Nested nested = aggregations.get(name);
        Terms genreCount = nested.getAggregations().get(genere_count_agg);
        List<Terms.Bucket> buckets = genreCount.getBuckets();
        assertTrue(buckets.size() > 0);
        Avg avgPrice = nested.getAggregations().get(avg_price_agg);
        assertTrue(avgPrice.getValue() > 0);

        assertNotNull(aggregations);
    }

    @Test
    @Ignore
    public void testSearchWithChildrenAggregation() throws ExecutionException, InterruptedException {
        IndexRequest post = newPost();
        index(post);
        index(newComment(post.id()));
        index(newComment(post.id()));
        index(newComment(post.id()));
        index(newComment(post.id()));

        SearchRequestBuilder search = client.prepareSearch(POSTS_INDEX).setTypes(POST_TYPE);
        String name = "agg";


        ChildrenBuilder builder = AggregationBuilders.children(name).childType(COMMENT_TYPE);
        search.addAggregation(builder);
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);

        Children children = aggregations.get(name);
        assertNotNull(children);

        assertTrue(children.getDocCount() > 0);
    }

    @Test
    public void testSearchWithMaxAggregation() throws ExecutionException, InterruptedException {
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";
        search.addAggregation(AggregationBuilders.max(name).field("amount"));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Max aggregation = aggregations.get(name);
        assertNotNull(aggregation);
        assertTrue(aggregation.getValue() > 0);
    }

    @Test
    public void testSearchWithMaxAggregationWithEmptyIndex() throws ExecutionException, InterruptedException {

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";
        search.addAggregation(AggregationBuilders.max(name).field("amount"));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Max aggregation = aggregations.get(name);
        assertNotNull(aggregation);
        assertTrue(aggregation.getValue() == Double.POSITIVE_INFINITY);
    }

    @Test
    public void testSearchWithMinAggregation() throws ExecutionException, InterruptedException {
        indexDocument();

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";
        search.addAggregation(AggregationBuilders.min(name).field("amount"));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Aggregation aggregation = aggregations.get(name);
        assertNotNull(aggregation);
        assertTrue(aggregation instanceof Min);
        Min valueCount = (Min) aggregation;
        assertTrue(valueCount.getValue() != 0);
    }


    @Test
    public void testSearchWithPercentilesAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";
        search.addAggregation(AggregationBuilders.percentiles(name).field("amount").percentiles(.9D, .8D, .7D));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Percentiles percentiles = aggregations.get(name);
        double percentile = percentiles.percentile(.9D);
        assertTrue(percentile != 0D);
        Map<Double, Percentile> percentileMap = Maps.newHashMap();
        Iterator<Percentile> percentileIterator = percentiles.iterator();
        while (percentileIterator.hasNext()) {
            Percentile next = percentileIterator.next();
            percentileMap.put(next.getPercent(), next);
        }
        assertEquals(3, percentileMap.size());
        for (Map.Entry<Double, Percentile> entry : percentileMap.entrySet()) {
            assertEquals(percentiles.percentile(entry.getKey()), entry.getValue().getValue(), 1);
        }
    }

    @Test
    public void testSearchWithPercentileRanksAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";
        search.addAggregation(AggregationBuilders.percentileRanks(name).field("amount").percentiles(.9D, .8D, .7D));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        SearchHits hits = response.getHits();
        assertNotNull(hits);
        SearchHit[] hits1 = hits.hits();
        assertNotNull(hits1);
        assertEquals(0, hits1.length);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        PercentileRanks percentileRanks = aggregations.get(name);
        double percentile = percentileRanks.percent(.9D);
        assertTrue(percentile != 0D);
        Map<Double, Percentile> percentileMap = Maps.newHashMap();
        Iterator<Percentile> percentileIterator = percentileRanks.iterator();
        while (percentileIterator.hasNext()) {
            Percentile next = percentileIterator.next();
            percentileMap.put(next.getPercent(), next);
        }
        assertEquals(3, percentileMap.size());
    }

    @Test
    public void testSearchWithRangeAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";
        String maxKey = "1toMax";
        search.addAggregation(AggregationBuilders.range(name).field("amount")
                .addRange(0, 1)
                .addRange(maxKey, 1, Integer.MAX_VALUE));
        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Range range = aggregations.get(name);
        Range.Bucket oneToMaxBucket = range.getBucketByKey(maxKey);
        assertNotNull(oneToMaxBucket);

        Collection<? extends Range.Bucket> buckets = range.getBuckets();
        assertNotNull(buckets);

    }

    @Test
    public void testSearchWithDateRangeAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";
        String minKey = "first";
        search.addAggregation(AggregationBuilders.dateRange(name).field("datePretty")
                .addRange(minKey, new DateTime().minusDays(500), new DateTime().minusDays(400))
                .addRange(new DateTime().minusDays(400), new DateTime().minusDays(300))
                .addRange(new DateTime().minusDays(300), new DateTime().minusDays(200))
                .addRange(new DateTime().minusDays(200), new DateTime().minusDays(100))
                .addRange(new DateTime().minusDays(100), new DateTime().minusDays(0)));

        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Range range = aggregations.get(name);
        Range.Bucket oneToMaxBucket = range.getBucketByKey(minKey);
        assertNotNull(oneToMaxBucket);

        Collection<? extends Range.Bucket> buckets = range.getBuckets();
        assertNotNull(buckets);

    }

    @Test
    public void testSearchWithTopHitsAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";
        search.addAggregation(AggregationBuilders.topHits(name).addFieldDataField("datePretty"));

        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        TopHits topHits = aggregations.get(name);
        assertNotNull(topHits);

        SearchHits hits = topHits.getHits();
        assertTrue(hits.getTotalHits() > 0);
        assertTrue(hits.getMaxScore() > 0);

        assertNotNull(hits);
        assertTrue(hits.hits().length > 0);
        for (SearchHit hit : hits.hits()) {
            assertTrue(hit.getScore() > 0);
            assertNotNull(hit.getId());
            assertNotNull(hit.getFields());
            assertNotNull(hit.sourceAsMap());
        }

    }

    @Test
    public void testSearchWithStatsAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";
        search.addAggregation(AggregationBuilders.stats(name).field("amount"));

        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Stats stats = aggregations.get(name);
        assertNotNull(stats);
        assertTrue(stats.getAvg() > 0);
        assertTrue(stats.getMax() != 0);
        assertTrue(stats.getMin() < stats.getMax());
        assertTrue(stats.getCount() > 0);
        assertTrue(stats.getSum() > 0);
    }

    @Test
    public void testSearchWithExtendedStatsAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";
        search.addAggregation(AggregationBuilders.extendedStats(name).field("amount"));

        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        ExtendedStats stats = aggregations.get(name);
        assertNotNull(stats);
        assertTrue(stats.getAvg() > 0);
        assertTrue(stats.getMax() != 0);
        assertTrue(stats.getMin() < stats.getMax());
        assertTrue(stats.getCount() > 0);
        assertTrue(stats.getSum() > 0);
        assertTrue(stats.getStdDeviation() != 0);
        assertTrue(stats.getSumOfSquares() != 0);
        assertTrue(stats.getVariance() != 0);

    }

    @Test
    public void testSearchWithIpRangeAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";
        String mask = "255.255.0.0/32";
        search.addAggregation(AggregationBuilders.ipRange(name).field("ipAddress").addMaskRange(mask));

        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        IPv4Range iPv4Range = aggregations.get(name);
        assertNotNull(iPv4Range);
        IPv4Range.Bucket bucketByKey = iPv4Range.getBucketByKey(mask);
        assertNotNull(bucketByKey);
    }

    @Test
    public void testSearchWithGeoBoundsAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";
        search.addAggregation(AggregationBuilders.geoBounds(name).field("currentLocation"));

        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        GeoBounds geoBounds = aggregations.get(name);
        assertNotNull(geoBounds);
        validateRangeBucket(geoBounds.topLeft());
        validateRangeBucket(geoBounds.bottomRight());
    }

    @Test
    public void testSearchWithGeoDistanceAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";
        search.addAggregation(AggregationBuilders.geoDistance(name)
                .field("currentLocation")
                .unit(DistanceUnit.MILES)
                .addRange(0, 2000)
                .lat(40)
                .lon(-71.34)
                .distanceType(org.elasticsearch.common.geo.GeoDistance.PLANE));

        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        GeoDistance geoDistance = aggregations.get(name);
        assertNotNull(geoDistance);
        Collection<? extends GeoDistance.Bucket> buckets = geoDistance.getBuckets();
        assertNotNull(buckets);
        validate(buckets);
    }

    @Test
    public void testSearchWithGeoHashGridAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";
        search.addAggregation(AggregationBuilders.geohashGrid(name)
                .field("currentLocation")
                .precision(5)
                .size(5));

        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        GeoHashGrid geoHashGrid = aggregations.get(name);
        assertNotNull(geoHashGrid);
        Collection<GeoHashGrid.Bucket> buckets = geoHashGrid.getBuckets();
        assertNotNull(buckets);
        validate(buckets);
    }

    @Test
    public void testSearchWithHistogramAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";
        search.addAggregation(AggregationBuilders.histogram(name)
                .field("amount")
                .interval(1000));

        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Histogram histogram = aggregations.get(name);
        assertNotNull(histogram);
        List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
        validate(buckets);


    }

    @Test
    public void testSearchWithDateHistogramAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";
        DateHistogramBuilder dateHistogramBuilder = AggregationBuilders.dateHistogram(name)
                .field("datePretty")
                .preOffset("1d")
                .reversePostTz(true)
                .interval(DateHistogram.Interval.YEAR);
        search.addAggregation(dateHistogramBuilder);

        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Histogram histogram = aggregations.get(name);
        assertNotNull(histogram);
        List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
        validate(buckets);
    }

    @Test
    public void testSearchWithSignificantStringTermsAggregation() throws ExecutionException, InterruptedException {
        indexDocument(1000);

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";
        search.setQuery(QueryBuilders.termQuery("genre", "action"));
        search.addAggregation(AggregationBuilders.significantTerms(name)
                .field("color")
                .size(10));

        search.setSize(10); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        SignificantTerms significantTerms = aggregations.get(name);
        assertNotNull(significantTerms);
        for (MultiBucketsAggregation.Bucket bucket : significantTerms.getBuckets()) {
            assertNotNull(bucket.getKeyAsText());
            assertNotNull(bucket.getKey());
        }
    }

    @Test
    public void testSearchWithSignificantLongTermsAggregation() throws ExecutionException, InterruptedException {
        indexDocument(1000);

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";
        search.setQuery(QueryBuilders.termQuery("genre", "action"));
        search.addAggregation(AggregationBuilders.significantTerms(name)
                .minDocCount(1)
                .field("sentiment")
                .size(8));

        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        SignificantTerms significantTerms = aggregations.get(name);
        assertNotNull(significantTerms);
        for (MultiBucketsAggregation.Bucket bucket : significantTerms.getBuckets()) {
            assertNotNull(bucket.getKeyAsText());
            assertNotNull(bucket.getKey());
        }
    }

    @Test
    public void testSearchWithFiltersAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);
        refresh();

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";
        search.setQuery(QueryBuilders.termQuery("genre", "action"));
        search.addAggregation(AggregationBuilders.filters(name)
                .filter("sentiment_filter", FilterBuilders.termFilter("sentiment", 5))
                .filter("red_filter", FilterBuilders.termFilter("color", Color.red.name())));


        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Filters filters = aggregations.get(name);
        assertNotNull(filters);
        validate(filters.getBuckets());

    }

    @Test
    public void testSearchWithScriptedMetricAggregation() throws ExecutionException, InterruptedException {
        indexDocument(100);

        SearchRequestBuilder search = client.prepareSearch(index);
        String name = "agg";
        Map params = Maps.newHashMap();
        params.put("_agg", Maps.newHashMap());
        search.addAggregation(AggregationBuilders.scriptedMetric(name)
                .params(params)
                .initScript("params._agg.positive_sentiment = []")
                .mapScript(" if (doc['sentiment'].value > 4) { params._agg.positive_sentiment.add(doc['sentiment']) } "));


        search.setSize(0); // no hits please

        SearchResponse response = client.search(search.request()).get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        ScriptedMetric scriptedMetric = aggregations.get(name);
        assertNotNull(scriptedMetric);

        Object aggregation = scriptedMetric.aggregation();
        assertNotNull(aggregation);
    }

    @Test
    public void testScanAndScroll() throws ExecutionException, InterruptedException {
        int recordWritten = 1000;
        indexDocument(recordWritten);
        int blockSize = 10;
        TimeValue scrollKeepAlive = TimeValue.timeValueHours(1);
        SearchResponse response;
        response = client.prepareSearch(index)
                .setScroll(scrollKeepAlive)
                .setSize(blockSize)
                .setQuery(QueryBuilders.matchAllQuery())
                .execute().actionGet();

        assertNotNull(response.getScrollId());


        int recordsRead = 0;
        recordsRead += response.getHits().hits().length;
        for (; ; ) {
            response = client.prepareSearchScroll(response.getScrollId()).setScroll(scrollKeepAlive).execute().actionGet();
            if (response.getHits().hits().length == 0) {
                break;
            }
            recordsRead += response.getHits().hits().length;

        }
        assertEquals(recordWritten, recordsRead);

        response = client.prepareSearch(index)
                .setScroll(scrollKeepAlive)
                .setSize(blockSize)
                .setQuery(QueryBuilders.matchAllQuery())
                .execute().actionGet();

        assertNotNull(response.getScrollId());
        ClearScrollResponse clearScrollResponse = client.prepareClearScroll().addScrollId(response.getScrollId()).get();
        assertTrue(clearScrollResponse.isSucceeded());
        try {
            client.prepareSearchScroll(response.getScrollId()).setScroll(scrollKeepAlive).execute().actionGet();
            fail("Should have thrown an exception of SearchPhaseExecutionException");
        } catch (SearchPhaseExecutionException ignore) {
            // all good
            return;
        }
        fail("Should have thrown an exception of SearchPhaseExecutionException");
    }

    @Test
    public void testDeleteByQuery() throws ExecutionException, InterruptedException {
        indexDocument(100);
        DeleteByQueryResponse response;
        response = client.prepareDeleteByQuery(index)
                .setTypes(STATS_TYPE)
                .setQuery(QueryBuilders.termQuery("color", Color.red)).get();
        for (IndexDeleteByQueryResponse queryResponse : response) {
            assertEquals(index, queryResponse.getIndex());
        }
    }

    @Test
    public void testIndexedScript() throws ExecutionException, InterruptedException {
        indexDocument(100);

        PutIndexedScriptResponse response;
        Map<String, Object> script = Maps.newHashMap();
        script.put("script", "log(_score * 2) + my_modifier");
        String id = "indexedCalculateScore-" + UUID.randomUUID().toString();
        response = client.preparePutIndexedScript()
                .setScriptLang("groovy")
                .setId(id)
                .setSource(script)
                .get();
        assertNotNull(response);
        assertNotNull(response.getId());
        assertTrue(response.isCreated());

        GetIndexedScriptResponse getIndexedScriptResponse;
        getIndexedScriptResponse = client.getIndexedScript(new GetIndexedScriptRequest("groovy", id)).actionGet();
        assertNotNull(getIndexedScriptResponse);
        assertTrue(getIndexedScriptResponse.isExists());

        DeleteIndexedScriptResponse deleteIndexedScriptResponse;
        deleteIndexedScriptResponse = client.deleteIndexedScript(new DeleteIndexedScriptRequest("groovy", id)).actionGet();
        assertNotNull(deleteIndexedScriptResponse);
        assertEquals(id, deleteIndexedScriptResponse.getId());
    }

    @Test
    public void testUpdateScriptWithBulkProcessor() throws ExecutionException, InterruptedException {
        List<IndexResponse> indexResponses = indexDocument(100);
        BulkRequestBuilder bulk = client.prepareBulk();
        UpdateRequest updateRequest = client.prepareUpdate(index, type, indexResponses.get(0).getId())
                .addScriptParam("color", "red")
                .setScript("ctx._source.color = color", ScriptService.ScriptType.INLINE).request();
        bulk.add(updateRequest);
        BulkResponse bulkItemResponses = bulk.get();
        assertFalse(bulkItemResponses.hasFailures());
    }


    @Test
    public void testExistsRequest() throws ExecutionException, InterruptedException {
        indexDocument(100);
        super.refresh();

        ExistsResponse existsResponse = client.prepareExists(index)
                .setTypes(STATS_TYPE)
                .setQuery(QueryBuilders.termQuery("color", "red")).get();
        assertNotNull(existsResponse);
        assertTrue(existsResponse.exists());
    }

    @Test
    public void testTermsLookupFilter() throws ExecutionException, InterruptedException {
        indexDocument(100);

        IndexResponse indexResponse = indexDocument();
        String index = indexResponse.getIndex();
        String type = indexResponse.getType();
        String id = indexResponse.getId();

        TermsLookupFilterBuilder filter = FilterBuilders.termsLookupFilter("color");
        filter.lookupIndex(index);
        filter.lookupType(type);
        filter.lookupId(id);
        filter.lookupPath("color");
        filter.cache(true);
        filter.cacheKey("LST_TEST_" + UUID.randomUUID().toString());
        SearchResponse lookupResponse = client.prepareSearch(index).setQuery(QueryBuilders.constantScoreQuery(filter)).get();
        assertNotNull(lookupResponse);
        assertNotNull(lookupResponse.getHits());
        assertTrue(lookupResponse.getHits().getTotalHits() > 1);
        assertTrue(lookupResponse.getHits().getTotalHits() < 101);
    }

    @Test
    public void testClearCache() {
        client.admin().indices().clearCache(new ClearIndicesCacheRequest().queryBuilderCache(true).filterCache(true));
    }

    @Test
    public void testExplainRequest() throws ExecutionException, InterruptedException {
        IndexRequest request = newIndexRequest();
        index(request);
        ExplainResponse response = client.prepareExplain(index, STATS_TYPE, request.id())
                .setQuery(QueryBuilders.termQuery("color", request.sourceAsMap().get("color"))).get();
        assertNotNull(response);
    }

    @Test
    public void testMultiGetRequest() throws ExecutionException, InterruptedException {
        List<IndexResponse> indexResponses = indexDocument(3);
        MultiGetRequestBuilder requestBuilder = client.prepareMultiGet();

        for (IndexResponse indexResponse : indexResponses) {
            requestBuilder.add(indexResponse.getIndex(), indexResponse.getType(), indexResponse.getId());
        }
        MultiGetResponse responses = requestBuilder.get();
        assertNotNull(responses);
        MultiGetItemResponse[] responses1 = responses.getResponses();
        assertEquals(indexResponses.size(), responses1.length);
        for (MultiGetItemResponse response : responses) {
            assertNotNull(response.getIndex());
            assertNotNull(response.getType());
            assertNotNull(response.getId());
            Map<String, Object> sourceAsMap = response.getResponse().getSourceAsMap();
            assertNotNull(sourceAsMap);
            assertTrue(sourceAsMap.size() > 0);
        }
    }

    @Test
    public void testMultiSearchRequest() throws ExecutionException, InterruptedException {
        List<IndexResponse> indexResponses = indexDocument(3);
        MultiSearchRequestBuilder requestBuilder = client.prepareMultiSearch();

        for (IndexResponse indexResponse : indexResponses) {
            IdsQueryBuilder query = QueryBuilders.idsQuery(indexResponse.getType()).ids(indexResponse.getId());
            requestBuilder.add(client.prepareSearch(indexResponse.getIndex()).setQuery(query));
        }
        MultiSearchResponse multiSearchResponse = requestBuilder.get();
        assertNotNull(multiSearchResponse);

        MultiSearchResponse.Item[] responses = multiSearchResponse.getResponses();
        assertNotNull(responses);
        assertEquals(indexResponses.size(), responses.length);
        for (MultiSearchResponse.Item item : responses) {
            SearchResponse response = item.getResponse();
            assertNotNull(response);
            assertNotNull(response.getHits());
            assertEquals(1, response.getHits().getHits().length);
        }
    }

    @Test
    public void testMultiSearchRequestWithFailures() throws ExecutionException, InterruptedException {
        List<IndexResponse> indexResponses = indexDocument(3);
        MultiSearchRequestBuilder requestBuilder = client.prepareMultiSearch();

        for (IndexResponse indexResponse : indexResponses) {
            IdsQueryBuilder query = QueryBuilders.idsQuery(indexResponse.getType()).ids(indexResponse.getId());
            SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexResponse.getIndex() + " " + indexResponse.getIndex());
            requestBuilder.add(searchRequestBuilder.setQuery(query));
        }
        MultiSearchResponse multiSearchResponse = requestBuilder.get();
        assertNotNull(multiSearchResponse);

        MultiSearchResponse.Item[] responses = multiSearchResponse.getResponses();
        assertNotNull(responses);
        assertEquals(indexResponses.size(), responses.length);
        for (MultiSearchResponse.Item item : responses) {
            assertTrue(item.isFailure());
            assertTrue(Strings.isNotEmpty(item.getFailureMessage()));
        }
    }

    @Test
    public void testDateZeroValue() throws IOException, ExecutionException, InterruptedException {
        String template = loadTemplate("/org/elasticsearch/client/rest/test-index-dynamic2.json");
        String source = loadTemplate("/org/elasticsearch/client/rest/data3.json");
        String dummyDoc = loadTemplate("/org/elasticsearch/client/rest/data5.json");
        PutIndexTemplateResponse templateResponse = indicesAdminClient.preparePutTemplate("dyn_template").setSource(template).get();
        assertAcknowledged(templateResponse);

        String indexName = "auto_1";
        try {
            indicesAdminClient.prepareDelete(indexName).get();
        } catch (ElasticsearchException e) {
            //ignore
        }
        CreateIndexResponse response = indicesAdminClient.prepareCreate(indexName).get();
        assertAcknowledged(response);

        IndexResponse response1 = client.prepareIndex(indexName, "stat", UUID.randomUUID().toString())
                .setSource(dummyDoc)
                .setRefresh(true)
                .setReplicationType(ReplicationType.ASYNC)
                .get();
        client.prepareDelete(indexName, "stat", response1.getId()).get();


        IndexRequest request1 = client.prepareIndex(indexName, "stat", UUID.randomUUID().toString())
                .setSource(source)
                .setRefresh(true)
                .setReplicationType(ReplicationType.ASYNC).request();
        BulkRequest request = new BulkRequest();
        request.add(request1);
        BulkResponse bulkItemResponse = client.bulk(request).get();
        for (BulkItemResponse itemResponse : bulkItemResponse.getItems()) {
            assertTrue("Item failed to index", itemResponse.isFailed());
        }
    }

    @Test
    public void testSuggestSearch() throws ExecutionException, InterruptedException {
        List<IndexResponse> indexResponses = indexDocument(3);

        SuggestResponse response = client.prepareSuggest(index)
                .addSuggestion(SuggestBuilders.termSuggestion("perhaps")
                        .suggestMode("always")
                        .text("read")
                        .field("color")).get();
        assertNotNull(response);
        assertNotNull(response.getSuggest());
        Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> perhaps = response.getSuggest().getSuggestion("perhaps");
        assertNotNull(perhaps);
        List<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> entries = perhaps.getEntries();
        assertNotNull(entries);
        assertTrue(entries.size() > 0);
        for (Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option> entry : entries) {
            assertNotNull(entry.getText());
            assertNotNull(entry.getOptions());
            assertTrue(entry.getOptions().size() > 0);
            for (Suggest.Suggestion.Entry.Option option : entry.getOptions()) {
                assertNotNull(option.getText());
                assertTrue(option.getScore() > 0);
            }
        }
    }

    @Test
    @Ignore
    public void testMoreLikeThisSearch() throws ExecutionException, InterruptedException {
        //todo bdk implement
    }

    @Test
    @Ignore
    public void testValidate() throws ExecutionException, InterruptedException {
        //todo bdk implement
    }


    private void validateScrollResponse(SearchResponse response) {
        assertNotNull(response);
        assertNotNull(response.getHits());
        assertNotNull(response.getHits().hits());
        assertEquals(2, response.getHits().hits().length);
    }


    private void validate(Collection<? extends MultiBucketsAggregation.Bucket> buckets) {
        for (MultiBucketsAggregation.Bucket bucket : buckets) {
            assertTrue(bucket.getDocCount() > 0);
            assertNotNull(bucket.getKeyAsText());
            assertNotNull(bucket.getKey());
            assertTrue(bucket.getDocCount() > 0);

            if (bucket instanceof Range.Bucket) {
                Range.Bucket rangeBucket = (Range.Bucket) bucket;
                assertNotNull(rangeBucket.getFrom());
                assertNotNull(rangeBucket.getTo());
            }
        }
    }

    private void validateRangeBucket(GeoPoint topLeft) {
        assertNotNull(topLeft);
        assertTrue(topLeft.getLat() != 0);
        assertTrue(topLeft.getLon() != 0);
    }


    private IndexResponse index(IndexRequest request) throws ExecutionException, InterruptedException {
        IndexResponse indexResponse = this.client.index(request).get();
        assertTrue(indexResponse.isCreated());
        return indexResponse;
    }

}