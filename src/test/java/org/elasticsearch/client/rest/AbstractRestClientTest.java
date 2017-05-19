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

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Brandon Kearby
 *         September 26, 2016
 */
public abstract class AbstractRestClientTest {

    protected IndicesAdminClient indicesAdminClient;
    protected ClusterAdminClient clusterAdminClient;
    protected String index;
    protected String type = "stats";
    protected Client client;

    public static final String POSTS_INDEX = "posts";
    public static final String POST_TYPE = "post";
    public static final String COMMENT_TYPE = "comment";
    public static final String STATS_TYPE = "stats";

    enum Color {
        red,
        green,
        blue,
        orange,
        black,
        white,
        purple,
        brown,
        silver,
        gold
    }

    enum Genre {
        comedy,
        horror,
        drama,
        action,
        documentary
    }

    private static final boolean USE_REST = true;

    @Before
    public void setUp() {
        if (USE_REST) {
            client = createRestClient();
        } else {
            client = createTransportClient();
        }

        this.indicesAdminClient = client.admin().indices();
        this.clusterAdminClient = client.admin().cluster();
        this.index = createIndex();
    }

    protected TransportClient createTransportClient() {
        TransportClient transportClient = new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch-1.4.1"));
        transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
        return transportClient;
    }

    protected RestClient createRestClient() {
        return RestClient.builder("localhost")
                .setMaxRetryTimeout(new TimeValue(60, TimeUnit.SECONDS))
                .setMaxResponseSize(new ByteSizeValue(1, ByteSizeUnit.GB))
                .setSocketTimeout(new TimeValue(60, TimeUnit.SECONDS))
                .build();
    }

    protected List<IndexResponse> indexDocument(int numberOfDocs) throws InterruptedException, ExecutionException {
        List<IndexResponse> responses = Lists.newArrayList();
        for (int i = 0; i < numberOfDocs; i++) {
            responses.add(indexDocument());
        }
        return responses;
    }

    protected IndexResponse indexDocument() throws InterruptedException, ExecutionException {
        IndexRequest request = newIndexRequest();
        IndexResponse indexResponse = this.client.index(request).get();
        assertTrue(indexResponse.isCreated());

        return indexResponse;
    }

    protected void indexDocument(Client... clients) throws InterruptedException, ExecutionException {
        IndexRequest request = newIndexRequest();
        for (Client client : clients) {
            IndexResponse indexResponse = client.index(request).get();
            assertTrue(indexResponse.isCreated());
        }
    }

    protected IndexRequest newPost() {
        IndexRequest request = newCommentOrPost(POST_TYPE);
        return request;
    }

    protected IndexRequest newComment(String postId) {
        assert Strings.isNotEmpty(postId);
        IndexRequest indexRequest = newCommentOrPost(COMMENT_TYPE);
        indexRequest.parent(postId);
        return indexRequest;
    }


    protected IndexRequest newCommentOrPost(String type) {
        String id = UUID.randomUUID().toString();
        IndexRequest request = new IndexRequest(POSTS_INDEX, type, id);
        Map<String, Object> source = Maps.newHashMap();
        source.put("title", randomName() + " " + randomName());
        source.put("description", randomName() + " " + randomName() + " " + randomName() + " " + randomName());
        source.put("dateCreated", new DateTime());
        request.source(source);
        return request;
    }

    protected IndexRequest newIndexRequest() {
        String id = UUID.randomUUID().toString();
        IndexRequest request = new IndexRequest(index, STATS_TYPE, id);
        Map<String, Object> source = Maps.newHashMap();
        source.put("datePretty", new DateTime().minusDays(Math.abs(new Random().nextInt() % 500)));
        source.put("sentiment", Math.abs(new Random().nextInt() % 10));
        source.put("color", randomColor().name());
        source.put("genre", randomGenre().name());
        source.put("amount", Math.abs(new Random().nextDouble()));
        Map<String, Object> reach = Maps.newHashMap();
        reach.put("type", "point");
        reach.put("coordinates", Arrays.asList(-1 * Math.abs(new Random().nextDouble() % 90), Math.abs(new Random().nextDouble() % 180)));
        source.put("reach", reach);

        Map<String, Object> latLon = Maps.newLinkedHashMap();
        latLon.put("lat", -1 * Math.abs(new Random().nextInt() % 90));
        latLon.put("lon", Math.abs(new Random().nextInt() % 180));
        source.put("currentLocation", latLon);
        source.put("ipAddress", Joiner.on('.').join(Math.abs(new Random().nextInt() % 255), Math.abs(new Random().nextInt() % 255), Math.abs(new Random().nextInt() % 255), Math.abs(new Random().nextInt() % 255)));

        Map<String, Object> author = Maps.newHashMap();
        author.put("name", randomName());
        List<Map<String, Object>> books = Lists.newArrayList();
        for (int i = Math.abs(new Random().nextInt()) % 15; i >= 0; i--) {
            Map<String, Object> book = Maps.newHashMap();
            book.put("title", randomName());
            book.put("genre", randomGenre());
            book.put("price", Math.abs(new Random().nextFloat()));
            books.add(book);
        }
        author.put("books", books);
        source.put("author", author);

        request.source(source);
        request.refresh(true);
        return request;
    }

    protected List<String> names;

    protected String randomName() {
        if (names == null) {
            names = Lists.newArrayList();
            InputStream in = this.getClass().getResourceAsStream("/config/names.txt");
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, Charsets.UTF_8));
            String name;
            try {
                while ((name = reader.readLine()) != null) {
                    names.add(name);
                }
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            } finally {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
        }
        assert names.size() > 0;
        return names.get(Math.abs(new Random().nextInt()) % names.size());

    }

    protected RestClientTest.Color randomColor() {
        return RestClientTest.Color.values()[Math.abs(new Random().nextInt()) % RestClientTest.Color.values().length];
    }

    protected RestClientTest.Genre randomGenre() {
        return RestClientTest.Genre.values()[Math.abs(new Random().nextInt()) % RestClientTest.Genre.values().length];
    }


    @After
    public void tearDown() {
        deleteIndex(index);
    }

    protected String loadTestIndex() {
        return loadTemplate("/org/elasticsearch/client/rest/test-index.json");
    }

    protected String loadTestIndexTemplate() {
        return loadTemplate("/org/elasticsearch/client/rest/test-index-template.json");
    }

    protected String loadTemplate(String name) {
        InputStream in = this.getClass().getResourceAsStream(name);
        return Strings.valueOf(in);
    }

    protected String loadTestIndexPutMapping() {
        return loadTemplate("/org/elasticsearch/client/rest/test-index-put-mapping.json");
    }

    protected void deleteIndex(String index) {
        DeleteIndexResponse response = indicesAdminClient.prepareDelete(index).get();
        assertAcknowledged(response);
    }

    protected void deleteIndex(String index, IndicesAdminClient client) {
        DeleteIndexResponse response = client.prepareDelete(index).get();
        assertAcknowledged(response);
    }

    protected String createIndex() {
        String index = UUID.randomUUID().toString();
        CreateIndexResponse response = indicesAdminClient.prepareCreate(index)
                .setSource(loadTestIndex()).get();
        assertAcknowledged(response);
        return index;
    }

    protected void assertAcknowledged(AcknowledgedResponse response) {
        assertTrue(response.isAcknowledged());
    }


    protected void assertBroadcastOperationResponse(BroadcastOperationResponse response) {
        assertTrue(response.getSuccessfulShards() > 0);
        assertEquals(0, response.getFailedShards());
    }


    public void refresh() {
        this.indicesAdminClient.prepareRefresh(index).get();
    }
}
