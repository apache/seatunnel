/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.flink.elasticsearch.sink;

import static org.apache.seatunnel.flink.elasticsearch.config.Config.HOSTS;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ElasticsearchOutputFormat<T> extends RichOutputFormat<T> {

    private static final long serialVersionUID = 2048590860723433896L;
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchOutputFormat.class);

    private final Config config;

    private final ElasticsearchSinkFunction<T> elasticsearchSinkFunction;

    private transient RequestIndexer requestIndexer;

    private transient BulkProcessor bulkProcessor;

    public ElasticsearchOutputFormat(Config userConfig, ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {
        this.config = userConfig;
        this.elasticsearchSinkFunction = elasticsearchSinkFunction;
    }

    @Override
    public void configure(Configuration configuration) {
        List<String> hosts = config.getStringList(HOSTS);
        HttpHost[] httpHosts = hosts.stream()
            .map(host -> HttpHost.create(host)).toArray(length -> new HttpHost[length]);
        RestClientBuilder builder = RestClient.builder(httpHosts);
        RestHighLevelClient client = new RestHighLevelClient(builder);

        BulkProcessor.Builder bulkProcessorBuilder = BulkProcessor.builder((request, listener) -> {
            client.bulkAsync(request, RequestOptions.DEFAULT, listener);
        }, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                LOGGER.error(failure.getMessage(), failure);
            }

        });

        // This makes flush() blocking
        //  bulkProcessorBuilder.setConcurrentRequests(0);

        bulkProcessor = bulkProcessorBuilder.build();
        requestIndexer = new RequestIndexer() {
            @Override
            public void add(DeleteRequest... deleteRequests) {

            }

            @Override
            public void add(IndexRequest... indexRequests) {
                for (IndexRequest indexRequest : indexRequests) {

                    bulkProcessor.add(indexRequest);
                }
            }

            @Override
            public void add(UpdateRequest... updateRequests) {

            }
        };
    }

    @Override
    public void open(int i, int i1) {

    }

    @Override
    public void writeRecord(T t) {
        elasticsearchSinkFunction.process(t, getRuntimeContext(), requestIndexer);
    }

    @Override
    public void close() {
        this.bulkProcessor.flush();
    }

}
