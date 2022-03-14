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

package org.apache.seatunnel.flink.sink;

import static org.apache.seatunnel.Config.HOSTS;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.List;

public class ElasticsearchOutputFormat<T> extends RichOutputFormat<T> {

    private static final long serialVersionUID = 2048590860723433896L;
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchOutputFormat.class);

    private Config config;

    private static final String PREFIX = "es.";

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
        Settings.Builder settings = Settings.builder();

        config.entrySet().forEach(entry -> {
            String key = entry.getKey();
            Object value = entry.getValue().unwrapped();
            if (key.startsWith(PREFIX)) {
                settings.put(key.substring(PREFIX.length()), value.toString());
            }
        });

        TransportClient transportClient = new PreBuiltTransportClient(settings.build());

        for (String host : hosts) {
            try {
                transportClient.addTransportAddresses(new TransportAddress(InetAddress.getByName(host.split(":")[0]), Integer.parseInt(host.split(":")[1])));
            } catch (Exception e) {
                LOGGER.warn("Host '{}' parse failed.", host, e);
            }
        }

        BulkProcessor.Builder bulkProcessorBuilder = BulkProcessor.builder(transportClient, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
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
