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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.common.utils.StringTemplate;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSink;
import org.apache.seatunnel.flink.stream.FlinkStreamSink;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.types.Row;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.http.HttpHost;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Elasticsearch implements FlinkStreamSink<Row, Row>, FlinkBatchSink<Row, Row> {

    private Config config;
    private String indexName;

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        if (config.hasPath("hosts")) {
            return new CheckResult(true, "");
        }
        return new CheckResult(false, "please specify [hosts] as a non-empty string list");
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        Config defaultConfig = ConfigFactory.parseMap(new HashMap<String, String>(2) {
            {
                put("index", "seatunnel");
                put("index_type", "log");
                put("index_time_format", "yyyy.MM.dd");
            }
        });
        config = config.withFallback(defaultConfig);
    }

    @Override
    public DataStreamSink<Row> outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {

        List<HttpHost> httpHosts = new ArrayList<>();
        List<String> hosts = config.getStringList("hosts");
        for (String host : hosts) {
            httpHosts.add(new HttpHost(host.split(":")[0], Integer.parseInt(host.split(":")[1]), "http"));
        }

        RowTypeInfo rowTypeInfo = (RowTypeInfo) dataStream.getType();
        String[] fieldNames = rowTypeInfo.getFieldNames();
        indexName = StringTemplate.substitute(config.getString("index"), config.getString("index_time_format"));
        ElasticsearchSink.Builder<Row> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Row>() {
                    public IndexRequest createIndexRequest(Row element) {
                        Map<String, Object> json = new HashMap<>(100);
                        int elementLen = element.getArity();
                        for (int i = 0; i < elementLen; i++) {
                            json.put(fieldNames[i], element.getField(i));
                        }

                        return Requests.indexRequest()
                                .index(indexName)
                                .type(config.getString("index_type"))
                                .source(json);
                    }

                    @Override
                    public void process(Row element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);

        // finally, build and add the sink to the job's pipeline
        return dataStream.addSink(esSinkBuilder.build());
    }

    @Override
    public DataSink<Row> outputBatch(FlinkEnvironment env, DataSet<Row> dataSet) {

        RowTypeInfo rowTypeInfo = (RowTypeInfo) dataSet.getType();
        String[] fieldNames = rowTypeInfo.getFieldNames();
        indexName = StringTemplate.substitute(config.getString("index"), config.getString("index_time_format"));
        return dataSet.output(new ElasticsearchOutputFormat<>(config, new ElasticsearchSinkFunction<Row>() {
            @Override
            public void process(Row element, RuntimeContext ctx, RequestIndexer indexer) {
                indexer.add(createIndexRequest(element));
            }

            private IndexRequest createIndexRequest(Row element) {
                Map<String, Object> json = new HashMap<>(100);
                int elementLen = element.getArity();
                for (int i = 0; i < elementLen; i++) {
                    json.put(fieldNames[i], element.getField(i));
                }

                return Requests.indexRequest()
                        .index(indexName)
                        .type(config.getString("index_type"))
                        .source(json);
            }
        }));
    }
}
