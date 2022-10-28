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

import static org.apache.seatunnel.flink.elasticsearch.config.Config.DEFAULT_INDEX;
import static org.apache.seatunnel.flink.elasticsearch.config.Config.DEFAULT_INDEX_TIME_FORMAT;
import static org.apache.seatunnel.flink.elasticsearch.config.Config.HOSTS;
import static org.apache.seatunnel.flink.elasticsearch.config.Config.INDEX;
import static org.apache.seatunnel.flink.elasticsearch.config.Config.INDEX_TIME_FORMAT;
import static org.apache.seatunnel.flink.elasticsearch.config.Config.PARALLELISM;

import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.utils.StringTemplate;
import org.apache.seatunnel.flink.BaseFlinkSink;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSink;
import org.apache.seatunnel.flink.stream.FlinkStreamSink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import com.google.auto.service.AutoService;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@AutoService(BaseFlinkSink.class)
public class Elasticsearch implements FlinkStreamSink, FlinkBatchSink {

    private static final long serialVersionUID = 8445868321245456793L;
    private static final int DEFAULT_CONFIG_SIZE = 3;

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
        return CheckConfigUtil.checkAllExists(config, HOSTS);
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        Map<String, String> configMap = new HashMap<>(DEFAULT_CONFIG_SIZE);
        configMap.put(INDEX, DEFAULT_INDEX);
        configMap.put(INDEX_TIME_FORMAT, DEFAULT_INDEX_TIME_FORMAT);
        Config defaultConfig = ConfigFactory.parseMap(configMap);
        config = config.withFallback(defaultConfig);
    }

    @Override
    public String getPluginName() {
        return "ElasticSearch";
    }

    @Override
    public void outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {

        List<HttpHost> httpHosts = new ArrayList<>();
        List<String> hosts = config.getStringList(HOSTS);
        for (String host : hosts) {
            httpHosts.add(new HttpHost(host.split(":")[0], Integer.parseInt(host.split(":")[1]), "http"));
        }

        RowTypeInfo rowTypeInfo = (RowTypeInfo) dataStream.getType();
        indexName = StringTemplate.substitute(config.getString(INDEX), config.getString(INDEX_TIME_FORMAT));
        ElasticsearchSink.Builder<Row> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts, (ElasticsearchSinkFunction<Row>) (element, ctx, indexer) ->
                indexer.add(createIndexRequest(rowTypeInfo.getFieldNames(), element))
        );

        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);

        // finally, build and add the sink to the job's pipeline
        if (config.hasPath(PARALLELISM)) {
            int parallelism = config.getInt(PARALLELISM);
            dataStream.addSink(esSinkBuilder.build()).setParallelism(parallelism);
        } else {
            dataStream.addSink(esSinkBuilder.build());
        }
    }

    @Override
    public void outputBatch(FlinkEnvironment env, DataSet<Row> dataSet) {

        RowTypeInfo rowTypeInfo = (RowTypeInfo) dataSet.getType();
        indexName = StringTemplate.substitute(config.getString(INDEX), config.getString(INDEX_TIME_FORMAT));
        DataSink<Row> dataSink = dataSet.output(new ElasticsearchOutputFormat<>(config,
                (ElasticsearchSinkFunction<Row>) (element, ctx, indexer) ->
                        indexer.add(createIndexRequest(rowTypeInfo.getFieldNames(), element))));

        if (config.hasPath(PARALLELISM)) {
            int parallelism = config.getInt(PARALLELISM);
            dataSink.setParallelism(parallelism);
        }

    }

    private IndexRequest createIndexRequest(String[] fieldNames, Row element) {
        int elementLen = element.getArity();
        Map<String, Object> json = new HashMap<>(elementLen);
        for (int i = 0; i < elementLen; i++) {
            json.put(fieldNames[i], element.getField(i));
        }
        return Requests.indexRequest()
                .index(indexName)
                .source(json);
    }
}
