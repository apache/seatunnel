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

package org.apache.seatunnel.connectors.elasticsearch.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.elasticsearch.config.SinkConfig;
import org.apache.seatunnel.connectors.elasticsearch.constant.BulkConfig;
import org.apache.seatunnel.connectors.elasticsearch.constant.ElasticsearchVersion;
import org.apache.seatunnel.connectors.elasticsearch.dto.BulkResponse;
import org.apache.seatunnel.connectors.elasticsearch.dto.IndexInfo;
import org.apache.seatunnel.connectors.elasticsearch.exception.BulkElasticsearchException;
import org.apache.seatunnel.connectors.elasticsearch.serialize.ElasticsearchRowSerializer;
import org.apache.seatunnel.connectors.elasticsearch.serialize.SeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.elasticsearch.state.ElasticsearchCommitInfo;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * ElasticsearchSinkWriter is a sink writer that will write {@link SeaTunnelRow} to Elasticsearch.
 */
@Slf4j
public class ElasticsearchSinkWriter<ElasticsearchSinkStateT> implements SinkWriter<SeaTunnelRow, ElasticsearchCommitInfo, ElasticsearchSinkStateT> {

    private final SinkWriter.Context context;

    private final SeaTunnelRowSerializer seaTunnelRowSerializer;
    private final List<String> requestEsList;
    private EsRestClient esRestClient;

    public ElasticsearchSinkWriter(
            SinkWriter.Context context,
            SeaTunnelRowType seaTunnelRowType,
            Config pluginConfig,
            List<ElasticsearchSinkStateT> elasticsearchStates) {
        this.context = context;

        IndexInfo indexInfo = new IndexInfo(pluginConfig);
        initRestClient(pluginConfig);
        ElasticsearchVersion elasticsearchVersion = ElasticsearchVersion.get(EsRestClient.getClusterVersion());
        this.seaTunnelRowSerializer = new ElasticsearchRowSerializer(elasticsearchVersion, indexInfo, seaTunnelRowType);

        this.requestEsList = new ArrayList<>(BulkConfig.MAX_BATCH_SIZE);
    }

    private void initRestClient(org.apache.seatunnel.shade.com.typesafe.config.Config pluginConfig) {
        List<String> hosts = pluginConfig.getStringList(SinkConfig.HOSTS);
        String username = null;
        String password = null;
        if (pluginConfig.hasPath(SinkConfig.USERNAME)) {
            username = pluginConfig.getString(SinkConfig.USERNAME);
            if (pluginConfig.hasPath(SinkConfig.PASSWORD)) {
                password = pluginConfig.getString(SinkConfig.PASSWORD);
            }
        }
        esRestClient = EsRestClient.getInstance(hosts, username, password);
    }

    @Override
    public void write(SeaTunnelRow element) {
        String indexRequestRow = seaTunnelRowSerializer.serializeRow(element);
        requestEsList.add(indexRequestRow);
        if (requestEsList.size() >= BulkConfig.MAX_BATCH_SIZE) {
            bulkEsWithRetry(this.esRestClient, this.requestEsList, BulkConfig.MAX_RETRY_SIZE);
            requestEsList.clear();
        }
    }

    @Override
    public Optional<ElasticsearchCommitInfo> prepareCommit() {
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {
    }

    public void bulkEsWithRetry(EsRestClient esRestClient, List<String> requestEsList, int maxRetry) {
        for (int tryCnt = 1; tryCnt <= maxRetry; tryCnt++) {
            if (requestEsList.size() > 0) {
                String requestBody = String.join("\n", requestEsList) + "\n";
                try {
                    BulkResponse bulkResponse = esRestClient.bulk(requestBody);
                    if (!bulkResponse.isErrors()) {
                        break;
                    }
                } catch (Exception ex) {
                    if (tryCnt == maxRetry) {
                        throw new BulkElasticsearchException("bulk es error,try count=%d", ex);
                    }
                    log.warn(String.format("bulk es error,try count=%d", tryCnt), ex);
                }

            }
        }
    }

    @Override
    public void close() throws IOException {
        bulkEsWithRetry(this.esRestClient, this.requestEsList, BulkConfig.MAX_RETRY_SIZE);
        esRestClient.close();
    }
}
