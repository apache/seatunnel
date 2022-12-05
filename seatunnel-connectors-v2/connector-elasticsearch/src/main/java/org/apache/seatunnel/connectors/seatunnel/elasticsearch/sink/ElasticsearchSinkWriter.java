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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.common.utils.RetryUtils.RetryMaterial;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.constant.ElasticsearchVersion;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.BulkResponse;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.IndexInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorException;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.ElasticsearchRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.SeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.state.ElasticsearchCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.state.ElasticsearchSinkState;

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
public class ElasticsearchSinkWriter implements SinkWriter<SeaTunnelRow, ElasticsearchCommitInfo, ElasticsearchSinkState> {

    private final SinkWriter.Context context;

    private final int maxBatchSize;

    private final SeaTunnelRowSerializer seaTunnelRowSerializer;
    private final List<String> requestEsList;
    private EsRestClient esRestClient;
    private RetryMaterial retryMaterial;
    private static final long DEFAULT_SLEEP_TIME_MS = 200L;

    public ElasticsearchSinkWriter(
        SinkWriter.Context context,
        SeaTunnelRowType seaTunnelRowType,
        Config pluginConfig,
        int maxBatchSize,
        int maxRetryCount,
        List<ElasticsearchSinkState> elasticsearchStates) {
        this.context = context;
        this.maxBatchSize = maxBatchSize;

        IndexInfo indexInfo = new IndexInfo(pluginConfig);
        esRestClient = EsRestClient.createInstance(pluginConfig);
        ElasticsearchVersion elasticsearchVersion = ElasticsearchVersion.get(esRestClient.getClusterVersion());
        this.seaTunnelRowSerializer = new ElasticsearchRowSerializer(elasticsearchVersion, indexInfo, seaTunnelRowType);

        this.requestEsList = new ArrayList<>(maxBatchSize);
        this.retryMaterial = new RetryMaterial(maxRetryCount, true,
            exception -> true, DEFAULT_SLEEP_TIME_MS);
    }

    @Override
    public void write(SeaTunnelRow element) {
        String indexRequestRow = seaTunnelRowSerializer.serializeRow(element);
        requestEsList.add(indexRequestRow);
        if (requestEsList.size() >= maxBatchSize) {
            bulkEsWithRetry(this.esRestClient, this.requestEsList);
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

    public void bulkEsWithRetry(EsRestClient esRestClient, List<String> requestEsList) {
        try {
            RetryUtils.retryWithException(() -> {
                if (requestEsList.size() > 0) {
                    String requestBody = String.join("\n", requestEsList) + "\n";
                    BulkResponse bulkResponse = esRestClient.bulk(requestBody);
                    if (bulkResponse.isErrors()) {
                        throw new ElasticsearchConnectorException(ElasticsearchConnectorErrorCode.BULK_RESPONSE_ERROR,
                            "bulk es error: " + bulkResponse.getResponse());
                    }
                    return bulkResponse;
                }
                return null;
            }, retryMaterial);
        } catch (Exception e) {
            throw new ElasticsearchConnectorException(CommonErrorCode.SQL_OPERATION_FAILED, "ElasticSearch execute batch statement error", e);
        }
    }

    @Override
    public void close() throws IOException {
        bulkEsWithRetry(this.esRestClient, this.requestEsList);
        esRestClient.close();
    }
}
