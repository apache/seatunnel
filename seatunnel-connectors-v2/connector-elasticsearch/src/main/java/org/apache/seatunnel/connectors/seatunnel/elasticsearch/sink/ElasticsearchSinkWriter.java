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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.common.utils.RetryUtils.RetryMaterial;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.BulkResponse;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.IndexInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorException;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.ElasticsearchRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.SeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.state.ElasticsearchCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.state.ElasticsearchSinkState;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ElasticsearchSinkWriter is a sink writer that will write {@link SeaTunnelRow} to Elasticsearch.
 */
@Slf4j
public class ElasticsearchSinkWriter
        implements SinkWriter<SeaTunnelRow, ElasticsearchCommitInfo, ElasticsearchSinkState> {

    private final SinkWriter.Context context;

    private final int maxBatchSize;

    private final int batchIntervalMs;

    private final SeaTunnelRowSerializer seaTunnelRowSerializer;
    private final List<String> requestEsList;
    private EsRestClient esRestClient;
    private RetryMaterial retryMaterial;
    private static final long DEFAULT_SLEEP_TIME_MS = 200L;

    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient boolean isClose;

    public ElasticsearchSinkWriter(
            SinkWriter.Context context,
            SeaTunnelRowType seaTunnelRowType,
            Config pluginConfig,
            int maxBatchSize,
            int maxRetryCount,
            int batchIntervalMs,
            List<ElasticsearchSinkState> elasticsearchStates) {
        this.context = context;
        this.maxBatchSize = maxBatchSize;
        this.batchIntervalMs = batchIntervalMs;

        IndexInfo indexInfo = new IndexInfo(pluginConfig);
        esRestClient = EsRestClient.createInstance(pluginConfig);
        this.seaTunnelRowSerializer =
                new ElasticsearchRowSerializer(
                        esRestClient.getClusterInfo(), indexInfo, seaTunnelRowType);

        this.requestEsList = new ArrayList<>(maxBatchSize);
        this.retryMaterial =
                new RetryMaterial(maxRetryCount, true, exception -> true, DEFAULT_SLEEP_TIME_MS);
        // Initialize the interval flush
        if(this.batchIntervalMs>0){
            open();
            log.info("The initial scheduling is complete batch_interval_ms :"+batchIntervalMs);
        }
    }

    @Override
    public void write(SeaTunnelRow element) {
        if (RowKind.UPDATE_BEFORE.equals(element.getRowKind())) {
            return;
        }

        String indexRequestRow = seaTunnelRowSerializer.serializeRow(element);
        requestEsList.add(indexRequestRow);
        if (requestEsList.size() >= maxBatchSize) {
            bulkEsWithRetry(this.esRestClient, this.requestEsList);
        }
    }

    public void open() {
        this.scheduler =
                Executors.newScheduledThreadPool(
                        1,
                        runnable -> {
                            AtomicInteger cnt = new AtomicInteger(0);
                            Thread thread = new Thread(runnable);
                            thread.setDaemon(true);
                            thread.setName(
                                    "sink-elasticsearch-interval" + "-" + cnt.incrementAndGet());
                            return thread;
                        });
        this.scheduledFuture =
                this.scheduler.scheduleWithFixedDelay(
                        () -> {
                            synchronized (ElasticsearchSinkWriter.this) {
                                if (requestEsList.size() > 0 && !isClose) {
                                    bulkEsWithRetry(this.esRestClient, this.requestEsList);
                                }
                            }
                        },
                        this.batchIntervalMs,
                        this.batchIntervalMs,
                        TimeUnit.MILLISECONDS);
    }

    @Override
    public Optional<ElasticsearchCommitInfo> prepareCommit() {
        bulkEsWithRetry(this.esRestClient, this.requestEsList);
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    public synchronized void bulkEsWithRetry(
            EsRestClient esRestClient, List<String> requestEsList) {
        try {
            RetryUtils.retryWithException(
                    () -> {
                        if (requestEsList.size() > 0) {
                            String requestBody = String.join("\n", requestEsList) + "\n";
                            BulkResponse bulkResponse = esRestClient.bulk(requestBody);
                            if (bulkResponse.isErrors()) {
                                throw new ElasticsearchConnectorException(
                                        ElasticsearchConnectorErrorCode.BULK_RESPONSE_ERROR,
                                        "bulk es error: " + bulkResponse.getResponse());
                            }
                            log.info(
                                    "bulk es successfully written to the rowNum: "
                                            + requestEsList.size());
                            return bulkResponse;
                        }
                        return null;
                    },
                    retryMaterial);
            requestEsList.clear();
        } catch (Exception e) {
            throw new ElasticsearchConnectorException(
                    CommonErrorCode.SQL_OPERATION_FAILED,
                    "ElasticSearch execute batch statement error",
                    e);
        }
    }

    @Override
    public void close() throws IOException {
        bulkEsWithRetry(this.esRestClient, this.requestEsList);
        this.isClose = true;
        if (esRestClient != null) {
            esRestClient.close();
        }
        if (this.scheduler != null) {
            this.scheduler.shutdown();
        }
    }
}
