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

package org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink;

import org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.entity.StreamLoadEntity;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.entity.StreamLoadResponse;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.HttpHelper;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.HttpRequestBuilder;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_LABEL_ABORTED;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_LABEL_COMMITTED;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_LABEL_EXISTED;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_LABEL_PREPARE;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_LABEL_PREPARED;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_LABEL_UNKNOWN;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_LABEL_VISIBLE;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_OK;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_SUCCESS;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_TRANSACTION_PUBLISH_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorErrorCode.FLUSH_DATA_FAILED;

@Slf4j
public class TransactionStreamLoader implements StreamLoader {

    private HttpClientBuilder clientBuilder;
    private StreamLoadManager manager;
    private SinkConfig sinkConfig;
    private StreamLoadHelper streamLoadHelper;
    private ExecutorService executorService;
    private LabelGenerator labelGenerator;
    private HttpHelper httpHelper;

    public TransactionStreamLoader(SinkConfig sinkConfig, StreamLoadManager manager) {
        this(sinkConfig);
        this.manager = manager;
    }

    public TransactionStreamLoader(SinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
        this.streamLoadHelper = new StreamLoadHelper(sinkConfig);
        this.labelGenerator = new LabelGenerator(sinkConfig);
        this.httpHelper = new HttpHelper();
        this.clientBuilder =
                HttpClients.custom()
                        .setRedirectStrategy(
                                new DefaultRedirectStrategy() {
                                    @Override
                                    protected boolean isRedirectable(String method) {
                                        return true;
                                    }
                                });

        ThreadPoolExecutor threadPoolExecutor =
                new ThreadPoolExecutor(
                        10,
                        10,
                        10,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(),
                        r -> {
                            Thread thread =
                                    new Thread(
                                            null, r, "I/O client dispatch - " + UUID.randomUUID());
                            thread.setDaemon(true);
                            thread.setUncaughtExceptionHandler(
                                    (t, e) -> {
                                        log.error(
                                                "Stream loader "
                                                        + Thread.currentThread().getName()
                                                        + " error",
                                                e);
                                        manager.callback(e);
                                    });
                            return thread;
                        });
        threadPoolExecutor.allowCoreThreadTimeOut(true);
        this.executorService = threadPoolExecutor;
    }

    @Override
    public void close() {}

    @Override
    public Future<StreamLoadResponse> send(TableRegion region) {
        return executorService.submit(() -> doSend(region));
    }

    public StreamLoadResponse doSend(TableRegion region) {
        try {
            String sendUrl = streamLoadHelper.getSendUrl(sinkConfig.getNodeUrls());
            String label = region.getLabel();
            HttpRequestBuilder httpBuilder =
                    new HttpRequestBuilder(sinkConfig)
                            .setUrl(sendUrl)
                            .streamLoad(region.getLabel())
                            .setEntity(
                                    new StreamLoadEntity(
                                            region,
                                            region.getDataFormat(),
                                            region.getEntityMeta()));

            log.info("stream loading, label : {}, request : {}", label, httpBuilder.build());

            try {
                long startNanoTime = System.nanoTime();
                String responseBody = httpHelper.doHttpExecute(clientBuilder, httpBuilder.build());
                log.info(
                        "stream load completed, label : {}, database : {}, table : {}, body : {}",
                        label,
                        region.getDatabase(),
                        region.getTable(),
                        responseBody);

                StreamLoadResponse streamLoadResponse = new StreamLoadResponse();
                StreamLoadResponse.StreamLoadResponseBody streamLoadBody =
                        JsonUtils.parseObject(
                                responseBody, StreamLoadResponse.StreamLoadResponseBody.class);
                streamLoadResponse.setBody(streamLoadBody);
                String status = streamLoadBody.getStatus();

                checkStatusNull("stream load", status, label, responseBody);
                if (RESULT_SUCCESS.equals(status)
                        || RESULT_OK.equals(status)
                        || RESULT_TRANSACTION_PUBLISH_TIMEOUT.equals(status)) {
                    streamLoadResponse.setCostNanoTime(System.nanoTime() - startNanoTime);
                    region.complete(streamLoadResponse);
                } else if (RESULT_LABEL_EXISTED.equals(status)) {
                    String labelState =
                            streamLoadHelper.getLabelState(
                                    region.getDatabase(),
                                    label,
                                    Collections.singleton(RESULT_LABEL_PREPARE));
                    if (RESULT_LABEL_COMMITTED.equals(labelState)
                            || RESULT_LABEL_VISIBLE.equals(labelState)) {
                        streamLoadResponse.setCostNanoTime(System.nanoTime() - startNanoTime);
                        region.complete(streamLoadResponse);
                    } else {
                        String errorMsg =
                                String.format(
                                        "stream load failed because label existed, "
                                                + "db: %s, table: %s, label: %s, label state: %s",
                                        region.getDatabase(), region.getTable(), label, labelState);
                        throw new StarRocksConnectorException(FLUSH_DATA_FAILED, errorMsg);
                    }
                } else {
                    String errorLog = streamLoadHelper.getErrorLog(streamLoadBody.getErrorURL());
                    String errorMsg =
                            String.format(
                                    "stream load failed because of error, db: %s, table: %s, label: %s, "
                                            + "\nresponseBody: %s\nerrorLog: %s",
                                    region.getDatabase(),
                                    region.getTable(),
                                    label,
                                    responseBody,
                                    errorLog);
                    throw new StarRocksConnectorException(FLUSH_DATA_FAILED, errorMsg);
                }
                return streamLoadResponse;
            } catch (StarRocksConnectorException e) {
                throw e;
            } catch (Exception e) {
                String errorMsg =
                        String.format(
                                "stream load failed because of unknown exception, db: %s, table: %s, "
                                        + "label: %s",
                                region.getDatabase(), region.getTable(), label);
                throw new StarRocksConnectorException(FLUSH_DATA_FAILED, errorMsg, e);
            }
        } catch (Exception e) {
            log.error(
                    "exception happens when sending data, thread: {}",
                    Thread.currentThread().getName(),
                    e);
            region.callback(e);
        }
        return null;
    }

    @Override
    public boolean begin(String label) {
        String beginUrl = streamLoadHelper.getBeginUrl(sinkConfig.getNodeUrls());
        HttpRequestBuilder httpBuilder =
                new HttpRequestBuilder(sinkConfig).setUrl(beginUrl).begin(label);

        log.info("transaction begin, request : {}", httpBuilder.build());

        try {
            String responseBody = httpHelper.doHttpExecute(clientBuilder, httpBuilder.build());
            log.info(
                    "transaction began, db: {}, table: {}, label: {}, body : {}",
                    sinkConfig.getDatabase(),
                    sinkConfig.getTable(),
                    label,
                    responseBody);

            ObjectNode bodyJson = JsonUtils.parseObject(responseBody);
            String status = bodyJson.get("Status").asText();

            if (status == null) {
                String errMsg =
                        String.format(
                                "Can't find 'Status' in the response of transaction begin request. "
                                        + "Transaction load is supported since StarRocks 2.4, and please make sure your "
                                        + "StarRocks version support transaction load first. db: %s, table: %s, label: %s, response: %s",
                                sinkConfig.getDatabase(),
                                sinkConfig.getTable(),
                                label,
                                responseBody);
                log.error(errMsg);
                throw new StarRocksConnectorException(FLUSH_DATA_FAILED, errMsg);
            }

            switch (status) {
                case RESULT_OK:
                    return true;
                case RESULT_LABEL_EXISTED:
                    return false;
                default:
                    log.error(
                            "transaction begin failed, db : {}, label : {}",
                            sinkConfig.getDatabase(),
                            label);
                    return false;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean prepare(String label) {
        String prepareUrl = streamLoadHelper.getPrepareUrl(sinkConfig.getNodeUrls());
        HttpRequestBuilder httpBuilder =
                new HttpRequestBuilder(sinkConfig).setUrl(prepareUrl).prepare(label);

        log.info("Transaction prepare, label : {}, request : {}", label, httpBuilder.build());

        try {
            String responseBody = httpHelper.doHttpExecute(clientBuilder, httpBuilder.build());

            log.info("Transaction prepared, label : {}, body : {}", label, responseBody);

            StreamLoadResponse streamLoadResponse = new StreamLoadResponse();
            StreamLoadResponse.StreamLoadResponseBody streamLoadBody =
                    JsonUtils.parseObject(
                            responseBody, StreamLoadResponse.StreamLoadResponseBody.class);
            streamLoadResponse.setBody(streamLoadBody);
            String status = streamLoadBody.getStatus();

            checkStatusNull("prepare transaction", status, label, responseBody);

            switch (status) {
                case RESULT_OK:
                    manager.callback(streamLoadResponse);
                    return true;
                case StreamLoadHelper.RESULT_TRANSACTION_NOT_EXISTED:
                    {
                        // currently this could happen after timeout which is specified in http
                        // header,
                        // but as a protection we check the state again
                        String labelState =
                                streamLoadHelper.getLabelState(
                                        sinkConfig.getDatabase(),
                                        label,
                                        Collections.singleton(
                                                StreamLoadHelper.RESULT_LABEL_PREPARE));
                        if (!StreamLoadHelper.RESULT_LABEL_PREPARED.equals(labelState)) {
                            String errMsg =
                                    String.format(
                                            "Transaction prepare failed because of unexpected state, "
                                                    + "label: %s, state: %s",
                                            label, labelState);
                            log.error(errMsg);
                            throw new StarRocksConnectorException(FLUSH_DATA_FAILED, errMsg);
                        } else {
                            return true;
                        }
                    }
            }
            String errorLog = streamLoadHelper.getErrorLog(streamLoadBody.getErrorURL());
            log.error(
                    "Transaction prepare failed, db: {}, table: {}, label: {}, \nresponseBody: {}\nerrorLog: {}",
                    sinkConfig.getDatabase(),
                    sinkConfig.getTable(),
                    label,
                    responseBody,
                    errorLog);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    @Override
    public boolean commit(String label) {
        String commitUrl = streamLoadHelper.getCommitUrl(sinkConfig.getNodeUrls());
        HttpRequestBuilder httpBuilder =
                new HttpRequestBuilder(sinkConfig).setUrl(commitUrl).commit(label);

        log.info("Transaction commit, label: {}, request : {}", label, httpBuilder.build());

        try {
            String responseBody = httpHelper.doHttpExecute(clientBuilder, httpBuilder.build());

            log.info("Transaction committed, label: {}, body : {}", label, responseBody);

            StreamLoadResponse streamLoadResponse = new StreamLoadResponse();
            StreamLoadResponse.StreamLoadResponseBody streamLoadBody =
                    JsonUtils.parseObject(
                            responseBody, StreamLoadResponse.StreamLoadResponseBody.class);
            streamLoadResponse.setBody(streamLoadBody);
            String status = streamLoadBody.getStatus();

            checkStatusNull("commit transaction", status, label, responseBody);
            if (RESULT_OK.equals(status)) {
                return true;
            }

            // there are many corner cases that can lead to non-ok status. some of them are
            // 1. TXN_NOT_EXISTS: transaction timeout and the label is cleanup up
            // 2. Failed: the error message can be "has no backend", The case is that FE leader
            // restarts, and after
            //    that commit the transaction repeatedly because flink job continues failover for
            //    some reason , but
            //    the transaction actually success, and this commit should be successful
            // To reduce the dependency for the returned status type, always check the label state
            String labelState =
                    streamLoadHelper.getLabelState(
                            sinkConfig.getDatabase(), label, Collections.emptySet());
            if (StreamLoadHelper.RESULT_LABEL_COMMITTED.equals(labelState)
                    || StreamLoadHelper.RESULT_LABEL_VISIBLE.equals(labelState)) {
                return true;
            }

            String errorLog = streamLoadHelper.getErrorLog(streamLoadBody.getErrorURL());
            log.error(
                    "transaction commit failed, db: {}, table: {}, label: {}, label state: {}, \nresponseBody: {}\nerrorLog: {}",
                    sinkConfig.getDatabase(),
                    sinkConfig.getTable(),
                    label,
                    labelState,
                    responseBody,
                    errorLog);

            String exceptionMsg =
                    String.format(
                            "transaction commit failed, db: %s, table: %s, label: %s, commit response status: %s,"
                                    + " label state: %s",
                            sinkConfig.getDatabase(),
                            sinkConfig.getTable(),
                            label,
                            status,
                            labelState);
            // transaction not exist often happens after transaction timeouts
            if (StreamLoadHelper.RESULT_TRANSACTION_NOT_EXISTED.equals(status)
                    || RESULT_LABEL_UNKNOWN.equals(labelState)) {
                exceptionMsg +=
                        ". commit response status with TXN_NOT_EXISTS or label state with UNKNOWN often happens when transaction"
                                + " timeouts, and please check StarRocks FE leader's log to confirm it. You can find the transaction id for the label"
                                + " in the FE log first, and search with the transaction id and the keyword 'expired'";
            }
            throw new StarRocksConnectorException(FLUSH_DATA_FAILED, exceptionMsg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void abortPreCommit(long chkID, int subTaskIndex) throws Exception {
        long startChkID = chkID;
        while (true) {
            try {
                String label = new LabelGenerator(sinkConfig).genLabel(startChkID, subTaskIndex);
                log.info("try to abort transaction for label {}", label);
                String state = streamLoadHelper.getLabelState(sinkConfig.getDatabase(), label);

                switch (state) {
                        // job is not finished and current label status is "prepare or prepared", so
                        // abort it.
                    case RESULT_LABEL_PREPARE:
                    case RESULT_LABEL_PREPARED:
                        log.info("abort unfinished transaction for exist label {}", label);
                        rollback(label);
                        break;
                        // label already exist and job is finished
                    case RESULT_LABEL_COMMITTED:
                    case RESULT_LABEL_VISIBLE:
                        throw new StarRocksConnectorException(
                                FLUSH_DATA_FAILED,
                                "Load status is "
                                        + RESULT_LABEL_EXISTED
                                        + " and load job finished, "
                                        + "change you label prefix or restore from latest savepoint!");
                    case RESULT_LABEL_UNKNOWN:
                    case RESULT_LABEL_ABORTED:
                    default:
                        return;
                }
                startChkID++;
            } catch (StarRocksConnectorException e) {
                log.warn("failed to stream load data", e);
                throw e;
            }
        }
    }

    @Override
    public boolean rollback(String label) {
        String rollbackUrl = streamLoadHelper.getRollbackUrl(sinkConfig.getNodeUrls());
        HttpRequestBuilder httpBuilder =
                new HttpRequestBuilder(sinkConfig).setUrl(rollbackUrl).rollback(label);
        log.info("transaction rollback, label : {}", label);

        try {
            String responseBody = httpHelper.doHttpExecute(clientBuilder, httpBuilder.build());

            log.info("transaction rollback, label: {}, body : {}", label, responseBody);

            ObjectNode bodyJson = JsonUtils.parseObject(responseBody);
            String status = bodyJson.get("Status").asText();

            checkStatusNull("rollback transaction", status, label, responseBody);
            if (RESULT_SUCCESS.equals(status) || RESULT_OK.equals(status)) {
                return true;
            }
            log.error(
                    "transaction rollback failed, db: {}, table: {}, label : {}",
                    sinkConfig.getDatabase(),
                    sinkConfig.getTable(),
                    label);
            return false;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkStatusNull(String method, String status, String label, String response) {
        if (StringUtils.isEmpty(status)) {
            String errMsg =
                    String.format(
                            "%s status is null. db: %s, table: %s, label: %s, response: %s",
                            method,
                            sinkConfig.getDatabase(),
                            sinkConfig.getTable(),
                            label,
                            response);
            log.error(errMsg);
            throw new StarRocksConnectorException(FLUSH_DATA_FAILED, errMsg);
        }
    }
}
