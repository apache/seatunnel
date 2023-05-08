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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.LabelGenerator;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.StreamLoadResponse;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.StreamLoadSnapshot;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.PATH_TRANSACTION_BEGIN;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.PATH_TRANSACTION_COMMIT;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.PATH_TRANSACTION_PRE_COMMIT;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.PATH_TRANSACTION_ROLLBACK;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.PATH_TRANSACTION_SEND;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_LABEL_COMMITTED;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_LABEL_EXISTED;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_LABEL_PREPARE;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_LABEL_VISIBLE;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_OK;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_SUCCESS;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_TRANSACTION_EXIST_FINISHED;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_TRANSACTION_EXIST_RUNNING;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_TRANSACTION_PUBLISH_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorErrorCode.FLUSH_DATA_FAILED;

@Slf4j
public class TransactionStreamLoader implements StreamLoader {

    private Header[] defaultTxnHeaders;

    private Header[] beginTxnHeader;
    private Header[] sendHeader;

    private HttpClientBuilder clientBuilder;

    private StreamLoadManager manager;

    private SinkConfig sinkConfig;
    private final StreamLoadHelper streamLoadHelper = new StreamLoadHelper();

    private ExecutorService executorService;
    private LabelGenerator labelGenerator;

    public TransactionStreamLoader(SinkConfig sinkConfig, StreamLoadManager manager) {
        this(sinkConfig);
        this.manager = manager;
        this.labelGenerator = new LabelGenerator(sinkConfig);
    }

    public TransactionStreamLoader(SinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
        initTxHeaders();
        clientBuilder =
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

    protected void initTxHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put(
                HttpHeaders.AUTHORIZATION,
                StreamLoadHelper.getBasicAuthHeader(
                        sinkConfig.getUsername(), sinkConfig.getPassword()));
        this.defaultTxnHeaders =
                headers.entrySet().stream()
                        .map(entry -> new BasicHeader(entry.getKey(), entry.getValue()))
                        .toArray(Header[]::new);

        Map<String, String> beginHeaders = new HashMap<>(headers);
        beginHeaders.put("timeout", "600");
        this.beginTxnHeader =
                beginHeaders.entrySet().stream()
                        .map(entry -> new BasicHeader(entry.getKey(), entry.getValue()))
                        .toArray(Header[]::new);

        Map<String, String> sendHeader = new HashMap<>(headers);
        sendHeader.put(HttpHeaders.EXPECT, "100-continue");
        if (null != sinkConfig.getStreamLoadProps()) {
            for (Map.Entry<String, Object> entry : sinkConfig.getStreamLoadProps().entrySet()) {
                sendHeader.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
        this.sendHeader =
                sendHeader.entrySet().stream()
                        .map(entry -> new BasicHeader(entry.getKey(), entry.getValue()))
                        .toArray(Header[]::new);
    }

    public void start(StreamLoadManager manager) {
        this.manager = manager;
    }

    @Override
    public void close() {}

    @Override
    public Future<StreamLoadResponse> send(TableRegion region) {
        return executorService.submit(() -> doSend(region));
    }

    public StreamLoadResponse doSend(TableRegion region) {
        try {
            String host = streamLoadHelper.getAvailableHost(sinkConfig.getNodeUrls());
            String sendUrl = getSendUrl(host);
            String label = region.getLabel();
            HttpPut httpPut = new HttpPut(sendUrl);
            httpPut.setHeaders(sendHeader);

            httpPut.setConfig(
                    RequestConfig.custom()
                            .setExpectContinueEnabled(true)
                            .setRedirectsEnabled(true)
                            .build());
            httpPut.setEntity(
                    new StreamLoadEntity(region, region.getDataFormat(), region.getEntityMeta()));
            httpPut.addHeader("label", label);
            httpPut.addHeader("db", region.getDatabase());
            httpPut.addHeader("table", region.getTable());

            httpPut.setConfig(
                    RequestConfig.custom()
                            .setExpectContinueEnabled(true)
                            .setRedirectsEnabled(true)
                            .build());

            log.info(
                    "Stream loading, label : {}, request : {}, header: {}",
                    label,
                    httpPut,
                    JsonUtils.toJsonString(sendHeader));
            try (CloseableHttpClient client = clientBuilder.build()) {
                long startNanoTime = System.nanoTime();
                String responseBody;
                try (CloseableHttpResponse response = client.execute(httpPut)) {
                    responseBody =
                            parseHttpResponse(
                                    "load",
                                    region.getDatabase(),
                                    region.getTable(),
                                    label,
                                    response);
                }

                log.info(
                        "Stream load completed, label : {}, database : {}, table : {}, body : {}",
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
                if (status == null) {
                    throw new StarRocksConnectorException(
                            FLUSH_DATA_FAILED,
                            String.format(
                                    "Stream load status is null. db: %s, table: %s, "
                                            + "label: %s, response body: %s",
                                    region.getDatabase(), region.getTable(), label, responseBody));
                }

                if (RESULT_SUCCESS.equals(status)
                        || RESULT_OK.equals(status)
                        || RESULT_TRANSACTION_PUBLISH_TIMEOUT.equals(status)) {
                    streamLoadResponse.setCostNanoTime(System.nanoTime() - startNanoTime);
                    region.complete(streamLoadResponse);
                } else if (RESULT_LABEL_EXISTED.equals(status)) {
                    String labelState =
                            getLabelState(
                                    host,
                                    region.getDatabase(),
                                    label,
                                    Collections.singleton(RESULT_LABEL_PREPARE));
                    if (RESULT_LABEL_COMMITTED.equals(labelState)
                            || RESULT_LABEL_VISIBLE.equals(labelState)) {
                        streamLoadResponse.setCostNanoTime(System.nanoTime() - startNanoTime);
                        region.complete(streamLoadResponse);
                    } else {
                        String errorMsage =
                                String.format(
                                        "Stream load failed because label existed, "
                                                + "db: %s, table: %s, label: %s, label state: %s",
                                        region.getDatabase(), region.getTable(), label, labelState);
                        throw new StarRocksConnectorException(FLUSH_DATA_FAILED, errorMsage);
                    }
                } else {
                    String errorLog = getErrorLog(streamLoadBody.getErrorURL());
                    String errorMsg =
                            String.format(
                                    "Stream load failed because of error, db: %s, table: %s, label: %s, "
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
                                "Stream load failed because of unknown exception, db: %s, table: %s, "
                                        + "label: %s",
                                region.getDatabase(), region.getTable(), label);
                throw new StarRocksConnectorException(FLUSH_DATA_FAILED, errorMsg, e);
            }
        } catch (Exception e) {
            log.error(
                    "Exception happens when sending data, thread: {}",
                    Thread.currentThread().getName(),
                    e);
            region.callback(e);
        }
        return null;
    }

    @Override
    public boolean prepare(StreamLoadSnapshot.Transaction transaction) {
        return false;
    }

    @Override
    public boolean commit(StreamLoadSnapshot.Transaction transaction) {
        return false;
    }

    @Override
    public boolean rollback(StreamLoadSnapshot.Transaction transaction) {
        return false;
    }

    @Override
    public boolean prepare(StreamLoadSnapshot snapshot) {
        return false;
    }

    @Override
    public boolean commit(StreamLoadSnapshot snapshot) {
        return false;
    }

    @Override
    public boolean begin(String label) {
        String host = streamLoadHelper.getAvailableHost(sinkConfig.getNodeUrls());
        String beginUrl = getBeginUrl(host);
        log.info("Transaction start, label : {}", label);

        HttpPost httpPost = new HttpPost(beginUrl);
        httpPost.setHeaders(beginTxnHeader);
        httpPost.addHeader("label", label);
        httpPost.addHeader("db", sinkConfig.getDatabase());
        httpPost.addHeader("table", sinkConfig.getTable());

        httpPost.setConfig(
                RequestConfig.custom()
                        .setExpectContinueEnabled(true)
                        .setRedirectsEnabled(true)
                        .build());

        String db = sinkConfig.getDatabase();
        String table = sinkConfig.getTable();
        log.info(
                "Transaction start, db: {}, table: {}, label: {}, request : {}",
                db,
                table,
                label,
                httpPost);

        try (CloseableHttpClient client = clientBuilder.build()) {
            String responseBody;
            try (CloseableHttpResponse response = client.execute(httpPost)) {
                responseBody =
                        parseHttpResponse(
                                "begin transaction",
                                sinkConfig.getDatabase(),
                                sinkConfig.getTable(),
                                label,
                                response);
            }
            log.info(
                    "Transaction started, db: {}, table: {}, label: {}, body : {}",
                    db,
                    table,
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
                                db, table, label, responseBody);
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
                            "Transaction start failed, db : {}, label : {}",
                            sinkConfig.getDatabase(),
                            label);
                    return false;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean prepare(String label) {
        String host = streamLoadHelper.getAvailableHost(sinkConfig.getNodeUrls());
        String prepareUrl = getPrepareUrl(host);

        HttpPost httpPost = new HttpPost(prepareUrl);
        httpPost.setHeaders(defaultTxnHeaders);
        httpPost.addHeader("label", label);
        httpPost.addHeader("db", sinkConfig.getDatabase());
        httpPost.addHeader("table", sinkConfig.getTable());

        httpPost.setConfig(
                RequestConfig.custom()
                        .setExpectContinueEnabled(true)
                        .setRedirectsEnabled(true)
                        .build());

        log.info("Transaction prepare, label : {}, request : {}", label, httpPost);

        try (CloseableHttpClient client = clientBuilder.build()) {
            String responseBody;
            try (CloseableHttpResponse response = client.execute(httpPost)) {
                responseBody =
                        parseHttpResponse(
                                "prepare transaction",
                                sinkConfig.getDatabase(),
                                sinkConfig.getTable(),
                                label,
                                response);
            }
            log.info("Transaction prepared, label : {}, body : {}", label, responseBody);

            StreamLoadResponse streamLoadResponse = new StreamLoadResponse();
            StreamLoadResponse.StreamLoadResponseBody streamLoadBody =
                    JsonUtils.parseObject(
                            responseBody, StreamLoadResponse.StreamLoadResponseBody.class);
            streamLoadResponse.setBody(streamLoadBody);
            String status = streamLoadBody.getStatus();
            if (status == null) {
                throw new StarRocksConnectorException(
                        FLUSH_DATA_FAILED,
                        String.format(
                                "Prepare transaction status is null. db: %s, table: %s, "
                                        + "label: %s, response body: %s",
                                sinkConfig.getDatabase(),
                                sinkConfig.getTable(),
                                label,
                                responseBody));
            }

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
                                getLabelState(
                                        host,
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

    public boolean commit(String label) {
        String host = streamLoadHelper.getAvailableHost(sinkConfig.getNodeUrls());
        String commitUrl = getCommitUrl(host);

        HttpPost httpPost = new HttpPost(commitUrl);
        httpPost.setHeaders(defaultTxnHeaders);
        httpPost.addHeader("label", label);
        httpPost.addHeader("db", sinkConfig.getDatabase());
        httpPost.addHeader("table", sinkConfig.getTable());

        httpPost.setConfig(
                RequestConfig.custom()
                        .setExpectContinueEnabled(true)
                        .setRedirectsEnabled(true)
                        .build());

        log.info("Transaction commit, label: {}, request : {}", label, httpPost);

        try (CloseableHttpClient client = clientBuilder.build()) {
            String responseBody;
            try (CloseableHttpResponse response = client.execute(httpPost)) {
                responseBody =
                        parseHttpResponse(
                                "commit transaction",
                                sinkConfig.getDatabase(),
                                sinkConfig.getTable(),
                                label,
                                response);
            }
            log.info("Transaction committed, label: {}, body : {}", label, responseBody);

            StreamLoadResponse streamLoadResponse = new StreamLoadResponse();
            StreamLoadResponse.StreamLoadResponseBody streamLoadBody =
                    JsonUtils.parseObject(
                            responseBody, StreamLoadResponse.StreamLoadResponseBody.class);
            streamLoadResponse.setBody(streamLoadBody);
            String status = streamLoadBody.getStatus();
            if (status == null) {
                throw new StarRocksConnectorException(
                        FLUSH_DATA_FAILED,
                        String.format(
                                "Commit transaction status is null. db: %s, table: %s, "
                                        + "label: %s, response body: %s",
                                sinkConfig.getDatabase(),
                                sinkConfig.getTable(),
                                label,
                                responseBody));
            }

            if (RESULT_OK.equals(status)) {
                //                manager.callback(streamLoadResponse);
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
                    getLabelState(host, sinkConfig.getDatabase(), label, Collections.emptySet());
            if (StreamLoadHelper.RESULT_LABEL_COMMITTED.equals(labelState)
                    || StreamLoadHelper.RESULT_LABEL_VISIBLE.equals(labelState)) {
                return true;
            }

            String errorLog = streamLoadHelper.getErrorLog(streamLoadBody.getErrorURL());
            log.error(
                    "Transaction commit failed, db: {}, table: {}, label: {}, label state: {}, \nresponseBody: {}\nerrorLog: {}",
                    sinkConfig.getDatabase(),
                    sinkConfig.getTable(),
                    label,
                    labelState,
                    responseBody,
                    errorLog);

            String exceptionMsg =
                    String.format(
                            "Transaction commit failed, db: %s, table: %s, label: %s, commit response status: %s,"
                                    + " label state: %s",
                            sinkConfig.getDatabase(),
                            sinkConfig.getTable(),
                            label,
                            status,
                            labelState);
            // transaction not exist often happens after transaction timeouts
            if (StreamLoadHelper.RESULT_TRANSACTION_NOT_EXISTED.equals(status)
                    || StreamLoadHelper.RESULT_LABEL_UNKNOWN.equals(labelState)) {
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

    @Override
    public void abortPreCommit(long chkID, int subTaskIndex) throws Exception {
        long startChkID = chkID;
        log.info("abort for chkId {}. subTaskIndex {}.", chkID, subTaskIndex);
        while (true) {
            try {
                String label = new LabelGenerator(sinkConfig).genLabel(startChkID, subTaskIndex);
                String host = streamLoadHelper.getAvailableHost(sinkConfig.getNodeUrls());
                String beginUrl = getBeginUrl(host);
                log.info("Transaction start, label : {}", label);

                HttpPost httpPost = new HttpPost(beginUrl);
                httpPost.setHeaders(beginTxnHeader);
                httpPost.addHeader("label", label);
                httpPost.addHeader("db", sinkConfig.getDatabase());
                httpPost.addHeader("table", sinkConfig.getTable());

                httpPost.setConfig(
                        RequestConfig.custom()
                                .setExpectContinueEnabled(true)
                                .setRedirectsEnabled(true)
                                .build());

                String db = sinkConfig.getDatabase();
                String table = sinkConfig.getTable();
                log.info(
                        "Transaction start, db: {}, table: {}, label: {}, request : {}",
                        db,
                        table,
                        label,
                        httpPost);
                CloseableHttpClient client = clientBuilder.build();
                String responseBody;
                try (CloseableHttpResponse response = client.execute(httpPost)) {
                    responseBody =
                            parseHttpResponse(
                                    "begin transaction",
                                    sinkConfig.getDatabase(),
                                    sinkConfig.getTable(),
                                    label,
                                    response);
                }
                StreamLoadResponse streamLoadResponse = new StreamLoadResponse();
                StreamLoadResponse.StreamLoadResponseBody streamLoadBody =
                        JsonUtils.parseObject(
                                responseBody, StreamLoadResponse.StreamLoadResponseBody.class);
                streamLoadResponse.setBody(streamLoadBody);
                String status = streamLoadBody.getStatus();

                if (RESULT_LABEL_EXISTED.equals(status)) {
                    // label already exist and job finished
                    if (RESULT_TRANSACTION_EXIST_FINISHED.equals(
                            streamLoadBody.getExistingJobStatus())) {
                        throw new StarRocksConnectorException(
                                FLUSH_DATA_FAILED,
                                "Load status is "
                                        + RESULT_LABEL_EXISTED
                                        + " and load job finished, "
                                        + "change you label prefix or restore from latest savepoint!");
                    }
                    // job not finished, abort.
                    if (RESULT_TRANSACTION_EXIST_RUNNING.equals(
                            streamLoadBody.getExistingJobStatus())) {
                        log.info("abort transaction for exist label {}", label);
                        rollback(label);
                    } else {
                        throw new StarRocksConnectorException(
                                FLUSH_DATA_FAILED,
                                "Load Status is "
                                        + RESULT_LABEL_EXISTED
                                        + ", job Status is not RUNNING!"
                                        + "response: "
                                        + responseBody);
                    }
                } else {
                    log.info("abort transaction {} for check label", label);
                    rollback(label);
                    break;
                }
                startChkID++;
            } catch (StarRocksConnectorException e) {
                log.warn("failed to stream load data", e);
                throw e;
            }
        }
    }

    public boolean rollback(String label) {
        String host = streamLoadHelper.getAvailableHost(sinkConfig.getNodeUrls());
        String rollbackUrl = getRollbackUrl(host);
        log.info("Transaction rollback, label : {}", label);

        HttpPost httpPost = new HttpPost(rollbackUrl);
        httpPost.setHeaders(defaultTxnHeaders);
        httpPost.addHeader("label", label);
        httpPost.addHeader("db", sinkConfig.getDatabase());
        httpPost.addHeader("table", sinkConfig.getTable());

        try (CloseableHttpClient client = clientBuilder.build()) {
            String responseBody;
            try (CloseableHttpResponse response = client.execute(httpPost)) {
                responseBody =
                        parseHttpResponse(
                                "abort transaction",
                                sinkConfig.getDatabase(),
                                sinkConfig.getTable(),
                                label,
                                response);
            }
            log.info("Transaction rollback, label: {}, body : {}", label, responseBody);

            ObjectNode bodyJson = JsonUtils.parseObject(responseBody);
            String status = bodyJson.get("Status").asText();
            if (status == null) {
                String errMsg =
                        String.format(
                                "Abort transaction status is null. db: %s, table: %s, label: %s, response: %s",
                                sinkConfig.getDatabase(),
                                sinkConfig.getTable(),
                                label,
                                responseBody);
                log.error(errMsg);
                throw new StarRocksConnectorException(FLUSH_DATA_FAILED, errMsg);
            }

            if (RESULT_SUCCESS.equals(status) || RESULT_OK.equals(status)) {
                return true;
            }
            log.error(
                    "Transaction rollback failed, db: {}, table: {}, label : {}",
                    sinkConfig.getDatabase(),
                    sinkConfig.getTable(),
                    label);
            return false;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected String getLabelState(
            String host, String database, String label, Set<String> retryStates) throws Exception {
        int idx = 0;
        for (; ; ) {
            TimeUnit.SECONDS.sleep(Math.min(++idx, 5));
            try (CloseableHttpClient client = HttpClients.createDefault()) {
                String url = host + "/api/" + database + "/get_load_state?label=" + label;
                HttpGet httpGet = new HttpGet(url);
                httpGet.addHeader(
                        "Authorization",
                        streamLoadHelper.getBasicAuthHeader(
                                sinkConfig.getUsername(), sinkConfig.getPassword()));
                httpGet.setHeader("Connection", "close");
                try (CloseableHttpResponse response = client.execute(httpGet)) {
                    int responseStatusCode = response.getStatusLine().getStatusCode();
                    String entityContent = EntityUtils.toString(response.getEntity());
                    log.info(
                            "Response for get_load_state, label: {}, response status code: {}, response body : {}",
                            label,
                            responseStatusCode,
                            entityContent);
                    if (responseStatusCode != 200) {
                        throw new StarRocksConnectorException(
                                FLUSH_DATA_FAILED,
                                String.format(
                                        "Could not get load state because of incorrect response status code %s, "
                                                + "label: %s, response body: %s",
                                        responseStatusCode, label, entityContent));
                    }

                    StreamLoadResponse.StreamLoadResponseBody responseBody =
                            JsonUtils.parseObject(
                                    entityContent, StreamLoadResponse.StreamLoadResponseBody.class);
                    String state = responseBody.getState();
                    if (state == null) {
                        log.error(
                                "Fail to get load state, label: {}, load information: {}",
                                label,
                                JsonUtils.toJsonString(responseBody));
                        throw new StarRocksConnectorException(
                                FLUSH_DATA_FAILED,
                                String.format(
                                        "Could not get load state because of state is null,"
                                                + "label: %s, load information: %s",
                                        label, entityContent));
                    }

                    if (retryStates.contains(state)) {
                        continue;
                    }

                    return state;
                }
            }
        }
    }

    protected String parseHttpResponse(
            String requestType,
            String db,
            String table,
            String label,
            CloseableHttpResponse response)
            throws StarRocksConnectorException {
        int code = response.getStatusLine().getStatusCode();
        if (307 == code) {
            String errorMsg =
                    String.format(
                            "Request %s failed because http response code is 307 which means 'Temporary Redirect'. "
                                    + "This can happen when FE responds the request slowly , you should find the reason first. The reason may be "
                                    + "StarRocks FE/Flink GC, network delay, or others. db: %s, table: %s, label: %s, response status line: %s",
                            requestType, db, table, label, response.getStatusLine());
            log.error("{}", errorMsg);
            throw new StarRocksConnectorException(FLUSH_DATA_FAILED, errorMsg);
        } else if (200 != code) {
            String errorMsg =
                    String.format(
                            "Request %s failed because http response code is not 200. db: %s, table: %s,"
                                    + "label: %s, response status line: %s",
                            requestType, db, table, label, response.getStatusLine());
            log.error("{}", errorMsg);
            throw new StarRocksConnectorException(FLUSH_DATA_FAILED, errorMsg);
        }

        HttpEntity respEntity = response.getEntity();
        if (respEntity == null) {
            String errorMsg =
                    String.format(
                            "Request %s failed because response entity is null. db: %s, table: %s,"
                                    + "label: %s, response status line: %s",
                            requestType, db, table, label, response.getStatusLine());
            log.error("{}", errorMsg);
            throw new StarRocksConnectorException(FLUSH_DATA_FAILED, errorMsg);
        }

        try {
            return EntityUtils.toString(respEntity);
        } catch (Exception e) {
            String errorMsg =
                    String.format(
                            "Request %s failed because fail to convert response entity to string. "
                                    + "db: %s, table: %s, label: %s, response status line: %s, response entity: %s",
                            requestType,
                            db,
                            table,
                            label,
                            response.getStatusLine(),
                            response.getEntity());
            log.error("{}", errorMsg, e);
            throw new StarRocksConnectorException(FLUSH_DATA_FAILED, errorMsg, e);
        }
    }

    protected String getErrorLog(String errorUrl) {
        if (errorUrl == null || !errorUrl.startsWith("http")) {
            return null;
        }

        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(errorUrl);
            try (CloseableHttpResponse resp = httpclient.execute(httpGet)) {
                int code = resp.getStatusLine().getStatusCode();
                if (200 != code) {
                    log.warn(
                            "Request error log failed with error code: {}, errorUrl: {}",
                            code,
                            errorUrl);
                    return null;
                }

                HttpEntity respEntity = resp.getEntity();
                if (respEntity == null) {
                    log.warn("Request error log failed with null entity, errorUrl: {}", errorUrl);
                    return null;
                }
                String errorLog = EntityUtils.toString(respEntity);
                if (errorLog != null && errorLog.length() > 3000) {
                    errorLog = errorLog.substring(0, 3000);
                }
                return errorLog;
            }
        } catch (Exception e) {
            log.warn("Failed to get error log: {}.", errorUrl, e);
            return String.format(
                    "Failed to get error log: %s, exception message: %s", errorUrl, e.getMessage());
        }
    }

    public static String getBeginUrl(String host) {
        return host + PATH_TRANSACTION_BEGIN;
    }

    public static String getSendUrl(String host) {
        return host + PATH_TRANSACTION_SEND;
    }

    public static String getPrepareUrl(String host) {
        return host + PATH_TRANSACTION_PRE_COMMIT;
    }

    public static String getCommitUrl(String host) {
        return host + PATH_TRANSACTION_COMMIT;
    }

    public static String getRollbackUrl(String host) {
        return host + PATH_TRANSACTION_ROLLBACK;
    }
}
