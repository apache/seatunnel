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

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.HttpHelper;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.StreamLoadResponse;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorErrorCode.FLUSH_DATA_FAILED;

@Slf4j
public class StreamLoadHelper {

    public static final String PATH_STREAM_LOAD = "/api/{db}/{table}/_stream_load";
    public static final String PATH_TRANSACTION_BEGIN = "/api/transaction/begin";
    public static final String PATH_TRANSACTION_SEND = "/api/transaction/load";
    public static final String PATH_TRANSACTION_ROLLBACK = "/api/transaction/rollback";
    public static final String PATH_TRANSACTION_PRE_COMMIT = "/api/transaction/prepare";
    public static final String PATH_TRANSACTION_COMMIT = "/api/transaction/commit";

    public static final String PATH_STREAM_LOAD_STATE = "/api/{db}/get_load_state?label={label}";

    public static final String RESULT_FAILED = "Fail";
    public static final String RESULT_SUCCESS = "Success";
    public static final String RESULT_OK = "OK";
    public static final String RESULT_LABEL_EXISTED = "Label Already Exists";
    public static final String RESULT_TRANSACTION_EXIST_FINISHED = "FINISHED";
    public static final String RESULT_TRANSACTION_EXIST_RUNNING = "RUNNING";

    public static final String RESULT_LABEL_VISIBLE = "VISIBLE";
    public static final String RESULT_LABEL_COMMITTED = "COMMITTED";
    public static final String RESULT_LABEL_PREPARE = "PREPARE";
    public static final String RESULT_LABEL_PREPARED = "PREPARED";
    public static final String RESULT_LABEL_ABORTED = "ABORTED";
    public static final String RESULT_LABEL_UNKNOWN = "UNKNOWN";
    public static final String RESULT_TRANSACTION_NOT_EXISTED = "TXN_NOT_EXISTS";
    public static final String RESULT_TRANSACTION_COMMIT_TIMEOUT = "Commit Timeout";
    public static final String RESULT_TRANSACTION_PUBLISH_TIMEOUT = "Publish Timeout";
    private final SinkConfig sinkConfig;

    public StreamLoadHelper(SinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
    }

    private int pos;

    private static final int ERROR_LOG_MAX_LENGTH = 3000;

    public static String getBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encodedAuth);
    }

    public String getAvailableHost(List<String> hostList) {
        long tmp = pos + hostList.size();
        for (; pos < tmp; pos++) {
            String host = "http://" + hostList.get((int) (pos % hostList.size()));
            if (new HttpHelper().tryHttpConnection(host)) {
                return host;
            }
        }
        return null;
    }

    public String getErrorLog(String errorUrl) {
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
                if (errorLog != null && errorLog.length() > ERROR_LOG_MAX_LENGTH) {
                    errorLog = errorLog.substring(0, ERROR_LOG_MAX_LENGTH);
                }
                return errorLog;
            }
        } catch (Exception e) {
            log.warn("Failed to get error log: {}.", errorUrl, e);
            return String.format(
                    "Failed to get error log: %s, exception message: %s", errorUrl, e.getMessage());
        }
    }

    public String parseHttpResponse(
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

    public String getLabelState(String database, String label, Set<String> retryStates)
            throws Exception {
        int idx = 0;
        for (; ; ) {
            TimeUnit.SECONDS.sleep(Math.min(++idx, 5));
            try (CloseableHttpClient client = HttpClients.createDefault()) {
                String url = String.format(PATH_STREAM_LOAD_STATE, database, label);
                HttpGet httpGet = new HttpGet(url);
                httpGet.addHeader(
                        "Authorization",
                        getBasicAuthHeader(sinkConfig.getUsername(), sinkConfig.getPassword()));
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

    public String getBeginUrl(List<String> hostList) {
        return getAvailableHost(hostList) + PATH_TRANSACTION_BEGIN;
    }

    public String getSendUrl(List<String> hostList) {
        return getAvailableHost(hostList) + PATH_TRANSACTION_SEND;
    }

    public String getPrepareUrl(List<String> hostList) {
        return getAvailableHost(hostList) + PATH_TRANSACTION_PRE_COMMIT;
    }

    public String getCommitUrl(List<String> hostList) {
        return getAvailableHost(hostList) + PATH_TRANSACTION_COMMIT;
    }

    public String getRollbackUrl(List<String> hostList) {
        return getAvailableHost(hostList) + PATH_TRANSACTION_ROLLBACK;
    }
}
