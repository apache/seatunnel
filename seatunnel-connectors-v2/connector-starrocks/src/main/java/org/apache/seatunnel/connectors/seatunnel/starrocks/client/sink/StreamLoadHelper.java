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
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.HttpRequestBuilder;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.entity.StreamLoadResponse;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorErrorCode.FLUSH_DATA_FAILED;

@Slf4j
public class StreamLoadHelper {

    public static final String PATH_STREAM_LOAD = "/api/%s/%s/_stream_load";
    public static final String PATH_STREAM_LOAD_STATE = "/api/%s/get_load_state?label=%s";

    public static final String PATH_TRANSACTION_BEGIN = "/api/transaction/begin";
    public static final String PATH_TRANSACTION_SEND = "/api/transaction/load";
    public static final String PATH_TRANSACTION_ROLLBACK = "/api/transaction/rollback";
    public static final String PATH_TRANSACTION_PRE_COMMIT = "/api/transaction/prepare";
    public static final String PATH_TRANSACTION_COMMIT = "/api/transaction/commit";

    public static final String RESULT_FAILED = "Fail";
    public static final String RESULT_SUCCESS = "Success";
    public static final String RESULT_OK = "OK";
    public static final String RESULT_LABEL_EXISTED = "Label Already Exists";
    public static final String RESULT_TRANSACTION_EXIST_FINISHED = "FINISHED";
    public static final String RESULT_TRANSACTION_EXIST_RUNNING = "RUNNING";

    public static final String RESULT_LABEL_PREPARE = "PREPARE";
    public static final String RESULT_LABEL_PREPARED = "PREPARED";
    public static final String RESULT_LABEL_COMMITTED = "COMMITTED";
    public static final String RESULT_LABEL_VISIBLE = "VISIBLE";
    public static final String RESULT_LABEL_ABORTED = "ABORTED";
    public static final String RESULT_LABEL_UNKNOWN = "UNKNOWN";
    public static final String RESULT_TRANSACTION_NOT_EXISTED = "TXN_NOT_EXISTS";
    public static final String RESULT_TRANSACTION_COMMIT_TIMEOUT = "Commit Timeout";
    public static final String RESULT_TRANSACTION_PUBLISH_TIMEOUT = "Publish Timeout";
    private final SinkConfig sinkConfig;
    private HttpHelper httpHelper;

    public StreamLoadHelper(SinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
        this.httpHelper = new HttpHelper();
    }

    private int pos;

    private static final int ERROR_LOG_MAX_LENGTH = 3000;

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
        HttpRequestBuilder httpBuilder = new HttpRequestBuilder().HttpGet().setUrl(errorUrl);
        try {

            String errorLog = httpHelper.doHttpExecute(null, httpBuilder.build());
            if (errorLog != null && errorLog.length() > ERROR_LOG_MAX_LENGTH) {
                errorLog = errorLog.substring(0, ERROR_LOG_MAX_LENGTH);
            }
            return errorLog;

        } catch (Exception e) {
            log.warn("Failed to get error log: {}.", errorUrl, e);
            return String.format(
                    "Failed to get error log: %s, exception message: %s", errorUrl, e.getMessage());
        }
    }

    public String getLabelState(String database, String label) throws Exception {
        HttpRequestBuilder httpBuilder =
                new HttpRequestBuilder(sinkConfig)
                        .setUrl(getLabelStateUrl(sinkConfig.getNodeUrls(), database, label))
                        .getLoadState();

        String entityContent = httpHelper.doHttpExecute(null, httpBuilder.build());

        log.info(
                "Response for get_load_state, label: {}, response status code: {}, response body : {}",
                label,
                entityContent);

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

        return state;
    }

    public String getLabelState(String database, String label, Set<String> retryStates)
            throws Exception {
        int idx = 0;
        for (; ; ) {
            TimeUnit.SECONDS.sleep(Math.min(++idx, 5));
            String state = getLabelState(database, label);
            if (retryStates.contains(state)) {
                continue;
            }
            return state;
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

    public String getLabelStateUrl(List<String> hostList, String database, String label) {
        return getAvailableHost(hostList) + String.format(PATH_STREAM_LOAD_STATE, database, label);
    }
}
