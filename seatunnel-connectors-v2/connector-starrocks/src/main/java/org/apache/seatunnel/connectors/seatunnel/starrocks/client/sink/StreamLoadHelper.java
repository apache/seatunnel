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

import org.apache.seatunnel.connectors.seatunnel.starrocks.client.HttpHelper;

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
import java.util.regex.Pattern;

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

    public static final Pattern LABEL_EXIST_PATTERN =
            Pattern.compile(
                    "errCode = 2, detailMessage = Label \\[(.*)\\] "
                            + "has already been used, relate to txn \\[(\\d+)\\]");
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
}
