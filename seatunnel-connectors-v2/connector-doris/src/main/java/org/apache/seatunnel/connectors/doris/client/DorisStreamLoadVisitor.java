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

package org.apache.seatunnel.connectors.doris.client;

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.doris.config.SinkConfig;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.util.DelimiterParserUtil;

import org.apache.commons.codec.binary.Base64;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class DorisStreamLoadVisitor {
    private final HttpHelper httpHelper = new HttpHelper();
    private static final int MAX_SLEEP_TIME = 5;

    private final SinkConfig sinkConfig;
    private long pos;
    private static final String RESULT_FAILED = "Fail";
    private static final String RESULT_SUCCESS = "Success";
    private static final String RESULT_LABEL_EXISTED = "Label Already Exists";
    private static final String LAEBL_STATE_VISIBLE = "VISIBLE";
    private static final String LAEBL_STATE_COMMITTED = "COMMITTED";
    private static final String RESULT_LABEL_PREPARE = "PREPARE";
    private static final String RESULT_LABEL_ABORTED = "ABORTED";
    private static final String RESULT_LABEL_UNKNOWN = "UNKNOWN";

    private List<String> fieldNames;

    public DorisStreamLoadVisitor(SinkConfig sinkConfig, List<String> fieldNames) {
        this.sinkConfig = sinkConfig;
        this.fieldNames = fieldNames;
    }

    public Boolean doStreamLoad(DorisFlushTuple flushData) throws IOException {
        String host = getAvailableHost();
        if (null == host) {
            throw new DorisConnectorException(
                    CommonErrorCode.ILLEGAL_ARGUMENT,
                    "None of the host in `load_url` could be connected.");
        }
        String loadUrl =
                String.format(
                        "%s/api/%s/%s/_stream_load",
                        host, sinkConfig.getDatabase(), sinkConfig.getTable());
        if (log.isDebugEnabled()) {
            log.debug(
                    String.format(
                            "Start to join batch data: rows[%d] bytes[%d] label[%s].",
                            flushData.getRows().size(),
                            flushData.getBytes(),
                            flushData.getLabel()));
        }
        Map<String, Object> loadResult =
                httpHelper.doHttpPut(
                        loadUrl,
                        joinRows(flushData.getRows(), flushData.getBytes().intValue()),
                        getStreamLoadHttpHeader(flushData.getLabel()));
        final String keyStatus = "Status";
        if (null == loadResult || !loadResult.containsKey(keyStatus)) {
            throw new DorisConnectorException(
                    CommonErrorCode.FLUSH_DATA_FAILED,
                    "Unable to flush data to Doris: unknown result status. " + loadResult);
        }
        if (log.isDebugEnabled()) {
            log.debug(
                    String.format("StreamLoad response:\n%s"), JsonUtils.toJsonString(loadResult));
        }
        if (RESULT_FAILED.equals(loadResult.get(keyStatus))) {
            String errorMsg = "Failed to flush data to Doris.\n";
            String message = "";
            if (loadResult.containsKey("Message")) {
                message = loadResult.get("Message") + "\n";
            }
            String errorURL = "";
            if (loadResult.containsKey("ErrorURL")) {
                try {
                    errorURL = httpHelper.doHttpGet(loadResult.get("ErrorURL").toString()) + "\n";
                } catch (IOException e) {
                    log.warn("Get Error URL failed. {} ", loadResult.get("ErrorURL"), e);
                }
            } else {
                errorURL = JsonUtils.toJsonString(loadResult) + "\n";
            }
            throw new DorisConnectorException(
                    CommonErrorCode.FLUSH_DATA_FAILED,
                    String.format("%s%s%s", errorMsg, message, errorURL));
        } else if (RESULT_LABEL_EXISTED.equals(loadResult.get(keyStatus))) {
            log.debug(
                    String.format("StreamLoad response:\n%s"), JsonUtils.toJsonString(loadResult));
            // has to block-checking the state to get the final result
            checkLabelState(host, flushData.getLabel());
        }
        return RESULT_SUCCESS.equals(loadResult.get(keyStatus));
    }

    private String getAvailableHost() {
        List<String> hostList = sinkConfig.getNodeUrls();
        long tmp = pos + hostList.size();
        for (; pos < tmp; pos++) {
            String host = String.format("http://%s", hostList.get((int) (pos % hostList.size())));
            if (httpHelper.tryHttpConnection(host)) {
                return host;
            }
        }
        return null;
    }

    private byte[] joinRows(List<byte[]> rows, int totalBytes) {
        if (SinkConfig.StreamLoadFormat.CSV.equals(sinkConfig.getLoadFormat())) {
            Map<String, String> props = sinkConfig.getStreamLoadProps();
            byte[] lineDelimiter =
                    DelimiterParserUtil.parse((String) props.get("row_delimiter"), "\n")
                            .getBytes(StandardCharsets.UTF_8);
            ByteBuffer bos = ByteBuffer.allocate(totalBytes + rows.size() * lineDelimiter.length);
            for (byte[] row : rows) {
                bos.put(row);
                bos.put(lineDelimiter);
            }
            return bos.array();
        }

        if (SinkConfig.StreamLoadFormat.JSON.equals(sinkConfig.getLoadFormat())) {
            ByteBuffer bos =
                    ByteBuffer.allocate(totalBytes + (rows.isEmpty() ? 2 : rows.size() + 1));
            bos.put("[".getBytes(StandardCharsets.UTF_8));
            byte[] jsonDelimiter = ",".getBytes(StandardCharsets.UTF_8);
            boolean isFirstElement = true;
            for (byte[] row : rows) {
                if (!isFirstElement) {
                    bos.put(jsonDelimiter);
                }
                bos.put(row);
                isFirstElement = false;
            }
            bos.put("]".getBytes(StandardCharsets.UTF_8));
            return bos.array();
        }
        throw new DorisConnectorException(
                CommonErrorCode.FLUSH_DATA_FAILED,
                "Failed to join rows data, unsupported `format` from stream load properties:");
    }

    @SuppressWarnings("unchecked")
    private void checkLabelState(String host, String label) throws IOException {
        int idx = 0;
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(Math.min(++idx, MAX_SLEEP_TIME));
            } catch (InterruptedException ex) {
                break;
            }
            try {
                String queryLoadStateUrl =
                        String.format(
                                "%s/api/%s/get_load_state?label=%s",
                                host, sinkConfig.getDatabase(), label);
                Map<String, Object> result =
                        httpHelper.doHttpGet(queryLoadStateUrl, getLoadStateHttpHeader(label));
                if (result == null) {
                    throw new DorisConnectorException(
                            CommonErrorCode.FLUSH_DATA_FAILED,
                            String.format(
                                    "Failed to flush data to Doris, Error "
                                            + "could not get the final state of label[%s].\n",
                                    label),
                            null);
                }
                String labelState = (String) result.get("state");
                if (null == labelState) {
                    throw new DorisConnectorException(
                            CommonErrorCode.FLUSH_DATA_FAILED,
                            String.format(
                                    "Failed to flush data to Doris, Error "
                                            + "could not get the final state of label[%s]. response[%s]\n",
                                    label, JsonUtils.toJsonString(result)),
                            null);
                }
                log.info(String.format("Checking label[%s] state[%s]\n", label, labelState));
                switch (labelState) {
                    case LAEBL_STATE_VISIBLE:
                    case LAEBL_STATE_COMMITTED:
                        return;
                    case RESULT_LABEL_PREPARE:
                        continue;
                    case RESULT_LABEL_ABORTED:
                        throw new DorisConnectorException(
                                CommonErrorCode.FLUSH_DATA_FAILED,
                                String.format(
                                        "Failed to flush data to Doris, Error "
                                                + "label[%s] state[%s]\n",
                                        label, labelState),
                                true);
                    case RESULT_LABEL_UNKNOWN:
                    default:
                        throw new DorisConnectorException(
                                CommonErrorCode.FLUSH_DATA_FAILED,
                                String.format(
                                        "Failed to flush data to Doris, Error "
                                                + "label[%s] state[%s]\n",
                                        label, labelState));
                }
            } catch (IOException e) {
                throw new DorisConnectorException(CommonErrorCode.FLUSH_DATA_FAILED, e);
            }
        }
    }

    private String getBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return String.format("Basic %s", new String(encodedAuth));
    }

    private Map<String, String> getStreamLoadHttpHeader(String label) {
        Map<String, String> headerMap = new HashMap<>();
        if (null != fieldNames
                && !fieldNames.isEmpty()
                && SinkConfig.StreamLoadFormat.CSV.equals(sinkConfig.getLoadFormat())) {
            headerMap.put(
                    "columns",
                    String.join(
                            ",",
                            fieldNames.stream()
                                    .map(f -> String.format("`%s`", f))
                                    .collect(Collectors.toList())));
        }
        if (null != sinkConfig.getStreamLoadProps()) {
            for (Map.Entry<String, String> entry : sinkConfig.getStreamLoadProps().entrySet()) {
                headerMap.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
        headerMap.put("strip_outer_array", "true");
        headerMap.put("Expect", "100-continue");
        headerMap.put("label", label);
        headerMap.put("Content-Type", "application/x-www-form-urlencoded");
        headerMap.put(
                "Authorization",
                getBasicAuthHeader(sinkConfig.getUsername(), sinkConfig.getPassword()));
        return headerMap;
    }

    private Map<String, String> getLoadStateHttpHeader(String label) {
        Map<String, String> headerMap = new HashMap<>();
        headerMap.put(
                "Authorization",
                getBasicAuthHeader(sinkConfig.getUsername(), sinkConfig.getPassword()));
        headerMap.put("Connection", "close");
        return headerMap;
    }
}
