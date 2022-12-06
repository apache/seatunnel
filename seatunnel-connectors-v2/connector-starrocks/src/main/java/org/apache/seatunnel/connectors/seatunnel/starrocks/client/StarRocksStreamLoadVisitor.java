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

package org.apache.seatunnel.connectors.seatunnel.starrocks.client;

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;
import org.apache.seatunnel.connectors.seatunnel.starrocks.serialize.StarRocksDelimiterParser;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StarRocksStreamLoadVisitor {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksStreamLoadVisitor.class);

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

    public StarRocksStreamLoadVisitor(SinkConfig sinkConfig, List<String> fieldNames) {
        this.sinkConfig = sinkConfig;
        this.fieldNames = fieldNames;
    }

    public Boolean doStreamLoad(StarRocksFlushTuple flushData) throws IOException {
        String host = getAvailableHost();
        if (null == host) {
            throw new StarRocksConnectorException(CommonErrorCode.ILLEGAL_ARGUMENT, "None of the host in `load_url` could be connected.");
        }
        String loadUrl = new StringBuilder(host)
                .append("/api/")
                .append(sinkConfig.getDatabase())
                .append("/")
                .append(sinkConfig.getTable())
                .append("/_stream_load")
                .toString();
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Start to join batch data: rows[%d] bytes[%d] label[%s].", flushData.getRows().size(), flushData.getBytes(), flushData.getLabel()));
        }
        Map<String, Object> loadResult = httpHelper.doHttpPut(loadUrl, joinRows(flushData.getRows(), flushData.getBytes().intValue()), getStreamLoadHttpHeader(flushData.getLabel()));
        final String keyStatus = "Status";
        if (null == loadResult || !loadResult.containsKey(keyStatus)) {
            LOG.error("unknown result status. {}", loadResult);
            throw new StarRocksConnectorException(StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,  "Unable to flush data to StarRocks: unknown result status. " + loadResult);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(new StringBuilder("StreamLoad response:\n").append(JsonUtils.toJsonString(loadResult)).toString());
        }
        if (RESULT_FAILED.equals(loadResult.get(keyStatus))) {
            StringBuilder errorBuilder = new StringBuilder("Failed to flush data to StarRocks.\n");
            if (loadResult.containsKey("Message")) {
                errorBuilder.append(loadResult.get("Message"));
                errorBuilder.append('\n');
            }
            if (loadResult.containsKey("ErrorURL")) {
                LOG.error("StreamLoad response: {}", loadResult);
                try {
                    errorBuilder.append(httpHelper.doHttpGet(loadResult.get("ErrorURL").toString()));
                    errorBuilder.append('\n');
                } catch (IOException e) {
                    LOG.warn("Get Error URL failed. {} ", loadResult.get("ErrorURL"), e);
                }
            } else {
                errorBuilder.append(JsonUtils.toJsonString(loadResult));
                errorBuilder.append('\n');
            }
            throw new StarRocksConnectorException(StarRocksConnectorErrorCode.FLUSH_DATA_FAILED, errorBuilder.toString());
        } else if (RESULT_LABEL_EXISTED.equals(loadResult.get(keyStatus))) {
            LOG.debug(new StringBuilder("StreamLoad response:\n").append(JsonUtils.toJsonString(loadResult)).toString());
            // has to block-checking the state to get the final result
            checkLabelState(host, flushData.getLabel());
        }
        return RESULT_SUCCESS.equals(loadResult.get(keyStatus));
    }

    private String getAvailableHost() {
        List<String> hostList = sinkConfig.getNodeUrls();
        long tmp = pos + hostList.size();
        for (; pos < tmp; pos++) {
            String host = new StringBuilder("http://").append(hostList.get((int) (pos % hostList.size()))).toString();
            if (httpHelper.tryHttpConnection(host)) {
                return host;
            }
        }
        return null;
    }

    private byte[] joinRows(List<byte[]> rows, int totalBytes) {
        if (SinkConfig.StreamLoadFormat.CSV.equals(sinkConfig.getLoadFormat())) {
            Map<String, Object> props = sinkConfig.getStreamLoadProps();
            byte[] lineDelimiter = StarRocksDelimiterParser.parse((String) props.get("row_delimiter"), "\n").getBytes(StandardCharsets.UTF_8);
            ByteBuffer bos = ByteBuffer.allocate(totalBytes + rows.size() * lineDelimiter.length);
            for (byte[] row : rows) {
                bos.put(row);
                bos.put(lineDelimiter);
            }
            return bos.array();
        }

        if (SinkConfig.StreamLoadFormat.JSON.equals(sinkConfig.getLoadFormat())) {
            ByteBuffer bos = ByteBuffer.allocate(totalBytes + (rows.isEmpty() ? 2 : rows.size() + 1));
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
        throw new StarRocksConnectorException(StarRocksConnectorErrorCode.FLUSH_DATA_FAILED, "Failed to join rows data, unsupported `format` from stream load properties:");
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
                String queryLoadStateUrl = new StringBuilder(host).append("/api/").append(sinkConfig.getDatabase()).append("/get_load_state?label=").append(label).toString();
                Map<String, Object> result = httpHelper.doHttpGet(queryLoadStateUrl, getLoadStateHttpHeader(label));
                if (result == null) {
                    throw new StarRocksConnectorException(StarRocksConnectorErrorCode.FLUSH_DATA_FAILED, String.format("Failed to flush data to StarRocks, Error " +
                            "could not get the final state of label[%s].\n", label), null);
                }
                String labelState = (String) result.get("state");
                if (null == labelState) {
                    throw new StarRocksConnectorException(StarRocksConnectorErrorCode.FLUSH_DATA_FAILED, String.format("Failed to flush data to StarRocks, Error " +
                            "could not get the final state of label[%s]. response[%s]\n", label, JsonUtils.toJsonString(result)), null);
                }
                LOG.info(String.format("Checking label[%s] state[%s]\n", label, labelState));
                switch (labelState) {
                    case LAEBL_STATE_VISIBLE:
                    case LAEBL_STATE_COMMITTED:
                        return;
                    case RESULT_LABEL_PREPARE:
                        continue;
                    case RESULT_LABEL_ABORTED:
                        throw new StarRocksConnectorException(StarRocksConnectorErrorCode.FLUSH_DATA_FAILED, String.format("Failed to flush data to StarRocks, Error " +
                                "label[%s] state[%s]\n", label, labelState), true);
                    case RESULT_LABEL_UNKNOWN:
                    default:
                        throw new StarRocksConnectorException(StarRocksConnectorErrorCode.FLUSH_DATA_FAILED, String.format("Failed to flush data to StarRocks, Error " +
                                "label[%s] state[%s]\n", label, labelState));
                }
            } catch (IOException e) {
                throw new StarRocksConnectorException(StarRocksConnectorErrorCode.FLUSH_DATA_FAILED, e);
            }
        }
    }

    private String getBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return new StringBuilder("Basic ").append(new String(encodedAuth)).toString();
    }

    private Map<String, String> getStreamLoadHttpHeader(String label) {
        Map<String, String> headerMap = new HashMap<>();
        if (null != fieldNames && !fieldNames.isEmpty() && SinkConfig.StreamLoadFormat.CSV.equals(sinkConfig.getLoadFormat())) {
            headerMap.put("columns", String.join(",", fieldNames.stream().map(f -> String.format("`%s`", f)).collect(Collectors.toList())));
        }
        if (null != sinkConfig.getStreamLoadProps()) {
            for (Map.Entry<String, Object> entry : sinkConfig.getStreamLoadProps().entrySet()) {
                headerMap.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
        headerMap.put("strip_outer_array", "true");
        headerMap.put("Expect", "100-continue");
        headerMap.put("label", label);
        headerMap.put("Content-Type", "application/x-www-form-urlencoded");
        headerMap.put("Authorization", getBasicAuthHeader(sinkConfig.getUsername(), sinkConfig.getPassword()));
        return headerMap;
    }

    private Map<String, String> getLoadStateHttpHeader(String label) {
        Map<String, String> headerMap = new HashMap<>();
        headerMap.put("Authorization", getBasicAuthHeader(sinkConfig.getUsername(), sinkConfig.getPassword()));
        headerMap.put("Connection", "close");
        return headerMap;
    }
}
