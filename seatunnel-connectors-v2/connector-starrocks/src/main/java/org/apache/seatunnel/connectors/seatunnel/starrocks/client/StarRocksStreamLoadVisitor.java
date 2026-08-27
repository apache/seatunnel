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

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;
import org.apache.seatunnel.connectors.seatunnel.starrocks.serialize.StarRocksDelimiterParser;
import org.apache.seatunnel.connectors.seatunnel.starrocks.serialize.StarRocksSinkOP;

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

    private final HttpHelper httpHelper;
    private static final int MAX_SLEEP_TIME = 5;
    private static final long DEFAULT_LABEL_STATE_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(3);

    private final SinkConfig sinkConfig;
    private long pos;
    private static final String RESULT_FAILED = "Fail";
    private static final String RESULT_SUCCESS = "Success";
    private static final String RESULT_PUBLISH_TIMEOUT = "Publish Timeout";
    private static final String RESULT_LABEL_EXISTED = "Label Already Exists";
    private static final String LABEL_STATE_VISIBLE = "VISIBLE";
    private static final String LABEL_STATE_COMMITTED = "COMMITTED";
    private static final String RESULT_LABEL_PREPARE = "PREPARE";
    private static final String RESULT_LABEL_ABORTED = "ABORTED";
    private static final String RESULT_LABEL_UNKNOWN = "UNKNOWN";

    private final TableSchema tableSchema;

    /**
     * Maximum total time spent waiting for one reused label to leave the PREPARE state. The bound
     * prevents a checkpoint flush from waiting forever on an unresolved transaction.
     */
    private final long labelStateTimeoutMs;

    public StarRocksStreamLoadVisitor(SinkConfig sinkConfig, TableSchema tableSchema) {
        this(sinkConfig, tableSchema, new HttpHelper(sinkConfig), DEFAULT_LABEL_STATE_TIMEOUT_MS);
    }

    /**
     * Creates a visitor with an explicit HTTP helper so response handling can be verified without a
     * live StarRocks cluster.
     */
    StarRocksStreamLoadVisitor(
            SinkConfig sinkConfig, TableSchema tableSchema, HttpHelper httpHelper) {
        this(sinkConfig, tableSchema, httpHelper, DEFAULT_LABEL_STATE_TIMEOUT_MS);
    }

    /**
     * Creates a visitor with explicit HTTP transport and label-state timeout for deterministic
     * boundary tests.
     */
    StarRocksStreamLoadVisitor(
            SinkConfig sinkConfig,
            TableSchema tableSchema,
            HttpHelper httpHelper,
            long labelStateTimeoutMs) {
        this.sinkConfig = sinkConfig;
        this.tableSchema = tableSchema;
        this.httpHelper = httpHelper;
        this.labelStateTimeoutMs = Math.max(1, labelStateTimeoutMs);
        checkBatchMaxBytes(sinkConfig.getBatchMaxBytes(), sinkConfig.getBatchMaxSize());
    }

    public Boolean doStreamLoad(StarRocksFlushTuple flushData) throws IOException {
        String host = getAvailableHost();
        if (null == host) {
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.HOST_IS_NULL,
                    "None of the host in `load_url` could be connected.");
        }
        String loadUrl =
                new StringBuilder(host)
                        .append("/api/")
                        .append(sinkConfig.getDatabase())
                        .append("/")
                        .append(sinkConfig.getTable())
                        .append("/_stream_load")
                        .toString();
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    String.format(
                            "Start to join batch data: rows[%d] bytes[%d] label[%s].",
                            flushData.getRows().size(),
                            flushData.getBytes(),
                            flushData.getLabel()));
        }
        Map<String, Object> loadResult =
                httpHelper.doHttpPut(
                        loadUrl,
                        joinRows(flushData.getRows(), flushData.getBytes()),
                        getStreamLoadHttpHeader(flushData.getLabel()));
        final String keyStatus = "Status";
        if (null == loadResult || !loadResult.containsKey(keyStatus)) {
            LOG.error("unknown result status. {}", loadResult);
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
                    "Unable to flush data to StarRocks: unknown result status. " + loadResult);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("StreamLoad response:\n" + JsonUtils.toJsonString(loadResult));
        }
        Object resultStatus = loadResult.get(keyStatus);
        if (RESULT_SUCCESS.equals(resultStatus) || RESULT_PUBLISH_TIMEOUT.equals(resultStatus)) {
            return true;
        }
        if (RESULT_LABEL_EXISTED.equals(resultStatus)) {
            LOG.debug("StreamLoad response:\n" + JsonUtils.toJsonString(loadResult));
            // The original request may already be committed, so never resend it under a new label
            // until StarRocks reports the final state of the existing label.
            checkLabelState(host, flushData.getLabel());
            return true;
        }
        if (RESULT_FAILED.equals(resultStatus)) {
            if (isLabelAlreadyUsed(loadResult, flushData.getLabel())) {
                checkLabelState(host, flushData.getLabel());
                return true;
            }
            StringBuilder errorBuilder = new StringBuilder("Failed to flush data to StarRocks \n");
            errorBuilder
                    .append(sinkConfig.getDatabase())
                    .append("/")
                    .append(sinkConfig.getTable())
                    .append("\n");
            if (loadResult.containsKey("Message")) {
                errorBuilder.append(loadResult.get("Message"));
                errorBuilder.append('\n');
            }
            if (loadResult.containsKey("ErrorURL")) {
                LOG.error("StreamLoad response: {}", loadResult);
                try {
                    errorBuilder.append(
                            httpHelper.doHttpGet(loadResult.get("ErrorURL").toString()));
                    errorBuilder.append('\n');
                } catch (IOException e) {
                    LOG.warn("Get Error URL failed. {} ", loadResult.get("ErrorURL"), e);
                }
            } else {
                errorBuilder.append(JsonUtils.toJsonString(loadResult));
                errorBuilder.append('\n');
            }
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.FLUSH_DATA_FAILED, errorBuilder.toString());
        }
        throw new StarRocksConnectorException(
                StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
                "Unable to flush data to StarRocks: unexpected result status. "
                        + JsonUtils.toJsonString(loadResult));
    }

    private String getAvailableHost() {
        List<String> hostList = sinkConfig.getNodeUrls();
        long tmp = pos + hostList.size();
        for (; pos < tmp; pos++) {
            String host = "http://" + hostList.get((int) (pos % hostList.size()));
            if (httpHelper.tryHttpConnection(host)) {
                return host;
            }
        }
        return null;
    }

    private byte[] joinRows(List<byte[]> rows, Long totalBytes) {
        checkBatchMaxBytes(totalBytes, rows.size());
        if (SinkConfig.StreamLoadFormat.CSV.equals(sinkConfig.getLoadFormat())) {
            Map<String, Object> props = sinkConfig.getStreamLoadProps();
            byte[] lineDelimiter =
                    StarRocksDelimiterParser.parse((String) props.get("row_delimiter"), "\n")
                            .getBytes(StandardCharsets.UTF_8);
            ByteBuffer bos =
                    ByteBuffer.allocate(totalBytes.intValue() + rows.size() * lineDelimiter.length);
            for (byte[] row : rows) {
                bos.put(row);
                bos.put(lineDelimiter);
            }
            return bos.array();
        }

        if (SinkConfig.StreamLoadFormat.JSON.equals(sinkConfig.getLoadFormat())) {
            ByteBuffer bos =
                    ByteBuffer.allocate(
                            totalBytes.intValue() + (rows.isEmpty() ? 2 : rows.size() + 1));
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
        throw new StarRocksConnectorException(
                StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
                "Failed to join rows data, unsupported `format` from stream load properties:");
    }

    @SuppressWarnings("unchecked")
    private void checkLabelState(String host, String label) throws IOException {
        int idx = 0;
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(labelStateTimeoutMs);
        while (System.nanoTime() < deadlineNanos) {
            try {
                String queryLoadStateUrl =
                        new StringBuilder(host)
                                .append("/api/")
                                .append(sinkConfig.getDatabase())
                                .append("/get_load_state?label=")
                                .append(label)
                                .toString();
                Map<String, Object> result =
                        httpHelper.doHttpGet(
                                queryLoadStateUrl,
                                getLoadStateHttpHeader(label),
                                remainingTimeoutMs(deadlineNanos));
                if (result == null) {
                    throw new StarRocksConnectorException(
                            StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
                            String.format(
                                    "Failed to flush data to StarRocks, Error "
                                            + "could not get the final state of label[%s].\n",
                                    label),
                            null);
                }
                String labelState = (String) result.get("state");
                if (null == labelState) {
                    throw new StarRocksConnectorException(
                            StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
                            String.format(
                                    "Failed to flush data to StarRocks, Error "
                                            + "could not get the final state of label[%s]. response[%s]\n",
                                    label, JsonUtils.toJsonString(result)),
                            null);
                }
                LOG.info(String.format("Checking label[%s] state[%s]\n", label, labelState));
                switch (labelState) {
                    case LABEL_STATE_VISIBLE:
                    case LABEL_STATE_COMMITTED:
                        return;
                    case RESULT_LABEL_PREPARE:
                        sleepBeforeNextLabelCheck(++idx, deadlineNanos, label);
                        continue;
                    case RESULT_LABEL_ABORTED:
                        throw new StarRocksConnectorException(
                                StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
                                String.format(
                                        "Failed to flush data to StarRocks, Error "
                                                + "label[%s] state[%s]\n",
                                        label, labelState),
                                true);
                    case RESULT_LABEL_UNKNOWN:
                    default:
                        throw new StarRocksConnectorException(
                                StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
                                String.format(
                                        "Failed to flush data to StarRocks, Error "
                                                + "label[%s] state[%s]\n",
                                        label, labelState));
                }
            } catch (IOException e) {
                throw new StarRocksConnectorException(
                        StarRocksConnectorErrorCode.FLUSH_DATA_FAILED, e);
            }
        }
        throw new StarRocksConnectorException(
                StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
                String.format(
                        "Timed out after %d ms while checking the final state of label[%s].",
                        labelStateTimeoutMs, label));
    }

    /**
     * Converts the remaining label-state deadline into a positive HTTP timeout.
     *
     * @param deadlineNanos absolute deadline based on {@link System#nanoTime()}
     * @return bounded timeout accepted by the Apache HTTP client
     */
    private int remainingTimeoutMs(long deadlineNanos) {
        long remainingNanos = Math.max(1, deadlineNanos - System.nanoTime());
        long remainingMillis = Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
        return (int) Math.min(Integer.MAX_VALUE, remainingMillis);
    }

    /**
     * Waits before polling a PREPARE label again without exceeding the total state deadline. An
     * interrupted wait fails the flush while preserving the thread interruption signal.
     */
    private void sleepBeforeNextLabelCheck(int attempt, long deadlineNanos, String label) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            return;
        }
        long sleepNanos =
                Math.min(
                        TimeUnit.SECONDS.toNanos(Math.min(attempt, MAX_SLEEP_TIME)),
                        remainingNanos);
        try {
            TimeUnit.NANOSECONDS.sleep(sleepNanos);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
                    String.format(
                            "Interrupted while checking the final state of label[%s].", label),
                    ex);
        }
    }

    /**
     * Detects the legacy failure response used by some StarRocks versions for a reused label.
     *
     * @param loadResult Stream Load response body
     * @param label label used by the current batch
     * @return true when the message identifies the current label as already used
     */
    private boolean isLabelAlreadyUsed(Map<String, Object> loadResult, String label) {
        Object message = loadResult.get("Message");
        return message != null
                && message.toString()
                        .contains(String.format("Label [%s] has already been used", label));
    }

    private String getBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encodedAuth);
    }

    private Map<String, String> getStreamLoadHttpHeader(String label) {
        Map<String, String> headerMap = new HashMap<>();
        List<Column> columns = tableSchema.getColumns();
        List<String> fieldNames =
                columns.stream().map(Column::getName).collect(Collectors.toList());
        if (sinkConfig.isEnableUpsertDelete()) {
            fieldNames.add(StarRocksSinkOP.COLUMN_KEY);
        }
        if (!fieldNames.isEmpty()
                && SinkConfig.StreamLoadFormat.CSV.equals(sinkConfig.getLoadFormat())) {
            headerMap.put(
                    "columns",
                    fieldNames.stream()
                            .map(f -> String.format("`%s`", f))
                            .collect(Collectors.joining(",")));
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
        headerMap.put("format", sinkConfig.getLoadFormat().name().toUpperCase());
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

    void checkBatchMaxBytes(long batchMaxBytes, long batchMaxRows) {
        long batchMaxBytesLimit;
        if (SinkConfig.StreamLoadFormat.CSV.equals(sinkConfig.getLoadFormat())) {
            Map<String, Object> props = sinkConfig.getStreamLoadProps();
            byte[] lineDelimiter =
                    StarRocksDelimiterParser.parse((String) props.get("row_delimiter"), "\n")
                            .getBytes(StandardCharsets.UTF_8);
            batchMaxBytesLimit = Integer.MAX_VALUE - batchMaxRows * lineDelimiter.length;
        } else if (SinkConfig.StreamLoadFormat.JSON.equals(sinkConfig.getLoadFormat())) {
            batchMaxBytesLimit = Integer.MAX_VALUE - (batchMaxRows == 0 ? 2 : batchMaxRows + 1);
        } else {
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
                    "Failed to join rows data, unsupported `format` from stream load properties:");
        }

        if (batchMaxBytes > batchMaxBytesLimit) {
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
                    String.format(
                            "The batch_max_bytes[%d] of the data exceeds the maximum limit[%d], "
                                    + "please reset the batch_max_bytes.",
                            batchMaxBytes, batchMaxBytesLimit));
        }
    }
}
