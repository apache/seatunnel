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

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.HttpHelper;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.HttpRequestBuilder;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.entity.StarRocksFlushTuple;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.entity.StreamLoadResponse;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;
import org.apache.seatunnel.connectors.seatunnel.starrocks.serialize.StarRocksDelimiterParser;

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

import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.PATH_STREAM_LOAD;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_LABEL_ABORTED;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_LABEL_COMMITTED;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_LABEL_EXISTED;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_LABEL_PREPARE;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_LABEL_UNKNOWN;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_LABEL_VISIBLE;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink.StreamLoadHelper.RESULT_SUCCESS;

public class StarRocksStreamLoadVisitor {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksStreamLoadVisitor.class);

    private final HttpHelper httpHelper = new HttpHelper();
    private final StreamLoadHelper streamLoadHelper;

    private static final int MAX_SLEEP_TIME = 5;

    private final SinkConfig sinkConfig;

    private List<String> fieldNames;

    public StarRocksStreamLoadVisitor(SinkConfig sinkConfig, List<String> fieldNames) {
        this.sinkConfig = sinkConfig;
        this.fieldNames = fieldNames;
        streamLoadHelper = new StreamLoadHelper(sinkConfig);
    }

    public Boolean doStreamLoad(StarRocksFlushTuple flushData) throws IOException {
        String host = streamLoadHelper.getAvailableHost(sinkConfig.getNodeUrls());
        if (null == host) {
            throw new StarRocksConnectorException(
                    CommonErrorCode.ILLEGAL_ARGUMENT,
                    "None of the host in `load_url` could be connected.");
        }
        String loadUrl =
                String.format(
                        host + PATH_STREAM_LOAD, sinkConfig.getDatabase(), sinkConfig.getTable());

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    String.format(
                            "Start to join batch data: rows[%d] bytes[%d] label[%s].",
                            flushData.getRows().size(),
                            flushData.getBytes(),
                            flushData.getLabel()));
        }
        HttpRequestBuilder httpBuilder =
                new HttpRequestBuilder(sinkConfig)
                        .HttpPut()
                                .setUrl(loadUrl)
                                .baseAuth()
                                .addProperties(getStreamLoadHttpHeader(flushData.getLabel()))
                                .setEntity(
                                        joinRows(
                                                flushData.getRows(),
                                                flushData.getBytes().intValue()));

        String entityContent = httpHelper.doHttpExecute(null, httpBuilder.build());

        StreamLoadResponse.StreamLoadResponseBody loadResult =
                JsonUtils.parseObject(
                        entityContent, StreamLoadResponse.StreamLoadResponseBody.class);

        if (loadResult.getStatus() == null) {
            LOG.error("unknown result status. {}", loadResult);
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
                    "Unable to flush data to StarRocks: unknown result status. " + loadResult);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("StreamLoad response:\n" + JsonUtils.toJsonString(loadResult));
        }
        if (RESULT_FAILED.equals(loadResult.getStatus())) {
            StringBuilder errorBuilder = new StringBuilder("Failed to flush data to StarRocks \n");
            errorBuilder
                    .append(sinkConfig.getDatabase())
                    .append("/")
                    .append(sinkConfig.getTable())
                    .append("\n");
            if (loadResult.getMessage() != null) {
                errorBuilder.append(loadResult.getMessage());
                errorBuilder.append('\n');
            }
            if (loadResult.getErrorURL() != null) {
                LOG.error("StreamLoad response: {}", loadResult);
                String errorLog = streamLoadHelper.getErrorLog(loadResult.getErrorURL());
                errorBuilder.append(errorLog);
                errorBuilder.append('\n');
            } else {
                errorBuilder.append(JsonUtils.toJsonString(loadResult));
                errorBuilder.append('\n');
            }
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.FLUSH_DATA_FAILED, errorBuilder.toString());
        } else if (RESULT_LABEL_EXISTED.equals(loadResult.getStatus())) {
            LOG.debug("StreamLoad response:\n" + JsonUtils.toJsonString(loadResult));
            // has to block-checking the state to get the final result
            checkLabelState(host, flushData.getLabel());
        }
        return RESULT_SUCCESS.equals(loadResult.getStatus());
    }

    private byte[] joinRows(List<byte[]> rows, int totalBytes) {
        if (SinkConfig.StreamLoadFormat.CSV.equals(sinkConfig.getLoadFormat())) {
            Map<String, Object> props = sinkConfig.getStreamLoadProps();
            byte[] lineDelimiter =
                    StarRocksDelimiterParser.parse((String) props.get("row_delimiter"), "\n")
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
        throw new StarRocksConnectorException(
                StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
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
                String labelState = streamLoadHelper.getLabelState(sinkConfig.getDatabase(), label);
                LOG.info(String.format("Checking label[%s] state[%s]\n", label, labelState));
                switch (labelState) {
                    case RESULT_LABEL_VISIBLE:
                    case RESULT_LABEL_COMMITTED:
                        return;
                    case RESULT_LABEL_PREPARE:
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
            } catch (Exception e) {
                throw new StarRocksConnectorException(
                        StarRocksConnectorErrorCode.FLUSH_DATA_FAILED, e);
            }
        }
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
            for (Map.Entry<String, Object> entry : sinkConfig.getStreamLoadProps().entrySet()) {
                headerMap.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
        headerMap.put("strip_outer_array", "true");
        headerMap.put("Expect", "100-continue");
        headerMap.put("label", label);
        headerMap.put("Content-Type", "application/x-www-form-urlencoded");
        headerMap.put("format", sinkConfig.getLoadFormat().name().toUpperCase());
        return headerMap;
    }
}
