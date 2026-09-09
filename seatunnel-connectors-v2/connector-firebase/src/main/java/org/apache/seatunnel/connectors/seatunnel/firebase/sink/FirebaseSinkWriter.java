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

package org.apache.seatunnel.connectors.seatunnel.firebase.sink;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.firebase.client.FirebaseHttpClient;
import org.apache.seatunnel.connectors.seatunnel.firebase.config.FirebaseSinkOptions;

import org.apache.commons.collections4.CollectionUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class FirebaseSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final FirebaseHttpClient httpClient;
    private final SeaTunnelRowType rowType;
    private final String basePath;
    private final List<String> primaryKeys;
    private final String keyPrefix;
    private final String keyPostfix;
    private final String keyDelimiter;
    private final int batchSize;
    private final int retryMax;
    private final boolean ignoreNullValues;
    private final boolean supportDeletes;

    private final Map<String, Object> bufferMap;

    public FirebaseSinkWriter(CatalogTable catalogTable, ReadonlyConfig config) {
        this.httpClient = new FirebaseHttpClient(config);
        this.rowType = catalogTable.getSeaTunnelRowType();
        this.basePath = config.get(FirebaseSinkOptions.PATH).replaceAll("^/+|/+$", "");

        List<String> pkList = config.getOptional(FirebaseSinkOptions.PRIMARY_KEYS).orElse(null);
        if (CollectionUtils.isEmpty(pkList)
                && catalogTable.getTableSchema() != null
                && catalogTable.getTableSchema().getPrimaryKey() != null) {
            pkList = catalogTable.getTableSchema().getPrimaryKey().getColumnNames();
        }
        this.primaryKeys = pkList;

        this.keyPrefix = config.getOptional(FirebaseSinkOptions.KEY_PREFIX).orElse("");
        this.keyPostfix = config.getOptional(FirebaseSinkOptions.KEY_POSTFIX).orElse("");
        this.keyDelimiter = config.get(FirebaseSinkOptions.KEY_DELIMITER);
        this.batchSize = config.get(FirebaseSinkOptions.BATCH_SIZE);
        this.retryMax = config.get(FirebaseSinkOptions.RETRY_MAX);
        this.ignoreNullValues = config.get(FirebaseSinkOptions.IGNORE_NULL_VALUES);
        this.supportDeletes = config.get(FirebaseSinkOptions.SUPPORT_DELETES);

        this.bufferMap = new LinkedHashMap<>();
    }

    public FirebaseSinkWriter(
            FirebaseHttpClient client, CatalogTable catalogTable, ReadonlyConfig config) {
        this.httpClient = client;
        this.rowType = catalogTable.getSeaTunnelRowType();
        this.basePath = config.get(FirebaseSinkOptions.PATH).replaceAll("^/+|/+$", "");

        List<String> pkList = config.getOptional(FirebaseSinkOptions.PRIMARY_KEYS).orElse(null);
        if (CollectionUtils.isEmpty(pkList)
                && catalogTable.getTableSchema() != null
                && catalogTable.getTableSchema().getPrimaryKey() != null) {
            pkList = catalogTable.getTableSchema().getPrimaryKey().getColumnNames();
        }
        this.primaryKeys = pkList;

        this.keyPrefix = config.getOptional(FirebaseSinkOptions.KEY_PREFIX).orElse("");
        this.keyPostfix = config.getOptional(FirebaseSinkOptions.KEY_POSTFIX).orElse("");
        this.keyDelimiter = config.get(FirebaseSinkOptions.KEY_DELIMITER);
        this.batchSize = config.get(FirebaseSinkOptions.BATCH_SIZE);
        this.retryMax = config.get(FirebaseSinkOptions.RETRY_MAX);
        this.ignoreNullValues = config.get(FirebaseSinkOptions.IGNORE_NULL_VALUES);
        this.supportDeletes = config.get(FirebaseSinkOptions.SUPPORT_DELETES);

        this.bufferMap = new LinkedHashMap<>();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        RowKind rowKind = element.getRowKind();
        if (!supportDeletes && (rowKind == RowKind.DELETE || rowKind == RowKind.UPDATE_BEFORE)) {
            log.debug("Skipping row with RowKind {} because support_deletes is disabled", rowKind);
            return;
        }
        String nodeKey = resolveNodeKey(element);
        String relativePath = basePath.isEmpty() ? nodeKey : basePath + "/" + nodeKey;
        if (rowKind == RowKind.DELETE || rowKind == RowKind.UPDATE_BEFORE) {
            bufferMap.put(relativePath, null);
        } else {
            Map<String, Object> rowDataMap = serializeRowToMap(element);
            bufferMap.put(relativePath, rowDataMap);
        }

        if (bufferMap.size() >= batchSize) {
            flush();
        }
    }

    private String resolveNodeKey(SeaTunnelRow row) {
        String keyBody;

        if (CollectionUtils.isEmpty(primaryKeys)) {
            keyBody = UUID.randomUUID().toString();
        } else {
            List<String> values = new ArrayList<>();
            for (String pkName : primaryKeys) {
                int fieldIndex = rowType.indexOf(pkName);
                if (fieldIndex < 0) {
                    throw new SeaTunnelException(
                            String.format(
                                    "Primary key field '%s' not found in row schema", pkName));
                }
                Object fieldValue = row.getField(fieldIndex);
                if (fieldValue == null) {
                    throw new SeaTunnelException(
                            String.format("Primary key field '%s' cannot be null", pkName));
                }
                values.add(fieldValue.toString());
            }
            keyBody = String.join(keyDelimiter, values);
        }

        return keyPrefix + keyBody + keyPostfix;
    }

    private Map<String, Object> serializeRowToMap(SeaTunnelRow row) {
        Map<String, Object> map = new LinkedHashMap<>();
        String[] fieldNames = rowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            Object value = row.getField(i);
            if (value == null && ignoreNullValues) {
                continue;
            }
            map.put(fieldNames[i], value);
        }
        return map;
    }

    private void flush() throws IOException {
        if (bufferMap.isEmpty()) {
            return;
        }

        String jsonPayload;
        try {
            jsonPayload = OBJECT_MAPPER.writeValueAsString(bufferMap);
        } catch (Exception e) {
            throw new SeaTunnelException(
                    "Failed to serialize Firebase multi-location batch payload", e);
        }
        executeWithRetry(jsonPayload);
        bufferMap.clear();
    }

    private void executeWithRetry(String jsonPayload) {
        int attempt = 0;
        long backoffMs = 1000L;

        while (attempt <= retryMax) {
            try {
                attempt++;
                httpClient.executePatch("", jsonPayload);
                return;
            } catch (Exception e) {
                if (attempt > retryMax) {
                    throw new SeaTunnelException(
                            String.format(
                                    "Failed to write batch payload to Firebase after %d attempts",
                                    retryMax),
                            e);
                }
                log.warn(
                        "Attempt {}/{} failed to write batch to Firebase: {}. Retrying in {} ms...",
                        attempt,
                        retryMax,
                        e.getMessage(),
                        backoffMs);
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SeaTunnelException(
                            "Interrupted while waiting to retry Firebase batch write", ie);
                }
                backoffMs *= 2;
            }
        }
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}
