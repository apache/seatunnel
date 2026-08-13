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

package org.apache.seatunnel.connectors.seatunnel.firebase.source;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.firebase.client.FirebaseHttpClient;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
public class FirebaseSourceReader implements SourceReader<SeaTunnelRow, FirebaseSourceSplit> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final SourceReader.Context context;
    private final ReadonlyConfig config;
    private final CatalogTable catalogTable;
    private final FirebaseHttpClient httpClient;
    private final JsonDeserializationSchema deserializationSchema;
    private final Set<String> declaredFields;
    private final Queue<FirebaseSourceSplit> splits = new ConcurrentLinkedQueue<>();
    private volatile boolean noMoreSplits = false;

    public FirebaseSourceReader(
            SourceReader.Context context, ReadonlyConfig config, CatalogTable catalogTable) {
        this.context = context;
        this.config = config;
        this.catalogTable = catalogTable;
        this.httpClient = new FirebaseHttpClient(config);

        SeaTunnelRowType rowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        this.declaredFields = extractDeclaredFields(catalogTable);
        this.deserializationSchema = new JsonDeserializationSchema(false, false, rowType);
    }

    FirebaseSourceReader(
            SourceReader.Context context,
            ReadonlyConfig config,
            CatalogTable catalogTable,
            FirebaseHttpClient httpClient) {
        this.context = context;
        this.config = config;
        this.catalogTable = catalogTable;
        this.httpClient = httpClient;

        SeaTunnelRowType rowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        this.declaredFields = extractDeclaredFields(catalogTable);
        this.deserializationSchema = new JsonDeserializationSchema(false, false, rowType);
    }

    @Override
    public void open() throws Exception {
        // No persistent connection setup required for REST client
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        FirebaseSourceSplit split = splits.poll();
        if (split != null) {
            readSplit(split, output);
            return;
        }

        if (noMoreSplits) {
            log.info("notify engine , no more splits");
            context.signalNoMoreElement();
        } else {
            Thread.sleep(100);
        }
    }

    private void readSplit(FirebaseSourceSplit split, Collector<SeaTunnelRow> output)
            throws Exception {
        log.info(
                "Reading split [{}] with path [{}] and keys count [{}]",
                split.splitId(),
                split.getPath(),
                split.getKeys().size());
        List<String> keys = split.getKeys();

        if (keys != null && !keys.isEmpty()) {
            Map<String, Object> reconstructedMap = new LinkedHashMap<>();
            boolean containsChildRecordObjects = false;

            for (String key : keys) {
                String jsonPayload = httpClient.fetchNodeData(key);
                if (jsonPayload != null && !jsonPayload.trim().equals("null")) {

                    String trimmedPayload = jsonPayload.trim();

                    if (declaredFields.contains(key)) {
                        try {
                            Object parsedVal =
                                    OBJECT_MAPPER.readValue(trimmedPayload, Object.class);
                            reconstructedMap.put(key, parsedVal);
                        } catch (Exception e) {
                            reconstructedMap.put(key, trimmedPayload);
                        }
                    } else if (trimmedPayload.startsWith("{") || trimmedPayload.startsWith("[")) {
                        containsChildRecordObjects = true;
                        processJsonPayload(trimmedPayload, output);
                    } else {
                        reconstructedMap.put(key, trimmedPayload);
                    }
                }
            }

            if (!containsChildRecordObjects && !reconstructedMap.isEmpty()) {
                String singleRowJson = OBJECT_MAPPER.writeValueAsString(reconstructedMap);
                emitJsonRecord(singleRowJson, output);
            }
        } else {
            String jsonPayload = httpClient.fetchNodeData(null);
            processJsonPayload(jsonPayload, output);
        }
    }

    /** Extracts top-level schema field names into an unmodifiable Set once during construction. */
    private static Set<String> extractDeclaredFields(CatalogTable catalogTable) {
        if (catalogTable != null && catalogTable.getTableSchema() != null) {
            SeaTunnelRowType rowType = catalogTable.getTableSchema().toPhysicalRowDataType();
            if (rowType != null && rowType.getFieldNames() != null) {
                return Collections.unmodifiableSet(
                        new HashSet<>(Arrays.asList(rowType.getFieldNames())));
            }
        }
        return Collections.emptySet();
    }

    /** Helper method that consistently handles single records, record maps, and JSON arrays. */
    private void processJsonPayload(String jsonPayload, Collector<SeaTunnelRow> output)
            throws Exception {
        if (jsonPayload == null || jsonPayload.trim().equals("null")) {
            return;
        }
        String trimmed = jsonPayload.trim();
        if (trimmed.startsWith("{")) {
            Map<String, Object> recordMap =
                    OBJECT_MAPPER.readValue(trimmed, new TypeReference<Map<String, Object>>() {});
            if (isSingleRecordObject(recordMap)) {
                emitJsonRecord(trimmed, output);
            } else {
                for (Object value : recordMap.values()) {
                    if (value != null) {
                        String recordJson = OBJECT_MAPPER.writeValueAsString(value);
                        emitJsonRecord(recordJson, output);
                    }
                }
            }
        } else if (trimmed.startsWith("[")) {
            List<Object> recordList =
                    OBJECT_MAPPER.readValue(trimmed, new TypeReference<List<Object>>() {});
            for (Object value : recordList) {
                if (value != null) {
                    String recordJson = OBJECT_MAPPER.writeValueAsString(value);
                    emitJsonRecord(recordJson, output);
                }
            }
        } else {
            throw new SeaTunnelException(
                    "Unexpected JSON payload format from Firebase: " + jsonPayload);
        }
    }

    /**
     * Checks if a JSON map represents a single row record matching the catalog schema rather than a
     * dictionary of child records.
     */
    private boolean isSingleRecordObject(Map<String, Object> map) {
        if (map == null || map.isEmpty() || declaredFields.isEmpty()) {
            return false;
        }

        for (String fieldName : declaredFields) {
            if (map.containsKey(fieldName)) {
                return true;
            }
        }
        return false;
    }

    private void emitJsonRecord(String jsonRecord, Collector<SeaTunnelRow> output)
            throws IOException {
        if (jsonRecord != null && !jsonRecord.trim().equals("null")) {
            SeaTunnelRow row = deserializationSchema.deserialize(jsonRecord.getBytes());
            if (row != null) {
                output.collect(row);
            }
        }
    }

    @Override
    public List<FirebaseSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(splits);
    }

    @Override
    public void addSplits(List<FirebaseSourceSplit> newSplits) {
        splits.addAll(newSplits);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info(
                "Reader subtask [{}] received handleNoMoreSplits signal.",
                context.getIndexOfSubtask());
        this.noMoreSplits = true;
    }

    @Override
    public void close() throws IOException {
        // Cleanup resources if necessary
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
