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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.firebase.client.FirebaseHttpClient;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

@Slf4j
public class FirebaseSourceReader implements SourceReader<SeaTunnelRow, FirebaseSourceSplit> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final SourceReader.Context context;
    private final ReadonlyConfig config;
    private final CatalogTable catalogTable;
    private final FirebaseHttpClient httpClient;
    private final JsonDeserializationSchema deserializationSchema;

    private final Queue<FirebaseSourceSplit> splits = new ArrayDeque<>();
    private volatile boolean noMoreSplits = false;

    public FirebaseSourceReader(
            SourceReader.Context context, ReadonlyConfig config, CatalogTable catalogTable) {
        this.context = context;
        this.config = config;
        this.catalogTable = catalogTable;
        this.httpClient = new FirebaseHttpClient(config);

        SeaTunnelRowType rowType = catalogTable.getTableSchema().toPhysicalRowDataType();
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
        this.deserializationSchema = new JsonDeserializationSchema(false, false, rowType);
    }

    @Override
    public void open() throws Exception {
        // No persistent connection setup required for REST client
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        log.info("Polling next split...");
        synchronized (splits) {
            if (splits.isEmpty()) {
                log.info("splits are empty...");
                if (noMoreSplits) {
                    log.info("notify engine , no more splits");
                    context.signalNoMoreElement();
                } else {
                    Thread.sleep(100);
                }
                return;
            }
            FirebaseSourceSplit split = splits.poll();
            if (split != null) {
                readSplit(split, output);
            }
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
            // Processing Strategy A: Read records by explicit keys in batch
            for (String key : keys) {
                String jsonPayload = httpClient.fetchNodeData(key);
                emitJsonRecord(jsonPayload, output);
            }
        } else {
            // Processing Strategy B: Read entire single path directly
            String jsonPayload = httpClient.fetchNodeData(null);
            if (jsonPayload == null || jsonPayload.trim().equals("null")) {
                return;
            }
            jsonPayload = jsonPayload.trim();
            if (jsonPayload.startsWith("{")) {
                Map<String, Object> recordMap =
                        OBJECT_MAPPER.readValue(
                                jsonPayload, new TypeReference<Map<String, Object>>() {});

                for (Object value : recordMap.values()) {
                    String recordJson = OBJECT_MAPPER.writeValueAsString(value);
                    emitJsonRecord(recordJson, output);
                }
            } else if (jsonPayload.startsWith("[")) {
                List<Object> recordList =
                        OBJECT_MAPPER.readValue(jsonPayload, new TypeReference<List<Object>>() {});

                for (Object value : recordList) {
                    String recordJson = OBJECT_MAPPER.writeValueAsString(value);
                    emitJsonRecord(recordJson, output);
                }
            } else {
                throw new SeaTunnelException(
                        "Unexpected JSON payload format from Firebase: " + jsonPayload);
            }
        }
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
        synchronized (splits) {
            return new ArrayList<>(splits);
        }
    }

    @Override
    public void addSplits(List<FirebaseSourceSplit> newSplits) {
        synchronized (splits) {
            splits.addAll(newSplits);
        }
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
