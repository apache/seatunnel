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
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.firebase.client.FirebaseHttpClient;
import org.apache.seatunnel.connectors.seatunnel.firebase.config.FirebaseSourceOptions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FirebaseSourceReaderTest {
    private SourceReader.Context mockContext;
    private Collector<SeaTunnelRow> mockCollector;
    private FirebaseHttpClient mockHttpClient;
    private CatalogTable catalogTable;
    private ReadonlyConfig config;

    @BeforeEach
    void setUp() {
        mockContext = mock(SourceReader.Context.class);
        mockCollector = mock(Collector.class);
        mockHttpClient = mock(FirebaseHttpClient.class);

        // Build ReadonlyConfig
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FirebaseSourceOptions.PATH.key(), "users");
        configMap.put(FirebaseSourceOptions.URL.key(), "https://test-db.firebaseio.com");
        config = ReadonlyConfig.fromMap(configMap);

        // Build CatalogTable with schema matching {"name": String, "title": String}
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "name",
                                        BasicType.STRING_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        null))
                        .column(
                                PhysicalColumn.of(
                                        "title",
                                        BasicType.STRING_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        null))
                        .build();

        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("default", "default", "users"),
                        tableSchema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "Firebase Users Table");
    }

    @Test
    void testPollNextReadsKeysAndEmitsRows() throws Exception {
        FirebaseSourceReader reader =
                new FirebaseSourceReader(mockContext, config, catalogTable, mockHttpClient);

        FirebaseSourceSplit split =
                new FirebaseSourceSplit("split_0", "users", Collections.singletonList("user_101"));

        // Inject split into reader queue
        reader.addSplits(Collections.singletonList(split));

        // Mock HTTP response for user_101
        when(mockHttpClient.fetchNodeData("user_101"))
                .thenReturn("{\"name\": \"john doe\", \"title\": \"Backend Engineer\"}");

        // Trigger pollNext
        reader.pollNext(mockCollector);

        // Verify row collection and that stream is NOT closed yet
        verify(mockCollector, times(1)).collect(any(SeaTunnelRow.class));
        verify(mockContext, never()).signalNoMoreElement();
    }

    @Test
    void testPollNextSinglePathUnnestsRecordMap() throws Exception {
        FirebaseSourceReader reader =
                new FirebaseSourceReader(mockContext, config, catalogTable, mockHttpClient);

        // Single path split with empty keys
        FirebaseSourceSplit split = new FirebaseSourceSplit("split_single", "users");
        reader.addSplits(Collections.singletonList(split));

        // Mock JSON returning map of user records
        String mapJson =
                "{"
                        + "\"user_101\": {\"name\": \"any name 1\", \"title\": \"Backend Engineer\"},"
                        + "\"user_102\": {\"name\": \"any name 2\", \"title\": \"manager\"}"
                        + "}";
        when(mockHttpClient.fetchNodeData(null)).thenReturn(mapJson);

        reader.pollNext(mockCollector);

        // Should unnest and emit 2 distinct rows
        verify(mockCollector, times(2)).collect(any(SeaTunnelRow.class));
    }

    @Test
    void testSignalNoMoreElementWhenSplitsEmptyAndNoMoreSplitsSet() throws Exception {
        FirebaseSourceReader reader =
                new FirebaseSourceReader(mockContext, config, catalogTable, mockHttpClient);

        // 1. Signal no more splits from enumerator
        reader.handleNoMoreSplits();

        // 2. Poll next on empty queue
        reader.pollNext(mockCollector);

        // 3. Verify signalNoMoreElement was delivered to Zeta engine
        verify(mockContext, times(1)).signalNoMoreElement();
    }
}
