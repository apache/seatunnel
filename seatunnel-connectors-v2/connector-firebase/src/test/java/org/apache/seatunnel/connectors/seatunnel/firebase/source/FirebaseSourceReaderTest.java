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
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.firebase.client.FirebaseHttpClient;
import org.apache.seatunnel.connectors.seatunnel.firebase.config.FirebaseSourceOptions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        reader.addSplits(Collections.singletonList(split));

        when(mockHttpClient.fetchNodeData("user_101"))
                .thenReturn("{\"name\": \"john doe\", \"title\": \"Backend Engineer\"}");

        reader.pollNext(mockCollector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(mockCollector, times(1)).collect(captor.capture());

        SeaTunnelRow row = captor.getValue();
        assertNotNull(row);
        assertEquals("john doe", row.getField(0));
        assertEquals("Backend Engineer", row.getField(1));

        verify(mockContext, never()).signalNoMoreElement();
    }

    @Test
    void testPollNextSinglePathUnnestsRecordMap() throws Exception {
        FirebaseSourceReader reader =
                new FirebaseSourceReader(mockContext, config, catalogTable, mockHttpClient);

        FirebaseSourceSplit split = new FirebaseSourceSplit("split_single", "users");
        reader.addSplits(Collections.singletonList(split));

        String mapJson =
                "{"
                        + "\"user_101\": {\"name\": \"any name 1\", \"title\": \"Backend Engineer\"},"
                        + "\"user_102\": {\"name\": \"any name 2\", \"title\": \"manager\"}"
                        + "}";
        when(mockHttpClient.fetchNodeData(null)).thenReturn(mapJson);

        reader.pollNext(mockCollector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(mockCollector, times(2)).collect(captor.capture());

        List<SeaTunnelRow> rows = captor.getAllValues();
        assertEquals("any name 1", rows.get(0).getField(0));
        assertEquals("Backend Engineer", rows.get(0).getField(1));
        assertEquals("any name 2", rows.get(1).getField(0));
        assertEquals("manager", rows.get(1).getField(1));
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

    @Test
    void testPollNextWithUndeclaredObjectKeyDoesNotDiscardRow() throws Exception {
        FirebaseSourceReader reader =
                new FirebaseSourceReader(mockContext, config, catalogTable, mockHttpClient);

        // Split containing declared schema fields ("name", "title") and an undeclared object key
        // ("audit")
        FirebaseSourceSplit split =
                new FirebaseSourceSplit(
                        "split_0", "users", Arrays.asList("name", "title", "audit"));
        reader.addSplits(Collections.singletonList(split));

        // Mock HTTP responses for each key in the split
        when(mockHttpClient.fetchNodeData("name")).thenReturn("\"john doe\"");
        when(mockHttpClient.fetchNodeData("title")).thenReturn("\"Backend Engineer\"");
        // "audit" is NOT in catalogTable schema and returns a nested JSON object payload
        when(mockHttpClient.fetchNodeData("audit"))
                .thenReturn("{\"created_at\": 1700000000, \"updated_by\": \"system\"}");

        // Act
        reader.pollNext(mockCollector);

        // Assert: Verify that the row for declared fields was captured and emitted,
        // and was NOT silently discarded due to the presence of the "audit" JSON object key.
        ArgumentCaptor<SeaTunnelRow> rowCaptor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(mockCollector, times(1)).collect(rowCaptor.capture());

        SeaTunnelRow emittedRow = rowCaptor.getValue();
        assertNotNull(emittedRow);
        assertEquals("john doe", emittedRow.getField(0));
        assertEquals("Backend Engineer", emittedRow.getField(1));
    }

    @Test
    void testPollNextWithNullAndLiteralNullPayloads() throws Exception {
        FirebaseSourceReader reader =
                new FirebaseSourceReader(mockContext, config, catalogTable, mockHttpClient);

        FirebaseSourceSplit split =
                new FirebaseSourceSplit("split_nulls", "users", Arrays.asList("key1", "key2"));
        reader.addSplits(Collections.singletonList(split));

        when(mockHttpClient.fetchNodeData("key1")).thenReturn(null);
        when(mockHttpClient.fetchNodeData("key2")).thenReturn("  null  ");

        reader.pollNext(mockCollector);

        verify(mockCollector, never()).collect(any(SeaTunnelRow.class));
    }

    @Test
    void testPollNextSinglePathNullResponse() throws Exception {
        FirebaseSourceReader reader =
                new FirebaseSourceReader(mockContext, config, catalogTable, mockHttpClient);

        FirebaseSourceSplit split = new FirebaseSourceSplit("split_single_null", "users");
        reader.addSplits(Collections.singletonList(split));

        when(mockHttpClient.fetchNodeData(null)).thenReturn("null");

        reader.pollNext(mockCollector);

        verify(mockCollector, never()).collect(any(SeaTunnelRow.class));
    }

    @Test
    void testPollNextSinglePathReturnsJsonArrayOfRecords() throws Exception {
        FirebaseSourceReader reader =
                new FirebaseSourceReader(mockContext, config, catalogTable, mockHttpClient);

        FirebaseSourceSplit split = new FirebaseSourceSplit("split_array", "users");
        reader.addSplits(Collections.singletonList(split));

        String arrayJson =
                "["
                        + "{\"name\": \"Alice\", \"title\": \"Lead\"},"
                        + "{\"name\": \"Bob\", \"title\": \"Dev\"}"
                        + "]";
        when(mockHttpClient.fetchNodeData(null)).thenReturn(arrayJson);

        reader.pollNext(mockCollector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(mockCollector, times(2)).collect(captor.capture());

        List<SeaTunnelRow> rows = captor.getAllValues();
        assertEquals("Alice", rows.get(0).getField(0));
        assertEquals("Bob", rows.get(1).getField(0));
    }

    @Test
    void testPollNextWithUndeclaredArrayKey() throws Exception {
        FirebaseSourceReader reader =
                new FirebaseSourceReader(mockContext, config, catalogTable, mockHttpClient);

        FirebaseSourceSplit split =
                new FirebaseSourceSplit(
                        "split_arr_key", "users", Arrays.asList("name", "team_members"));
        reader.addSplits(Collections.singletonList(split));

        when(mockHttpClient.fetchNodeData("name")).thenReturn("\"Project X\"");
        when(mockHttpClient.fetchNodeData("team_members"))
                .thenReturn("[{\"name\": \"Member 1\", \"title\": \"QA\"}]");

        reader.pollNext(mockCollector);

        // Expect 1 row from reconstructedMap + 1 row un-nested from the array = 2 rows
        verify(mockCollector, times(2)).collect(any(SeaTunnelRow.class));
    }

    @Test
    void testProcessJsonPayloadWithPrimitiveThrowsException() throws Exception {
        FirebaseSourceReader reader =
                new FirebaseSourceReader(mockContext, config, catalogTable, mockHttpClient);

        FirebaseSourceSplit split = new FirebaseSourceSplit("split_prim", "users");
        reader.addSplits(Collections.singletonList(split));

        when(mockHttpClient.fetchNodeData(null)).thenReturn("\"just a string payload\"");

        assertThrows(SeaTunnelException.class, () -> reader.pollNext(mockCollector));
    }

    @Test
    void testPollNextFallbackToRawStringOnParseError() throws Exception {
        FirebaseSourceReader reader =
                new FirebaseSourceReader(mockContext, config, catalogTable, mockHttpClient);

        FirebaseSourceSplit split =
                new FirebaseSourceSplit("split_raw", "users", Collections.singletonList("name"));
        reader.addSplits(Collections.singletonList(split));

        // Malformed JSON value for name field
        when(mockHttpClient.fetchNodeData("name")).thenReturn("unquoted_raw_string");

        reader.pollNext(mockCollector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(mockCollector, times(1)).collect(captor.capture());
        assertEquals("unquoted_raw_string", captor.getValue().getField(0));
    }

    @Test
    void testPollNextDeeplyNestedContainerUnnesting() throws Exception {
        FirebaseSourceReader reader =
                new FirebaseSourceReader(mockContext, config, catalogTable, mockHttpClient);

        FirebaseSourceSplit split = new FirebaseSourceSplit("split_deep", "users");
        reader.addSplits(Collections.singletonList(split));

        // Deeply nested structure: root -> department -> team -> user record
        String deepJson =
                "{"
                        + "\"engineering\": {"
                        + "    \"backend\": {"
                        + "        \"user_999\": {\"name\": \"Charlie\", \"title\": \"Architect\"}"
                        + "    }"
                        + "}"
                        + "}";
        when(mockHttpClient.fetchNodeData(null)).thenReturn(deepJson);

        reader.pollNext(mockCollector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(mockCollector, times(1)).collect(captor.capture());
        assertEquals("Charlie", captor.getValue().getField(0));
        assertEquals("Architect", captor.getValue().getField(1));
    }

    @Test
    void testPollNextProcessesMultipleSplitsSequentially() throws Exception {
        FirebaseSourceReader reader =
                new FirebaseSourceReader(mockContext, config, catalogTable, mockHttpClient);

        FirebaseSourceSplit split1 =
                new FirebaseSourceSplit("s1", "users", Collections.singletonList("user_1"));
        FirebaseSourceSplit split2 =
                new FirebaseSourceSplit("s2", "users", Collections.singletonList("user_2"));

        reader.addSplits(Arrays.asList(split1, split2));

        when(mockHttpClient.fetchNodeData("user_1"))
                .thenReturn("{\"name\": \"User 1\", \"title\": \"Dev\"}");
        when(mockHttpClient.fetchNodeData("user_2"))
                .thenReturn("{\"name\": \"User 2\", \"title\": \"QA\"}");

        // First poll processes split1
        reader.pollNext(mockCollector);
        verify(mockCollector, times(1)).collect(any(SeaTunnelRow.class));

        // Second poll processes split2
        reader.pollNext(mockCollector);
        verify(mockCollector, times(2)).collect(any(SeaTunnelRow.class));
    }

    @Test
    void testSnapshotStateAndAddSplits() throws Exception {
        FirebaseSourceReader reader =
                new FirebaseSourceReader(mockContext, config, catalogTable, mockHttpClient);

        FirebaseSourceSplit split =
                new FirebaseSourceSplit("s1", "users", Collections.singletonList("k1"));

        reader.addSplits(Collections.singletonList(split));
        List<FirebaseSourceSplit> state = reader.snapshotState(1L);

        assertNotNull(state);
        assertEquals(1, state.size());
        assertEquals("s1", state.get(0).splitId());
    }

    @Test
    void testPollNextMergesScalarKeysIntoSingleRow() throws Exception {
        FirebaseSourceReader reader =
                new FirebaseSourceReader(mockContext, config, catalogTable, mockHttpClient);

        FirebaseSourceSplit split =
                new FirebaseSourceSplit("split_scalars", "users", Arrays.asList("name", "title"));
        reader.addSplits(Collections.singletonList(split));

        when(mockHttpClient.fetchNodeData("name")).thenReturn("\"Jane Doe\"");
        when(mockHttpClient.fetchNodeData("title")).thenReturn("\"Data Engineer\"");

        reader.pollNext(mockCollector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(mockCollector, times(1)).collect(captor.capture());

        SeaTunnelRow row = captor.getValue();
        assertEquals("Jane Doe", row.getField(0));
        assertEquals("Data Engineer", row.getField(1));
    }

    @Test
    void testPollNextPreservesNonAsciiCharacters() throws Exception {
        FirebaseSourceReader reader =
                new FirebaseSourceReader(mockContext, config, catalogTable, mockHttpClient);

        FirebaseSourceSplit split =
                new FirebaseSourceSplit(
                        "split_utf8", "users", Collections.singletonList("user_unicode"));
        reader.addSplits(Collections.singletonList(split));

        when(mockHttpClient.fetchNodeData("user_unicode"))
                .thenReturn(
                        "{\"name\": \"عربي / 王伟 / René\", \"title\": \"Software Engineer 🚀\"}");

        reader.pollNext(mockCollector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(mockCollector, times(1)).collect(captor.capture());

        SeaTunnelRow row = captor.getValue();
        assertEquals("عربي / 王伟 / René", row.getField(0));
        assertEquals("Software Engineer 🚀", row.getField(1));
    }
}
