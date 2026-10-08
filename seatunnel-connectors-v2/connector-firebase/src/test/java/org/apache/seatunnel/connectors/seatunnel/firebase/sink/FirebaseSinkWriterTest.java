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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.firebase.client.FirebaseHttpClient;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class FirebaseSinkWriterTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    @Mock private FirebaseHttpClient mockHttpClient;

    @Captor private ArgumentCaptor<String> payloadCaptor;

    private SeaTunnelRowType defaultRowType;
    private CatalogTable defaultCatalogTable;

    @BeforeEach
    void setUp() {
        defaultRowType =
                new SeaTunnelRowType(
                        new String[] {"id", "dept_id", "name", "age"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.INT_TYPE
                        });

        TableSchema schema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.INT_TYPE, 0, false, null, "id"))
                        .column(
                                PhysicalColumn.of(
                                        "dept_id",
                                        BasicType.STRING_TYPE,
                                        0,
                                        false,
                                        null,
                                        "dept_id"))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 0, true, null, "name"))
                        .column(PhysicalColumn.of("age", BasicType.INT_TYPE, 0, true, null, "age"))
                        .build();

        defaultCatalogTable =
                CatalogTable.of(
                        TableIdentifier.of("default", "default", "users"),
                        schema,
                        new HashMap<>(),
                        Collections.emptyList(),
                        "");
    }

    private ReadonlyConfig createConfig(Map<String, Object> map) {
        Map<String, Object> baseMap = new HashMap<>();
        baseMap.put("url", "https://test-db.firebaseio.com");
        baseMap.put("path", "users");
        baseMap.put("batch_size", 2);
        baseMap.put("retry_max", 2);
        baseMap.putAll(map);
        return ReadonlyConfig.fromMap(baseMap);
    }

    @Nested
    @DisplayName("1. Path and Key Resolution Tests")
    class KeyResolutionTests {
        @Test
        @DisplayName("Single Primary Key Resolution")
        void testSinglePrimaryKeyResolution() throws IOException {
            Map<String, Object> map = new HashMap<>();
            map.put("primary_keys", Collections.singletonList("id"));
            map.put("batch_size", 1); // Immediate flush

            FirebaseSinkWriter writer =
                    new FirebaseSinkWriter(mockHttpClient, defaultCatalogTable, createConfig(map));

            SeaTunnelRow row = new SeaTunnelRow(new Object[] {101, "eng", "John", 25});
            writer.write(row);

            verify(mockHttpClient).executePatch(eq(""), payloadCaptor.capture());
            Map<String, Object> payload =
                    OBJECT_MAPPER.readValue(
                            payloadCaptor.getValue(), new TypeReference<Map<String, Object>>() {});

            assertTrue(payload.containsKey("users/101"));
        }

        @Test
        @DisplayName("Multi-Column Composite Primary Key Resolution with Delimiter")
        void testCompositePrimaryKeyResolution() throws IOException {
            Map<String, Object> map = new HashMap<>();
            map.put("primary_keys", Arrays.asList("dept_id", "id"));
            map.put("key_delimiter", "#");
            map.put("batch_size", 1);

            FirebaseSinkWriter writer =
                    new FirebaseSinkWriter(mockHttpClient, defaultCatalogTable, createConfig(map));

            SeaTunnelRow row = new SeaTunnelRow(new Object[] {101, "eng", "John", 25});
            writer.write(row);

            verify(mockHttpClient).executePatch(eq(""), payloadCaptor.capture());
            Map<String, Object> payload =
                    OBJECT_MAPPER.readValue(
                            payloadCaptor.getValue(), new TypeReference<Map<String, Object>>() {});

            assertTrue(payload.containsKey("users/eng#101"));
        }

        @Test
        @DisplayName("Prefix and Postfix Customization")
        void testPrefixAndPostfixKeyResolution() throws IOException {
            Map<String, Object> map = new HashMap<>();
            map.put("primary_keys", Collections.singletonList("id"));
            map.put("key_prefix", "usr_");
            map.put("key_postfix", "_node");
            map.put("batch_size", 1);

            FirebaseSinkWriter writer =
                    new FirebaseSinkWriter(mockHttpClient, defaultCatalogTable, createConfig(map));

            SeaTunnelRow row = new SeaTunnelRow(new Object[] {50, "hr", "Alice", 30});
            writer.write(row);

            verify(mockHttpClient).executePatch(eq(""), payloadCaptor.capture());
            Map<String, Object> payload =
                    OBJECT_MAPPER.readValue(
                            payloadCaptor.getValue(), new TypeReference<Map<String, Object>>() {});

            assertTrue(payload.containsKey("users/usr_50_node"));
        }

        @Test
        @DisplayName("Primary Key fallback to CatalogTable PrimaryKey Schema")
        void testFallbackToCatalogTablePrimaryKey() throws IOException {
            TableSchema schemaWithPk =
                    TableSchema.builder()
                            .column(
                                    PhysicalColumn.of(
                                            "id", BasicType.INT_TYPE, 0, false, null, "id"))
                            .column(
                                    PhysicalColumn.of(
                                            "name", BasicType.STRING_TYPE, 0, true, null, "name"))
                            .primaryKey(
                                    PrimaryKey.of(
                                            "pk_users",
                                            Collections.singletonList(
                                                    "id"))) // <--- Use primaryKey() directly
                            .build();

            CatalogTable catalogTableWithPk =
                    CatalogTable.of(
                            TableIdentifier.of("default", "default", "users"),
                            schemaWithPk,
                            new HashMap<>(),
                            Collections.emptyList(),
                            "");

            Map<String, Object> map = new HashMap<>();
            map.put("batch_size", 1);

            FirebaseSinkWriter writer =
                    new FirebaseSinkWriter(mockHttpClient, catalogTableWithPk, createConfig(map));

            SeaTunnelRow row = new SeaTunnelRow(new Object[] {99, "FallbackUser"});
            writer.write(row);

            verify(mockHttpClient).executePatch(eq(""), payloadCaptor.capture());
            Map<String, Object> payload =
                    OBJECT_MAPPER.readValue(
                            payloadCaptor.getValue(), new TypeReference<Map<String, Object>>() {});

            assertTrue(payload.containsKey("users/99"));
        }

        @Test
        @DisplayName("Root path configuration (path is empty string)")
        void testRootPathConfiguration() throws IOException {
            Map<String, Object> map = new HashMap<>();
            map.put("path", "");
            map.put("primary_keys", Collections.singletonList("id"));
            map.put("batch_size", 1);

            FirebaseSinkWriter writer =
                    new FirebaseSinkWriter(mockHttpClient, defaultCatalogTable, createConfig(map));

            SeaTunnelRow row = new SeaTunnelRow(new Object[] {7, "eng", "Bob", 40});
            writer.write(row);

            verify(mockHttpClient).executePatch(eq(""), payloadCaptor.capture());
            Map<String, Object> payload =
                    OBJECT_MAPPER.readValue(
                            payloadCaptor.getValue(), new TypeReference<Map<String, Object>>() {});

            assertTrue(payload.containsKey("7"));
        }
    }

    @Nested
    @DisplayName("2. Row Operations & Serialization Tests")
    class OperationsAndSerializationTests {
        @Test
        @DisplayName("CDC DELETE row maps to null payload in multi-location PATCH")
        void testDeleteRowMapping() throws IOException {
            Map<String, Object> map = new HashMap<>();
            map.put("primary_keys", Collections.singletonList("id"));
            map.put("support_deletes", true);
            map.put("batch_size", 1);

            FirebaseSinkWriter writer =
                    new FirebaseSinkWriter(mockHttpClient, defaultCatalogTable, createConfig(map));

            SeaTunnelRow deleteRow = new SeaTunnelRow(new Object[] {101, "eng", "Kareem", 25});
            deleteRow.setRowKind(RowKind.DELETE);

            writer.write(deleteRow);

            verify(mockHttpClient).executePatch(eq(""), payloadCaptor.capture());
            Map<String, Object> payload =
                    OBJECT_MAPPER.readValue(
                            payloadCaptor.getValue(), new TypeReference<Map<String, Object>>() {});

            assertTrue(payload.containsKey("users/101"));
            assertNull(payload.get("users/101"));
        }

        @Test
        @DisplayName("Ignore DELETE records when support_deletes is false")
        void testDisabledDeleteSupport() throws IOException {
            Map<String, Object> map = new HashMap<>();
            map.put("primary_keys", Collections.singletonList("id"));
            map.put("support_deletes", false);
            map.put("batch_size", 1);

            FirebaseSinkWriter writer =
                    new FirebaseSinkWriter(mockHttpClient, defaultCatalogTable, createConfig(map));

            SeaTunnelRow deleteRow = new SeaTunnelRow(new Object[] {101, "eng", "John", 25});
            deleteRow.setRowKind(RowKind.DELETE);

            writer.write(deleteRow);
            writer.close();

            verify(mockHttpClient, never()).executePatch(anyString(), anyString());
        }

        @Test
        @DisplayName("ignore_null_values=true excludes null fields from row payload")
        void testIgnoreNullValuesTrue() throws IOException {
            Map<String, Object> map = new HashMap<>();
            map.put("primary_keys", Collections.singletonList("id"));
            map.put("ignore_null_values", true);
            map.put("batch_size", 1);

            FirebaseSinkWriter writer =
                    new FirebaseSinkWriter(mockHttpClient, defaultCatalogTable, createConfig(map));

            SeaTunnelRow rowWithNulls = new SeaTunnelRow(new Object[] {200, "eng", null, 28});
            writer.write(rowWithNulls);

            verify(mockHttpClient).executePatch(eq(""), payloadCaptor.capture());
            Map<String, Object> payload =
                    OBJECT_MAPPER.readValue(
                            payloadCaptor.getValue(), new TypeReference<Map<String, Object>>() {});

            @SuppressWarnings("unchecked")
            Map<String, Object> rowMap = (Map<String, Object>) payload.get("users/200");

            assertEquals(200, rowMap.get("id"));
            assertEquals("eng", rowMap.get("dept_id"));
            assertEquals(28, rowMap.get("age"));
            assertFalse(rowMap.containsKey("name"));
        }

        @Test
        @DisplayName("ignore_null_values=false retains null fields in payload")
        void testIgnoreNullValuesFalse() throws IOException {
            Map<String, Object> map = new HashMap<>();
            map.put("primary_keys", Collections.singletonList("id"));
            map.put("ignore_null_values", false);
            map.put("batch_size", 1);

            FirebaseSinkWriter writer =
                    new FirebaseSinkWriter(mockHttpClient, defaultCatalogTable, createConfig(map));

            SeaTunnelRow rowWithNulls = new SeaTunnelRow(new Object[] {200, "eng", null, 28});
            writer.write(rowWithNulls);

            verify(mockHttpClient).executePatch(eq(""), payloadCaptor.capture());
            Map<String, Object> payload =
                    OBJECT_MAPPER.readValue(
                            payloadCaptor.getValue(), new TypeReference<Map<String, Object>>() {});

            @SuppressWarnings("unchecked")
            Map<String, Object> rowMap = (Map<String, Object>) payload.get("users/200");

            assertTrue(rowMap.containsKey("name"));
            assertNull(rowMap.get("name"));
        }
    }

    @Nested
    @DisplayName("3. Batch Buffering & Retry Logic Tests")
    class BatchingAndRetryTests {
        @Test
        @DisplayName("Buffer flushes automatically when batch_size is reached")
        void testBatchSizeFlushTrigger() throws IOException {
            Map<String, Object> map = new HashMap<>();
            map.put("primary_keys", Collections.singletonList("id"));
            map.put("batch_size", 2);

            FirebaseSinkWriter writer =
                    new FirebaseSinkWriter(mockHttpClient, defaultCatalogTable, createConfig(map));

            SeaTunnelRow row1 = new SeaTunnelRow(new Object[] {1, "hr", "User1", 20});
            SeaTunnelRow row2 = new SeaTunnelRow(new Object[] {2, "hr", "User2", 22});

            writer.write(row1);
            verify(mockHttpClient, never()).executePatch(anyString(), anyString());

            writer.write(row2);
            verify(mockHttpClient, times(1)).executePatch(eq(""), payloadCaptor.capture());

            Map<String, Object> payload =
                    OBJECT_MAPPER.readValue(
                            payloadCaptor.getValue(), new TypeReference<Map<String, Object>>() {});

            assertEquals(2, payload.size());
            assertTrue(payload.containsKey("users/1"));
            assertTrue(payload.containsKey("users/2"));
        }

        @Test
        @DisplayName("close() flushes remaining uncommitted buffered records")
        void testCloseFlushesBuffer() throws IOException {
            Map<String, Object> map = new HashMap<>();
            map.put("primary_keys", Collections.singletonList("id"));
            map.put("batch_size", 5);

            FirebaseSinkWriter writer =
                    new FirebaseSinkWriter(mockHttpClient, defaultCatalogTable, createConfig(map));

            SeaTunnelRow row = new SeaTunnelRow(new Object[] {10, "fin", "User10", 35});
            writer.write(row);

            verify(mockHttpClient, never()).executePatch(anyString(), anyString());

            writer.close();
            verify(mockHttpClient, times(1)).executePatch(eq(""), payloadCaptor.capture());
        }

        @Test
        @DisplayName("Retry logic retries upon transient network failure and succeeds")
        void testRetryLogicSuccess() throws IOException {
            Map<String, Object> map = new HashMap<>();
            map.put("primary_keys", Collections.singletonList("id"));
            map.put("batch_size", 1);
            map.put("retry_max", 3);

            doThrow(new SeaTunnelException("Transient connection timeout"))
                    .doNothing()
                    .when(mockHttpClient)
                    .executePatch(anyString(), anyString());

            FirebaseSinkWriter writer =
                    new FirebaseSinkWriter(mockHttpClient, defaultCatalogTable, createConfig(map));

            SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "eng", "RetryUser", 25});
            writer.write(row);

            verify(mockHttpClient, times(2)).executePatch(eq(""), anyString());
        }

        @Test
        @DisplayName("Exhausting retry_max throws SeaTunnelException")
        void testExhaustRetriesThrowsException() {
            Map<String, Object> map = new HashMap<>();
            map.put("primary_keys", Collections.singletonList("id"));
            map.put("batch_size", 1);
            map.put("retry_max", 2);

            doThrow(new SeaTunnelException("Persistent 503 Server Error"))
                    .when(mockHttpClient)
                    .executePatch(anyString(), anyString());

            FirebaseSinkWriter writer =
                    new FirebaseSinkWriter(mockHttpClient, defaultCatalogTable, createConfig(map));

            SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "eng", "RetryUser", 25});

            SeaTunnelException ex = assertThrows(SeaTunnelException.class, () -> writer.write(row));
            assertTrue(
                    ex.getMessage()
                            .contains(
                                    "Failed to write batch payload to Firebase after 2 attempts"));
        }
    }
}
