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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.apache.hugegraph.structure.constant.IdStrategy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

class HugeGraphSinkConfigTest {
    // Automatically create mock objects using @Mock annotation
    @Mock private ReadonlyConfig mockConfig;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testOf_shouldCreateConfigFromReadonlyConfig() {
        // --- 1. Arrange ---
        // Define and stub the expected values from mockConfig.
        String expectedHost = "127.0.0.1";
        int expectedPort = 8080;
        String expectedGraph = "my_graph";
        String expectedUsername = "test_user";
        String expectedProperty = "{test_password}";

        // Required fields stubbing
        when(mockConfig.get(HugeGraphOptions.HOST)).thenReturn(expectedHost);
        when(mockConfig.get(HugeGraphOptions.PORT)).thenReturn(expectedPort);
        when(mockConfig.get(HugeGraphOptions.GRAPH_NAME)).thenReturn(expectedGraph);
        when(mockConfig.getOptional(HugeGraphOptions.BATCH_SIZE)).thenReturn(Optional.of(1024));
        when(mockConfig.getOptional(HugeGraphOptions.BATCH_INTERVAL_MS))
                .thenReturn(Optional.of(500));
        when(mockConfig.getOptional(HugeGraphOptions.MAX_RETRIES)).thenReturn(Optional.of(5));
        when(mockConfig.getOptional(HugeGraphOptions.RETRY_BACKOFF_MS))
                .thenReturn(Optional.of(200));

        // Optional fields stubbing
        when(mockConfig.getOptional(HugeGraphOptions.USERNAME))
                .thenReturn(Optional.of(expectedUsername));
        when(mockConfig.getOptional(HugeGraphOptions.PASSWORD)).thenReturn(Optional.empty());
        when(mockConfig.getOptional(HugeGraphOptions.GRAPH_SPACE)).thenReturn(Optional.empty());
        when(mockConfig.getOptional(HugeGraphSinkOptions.SELECTED_FIELDS))
                .thenReturn(Optional.empty());
        when(mockConfig.getOptional(HugeGraphSinkOptions.IGNORED_FIELDS))
                .thenReturn(Optional.empty());

        // --- 2. Act ---
        // Call the static method under test.
        HugeGraphSinkConfig actualSinkConfig = HugeGraphSinkConfig.of(mockConfig);

        // --- 3. Assert ---
        // Verify that the values in the returned sinkConfig object are as expected.
        assertNotNull(actualSinkConfig);
        assertEquals(expectedHost, actualSinkConfig.getHost());
        assertEquals(expectedPort, actualSinkConfig.getPort());
        assertEquals(expectedGraph, actualSinkConfig.getGraphName());
        assertEquals(1024, actualSinkConfig.getBatchSize());

        assertEquals(expectedUsername, actualSinkConfig.getUsername());
        assertNull(actualSinkConfig.getPassword());
    }

    @Test
    void testDefaultValues() {
        // 1. Arrange: Create a map with only required fields, omitting those with defaults
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "127.0.0.1");
        configMap.put("port", 8080);
        configMap.put("graph_name", "hugegraph");

        // Note: batch_size, batch_interval_ms, max_retries, retry_backoff_ms are omitted

        // 2. Act: Create ReadonlyConfig and parse it
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        HugeGraphSinkConfig sinkConfig = HugeGraphSinkConfig.of(config);

        // 3. Assert: Verify that the omitted fields are populated with their default values
        assertNotNull(sinkConfig);
        assertEquals(
                HugeGraphOptions.BATCH_SIZE.defaultValue(),
                sinkConfig.getBatchSize(),
                "Batch size should fall back to the default value");
        assertEquals(
                HugeGraphOptions.BATCH_INTERVAL_MS.defaultValue(),
                sinkConfig.getBatchIntervalMs(),
                "Batch interval should fall back to the default value");
        assertEquals(
                HugeGraphOptions.MAX_RETRIES.defaultValue(),
                sinkConfig.getMaxRetries(),
                "Max retries should fall back to the default value");
        assertEquals(
                HugeGraphOptions.RETRY_BACKOFF_MS.defaultValue(),
                sinkConfig.getRetryBackoffMs(),
                "Retry backoff should fall back to the default value");
    }

    @Test
    void testFullConfigMapping() {
        // 1. Arrange: Create a comprehensive configuration map
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.1");
        configMap.put("port", 8888);
        configMap.put("graph_name", "full_graph");
        configMap.put("graph_space", "full_space");
        configMap.put("username", "admin");
        configMap.put("password", "pa$$w0rd");
        configMap.put("batch_size", 100);
        configMap.put("batch_interval_ms", 2000);
        configMap.put("max_retries", 10);
        configMap.put("retry_backoff_ms", 1000);
        configMap.put("selected_fields", Collections.singletonList("name"));
        configMap.put("ignored_fields", Collections.singletonList("id"));

        Map<String, String> propertyMapping = new HashMap<>();
        propertyMapping.put("name", "vertex_name");
        configMap.put("property_mapping", propertyMapping);

        Map<String, Object> schema = new HashMap<>();
        schema.put("type", "VERTEX");
        schema.put("label", "device");
        schema.put("idStrategy", "CUSTOMIZE_UUID");
        schema.put("idFields", Collections.singletonList("device_id"));
        configMap.put("schema_config", schema);

        // 2. Act: Create ReadonlyConfig and parse it
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);
        HugeGraphSinkConfig sinkConfig = HugeGraphSinkConfig.of(readonlyConfig);

        // 3. Assert: Verify all fields are correctly parsed
        assertNotNull(sinkConfig);
        assertEquals("192.168.1.1", sinkConfig.getHost());
        assertEquals(8888, sinkConfig.getPort());
        assertEquals("full_graph", sinkConfig.getGraphName());
        assertEquals("full_space", sinkConfig.getGraphSpace());
        assertEquals("admin", sinkConfig.getUsername());
        assertEquals("pa$$w0rd", sinkConfig.getPassword());
        assertEquals(100, sinkConfig.getBatchSize());
        assertEquals(2000, sinkConfig.getBatchIntervalMs());
        assertEquals(10, sinkConfig.getMaxRetries());
        assertEquals(1000, sinkConfig.getRetryBackoffMs());

        // Assert collections and maps
        assertEquals(1, sinkConfig.getSelectedFields().size());
        assertEquals("name", sinkConfig.getSelectedFields().get(0));
        assertEquals(1, sinkConfig.getIgnoredFields().size());
        assertEquals("id", sinkConfig.getIgnoredFields().get(0));

        // Assert nested schema object
        assertNotNull(sinkConfig.getSchemaConfig());
        assertEquals(SchemaConfig.LabelType.VERTEX, sinkConfig.getSchemaConfig().getType());
        assertEquals("device", sinkConfig.getSchemaConfig().getLabel());
        assertEquals(IdStrategy.CUSTOMIZE_UUID, sinkConfig.getSchemaConfig().getIdStrategy());
        assertEquals(
                Collections.singletonList("device_id"), sinkConfig.getSchemaConfig().getIdFields());
    }

    @Test
    void testEdgeSchemaConfigParsing() {
        // 1. Arrange: Create a configuration map for an edge schema
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "localhost");
        configMap.put("port", 8080);
        configMap.put("graph_name", "edge_graph");

        Map<String, Object> schema = new HashMap<>();
        schema.put("type", "EDGE");
        schema.put("label", "knows");
        schema.put("tablePath", "db1.person_friends");

        Map<String, Object> sourceConfig = new HashMap<>();
        sourceConfig.put("label", "person");
        sourceConfig.put("idFields", Collections.singletonList("person_id"));
        schema.put("sourceConfig", sourceConfig);

        Map<String, Object> targetConfig = new HashMap<>();
        targetConfig.put("label", "person");
        targetConfig.put("idFields", Collections.singletonList("friend_id"));
        schema.put("targetConfig", targetConfig);

        configMap.put("schema_config", schema);

        // 2. Act: Create ReadonlyConfig and parse it
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);
        HugeGraphSinkConfig sinkConfig = HugeGraphSinkConfig.of(readonlyConfig);

        // 3. Assert: Verify the edge schema fields are correctly parsed
        assertNotNull(sinkConfig);
        assertNotNull(sinkConfig.getSchemaConfig());
        SchemaConfig schemaConfig = sinkConfig.getSchemaConfig();

        assertEquals(SchemaConfig.LabelType.EDGE, schemaConfig.getType());
        assertEquals("knows", schemaConfig.getLabel());
        assertEquals("db1.person_friends", schemaConfig.getTablePath());

        assertNotNull(schemaConfig.getSourceConfig());
        assertEquals("person", schemaConfig.getSourceConfig().getLabel());
        assertEquals(
                Collections.singletonList("person_id"),
                schemaConfig.getSourceConfig().getIdFields());

        assertNotNull(schemaConfig.getTargetConfig());
        assertEquals("person", schemaConfig.getTargetConfig().getLabel());
        assertEquals(
                Collections.singletonList("friend_id"),
                schemaConfig.getTargetConfig().getIdFields());
    }
}
