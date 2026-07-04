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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

class HugeGraphSourceConfigTest {

    @Mock private ReadonlyConfig mockConfig;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testOf_shouldCreateVertexSourceConfig() {
        String expectedHost = "127.0.0.1";
        int expectedPort = 8080;
        String expectedGraph = "my_graph";
        String expectedLabel = "person";

        when(mockConfig.get(HugeGraphOptions.HOST)).thenReturn(expectedHost);
        when(mockConfig.get(HugeGraphOptions.PORT)).thenReturn(expectedPort);
        when(mockConfig.get(HugeGraphOptions.GRAPH_NAME)).thenReturn(expectedGraph);
        when(mockConfig.get(HugeGraphSourceOptions.LABEL)).thenReturn(expectedLabel);
        when(mockConfig.get(HugeGraphSourceOptions.TYPE))
                .thenReturn(HugeGraphSourceOptions.LabelType.VERTEX);
        when(mockConfig.getOptional(HugeGraphOptions.GRAPH_SPACE)).thenReturn(Optional.empty());
        when(mockConfig.getOptional(HugeGraphOptions.USERNAME)).thenReturn(Optional.empty());
        when(mockConfig.getOptional(HugeGraphOptions.PASSWORD)).thenReturn(Optional.empty());
        when(mockConfig.getOptional(HugeGraphOptions.MAX_RETRIES))
                .thenReturn(Optional.of(HugeGraphOptions.MAX_RETRIES.defaultValue()));
        when(mockConfig.getOptional(HugeGraphOptions.RETRY_BACKOFF_MS))
                .thenReturn(Optional.of(HugeGraphOptions.RETRY_BACKOFF_MS.defaultValue()));
        when(mockConfig.getOptional(HugeGraphSourceOptions.PAGE_SIZE))
                .thenReturn(Optional.of(HugeGraphSourceOptions.PAGE_SIZE.defaultValue()));
        when(mockConfig.getOptional(HugeGraphSourceOptions.PROPERTIES))
                .thenReturn(Optional.empty());
        when(mockConfig.getOptional(HugeGraphSourceOptions.LIMIT)).thenReturn(Optional.empty());

        HugeGraphSourceConfig sourceConfig = HugeGraphSourceConfig.of(mockConfig);

        assertNotNull(sourceConfig);
        assertEquals(expectedHost, sourceConfig.getHost());
        assertEquals(expectedPort, sourceConfig.getPort());
        assertEquals(expectedGraph, sourceConfig.getGraphName());
        assertEquals(expectedLabel, sourceConfig.getLabel());
        assertEquals(HugeGraphSourceOptions.LabelType.VERTEX, sourceConfig.getType());
        assertEquals(500, sourceConfig.getPageSize());
        assertNull(sourceConfig.getLimit());
        assertNull(sourceConfig.getProperties());
    }

    @Test
    void testOf_shouldCreateEdgeSourceConfig() {
        String expectedHost = "localhost";
        int expectedPort = 8888;
        String expectedGraph = "edge_graph";
        String expectedLabel = "knows";

        when(mockConfig.get(HugeGraphOptions.HOST)).thenReturn(expectedHost);
        when(mockConfig.get(HugeGraphOptions.PORT)).thenReturn(expectedPort);
        when(mockConfig.get(HugeGraphOptions.GRAPH_NAME)).thenReturn(expectedGraph);
        when(mockConfig.get(HugeGraphSourceOptions.LABEL)).thenReturn(expectedLabel);
        when(mockConfig.get(HugeGraphSourceOptions.TYPE))
                .thenReturn(HugeGraphSourceOptions.LabelType.EDGE);
        when(mockConfig.getOptional(HugeGraphOptions.GRAPH_SPACE)).thenReturn(Optional.empty());
        when(mockConfig.getOptional(HugeGraphOptions.USERNAME)).thenReturn(Optional.empty());
        when(mockConfig.getOptional(HugeGraphOptions.PASSWORD)).thenReturn(Optional.empty());
        when(mockConfig.getOptional(HugeGraphOptions.MAX_RETRIES))
                .thenReturn(Optional.of(HugeGraphOptions.MAX_RETRIES.defaultValue()));
        when(mockConfig.getOptional(HugeGraphOptions.RETRY_BACKOFF_MS))
                .thenReturn(Optional.of(HugeGraphOptions.RETRY_BACKOFF_MS.defaultValue()));
        when(mockConfig.getOptional(HugeGraphSourceOptions.PAGE_SIZE))
                .thenReturn(Optional.of(HugeGraphSourceOptions.PAGE_SIZE.defaultValue()));
        when(mockConfig.getOptional(HugeGraphSourceOptions.PROPERTIES))
                .thenReturn(Optional.empty());
        when(mockConfig.getOptional(HugeGraphSourceOptions.LIMIT)).thenReturn(Optional.empty());

        HugeGraphSourceConfig sourceConfig = HugeGraphSourceConfig.of(mockConfig);

        assertNotNull(sourceConfig);
        assertEquals(expectedHost, sourceConfig.getHost());
        assertEquals(expectedPort, sourceConfig.getPort());
        assertEquals(expectedGraph, sourceConfig.getGraphName());
        assertEquals(expectedLabel, sourceConfig.getLabel());
        assertEquals(HugeGraphSourceOptions.LabelType.EDGE, sourceConfig.getType());
    }

    @Test
    void testOf_withCustomPageSizeAndLimit() {
        when(mockConfig.get(HugeGraphOptions.HOST)).thenReturn("127.0.0.1");
        when(mockConfig.get(HugeGraphOptions.PORT)).thenReturn(8080);
        when(mockConfig.get(HugeGraphOptions.GRAPH_NAME)).thenReturn("test_graph");
        when(mockConfig.get(HugeGraphSourceOptions.LABEL)).thenReturn("person");
        when(mockConfig.get(HugeGraphSourceOptions.TYPE))
                .thenReturn(HugeGraphSourceOptions.LabelType.VERTEX);
        when(mockConfig.getOptional(HugeGraphOptions.GRAPH_SPACE)).thenReturn(Optional.empty());
        when(mockConfig.getOptional(HugeGraphOptions.USERNAME)).thenReturn(Optional.empty());
        when(mockConfig.getOptional(HugeGraphOptions.PASSWORD)).thenReturn(Optional.empty());
        when(mockConfig.getOptional(HugeGraphOptions.MAX_RETRIES))
                .thenReturn(Optional.of(HugeGraphOptions.MAX_RETRIES.defaultValue()));
        when(mockConfig.getOptional(HugeGraphOptions.RETRY_BACKOFF_MS))
                .thenReturn(Optional.of(HugeGraphOptions.RETRY_BACKOFF_MS.defaultValue()));
        when(mockConfig.getOptional(HugeGraphSourceOptions.PAGE_SIZE))
                .thenReturn(Optional.of(1000));
        when(mockConfig.getOptional(HugeGraphSourceOptions.PROPERTIES))
                .thenReturn(Optional.of(Arrays.asList("age", "name")));
        when(mockConfig.getOptional(HugeGraphSourceOptions.LIMIT)).thenReturn(Optional.of(5000));

        HugeGraphSourceConfig sourceConfig = HugeGraphSourceConfig.of(mockConfig);

        assertNotNull(sourceConfig);
        assertEquals(1000, sourceConfig.getPageSize());
        assertEquals(5000, sourceConfig.getLimit());
        assertNotNull(sourceConfig.getProperties());
        assertEquals(2, sourceConfig.getProperties().size());
        assertEquals(Arrays.asList("age", "name"), sourceConfig.getProperties());
    }

    @Test
    void testOf_withFullConfiguration() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.10");
        configMap.put("port", 9999);
        configMap.put("graph_name", "full_graph");
        configMap.put("graph_space", "full_space");
        configMap.put("username", "admin");
        configMap.put("password", "secret123");
        configMap.put("max_retries", 5);
        configMap.put("retry_backoff_ms", 1000);
        configMap.put("label", "device");
        configMap.put("type", "VERTEX");
        configMap.put("page_size", 2000);
        configMap.put("properties", Arrays.asList("type", "status"));
        configMap.put("limit", 10000);

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);
        HugeGraphSourceConfig sourceConfig = HugeGraphSourceConfig.of(readonlyConfig);

        assertNotNull(sourceConfig);
        assertEquals("192.168.1.10", sourceConfig.getHost());
        assertEquals(9999, sourceConfig.getPort());
        assertEquals("full_graph", sourceConfig.getGraphName());
        assertEquals("full_space", sourceConfig.getGraphSpace());
        assertEquals("admin", sourceConfig.getUsername());
        assertEquals("secret123", sourceConfig.getPassword());
        assertEquals(5, sourceConfig.getMaxRetries());
        assertEquals(1000, sourceConfig.getRetryBackoffMs());
        assertEquals("device", sourceConfig.getLabel());
        assertEquals(HugeGraphSourceOptions.LabelType.VERTEX, sourceConfig.getType());
        assertEquals(2000, sourceConfig.getPageSize());
        assertEquals(10000, sourceConfig.getLimit());
        assertEquals(2, sourceConfig.getProperties().size());
    }

    @Test
    void testDefaultValues() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "localhost");
        configMap.put("port", 8080);
        configMap.put("graph_name", "my_graph");
        configMap.put("label", "test_label");
        configMap.put("type", "VERTEX");

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);
        HugeGraphSourceConfig sourceConfig = HugeGraphSourceConfig.of(readonlyConfig);

        assertNotNull(sourceConfig);
        assertEquals(500, sourceConfig.getPageSize(), "Default page_size should be 500");
        assertNull(sourceConfig.getLimit(), "Default limit should be null");
        assertNull(sourceConfig.getProperties(), "Default properties should be null");
    }
}
