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

package org.apache.seatunnel.connectors.seatunnel.hugegraph;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.source.HugeGraphSource;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.source.HugeGraphSourceFactory;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
@DisplayName("HugeGraph Source Integration Tests")
class HugeGraphIT {

    @Test
    @Disabled(
            "Requires running HugeGraph server. Enable this when HugeGraph is available for testing.")
    @DisplayName("Test reading vertices from HugeGraph")
    void testReadVertices() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "localhost");
        configMap.put("port", 8080);
        configMap.put("graph_name", "test_graph");
        configMap.put("label", "person");
        configMap.put("type", "VERTEX");
        configMap.put("page_size", 500);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        HugeGraphSourceFactory factory = new HugeGraphSourceFactory();

        SeaTunnelSource<?, ?, ?> source = factory.createSource(null).createSource();

        assertNotNull(source);
        assertEquals("HugeGraph", source.getPluginName());
    }

    @Test
    @Disabled(
            "Requires running HugeGraph server. Enable this when HugeGraph is available for testing.")
    @DisplayName("Test reading edges from HugeGraph")
    void testReadEdges() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "localhost");
        configMap.put("port", 8080);
        configMap.put("graph_name", "test_graph");
        configMap.put("label", "knows");
        configMap.put("type", "EDGE");
        configMap.put("page_size", 500);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        HugeGraphSourceFactory factory = new HugeGraphSourceFactory();

        SeaTunnelSource<?, ?, ?> source = factory.createSource(null).createSource();

        assertNotNull(source);
        assertEquals("HugeGraph", source.getPluginName());
    }

    @Test
    @Disabled(
            "Requires running HugeGraph server. Enable this when HugeGraph is available for testing.")
    @DisplayName("Test reading vertices with property filter")
    void testReadVerticesWithPropertyFilter() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "localhost");
        configMap.put("port", 8080);
        configMap.put("graph_name", "test_graph");
        configMap.put("label", "person");
        configMap.put("type", "VERTEX");
        configMap.put("page_size", 500);
        configMap.put("properties", Arrays.asList("name", "age"));

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        HugeGraphSourceFactory factory = new HugeGraphSourceFactory();

        SeaTunnelSource<?, ?, ?> source = factory.createSource(null).createSource();

        assertNotNull(source);
        assertEquals("HugeGraph", source.getPluginName());
    }

    @Test
    @Disabled(
            "Requires running HugeGraph server. Enable this when HugeGraph is available for testing.")
    @DisplayName("Test reading vertices with limit")
    void testReadVerticesWithLimit() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "localhost");
        configMap.put("port", 8080);
        configMap.put("graph_name", "test_graph");
        configMap.put("label", "person");
        configMap.put("type", "VERTEX");
        configMap.put("page_size", 500);
        configMap.put("limit", 100);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        HugeGraphSourceFactory factory = new HugeGraphSourceFactory();

        SeaTunnelSource<?, ?, ?> source = factory.createSource(null).createSource();

        assertNotNull(source);
        assertEquals("HugeGraph", source.getPluginName());
    }

    @Test
    @DisplayName("Test factory identifier")
    void testFactoryIdentifier() {
        HugeGraphSourceFactory factory = new HugeGraphSourceFactory();
        assertEquals("HugeGraph", factory.factoryIdentifier());
    }

    @Test
    @DisplayName("Test option rule requirements")
    void testOptionRuleRequirements() {
        HugeGraphSourceFactory factory = new HugeGraphSourceFactory();
        assertNotNull(factory.optionRule());
    }

    @Test
    @DisplayName("Test source class")
    void testSourceClass() {
        HugeGraphSourceFactory factory = new HugeGraphSourceFactory();
        assertEquals(HugeGraphSource.class, factory.getSourceClass());
    }
}
