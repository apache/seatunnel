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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSourceOptions;

import org.apache.hugegraph.structure.constant.DataType;
import org.apache.hugegraph.structure.schema.EdgeLabel;
import org.apache.hugegraph.structure.schema.PropertyKey;
import org.apache.hugegraph.structure.schema.VertexLabel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HugeGraphSourceFactorySchemaInferTest {

    private HugeGraphSourceFactory factory;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        factory = new HugeGraphSourceFactory();
    }

    @Test
    void testVertexSchemaInference() {
        HugeGraphSourceConfig sourceConfig = new HugeGraphSourceConfig();
        sourceConfig.setHost("localhost");
        sourceConfig.setPort(8080);
        sourceConfig.setGraphName("test_graph");
        sourceConfig.setLabel("person");
        sourceConfig.setType(HugeGraphSourceOptions.LabelType.VERTEX);

        VertexLabel vertexLabel = createMockVertexLabel("person");
        PropertyKey nameKey = createMockPropertyKey("name", DataType.TEXT);
        PropertyKey ageKey = createMockPropertyKey("age", DataType.INT);

        SeaTunnelRowType inferredRowType =
                new SeaTunnelRowType(
                        new String[] {"id", "label", "name", "age"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.INT_TYPE
                        });

        assertNotNull(inferredRowType);
        assertEquals(4, inferredRowType.getTotalFields());
        assertEquals("id", inferredRowType.getFieldName(0));
        assertEquals("label", inferredRowType.getFieldName(1));
        assertEquals("name", inferredRowType.getFieldName(2));
        assertEquals("age", inferredRowType.getFieldName(3));

        assertEquals(BasicType.STRING_TYPE, inferredRowType.getFieldType(0));
        assertEquals(BasicType.STRING_TYPE, inferredRowType.getFieldType(1));
        assertEquals(BasicType.STRING_TYPE, inferredRowType.getFieldType(2));
        assertEquals(BasicType.INT_TYPE, inferredRowType.getFieldType(3));
    }

    @Test
    void testEdgeSchemaInference() {
        HugeGraphSourceConfig sourceConfig = new HugeGraphSourceConfig();
        sourceConfig.setHost("localhost");
        sourceConfig.setPort(8080);
        sourceConfig.setGraphName("test_graph");
        sourceConfig.setLabel("knows");
        sourceConfig.setType(HugeGraphSourceOptions.LabelType.EDGE);

        SeaTunnelRowType inferredRowType =
                new SeaTunnelRowType(
                        new String[] {"id", "label", "source_id", "target_id", "since"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.INT_TYPE
                        });

        assertNotNull(inferredRowType);
        assertEquals(5, inferredRowType.getTotalFields());
        assertEquals("id", inferredRowType.getFieldName(0));
        assertEquals("label", inferredRowType.getFieldName(1));
        assertEquals("source_id", inferredRowType.getFieldName(2));
        assertEquals("target_id", inferredRowType.getFieldName(3));
        assertEquals("since", inferredRowType.getFieldName(4));

        assertEquals(BasicType.STRING_TYPE, inferredRowType.getFieldType(0));
        assertEquals(BasicType.STRING_TYPE, inferredRowType.getFieldType(2));
        assertEquals(BasicType.INT_TYPE, inferredRowType.getFieldType(4));
    }

    @Test
    void testDataTypeMapping() {
        Map<DataType, Object> typeMapping = new HashMap<>();
        typeMapping.put(DataType.BOOLEAN, BasicType.BOOLEAN_TYPE);
        typeMapping.put(DataType.INT, BasicType.INT_TYPE);
        typeMapping.put(DataType.LONG, BasicType.LONG_TYPE);
        typeMapping.put(DataType.FLOAT, BasicType.FLOAT_TYPE);
        typeMapping.put(DataType.DOUBLE, BasicType.DOUBLE_TYPE);
        typeMapping.put(DataType.TEXT, BasicType.STRING_TYPE);
        typeMapping.put(DataType.UUID, BasicType.STRING_TYPE);

        for (Map.Entry<DataType, Object> entry : typeMapping.entrySet()) {
            assertNotNull(
                    entry.getValue(), "Mapping for " + entry.getKey() + " should not be null");
        }
    }

    @Test
    void testCatalogTableCreation() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "localhost");
        configMap.put("port", 8080);
        configMap.put("graph_name", "test_graph");
        configMap.put("label", "person");
        configMap.put("type", "VERTEX");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        String identifier = factory.factoryIdentifier();
        assertEquals("HugeGraph", identifier);
    }

    private VertexLabel createMockVertexLabel(String label) {
        VertexLabel vertexLabel = mock(VertexLabel.class);
        when(vertexLabel.name()).thenReturn(label);
        when(vertexLabel.properties()).thenReturn(new HashSet<>(Arrays.asList("name", "age")));
        return vertexLabel;
    }

    private EdgeLabel createMockEdgeLabel(String label) {
        EdgeLabel edgeLabel = mock(EdgeLabel.class);
        when(edgeLabel.name()).thenReturn(label);
        when(edgeLabel.properties()).thenReturn(new HashSet<>(Arrays.asList("since")));
        return edgeLabel;
    }

    private PropertyKey createMockPropertyKey(String name, DataType dataType) {
        PropertyKey propertyKey = mock(PropertyKey.class);
        when(propertyKey.name()).thenReturn(name);
        when(propertyKey.dataType()).thenReturn(dataType);
        return propertyKey;
    }
}
