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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.mapper;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;

import org.apache.hugegraph.structure.constant.Cardinality;
import org.apache.hugegraph.structure.constant.DataType;
import org.apache.hugegraph.structure.constant.IdStrategy;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.structure.schema.PropertyKey;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class VertexMapperTest {

    @Test
    void testEmptyPropertiesWritesAllInputFields() {
        HugeGraphClient client = mock(HugeGraphClient.class);
        PropertyKey name = propertyKey("name", DataType.TEXT);
        PropertyKey age = propertyKey("age", DataType.INT);
        when(client.getVertexLabelId("person")).thenReturn("1");
        when(client.getPropertyKey("name")).thenReturn(name);
        when(client.getPropertyKey("age")).thenReturn(age);

        MappingConfig mapping = vertexMapping(IdStrategy.PRIMARY_KEY, "name");
        VertexMapper mapper = new VertexMapper(mapping, fields("name", "age"), client);

        Vertex vertex = mapper.map(new SeaTunnelRow(new Object[] {"Alice", 29}));

        assertEquals("Alice", vertex.properties().get("name"));
        assertEquals(29, vertex.properties().get("age"));
    }

    @Test
    void testCustomIdDoesNotRequirePropertyKey() {
        HugeGraphClient client = mock(HugeGraphClient.class);
        PropertyKey age = propertyKey("age", DataType.INT);
        when(client.getVertexLabelId("person")).thenReturn("1");
        when(client.getPropertyKey("age")).thenReturn(age);
        when(client.getPropertyKeyOrNull("external_id")).thenReturn(null);

        MappingConfig mapping = vertexMapping(IdStrategy.CUSTOMIZE_STRING, "external_id");
        mapping.setProperties(Arrays.asList("age"));
        VertexMapper mapper = new VertexMapper(mapping, fields("external_id", "age"), client);

        Vertex vertex = mapper.map(new SeaTunnelRow(new Object[] {"user-1", 29}));

        assertEquals("user-1", vertex.id());
        assertEquals(29, vertex.properties().get("age"));
    }

    @Test
    void testNullValueInRequiredIdSkipsVertex() {
        HugeGraphClient client = mock(HugeGraphClient.class);
        PropertyKey name = propertyKey("name", DataType.TEXT);
        when(client.getVertexLabelId("person")).thenReturn("1");
        when(client.getPropertyKey("name")).thenReturn(name);

        MappingConfig mapping = vertexMapping(IdStrategy.PRIMARY_KEY, "name");
        mapping.setNullValues(Arrays.asList("NULL"));
        VertexMapper mapper = new VertexMapper(mapping, fields("name"), client);

        assertNull(mapper.map(new SeaTunnelRow(new Object[] {"NULL"})));
    }

    private static MappingConfig vertexMapping(IdStrategy idStrategy, String idField) {
        MappingConfig mapping = new MappingConfig();
        mapping.setType(MappingConfig.LabelType.VERTEX);
        mapping.setLabel("person");
        mapping.setIdStrategy(idStrategy);
        mapping.setIdFields(Arrays.asList(idField));
        return mapping;
    }

    private static Map<String, Integer> fields(String... names) {
        Map<String, Integer> fields = new LinkedHashMap<>();
        for (int i = 0; i < names.length; i++) {
            fields.put(names[i], i);
        }
        return fields;
    }

    private static PropertyKey propertyKey(String name, DataType dataType) {
        PropertyKey propertyKey = mock(PropertyKey.class);
        when(propertyKey.name()).thenReturn(name);
        when(propertyKey.dataType()).thenReturn(dataType);
        when(propertyKey.cardinality()).thenReturn(Cardinality.SINGLE);
        return propertyKey;
    }
}
