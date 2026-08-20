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
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.apache.hugegraph.serializer.direct.util.SplicingIdGenerator;
import org.apache.hugegraph.structure.GraphElement;
import org.apache.hugegraph.structure.constant.Cardinality;
import org.apache.hugegraph.structure.constant.DataType;
import org.apache.hugegraph.structure.constant.IdStrategy;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.structure.schema.PropertyKey;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    @Test
    void testRawIdPassthroughCustomizeString() {
        // idFields = ["~id"] reuses the pre-assembled Source id verbatim so a CUSTOMIZE_STRING
        // vertex can be cloned without knowing its original key columns.
        HugeGraphClient client = mock(HugeGraphClient.class);
        when(client.getVertexLabelId("person")).thenReturn("1");
        when(client.getPropertyKeyOrNull("~id")).thenReturn(null);

        MappingConfig mapping = vertexMapping(IdStrategy.CUSTOMIZE_STRING, "~id");
        VertexMapper mapper = new VertexMapper(mapping, fields("~id"), client);

        Vertex vertex = mapper.map(new SeaTunnelRow(new Object[] {"user-42"}));
        assertEquals("user-42", vertex.id());
    }

    @Test
    void testRawIdPassthroughCustomizeNumber() {
        HugeGraphClient client = mock(HugeGraphClient.class);
        when(client.getVertexLabelId("person")).thenReturn("1");
        when(client.getPropertyKeyOrNull("~id")).thenReturn(null);

        MappingConfig mapping = vertexMapping(IdStrategy.CUSTOMIZE_NUMBER, "~id");
        VertexMapper mapper = new VertexMapper(mapping, fields("~id"), client);

        Vertex vertex = mapper.map(new SeaTunnelRow(new Object[] {123L}));
        assertEquals(123L, vertex.id());
    }

    @Test
    void testRawIdPassthroughCustomizeNumberFromString() {
        // The Source serializes ~id as a String; a CUSTOMIZE_NUMBER target must parse it back.
        HugeGraphClient client = mock(HugeGraphClient.class);
        when(client.getVertexLabelId("person")).thenReturn("1");
        when(client.getPropertyKeyOrNull("~id")).thenReturn(null);

        MappingConfig mapping = vertexMapping(IdStrategy.CUSTOMIZE_NUMBER, "~id");
        VertexMapper mapper = new VertexMapper(mapping, fields("~id"), client);

        Vertex vertex = mapper.map(new SeaTunnelRow(new Object[] {"456"}));
        assertEquals(456L, vertex.id());
    }

    @Test
    void testValueMappingIsScopedPerField() {
        // gender maps M->male; status maps M->married. A flat value_mapping would let one column's
        // rule bleed into the other (both M cells become "male"). Per-field scoping must keep them
        // independent.
        HugeGraphClient client = mock(HugeGraphClient.class);
        PropertyKey gender = propertyKey("gender", DataType.TEXT);
        PropertyKey status = propertyKey("status", DataType.TEXT);
        when(client.getVertexLabelId("person")).thenReturn("1");
        when(client.getPropertyKey("gender")).thenReturn(gender);
        when(client.getPropertyKey("status")).thenReturn(status);
        when(client.getPropertyKeyOrNull("id")).thenReturn(null);

        MappingConfig mapping = vertexMapping(IdStrategy.CUSTOMIZE_STRING, "id");
        mapping.setProperties(Arrays.asList("gender", "status"));
        Map<String, Map<Object, Object>> valueMapping = new HashMap<>();
        valueMapping.put("gender", Collections.singletonMap("M", "male"));
        valueMapping.put("status", Collections.singletonMap("M", "married"));
        mapping.setValueMapping(valueMapping);

        VertexMapper mapper = new VertexMapper(mapping, fields("id", "gender", "status"), client);
        Vertex vertex = mapper.map(new SeaTunnelRow(new Object[] {"u1", "M", "M"}));

        assertEquals("male", vertex.properties().get("gender"));
        assertEquals("married", vertex.properties().get("status"));
    }

    @Test
    void testUnfoldExpandsListIdIntoMultipleVertices() {
        HugeGraphClient client = mock(HugeGraphClient.class);
        PropertyKey age = propertyKey("age", DataType.INT);
        when(client.getVertexLabelId("person")).thenReturn("1");
        when(client.getPropertyKey("age")).thenReturn(age);
        when(client.getPropertyKeyOrNull("id")).thenReturn(null);

        MappingConfig mapping = vertexMapping(IdStrategy.CUSTOMIZE_STRING, "id");
        mapping.setProperties(Arrays.asList("age"));
        mapping.setUnfold(true);
        VertexMapper mapper = new VertexMapper(mapping, fields("id", "age"), client);

        List<GraphElement> elements =
                mapper.mapAll(new SeaTunnelRow(new Object[] {new String[] {"a", "b", "c"}, 30}));

        assertEquals(3, elements.size());
        assertEquals(
                Arrays.asList("a", "b", "c"),
                elements.stream().map(GraphElement::id).collect(Collectors.toList()));
        for (GraphElement element : elements) {
            // The unfolded id column is not written as a property; only shared props are.
            assertEquals(30, ((Vertex) element).properties().get("age"));
        }
    }

    @Test
    void testPrimaryKeyIdEscapesSeparatorLikeServer() {
        // A PRIMARY_KEY value containing the '!' separator must be backtick-escaped exactly as the
        // HugeGraph server assembles the id (SplicingIdGenerator.concatValues). EdgeMapper already
        // uses concatValues for the same concept; VertexMapper must match so DELETE / key-changing
        // UPDATE target the real vertex id instead of an ambiguous, unescaped join.
        HugeGraphClient client = mock(HugeGraphClient.class);
        PropertyKey name = propertyKey("name", DataType.TEXT);
        when(client.getVertexLabelId("person")).thenReturn("1");
        when(client.getPropertyKey("name")).thenReturn(name);

        MappingConfig mapping = vertexMapping(IdStrategy.PRIMARY_KEY, "name");
        VertexMapper mapper = new VertexMapper(mapping, fields("name"), client);

        Object id = mapper.extractId(new SeaTunnelRow(new Object[] {"a!b"}));

        String expected = "1:" + SplicingIdGenerator.concatValues(Collections.singletonList("a!b"));
        assertEquals(expected, id);
        // The naive join silently produces an ambiguous, server-mismatched id.
        assertNotEquals("1:a!b", id);
    }

    @Test
    void testMultiFieldCustomizeStringIdEscapesSeparatorToAvoidCollision() {
        // ("x:y","z") and ("x","y:z") both collapsed to "x:y:z" with a raw ':' join, so different
        // rows produced the same vertex id and overwrote each other. Escaping must keep them
        // distinct.
        HugeGraphClient client = mock(HugeGraphClient.class);
        when(client.getVertexLabelId("person")).thenReturn("1");
        when(client.getPropertyKeyOrNull("a")).thenReturn(null);
        when(client.getPropertyKeyOrNull("b")).thenReturn(null);

        MappingConfig mapping = new MappingConfig();
        mapping.setType(MappingConfig.LabelType.VERTEX);
        mapping.setLabel("person");
        mapping.setIdStrategy(IdStrategy.CUSTOMIZE_STRING);
        mapping.setIdFields(Arrays.asList("a", "b"));
        VertexMapper mapper = new VertexMapper(mapping, fields("a", "b"), client);

        Object id1 = mapper.extractId(new SeaTunnelRow(new Object[] {"x:y", "z"}));
        Object id2 = mapper.extractId(new SeaTunnelRow(new Object[] {"x", "y:z"}));

        assertNotEquals(id1, id2, "Distinct field tuples must not collapse to the same id");
    }

    @Test
    void testSingleFieldCustomizeStringIdIsVerbatim() {
        // A single id field is unambiguous, so its value (even containing ':') is used as-is —
        // escaping it would change ids already written for the common single-field case.
        HugeGraphClient client = mock(HugeGraphClient.class);
        when(client.getVertexLabelId("person")).thenReturn("1");
        when(client.getPropertyKeyOrNull("a")).thenReturn(null);

        MappingConfig mapping = new MappingConfig();
        mapping.setType(MappingConfig.LabelType.VERTEX);
        mapping.setLabel("person");
        mapping.setIdStrategy(IdStrategy.CUSTOMIZE_STRING);
        mapping.setIdFields(Collections.singletonList("a"));
        VertexMapper mapper = new VertexMapper(mapping, fields("a"), client);

        assertEquals("x:y", mapper.extractId(new SeaTunnelRow(new Object[] {"x:y"})));
    }

    @Test
    void testCustomizeNumberIdRejectsFractionalConsistently() {
        // A Number 1.9 was silently truncated to 1 while the string "1.9" threw — same logical
        // input,
        // different result. Both must now be rejected; integral decimals (1.0 / "1.0") are
        // accepted.
        assertEquals(1L, VertexMapper.coerceNumberId(1L));
        assertEquals(1L, VertexMapper.coerceNumberId("1"));
        assertEquals(1L, VertexMapper.coerceNumberId(1.0d));
        assertEquals(1L, VertexMapper.coerceNumberId("1.0"));
        assertThrows(HugeGraphConnectorException.class, () -> VertexMapper.coerceNumberId(1.9d));
        assertThrows(HugeGraphConnectorException.class, () -> VertexMapper.coerceNumberId("1.9"));
        assertThrows(HugeGraphConnectorException.class, () -> VertexMapper.coerceNumberId("abc"));
    }

    @Test
    void testVertexIdLengthLimitEnforced() {
        String maxLen = String.join("", Collections.nCopies(128, "a")); // exactly 128 bytes
        assertEquals(maxLen, VertexMapper.checkVertexIdLength(maxLen));

        String tooLong = String.join("", Collections.nCopies(129, "a")); // 129 bytes
        assertThrows(
                HugeGraphConnectorException.class, () -> VertexMapper.checkVertexIdLength(tooLong));
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
