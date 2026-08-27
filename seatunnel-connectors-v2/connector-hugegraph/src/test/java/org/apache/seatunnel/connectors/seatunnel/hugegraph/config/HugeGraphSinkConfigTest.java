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
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HugeGraphSinkConfigTest {

    @Test
    void testDefaultValues() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "127.0.0.1");
        configMap.put("port", 8080);
        configMap.put("graph_name", "hugegraph");

        // Provide a minimal mapping to avoid "neither mappings nor schema_config" error
        List<Map<String, Object>> mappings = new ArrayList<>();
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("type", "VERTEX");
        mapping.put("label", "test");
        mapping.put("idStrategy", "PRIMARY_KEY");
        mapping.put("idFields", Collections.singletonList("id"));
        mappings.add(mapping);
        configMap.put("mappings", mappings);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        HugeGraphSinkConfig sinkConfig = HugeGraphSinkConfig.of(config);

        assertNotNull(sinkConfig);
        assertEquals(HugeGraphOptions.BATCH_SIZE.defaultValue(), sinkConfig.getBatchSize());
        assertEquals(
                HugeGraphOptions.BATCH_INTERVAL_MS.defaultValue(), sinkConfig.getBatchIntervalMs());
        assertEquals(HugeGraphOptions.MAX_RETRIES.defaultValue(), sinkConfig.getMaxRetries());
        assertEquals(
                HugeGraphOptions.RETRY_BACKOFF_MS.defaultValue(), sinkConfig.getRetryBackoffMs());
        assertEquals(
                HugeGraphSchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                sinkConfig.getSchemaSaveMode());
        assertEquals(HugeGraphDataSaveMode.APPEND_DATA, sinkConfig.getDataSaveMode());
        assertFalse(sinkConfig.isDeleteVertexWithEdges());
        assertEquals("yyyy-MM-dd", sinkConfig.getMappings().get(0).getDateFormat());
        // timeZone is intentionally left unset when the user does not configure one; DataTypeUtil
        // then defaults to ZoneId.systemDefault(), matching the HugeGraph Source. Previously it
        // was hard-coded to GMT+8, silently shifting absolute times on non-China deployments.
        assertNull(sinkConfig.getMappings().get(0).getTimeZone());
    }

    @Test
    void testMultiMappingConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.1");
        configMap.put("port", 8888);
        configMap.put("graph_name", "test_graph");

        List<Map<String, Object>> mappings = new ArrayList<>();

        // Vertex mapping
        Map<String, Object> vertexMapping = new HashMap<>();
        vertexMapping.put("type", "VERTEX");
        vertexMapping.put("label", "person");
        vertexMapping.put("idStrategy", "PRIMARY_KEY");
        vertexMapping.put("idFields", Collections.singletonList("name"));
        vertexMapping.put("properties", Arrays.asList("name", "age"));
        mappings.add(vertexMapping);

        // Edge mapping
        Map<String, Object> edgeMapping = new HashMap<>();
        edgeMapping.put("type", "EDGE");
        edgeMapping.put("label", "knows");
        Map<String, Object> srcConfig = new HashMap<>();
        srcConfig.put("label", "person");
        srcConfig.put("idFields", Collections.singletonList("src_name"));
        edgeMapping.put("sourceConfig", srcConfig);
        Map<String, Object> tgtConfig = new HashMap<>();
        tgtConfig.put("label", "person");
        tgtConfig.put("idFields", Collections.singletonList("tgt_name"));
        edgeMapping.put("targetConfig", tgtConfig);
        edgeMapping.put("properties", Collections.singletonList("weight"));
        mappings.add(edgeMapping);

        configMap.put("mappings", mappings);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        HugeGraphSinkConfig sinkConfig = HugeGraphSinkConfig.of(config);

        assertNotNull(sinkConfig);
        assertNotNull(sinkConfig.getMappings());
        assertEquals(2, sinkConfig.getMappings().size());

        MappingConfig vertex = sinkConfig.getMappings().get(0);
        assertEquals(MappingConfig.LabelType.VERTEX, vertex.getType());
        assertEquals("person", vertex.getLabel());
        assertEquals(Collections.singletonList("name"), vertex.getIdFields());
        assertEquals(Arrays.asList("name", "age"), vertex.getProperties());

        MappingConfig edge = sinkConfig.getMappings().get(1);
        assertEquals(MappingConfig.LabelType.EDGE, edge.getType());
        assertEquals("knows", edge.getLabel());
        assertEquals("person", edge.getSourceConfig().getLabel());
        assertEquals("person", edge.getTargetConfig().getLabel());
    }

    @Test
    void testLegacySchemaConfigBackwardCompat() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "localhost");
        configMap.put("port", 8080);
        configMap.put("graph_name", "legacy_graph");

        // Old-style schema_config
        Map<String, Object> schema = new HashMap<>();
        schema.put("type", "VERTEX");
        schema.put("label", "device");
        schema.put("idStrategy", "CUSTOMIZE_STRING");
        schema.put("idFields", Collections.singletonList("device_id"));
        schema.put("properties", Arrays.asList("device_id", "name"));

        Map<String, Object> mappingNested = new HashMap<>();
        mappingNested.put("fieldMapping", Collections.singletonMap("name", "device_name"));
        schema.put("mapping", mappingNested);

        configMap.put("schema_config", schema);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        HugeGraphSinkConfig sinkConfig = HugeGraphSinkConfig.of(config);

        assertNotNull(sinkConfig);
        assertNotNull(sinkConfig.getMappings());
        assertEquals(1, sinkConfig.getMappings().size());

        MappingConfig converted = sinkConfig.getMappings().get(0);
        assertEquals(MappingConfig.LabelType.VERTEX, converted.getType());
        assertEquals("device", converted.getLabel());
        assertEquals(Collections.singletonMap("name", "device_name"), converted.getFieldMapping());
        assertEquals(
                HugeGraphSchemaSaveMode.ERROR_WHEN_SCHEMA_NOT_EXIST,
                sinkConfig.getSchemaSaveMode());
        assertTrue(sinkConfig.isDeleteVertexWithEdges());
    }

    @Test
    void testLegacySelectedFieldsAreAppliedToConvertedMapping() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "localhost");
        configMap.put("port", 8080);
        configMap.put("graph_name", "legacy_graph");
        configMap.put("selected_fields", Arrays.asList("id", "name"));

        Map<String, Object> schema = new HashMap<>();
        schema.put("type", "VERTEX");
        schema.put("label", "device");
        schema.put("idStrategy", "PRIMARY_KEY");
        schema.put("idFields", Collections.singletonList("id"));
        configMap.put("schema_config", schema);

        HugeGraphSinkConfig sinkConfig = HugeGraphSinkConfig.of(ReadonlyConfig.fromMap(configMap));
        sinkConfig.applyLegacyFieldSelection(
                new SeaTunnelRowType(
                        new String[] {"id", "name", "secret"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.STRING_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
                        }));

        assertEquals(Arrays.asList("id", "name"), sinkConfig.getMappings().get(0).getProperties());
    }

    @Test
    void testEdgeMappingWithFrequencyAndSortKeys() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "localhost");
        configMap.put("port", 8080);
        configMap.put("graph_name", "graph");

        List<Map<String, Object>> mappings = new ArrayList<>();
        Map<String, Object> edgeMapping = new HashMap<>();
        edgeMapping.put("type", "EDGE");
        edgeMapping.put("label", "transfer");
        edgeMapping.put("frequency", "MULTIPLE");
        edgeMapping.put("sortKeys", Collections.singletonList("timestamp"));

        Map<String, Object> srcConfig = new HashMap<>();
        srcConfig.put("label", "account");
        srcConfig.put("idFields", Collections.singletonList("from_id"));
        edgeMapping.put("sourceConfig", srcConfig);

        Map<String, Object> tgtConfig = new HashMap<>();
        tgtConfig.put("label", "account");
        tgtConfig.put("idFields", Collections.singletonList("to_id"));
        edgeMapping.put("targetConfig", tgtConfig);

        edgeMapping.put("properties", Arrays.asList("amount", "timestamp"));
        mappings.add(edgeMapping);
        configMap.put("mappings", mappings);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        HugeGraphSinkConfig sinkConfig = HugeGraphSinkConfig.of(config);

        MappingConfig edge = sinkConfig.getMappings().get(0);
        assertEquals("multiple", edge.getFrequency().string());
        assertEquals(Collections.singletonList("timestamp"), edge.getSortKeys());
    }

    @Test
    void testSchemaSaveModeConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "localhost");
        configMap.put("port", 8080);
        configMap.put("graph_name", "graph");
        configMap.put("schema_save_mode", "ERROR_WHEN_SCHEMA_NOT_EXIST");
        configMap.put("delete_vertex_with_edges", true);

        List<Map<String, Object>> mappings = new ArrayList<>();
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("type", "VERTEX");
        mapping.put("label", "v");
        mappings.add(mapping);
        configMap.put("mappings", mappings);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        HugeGraphSinkConfig sinkConfig = HugeGraphSinkConfig.of(config);

        assertEquals(
                HugeGraphSchemaSaveMode.ERROR_WHEN_SCHEMA_NOT_EXIST,
                sinkConfig.getSchemaSaveMode());
        assertTrue(sinkConfig.isDeleteVertexWithEdges());
    }

    @Test
    void testDataSaveModeConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "localhost");
        configMap.put("port", 8080);
        configMap.put("graph_name", "graph");
        configMap.put("data_save_mode", "DROP_DATA");

        List<Map<String, Object>> mappings = new ArrayList<>();
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("type", "VERTEX");
        mapping.put("label", "v");
        mappings.add(mapping);
        configMap.put("mappings", mappings);

        HugeGraphSinkConfig sinkConfig = HugeGraphSinkConfig.of(ReadonlyConfig.fromMap(configMap));
        assertEquals(HugeGraphDataSaveMode.DROP_DATA, sinkConfig.getDataSaveMode());
    }

    @Test
    void testNoMappingsOrSchemaConfigThrows() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "localhost");
        configMap.put("port", 8080);
        configMap.put("graph_name", "graph");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        assertThrows(HugeGraphConnectorException.class, () -> HugeGraphSinkConfig.of(config));
    }

    @Test
    void testGraphSpaceIsHonored() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "localhost");
        configMap.put("port", 8080);
        configMap.put("graph_name", "graph");
        configMap.put("graph_space", "my_space");

        List<Map<String, Object>> mappings = new ArrayList<>();
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("type", "VERTEX");
        mapping.put("label", "v");
        mappings.add(mapping);
        configMap.put("mappings", mappings);

        HugeGraphSinkConfig config = HugeGraphSinkConfig.of(ReadonlyConfig.fromMap(configMap));
        assertEquals("my_space", config.getConnectionConfig().getGraphSpace());
    }

    @Test
    void testGraphSpaceDefaultsToDefault() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "localhost");
        configMap.put("port", 8080);
        configMap.put("graph_name", "graph");

        List<Map<String, Object>> mappings = new ArrayList<>();
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("type", "VERTEX");
        mapping.put("label", "v");
        mappings.add(mapping);
        configMap.put("mappings", mappings);

        HugeGraphSinkConfig config = HugeGraphSinkConfig.of(ReadonlyConfig.fromMap(configMap));
        assertEquals("DEFAULT", config.getConnectionConfig().getGraphSpace());
    }

    @Test
    void testMappingsOverridesSchemaConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "localhost");
        configMap.put("port", 8080);
        configMap.put("graph_name", "graph");

        // Both present — mappings should win
        Map<String, Object> schema = new HashMap<>();
        schema.put("type", "VERTEX");
        schema.put("label", "old_label");
        configMap.put("schema_config", schema);

        List<Map<String, Object>> mappings = new ArrayList<>();
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("type", "VERTEX");
        mapping.put("label", "new_label");
        mappings.add(mapping);
        configMap.put("mappings", mappings);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        HugeGraphSinkConfig sinkConfig = HugeGraphSinkConfig.of(config);

        assertEquals(1, sinkConfig.getMappings().size());
        assertEquals("new_label", sinkConfig.getMappings().get(0).getLabel());
    }

    // --- source_table ALL-or-NOTHING validation ---

    @Test
    void validateSourceTableConsistencyAllSetIsOk() {
        assertDoesNotThrow(
                () ->
                        HugeGraphSinkConfig.validateSourceTableConsistency(
                                Arrays.asList(
                                        mappingWithSourceTable("person", "hugegraph.person"),
                                        mappingWithSourceTable("company", "hugegraph.company"))));
    }

    @Test
    void validateSourceTableConsistencyNoneSetIsOk() {
        assertDoesNotThrow(
                () ->
                        HugeGraphSinkConfig.validateSourceTableConsistency(
                                Arrays.asList(
                                        mappingWithSourceTable("person", null),
                                        mappingWithSourceTable("company", null))));
    }

    @Test
    void validateSourceTableConsistencyMixedThrows() {
        HugeGraphConnectorException ex =
                assertThrows(
                        HugeGraphConnectorException.class,
                        () ->
                                HugeGraphSinkConfig.validateSourceTableConsistency(
                                        Arrays.asList(
                                                mappingWithSourceTable(
                                                        "person", "hugegraph.person"),
                                                mappingWithSourceTable("company", null))));
        assertTrue(
                ex.getMessage().contains("ALL-or-NOTHING"),
                "Error must explain the ALL-or-NOTHING contract");
        assertTrue(
                ex.getMessage().contains("person"),
                "Error must name the mapping(s) that set source_table");
        assertTrue(
                ex.getMessage().contains("company"),
                "Error must name the mapping(s) missing source_table");
    }

    @Test
    void validateSourceTableConsistencyEmptySourceTableTreatedAsUnset() {
        // An empty string source_table is equivalent to unset — it must trigger the mixed error.
        HugeGraphConnectorException ex =
                assertThrows(
                        HugeGraphConnectorException.class,
                        () ->
                                HugeGraphSinkConfig.validateSourceTableConsistency(
                                        Arrays.asList(
                                                mappingWithSourceTable(
                                                        "person", "hugegraph.person"),
                                                mappingWithSourceTable("company", ""))));
        assertTrue(ex.getMessage().contains("ALL-or-NOTHING"));
    }

    private static MappingConfig mappingWithSourceTable(String label, String sourceTable) {
        MappingConfig m = new MappingConfig();
        m.setType(MappingConfig.LabelType.VERTEX);
        m.setLabel(label);
        m.setIdStrategy(org.apache.hugegraph.structure.constant.IdStrategy.PRIMARY_KEY);
        m.setIdFields(Collections.singletonList("id"));
        if (sourceTable != null) {
            m.setSourceTable(sourceTable);
        }
        return m;
    }
}
