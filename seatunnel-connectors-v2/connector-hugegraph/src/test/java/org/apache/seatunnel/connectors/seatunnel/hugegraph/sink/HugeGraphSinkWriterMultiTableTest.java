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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.sink;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.apache.hugegraph.structure.constant.Cardinality;
import org.apache.hugegraph.structure.constant.DataType;
import org.apache.hugegraph.structure.schema.PropertyKey;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the multi-table mapping contract: when {@code source_table} is configured, each writer
 * activates only the mappings that match its table; two writers for different tables must not share
 * mappings (no cross-write). When no mapping matches in multi-table mode, the writer must fail fast
 * rather than silently becoming a no-op.
 */
class HugeGraphSinkWriterMultiTableTest {

    private static final SeaTunnelRowType PERSON_ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"name", "age"},
                    new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});

    private static final SeaTunnelRowType COMPANY_ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"name", "industry"},
                    new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});

    // --- Cross-write isolation ---

    @Test
    void twoTablesTwoLabelsNoCrossWrite() {
        HugeGraphSinkConfig config = multiTableConfig();
        HugeGraphClient client = stubbedClient();

        HugeGraphSinkWriter personWriter =
                new HugeGraphSinkWriter(config, PERSON_ROW_TYPE, "hugegraph.person", client, 0);
        HugeGraphSinkWriter companyWriter =
                new HugeGraphSinkWriter(config, COMPANY_ROW_TYPE, "hugegraph.company", client, 1);

        List<HugeGraphSinkWriter.MappingEntry> personEntries = personWriter.mappingEntries();
        List<HugeGraphSinkWriter.MappingEntry> companyEntries = companyWriter.mappingEntries();

        assertEquals(1, personEntries.size(), "person writer should have exactly 1 mapping");
        assertEquals("person", personEntries.get(0).config.getLabel());

        assertEquals(1, companyEntries.size(), "company writer should have exactly 1 mapping");
        assertEquals("company", companyEntries.get(0).config.getLabel());
    }

    @Test
    void multiTableMappingDoesNotLeakBetweenWriters() {
        HugeGraphSinkConfig config = multiTableConfig();
        HugeGraphClient client = stubbedClient();

        HugeGraphSinkWriter personWriter =
                new HugeGraphSinkWriter(config, PERSON_ROW_TYPE, "hugegraph.person", client, 0);
        HugeGraphSinkWriter companyWriter =
                new HugeGraphSinkWriter(config, COMPANY_ROW_TYPE, "hugegraph.company", client, 1);

        // Verify person writer only writes to person label
        for (HugeGraphSinkWriter.MappingEntry entry : personWriter.mappingEntries()) {
            assertEquals("person", entry.config.getLabel());
            assertEquals(MappingConfig.LabelType.VERTEX, entry.config.getType());
        }

        // Verify company writer only writes to company label
        for (HugeGraphSinkWriter.MappingEntry entry : companyWriter.mappingEntries()) {
            assertEquals("company", entry.config.getLabel());
            assertEquals(MappingConfig.LabelType.VERTEX, entry.config.getType());
        }
    }

    // --- No-match fail-fast ---

    @Test
    void multiTableNoMatchThrowsException() {
        HugeGraphSinkConfig config = multiTableConfig();
        HugeGraphClient client = stubbedClient();

        HugeGraphConnectorException ex =
                assertThrows(
                        HugeGraphConnectorException.class,
                        () ->
                                new HugeGraphSinkWriter(
                                        config,
                                        PERSON_ROW_TYPE,
                                        "hugegraph.unknown_table",
                                        client,
                                        0));

        assertTrue(
                ex.getMessage().contains("No mapping matched"),
                "Error must state no mapping matched");
        assertTrue(
                ex.getMessage().contains("unknown_table"),
                "Error must include the actual tablePath");
        assertTrue(
                ex.getMessage().contains("source_table"),
                "Error must reference source_table so the user knows what to fix");
    }

    // --- Single-table backward compatibility ---

    @Test
    void singleTableAllMappingsActive() {
        HugeGraphSinkConfig config = singleTableConfig();
        HugeGraphClient client = stubbedClient();

        HugeGraphSinkWriter writer =
                new HugeGraphSinkWriter(config, PERSON_ROW_TYPE, "any.table.path", client, 0);

        List<HugeGraphSinkWriter.MappingEntry> entries = writer.mappingEntries();
        assertEquals(2, entries.size(), "both mappings should be active in single-table mode");
    }

    @Test
    void singleTableEmptyTablePathIsBackwardCompatible() {
        HugeGraphSinkConfig config = singleTableConfig();
        HugeGraphClient client = stubbedClient();

        HugeGraphSinkWriter writer = new HugeGraphSinkWriter(config, PERSON_ROW_TYPE, client, 0);

        List<HugeGraphSinkWriter.MappingEntry> entries = writer.mappingEntries();
        assertEquals(2, entries.size(), "all mappings active when tablePath is empty (old API)");
    }

    // --- Helpers ---

    private static HugeGraphSinkConfig multiTableConfig() {
        HugeGraphSinkConfig config = new HugeGraphSinkConfig();
        config.setMappings(
                Arrays.asList(
                        vertexMapping("person", "hugegraph.person"),
                        vertexMapping("company", "hugegraph.company")));
        return config;
    }

    private static HugeGraphSinkConfig singleTableConfig() {
        HugeGraphSinkConfig config = new HugeGraphSinkConfig();
        config.setMappings(
                Arrays.asList(vertexMapping("person", null), vertexMapping("company", null)));
        return config;
    }

    private static MappingConfig vertexMapping(String label, String sourceTable) {
        MappingConfig m = new MappingConfig();
        m.setType(MappingConfig.LabelType.VERTEX);
        m.setLabel(label);
        m.setIdStrategy(org.apache.hugegraph.structure.constant.IdStrategy.PRIMARY_KEY);
        m.setIdFields(Collections.singletonList("name"));
        if (sourceTable != null) {
            m.setSourceTable(sourceTable);
        }
        return m;
    }

    private static HugeGraphClient stubbedClient() {
        HugeGraphClient client = mock(HugeGraphClient.class);
        // VertexMapper constructor calls getVertexLabelId(label).
        when(client.getVertexLabelId(anyString())).thenReturn("1");
        // buildPropertyKeyCache calls getPropertyKey for each property field + id field.
        PropertyKey pk = mock(PropertyKey.class);
        when(pk.dataType()).thenReturn(DataType.TEXT);
        when(pk.cardinality()).thenReturn(Cardinality.SINGLE);
        when(client.getPropertyKey(anyString())).thenReturn(pk);
        return client;
    }
}
