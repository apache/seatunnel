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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.utils;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.apache.hugegraph.structure.constant.Frequency;
import org.apache.hugegraph.structure.constant.IdStrategy;
import org.apache.hugegraph.structure.schema.VertexLabel;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * validateConfigOnly must catch every deterministic (server-independent) config error so that
 * HugeGraphSink can run it before any server write. These tests exercise it with a null client — a
 * regression in the "does not touch the server" property would surface as an NPE.
 */
class SchemaValidatorTest {

    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id", "name", "created"},
                    new SeaTunnelDataType<?>[] {
                        BasicType.LONG_TYPE, BasicType.STRING_TYPE, BasicType.LONG_TYPE
                    });

    private final SchemaValidator validator = new SchemaValidator(null, ROW_TYPE);

    @Test
    void acceptsValidVertexMapping() {
        MappingConfig m = new MappingConfig();
        m.setType(MappingConfig.LabelType.VERTEX);
        m.setLabel("person");
        m.setIdStrategy(IdStrategy.PRIMARY_KEY);
        m.setIdFields(Collections.singletonList("id"));
        m.setProperties(Arrays.asList("name"));

        assertDoesNotThrow(() -> validator.validateConfigOnly(Collections.singletonList(m)));
    }

    @Test
    void acceptsValidEdgeMapping() {
        MappingConfig m = new MappingConfig();
        m.setType(MappingConfig.LabelType.EDGE);
        m.setLabel("knows");
        m.setSourceConfig(sourceTarget("person", "id"));
        m.setTargetConfig(sourceTarget("person", "id"));
        m.setFrequency(Frequency.SINGLE);
        m.setProperties(Collections.singletonList("name"));

        assertDoesNotThrow(() -> validator.validateConfigOnly(Collections.singletonList(m)));
    }

    @Test
    void rejectsVertexMissingIdFields() {
        MappingConfig m = new MappingConfig();
        m.setType(MappingConfig.LabelType.VERTEX);
        m.setLabel("person");
        m.setIdStrategy(IdStrategy.PRIMARY_KEY);

        assertThrows(
                HugeGraphConnectorException.class,
                () -> validator.validateConfigOnly(Collections.singletonList(m)));
    }

    @Test
    void rejectsEdgeMissingSourceConfig() {
        MappingConfig m = new MappingConfig();
        m.setType(MappingConfig.LabelType.EDGE);
        m.setLabel("knows");
        m.setTargetConfig(sourceTarget("person", "id"));

        assertThrows(
                HugeGraphConnectorException.class,
                () -> validator.validateConfigOnly(Collections.singletonList(m)));
    }

    @Test
    void rejectsMultipleEdgeWithoutSortKeys() {
        MappingConfig m = new MappingConfig();
        m.setType(MappingConfig.LabelType.EDGE);
        m.setLabel("visits");
        m.setSourceConfig(sourceTarget("person", "id"));
        m.setTargetConfig(sourceTarget("place", "id"));
        m.setFrequency(Frequency.MULTIPLE);

        HugeGraphConnectorException ex =
                assertThrows(
                        HugeGraphConnectorException.class,
                        () -> validator.validateConfigOnly(Collections.singletonList(m)));
        assertTrue(ex.getMessage().contains("sortKeys"));
    }

    @Test
    void rejectsPropertyReferencingUnknownSourceField() {
        MappingConfig m = new MappingConfig();
        m.setType(MappingConfig.LabelType.VERTEX);
        m.setLabel("person");
        m.setIdStrategy(IdStrategy.PRIMARY_KEY);
        m.setIdFields(Collections.singletonList("id"));
        m.setProperties(Collections.singletonList("does_not_exist"));

        assertThrows(
                HugeGraphConnectorException.class,
                () -> validator.validateConfigOnly(Collections.singletonList(m)));
    }

    @Test
    void firstMappingFailingStopsBeforeSecondIsChecked() {
        // Ordering matters: HugeGraphSink relies on this method to fail fast before ensureSchema
        // creates any schema for later mappings.
        MappingConfig bad = new MappingConfig();
        bad.setType(MappingConfig.LabelType.EDGE);
        bad.setLabel("knows");
        // missing sourceConfig / targetConfig
        MappingConfig good = new MappingConfig();
        good.setType(MappingConfig.LabelType.VERTEX);
        good.setLabel("person");
        good.setIdStrategy(IdStrategy.PRIMARY_KEY);
        good.setIdFields(Collections.singletonList("id"));

        assertThrows(
                HugeGraphConnectorException.class,
                () -> validator.validateConfigOnly(Arrays.asList(bad, good)));
    }

    @Test
    void rejectsBothNullableAndNotNullableKeys() {
        MappingConfig m = new MappingConfig();
        m.setType(MappingConfig.LabelType.VERTEX);
        m.setLabel("person");
        m.setIdStrategy(IdStrategy.PRIMARY_KEY);
        m.setIdFields(Collections.singletonList("id"));
        m.setNullableKeys(Collections.singletonList("name"));
        m.setNotNullableKeys(Collections.singletonList("created"));

        HugeGraphConnectorException ex =
                assertThrows(
                        HugeGraphConnectorException.class,
                        () -> validator.validateConfigOnly(Collections.singletonList(m)));
        assertTrue(ex.getMessage().contains("mutually"));
    }

    @Test
    void acceptsOnlyNotNullableKeys() {
        MappingConfig m = new MappingConfig();
        m.setType(MappingConfig.LabelType.VERTEX);
        m.setLabel("person");
        m.setIdStrategy(IdStrategy.PRIMARY_KEY);
        m.setIdFields(Collections.singletonList("id"));
        m.setNotNullableKeys(Collections.singletonList("name"));

        assertDoesNotThrow(() -> validator.validateConfigOnly(Collections.singletonList(m)));
    }

    @Test
    void rejectsRawIdPassthroughVertexWithPrimaryKey() {
        // ~id passthrough supplies the id externally; PRIMARY_KEY derives it from properties, so
        // this combination must be rejected up front.
        SchemaValidator rawValidator =
                new SchemaValidator(
                        null,
                        new SeaTunnelRowType(
                                new String[] {"~id", "name"},
                                new SeaTunnelDataType<?>[] {
                                    BasicType.STRING_TYPE, BasicType.STRING_TYPE
                                }));
        MappingConfig m = new MappingConfig();
        m.setType(MappingConfig.LabelType.VERTEX);
        m.setLabel("person");
        m.setIdStrategy(IdStrategy.PRIMARY_KEY);
        m.setIdFields(Collections.singletonList("~id"));

        HugeGraphConnectorException ex =
                assertThrows(
                        HugeGraphConnectorException.class,
                        () -> rawValidator.validateConfigOnly(Collections.singletonList(m)));
        assertTrue(ex.getMessage().contains("CUSTOMIZE"));
    }

    @Test
    void acceptsRawIdPassthroughVertexWithCustomize() {
        SchemaValidator rawValidator =
                new SchemaValidator(
                        null,
                        new SeaTunnelRowType(
                                new String[] {"~id", "name"},
                                new SeaTunnelDataType<?>[] {
                                    BasicType.STRING_TYPE, BasicType.STRING_TYPE
                                }));
        MappingConfig m = new MappingConfig();
        m.setType(MappingConfig.LabelType.VERTEX);
        m.setLabel("person");
        m.setIdStrategy(IdStrategy.CUSTOMIZE_STRING);
        m.setIdFields(Collections.singletonList("~id"));

        assertDoesNotThrow(() -> rawValidator.validateConfigOnly(Collections.singletonList(m)));
    }

    @Test
    void failsFastWhenExistingVertexLabelPrimaryKeyMismatch() {
        // Reproduces the schema-pollution loop: a VertexLabel already exists with PK=[id] but the
        // (corrected) config wants PK=[name]. This must abort BEFORE any creation, not after
        // ensureSchema has already written other schema.
        HugeGraphClient client = mock(HugeGraphClient.class);
        VertexLabel existing = mock(VertexLabel.class);
        when(existing.idStrategy()).thenReturn(IdStrategy.PRIMARY_KEY);
        when(existing.primaryKeys()).thenReturn(Collections.singletonList("id"));
        when(client.getVertexLabelOrNull("person")).thenReturn(existing);
        when(client.getVertexLabel("person")).thenReturn(existing);

        SchemaValidator serverValidator = new SchemaValidator(client, ROW_TYPE);
        MappingConfig m = new MappingConfig();
        m.setType(MappingConfig.LabelType.VERTEX);
        m.setLabel("person");
        m.setIdStrategy(IdStrategy.PRIMARY_KEY);
        m.setIdFields(Collections.singletonList("name"));

        HugeGraphConnectorException ex =
                assertThrows(
                        HugeGraphConnectorException.class,
                        () -> serverValidator.validateExistingLabels(Collections.singletonList(m)));
        assertTrue(ex.getMessage().contains("primary key mismatch"));
    }

    @Test
    void skipsLabelsThatDoNotYetExist() {
        // Nothing exists on the server yet -> validateExistingLabels is a no-op (ensureSchema will
        // create), so it must not throw or dereference a missing label.
        HugeGraphClient client = mock(HugeGraphClient.class);
        when(client.getVertexLabelOrNull("person")).thenReturn(null);

        SchemaValidator serverValidator = new SchemaValidator(client, ROW_TYPE);
        MappingConfig m = new MappingConfig();
        m.setType(MappingConfig.LabelType.VERTEX);
        m.setLabel("person");
        m.setIdStrategy(IdStrategy.PRIMARY_KEY);
        m.setIdFields(Collections.singletonList("id"));

        assertDoesNotThrow(
                () -> serverValidator.validateExistingLabels(Collections.singletonList(m)));
    }

    @Test
    void rejectsUnfoldWithPrimaryKeyStrategy() {
        MappingConfig m = new MappingConfig();
        m.setType(MappingConfig.LabelType.VERTEX);
        m.setLabel("person");
        m.setIdStrategy(IdStrategy.PRIMARY_KEY);
        m.setIdFields(Collections.singletonList("id"));
        m.setUnfold(true);

        HugeGraphConnectorException ex =
                assertThrows(
                        HugeGraphConnectorException.class,
                        () -> validator.validateConfigOnly(Collections.singletonList(m)));
        assertTrue(ex.getMessage().contains("CUSTOMIZE"));
    }

    @Test
    void rejectsUnfoldWithMultipleIdFields() {
        SchemaValidator v =
                new SchemaValidator(
                        null,
                        new SeaTunnelRowType(
                                new String[] {"a", "b"},
                                new SeaTunnelDataType<?>[] {
                                    BasicType.STRING_TYPE, BasicType.STRING_TYPE
                                }));
        MappingConfig m = new MappingConfig();
        m.setType(MappingConfig.LabelType.VERTEX);
        m.setLabel("person");
        m.setIdStrategy(IdStrategy.CUSTOMIZE_STRING);
        m.setIdFields(Arrays.asList("a", "b"));
        m.setUnfold(true);

        HugeGraphConnectorException ex =
                assertThrows(
                        HugeGraphConnectorException.class,
                        () -> v.validateConfigOnly(Collections.singletonList(m)));
        assertTrue(ex.getMessage().contains("exactly one id field"));
    }

    private static MappingConfig.SourceTargetConfig sourceTarget(String label, String idField) {
        MappingConfig.SourceTargetConfig st = new MappingConfig.SourceTargetConfig();
        st.setLabel(label);
        st.setIdFields(Collections.singletonList(idField));
        return st;
    }
}
