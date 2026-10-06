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

package org.apache.seatunnel.connectors.seatunnel.natsjetstream.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.connectors.seatunnel.natsjetstream.config.NatsJetStreamMessageFormat;
import org.apache.seatunnel.connectors.seatunnel.natsjetstream.config.NatsJetStreamSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.natsjetstream.exception.NatsJetStreamConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class NatsJetStreamSinkFactoryTest {

    private final NatsJetStreamSinkFactory factory = new NatsJetStreamSinkFactory();

    @Test
    void optionRuleAndFactoryIdentifier() {
        Assertions.assertNotNull(factory.optionRule());
        Assertions.assertEquals(
                NatsJetStreamSinkOptions.CONNECTOR_IDENTITY, factory.factoryIdentifier());
    }

    @Test
    void createSinkRejectsBlankUrl() {
        Map<String, Object> config = validJsonConfig();
        config.put(NatsJetStreamSinkOptions.URL.key(), "   ");

        assertInvalidOption(config, defaultCatalogTable(), "url", "must not be blank");
    }

    @Test
    void createSinkRejectsBlankJsonSubject() {
        Map<String, Object> config = validJsonConfig();
        config.put(NatsJetStreamSinkOptions.SUBJECT.key(), "   ");

        assertInvalidOption(config, defaultCatalogTable(), "subject", "must not be blank");
    }

    @Test
    void createSinkAcceptsValidJsonConfig() {
        Assertions.assertDoesNotThrow(() -> createSink(validJsonConfig(), defaultCatalogTable()));
    }

    @Test
    void createSinkRejectsMissingUrl() {
        Map<String, Object> config = validJsonConfig();
        config.remove(NatsJetStreamSinkOptions.URL.key());

        assertInvalidOption(config, defaultCatalogTable(), "url", "must not be blank");
    }

    @Test
    void createSinkRejectsMissingJsonSubject() {
        Map<String, Object> config = validJsonConfig();
        config.remove(NatsJetStreamSinkOptions.SUBJECT.key());

        assertInvalidOption(config, defaultCatalogTable(), "subject", "must not be blank");
    }

    @Test
    void createSinkRejectsIncompleteUsernamePasswordPair() {
        Map<String, Object> config = validJsonConfig();
        config.put(NatsJetStreamSinkOptions.USERNAME.key(), "alice");

        assertInvalidOption(
                config, defaultCatalogTable(), "password", "must be configured together");
    }

    @Test
    void createSinkRejectsConflictingAuthentication() {
        Map<String, Object> config = validJsonConfig();
        config.put(NatsJetStreamSinkOptions.USERNAME.key(), "alice");
        config.put(NatsJetStreamSinkOptions.PASSWORD.key(), "secret");
        config.put(NatsJetStreamSinkOptions.TOKEN.key(), "token");

        assertInvalidOption(
                config, defaultCatalogTable(), "token", "cannot be configured together");
    }

    @Test
    void createSinkRejectsUnknownFormatValue() {
        Map<String, Object> config = validJsonConfig();
        config.put(NatsJetStreamSinkOptions.FORMAT.key(), "yaml");

        Assertions.assertThrows(
                IllegalArgumentException.class, () -> createSink(config, defaultCatalogTable()));
    }

    @Test
    void createSinkRejectsNativeWithoutDataMapping() {
        Map<String, Object> config = validNativeConfig();
        config.put(NatsJetStreamSinkOptions.NATIVE_FIELDS.key(), new HashMap<String, String>());

        assertInvalidOption(
                config,
                defaultCatalogTable(),
                NatsJetStreamSinkOptions.NATIVE_FIELDS.key(),
                "must define at least the `data` field mapping");
    }

    @Test
    void createSinkRejectsNativeWithoutSubjectFallbackOrMapping() {
        Map<String, Object> config = validNativeConfig();
        config.remove(NatsJetStreamSinkOptions.SUBJECT.key());
        Map<String, String> mappings = new HashMap<>();
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_DATA, "payload");
        config.put(NatsJetStreamSinkOptions.NATIVE_FIELDS.key(), mappings);

        assertInvalidOption(config, defaultCatalogTable(), "subject", "must be configured");
    }

    @Test
    void createSinkRejectsNativeSubjectMappingToMissingFieldWithoutFallback() {
        Map<String, Object> config = validNativeConfig();
        config.remove(NatsJetStreamSinkOptions.SUBJECT.key());
        Map<String, String> mappings = new HashMap<>();
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_DATA, "payload");
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_SUBJECT, "missing_subject");
        config.put(NatsJetStreamSinkOptions.NATIVE_FIELDS.key(), mappings);

        assertInvalidField(
                config, defaultCatalogTable(), "missing_subject", "does not exist in table schema");
    }

    @Test
    void createSinkAcceptsNativeSubjectMappingToMissingFieldWithFallback() {
        Map<String, Object> config = validNativeConfig();
        Map<String, String> mappings = new HashMap<>();
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_DATA, "payload");
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_SUBJECT, "missing_subject");
        config.put(NatsJetStreamSinkOptions.NATIVE_FIELDS.key(), mappings);

        Assertions.assertDoesNotThrow(() -> createSink(config, defaultCatalogTable()));
    }

    @Test
    void createSinkRejectsNativeMissingMappedPayloadField() {
        Map<String, Object> config = validNativeConfig();
        Map<String, String> mappings = new HashMap<>();
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_DATA, "missing_payload");
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_SUBJECT, "dynamic_subject");
        config.put(NatsJetStreamSinkOptions.NATIVE_FIELDS.key(), mappings);

        assertInvalidField(
                config, defaultCatalogTable(), "missing_payload", "does not exist in table schema");
    }

    @Test
    void createSinkRejectsNativeWrongPayloadType() {
        Map<String, Object> config = validNativeConfig();
        Map<String, String> mappings = new HashMap<>();
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_DATA, "dynamic_subject");
        config.put(NatsJetStreamSinkOptions.NATIVE_FIELDS.key(), mappings);

        assertInvalidField(
                config, defaultCatalogTable(), "dynamic_subject", "must use `BYTES` type");
    }

    @Test
    void createSinkRejectsNativeWrongSubjectType() {
        Map<String, Object> config = validNativeConfig();
        Map<String, String> mappings = new HashMap<>();
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_DATA, "payload");
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_SUBJECT, "payload");
        config.put(NatsJetStreamSinkOptions.NATIVE_FIELDS.key(), mappings);

        assertInvalidFieldOrOption(
                config, defaultCatalogTable(), "payload", "native mapping `subject`");
    }

    @Test
    void createSinkRejectsNativeWrongHeadersType() {
        Map<String, Object> config = validNativeConfig();
        Map<String, String> mappings = new HashMap<>();
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_DATA, "payload");
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_HEADERS, "dynamic_subject");
        config.put(NatsJetStreamSinkOptions.NATIVE_FIELDS.key(), mappings);

        assertInvalidField(config, defaultCatalogTable(), "dynamic_subject", "MAP<STRING, STRING>");
    }

    @Test
    void createSinkAcceptsNativeCustomFieldMapping() {
        Map<String, Object> config = validNativeConfig();
        Map<String, String> mappings = new HashMap<>();
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_DATA, "payload");
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_SUBJECT, "dynamic_subject");
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_HEADERS, "attributes");
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_ID, "message_id");
        config.put(NatsJetStreamSinkOptions.NATIVE_FIELDS.key(), mappings);

        Assertions.assertDoesNotThrow(() -> createSink(config, defaultCatalogTable()));
    }

    @Test
    void createSinkAcceptsNativeDefaultMappingWithOnlyDataField() {
        Map<String, Object> config = validJsonConfig();
        config.put(NatsJetStreamSinkOptions.FORMAT.key(), NatsJetStreamMessageFormat.NATIVE.name());
        Assertions.assertDoesNotThrow(() -> createSink(config, dataOnlyCatalogTable()));
    }

    private void assertInvalidOption(
            Map<String, Object> config,
            CatalogTable catalogTable,
            String optionKey,
            String fragment) {
        NatsJetStreamConnectorException exception =
                Assertions.assertThrows(
                        NatsJetStreamConnectorException.class,
                        () -> createSink(config, catalogTable));
        Assertions.assertTrue(exception.getMessage().contains("option `" + optionKey + "`"));
        Assertions.assertTrue(exception.getMessage().contains(fragment));
    }

    private void assertInvalidField(
            Map<String, Object> config,
            CatalogTable catalogTable,
            String fieldName,
            String fragment) {
        NatsJetStreamConnectorException exception =
                Assertions.assertThrows(
                        NatsJetStreamConnectorException.class,
                        () -> createSink(config, catalogTable));
        Assertions.assertTrue(exception.getMessage().contains("field `" + fieldName + "`"));
        Assertions.assertTrue(exception.getMessage().contains(fragment));
    }

    private void assertInvalidFieldOrOption(
            Map<String, Object> config,
            CatalogTable catalogTable,
            String fieldOrOptionName,
            String fragment) {
        NatsJetStreamConnectorException exception =
                Assertions.assertThrows(
                        NatsJetStreamConnectorException.class,
                        () -> createSink(config, catalogTable));
        Assertions.assertTrue(
                exception.getMessage().contains("field `" + fieldOrOptionName + "`")
                        || exception.getMessage().contains("option `" + fieldOrOptionName + "`"));
        Assertions.assertTrue(exception.getMessage().contains(fragment));
    }

    private void createSink(Map<String, Object> config, CatalogTable catalogTable) {
        factory.createSink(
                new TableSinkFactoryContext(
                        catalogTable, ReadonlyConfig.fromMap(config), getClass().getClassLoader()));
    }

    private Map<String, Object> validJsonConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(NatsJetStreamSinkOptions.URL.key(), "nats://127.0.0.1:4222");
        config.put(NatsJetStreamSinkOptions.SUBJECT.key(), "orders.events");
        config.put(NatsJetStreamSinkOptions.FORMAT.key(), NatsJetStreamMessageFormat.JSON.name());
        return config;
    }

    private Map<String, Object> validNativeConfig() {
        Map<String, Object> config = validJsonConfig();
        config.put(NatsJetStreamSinkOptions.FORMAT.key(), NatsJetStreamMessageFormat.NATIVE.name());
        Map<String, String> mappings = new HashMap<>();
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_DATA, "payload");
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_SUBJECT, "dynamic_subject");
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_HEADERS, "attributes");
        mappings.put(NatsJetStreamSinkOptions.NATIVE_MAPPING_ID, "message_id");
        config.put(NatsJetStreamSinkOptions.NATIVE_FIELDS.key(), mappings);
        return config;
    }

    private CatalogTable dataOnlyCatalogTable() {
        List<Column> columns = new ArrayList<>();
        columns.add(
                PhysicalColumn.builder()
                        .name("data")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .nullable(false)
                        .build());
        TableSchema tableSchema = TableSchema.builder().columns(columns).build();
        return CatalogTable.of(
                TableIdentifier.of("default", "default", "nats_jetstream_data_only_sink"),
                tableSchema,
                new HashMap<>(),
                new ArrayList<>(),
                "nats jetstream data only sink test table");
    }

    private CatalogTable defaultCatalogTable() {
        List<Column> columns = new ArrayList<>();
        columns.add(
                PhysicalColumn.builder()
                        .name("dynamic_subject")
                        .dataType(BasicType.STRING_TYPE)
                        .nullable(true)
                        .build());
        columns.add(
                PhysicalColumn.builder()
                        .name("message_id")
                        .dataType(BasicType.STRING_TYPE)
                        .nullable(true)
                        .build());
        columns.add(
                PhysicalColumn.builder()
                        .name("attributes")
                        .dataType(new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE))
                        .nullable(true)
                        .build());
        columns.add(
                PhysicalColumn.builder()
                        .name("payload")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .nullable(false)
                        .build());
        TableSchema tableSchema = TableSchema.builder().columns(columns).build();
        return CatalogTable.of(
                TableIdentifier.of("default", "default", "nats_jetstream_sink"),
                tableSchema,
                new HashMap<>(),
                new ArrayList<>(),
                "nats jetstream sink test table");
    }
}
