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

package org.apache.seatunnel.connectors.seatunnel.pulsar.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorException;
import org.apache.seatunnel.format.avro.AvroDeserializationSchema;
import org.apache.seatunnel.format.avro.AvroFormatOptions;
import org.apache.seatunnel.format.avro.AvroSerializationSchema;
import org.apache.seatunnel.format.text.TextDeserializationSchema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class PulsarSourceTest {

    @Test
    void shouldDeserializeTextWithConfiguredFieldDelimiter() throws Exception {
        Map<String, Object> config = createBaseConfig("NEVER");
        config.put("topic", "persistent://public/default/events");
        config.put("format", "text");
        config.put("field_delimiter", "|");

        PulsarSource source =
                new PulsarSource(ReadonlyConfig.fromMap(config), createTextCatalogTable());
        DeserializationSchema<SeaTunnelRow> deserializationSchema =
                getDeserializationSchema(source);

        Assertions.assertTrue(deserializationSchema instanceof TextDeserializationSchema);
        SeaTunnelRow row =
                deserializationSchema.deserialize("1|Alice".getBytes(StandardCharsets.UTF_8));
        Assertions.assertEquals(1, row.getField(0));
        Assertions.assertEquals("Alice", row.getField(1));
    }

    @Test
    void shouldExposeProducedCatalogTablesForTablesConfigs() {
        Map<String, Object> config = createBaseConfig("NEVER");
        config.put(
                "tables_configs",
                Arrays.asList(
                        createTableConfig("db.orders", "persistent://public/default/orders"),
                        createTableConfig("db.users", "persistent://public/default/users")));

        PulsarSource source =
                new PulsarSource(
                        ReadonlyConfig.fromMap(config), CatalogTableUtil.buildSimpleTextTable());

        Assertions.assertEquals(2, source.getProducedCatalogTables().size());
        Assertions.assertEquals(
                "db.orders",
                source.getProducedCatalogTables().get(0).getTableId().toTablePath().toString());
        Assertions.assertEquals(
                "db.users",
                source.getProducedCatalogTables().get(1).getTableId().toTablePath().toString());
        Assertions.assertEquals(Boundedness.UNBOUNDED, source.getBoundedness());
    }

    @Test
    void shouldUseBoundednessOfAllTables() {
        Map<String, Object> config = createBaseConfig("LATEST");
        config.put(
                "tables_configs",
                Arrays.asList(
                        createTableConfig("db.orders", "persistent://public/default/orders"),
                        createTableConfig("db.users", "persistent://public/default/users")));

        PulsarSource source =
                new PulsarSource(
                        ReadonlyConfig.fromMap(config), CatalogTableUtil.buildSimpleTextTable());

        Assertions.assertEquals(Boundedness.BOUNDED, source.getBoundedness());
    }

    @Test
    void shouldRejectBatchModeForUnboundedMultiTableSource() {
        Map<String, Object> config = createBaseConfig("NEVER");
        config.put(
                "tables_configs",
                Arrays.asList(
                        createTableConfig("db.orders", "persistent://public/default/orders"),
                        createTableConfig("db.users", "persistent://public/default/users")));

        PulsarSource source =
                new PulsarSource(
                        ReadonlyConfig.fromMap(config), CatalogTableUtil.buildSimpleTextTable());

        Assertions.assertThrows(
                PulsarConnectorException.class,
                () -> source.setJobContext(new JobContext().setJobMode(JobMode.BATCH)));
    }

    @Test
    void shouldKeepSingleTableBatchCompatibilityForUnboundedSource() {
        Map<String, Object> config = createBaseConfig("NEVER");
        config.put("topic", "persistent://public/default/orders");

        PulsarSource source =
                new PulsarSource(
                        ReadonlyConfig.fromMap(config), CatalogTableUtil.buildSimpleTextTable());

        Assertions.assertDoesNotThrow(
                () -> source.setJobContext(new JobContext().setJobMode(JobMode.BATCH)));
    }

    @Test
    void shouldKeepSingleTableTablesConfigsBatchCompatibilityForUnboundedSource() {
        Map<String, Object> config = createBaseConfig("NEVER");
        config.put(
                "tables_configs",
                Arrays.asList(
                        createTableConfig("db.orders", "persistent://public/default/orders")));

        PulsarSource source =
                new PulsarSource(
                        ReadonlyConfig.fromMap(config), CatalogTableUtil.buildSimpleTextTable());

        Assertions.assertDoesNotThrow(
                () -> source.setJobContext(new JobContext().setJobMode(JobMode.BATCH)));
    }

    @Test
    void shouldRejectSingleTableWithoutSubscriptionNameInFactory() {
        Map<String, Object> config = createBaseConfig("NEVER");
        config.remove("subscription.name");
        config.put("topic", "persistent://public/default/orders");

        PulsarSourceFactory factory = new PulsarSourceFactory();

        Assertions.assertThrows(
                PulsarConnectorException.class,
                () ->
                        factory.createSource(
                                new TableSourceFactoryContext(
                                        ReadonlyConfig.fromMap(config),
                                        Thread.currentThread().getContextClassLoader())));
    }

    @Test
    void shouldAllowPerTableSubscriptionNameInFactoryForTablesConfigs() {
        Map<String, Object> config = createBaseConfig("NEVER");
        config.remove("subscription.name");
        Map<String, Object> tableConfig =
                createTableConfig("db.orders", "persistent://public/default/orders");
        tableConfig.put("subscription.name", "orders-sub");
        config.put("tables_configs", Arrays.asList(tableConfig));

        PulsarSourceFactory factory = new PulsarSourceFactory();

        Assertions.assertDoesNotThrow(
                () ->
                        factory.createSource(
                                        new TableSourceFactoryContext(
                                                ReadonlyConfig.fromMap(config),
                                                Thread.currentThread().getContextClassLoader()))
                                .createSource());
    }

    @Test
    void shouldNotTouchPulsarAdminDuringSourceConstructionForTopicPatternTables() {
        Map<String, Object> config = createBaseConfig("NEVER");
        config.put("admin.service-url", "invalid-admin-url");
        config.put(
                "tables_configs",
                Arrays.asList(
                        createPatternTableConfig(
                                "db.orders_pattern", "persistent://public/default/orders.*")));

        Assertions.assertDoesNotThrow(
                () ->
                        new PulsarSource(
                                ReadonlyConfig.fromMap(config),
                                CatalogTableUtil.buildSimpleTextTable()));
    }

    @Test
    void shouldSerializeMultiTableSourceForSparkExecution() {
        Map<String, Object> config = createBaseConfig("LATEST");
        config.put(
                "tables_configs",
                Arrays.asList(
                        createTableConfig("db.orders", "persistent://public/default/orders"),
                        createTableConfig("db.users", "persistent://public/default/users")));
        PulsarSource source =
                new PulsarSource(
                        ReadonlyConfig.fromMap(config), CatalogTableUtil.buildSimpleTextTable());

        Assertions.assertDoesNotThrow(() -> SerializationUtils.serialize(source));
    }

    // avro_row_nullable is a source-level option: the Pulsar reader has no writer-schema option, so
    // the reader schema is always derived from the row type and must follow the layout the upstream
    // writer used. The reader is driven through the real deserialization schema built for the
    // consumer metadata, so these tests fail if the option is dropped between config and format.
    @Test
    void shouldEnableAvroRowNullableInSingleTableSource() throws Exception {
        Map<String, Object> config = createAvroSourceConfig("LATEST");
        config.put("topic", "persistent://public/default/avro-events");
        config.put("avro_row_nullable", true);

        PulsarSource source =
                new PulsarSource(ReadonlyConfig.fromMap(config), createAvroCatalogTable());
        DeserializationSchema<SeaTunnelRow> deserializationSchema =
                getDeserializationSchema(source);

        Assertions.assertTrue(deserializationSchema instanceof AvroDeserializationSchema);
        SeaTunnelRowType rowType = getProducedRowType(source);
        byte[] message = new AvroSerializationSchema(rowType, true).serialize(rowWithoutPayload());

        SeaTunnelRow row = deserializationSchema.deserialize(message);

        Assertions.assertEquals(1, row.getField(0));
        Assertions.assertNull(row.getField(1));
    }

    @Test
    void shouldKeepLegacyAvroRowLayoutByDefaultInSingleTableSource() throws Exception {
        Map<String, Object> config = createAvroSourceConfig("LATEST");
        config.put("topic", "persistent://public/default/avro-events");

        PulsarSource source =
                new PulsarSource(ReadonlyConfig.fromMap(config), createAvroCatalogTable());
        DeserializationSchema<SeaTunnelRow> deserializationSchema =
                getDeserializationSchema(source);

        SeaTunnelRowType rowType = getProducedRowType(source);
        byte[] message = new AvroSerializationSchema(rowType).serialize(rowWithPayload(42));

        SeaTunnelRow row = deserializationSchema.deserialize(message);

        Assertions.assertEquals(1, row.getField(0));
        Assertions.assertEquals(42, ((SeaTunnelRow) row.getField(1)).getField(0));
    }

    @Test
    void shouldKeepLegacyAvroRowLayoutWhenOptionDisabledInSingleTableSource() throws Exception {
        Map<String, Object> config = createAvroSourceConfig("LATEST");
        config.put("topic", "persistent://public/default/avro-events");
        config.put("avro_row_nullable", false);

        PulsarSource source =
                new PulsarSource(ReadonlyConfig.fromMap(config), createAvroCatalogTable());
        DeserializationSchema<SeaTunnelRow> deserializationSchema =
                getDeserializationSchema(source);

        SeaTunnelRowType rowType = getProducedRowType(source);
        byte[] message = new AvroSerializationSchema(rowType).serialize(rowWithPayload(42));

        SeaTunnelRow row = deserializationSchema.deserialize(message);

        Assertions.assertEquals(42, ((SeaTunnelRow) row.getField(1)).getField(0));
    }

    // The per-table merged config is the value the reader actually sees, so a per-table flag must
    // be able to enable the nullable layout even when the global config does not set it.
    @Test
    void shouldEnableAvroRowNullablePerTableInTablesConfigs() throws Exception {
        Map<String, Object> config = createAvroSourceConfig("LATEST");
        Map<String, Object> tableConfig =
                createTableConfig("db.orders", "persistent://public/default/orders");
        tableConfig.put("avro_row_nullable", true);
        config.put("tables_configs", Arrays.asList(tableConfig));

        PulsarSource source =
                new PulsarSource(
                        ReadonlyConfig.fromMap(config), CatalogTableUtil.buildSimpleTextTable());
        DeserializationSchema<SeaTunnelRow> deserializationSchema =
                getDeserializationSchema(source);

        SeaTunnelRowType rowType = getProducedRowType(source);
        byte[] message = new AvroSerializationSchema(rowType, true).serialize(rowWithoutPayload());

        SeaTunnelRow row = deserializationSchema.deserialize(message);

        Assertions.assertEquals(1, row.getField(0));
        Assertions.assertNull(row.getField(1));
    }

    // Precedence: the per-table value overrides the global one, so a per-table false must keep the
    // legacy layout even when the global option is enabled.
    @Test
    void shouldGivePerTableAvroRowNullablePrecedenceOverGlobalValue() throws Exception {
        Map<String, Object> config = createAvroSourceConfig("LATEST");
        config.put("avro_row_nullable", true);
        Map<String, Object> tableConfig =
                createTableConfig("db.orders", "persistent://public/default/orders");
        tableConfig.put("avro_row_nullable", false);
        config.put("tables_configs", Arrays.asList(tableConfig));

        PulsarSource source =
                new PulsarSource(
                        ReadonlyConfig.fromMap(config), CatalogTableUtil.buildSimpleTextTable());
        DeserializationSchema<SeaTunnelRow> deserializationSchema =
                getDeserializationSchema(source);

        SeaTunnelRowType rowType = getProducedRowType(source);
        byte[] message = new AvroSerializationSchema(rowType).serialize(rowWithPayload(42));

        SeaTunnelRow row = deserializationSchema.deserialize(message);

        Assertions.assertEquals(1, row.getField(0));
        Assertions.assertEquals(42, ((SeaTunnelRow) row.getField(1)).getField(0));
    }

    @Test
    void shouldDeclareAvroRowNullableOptionInSourceFactory() {
        Assertions.assertTrue(
                new PulsarSourceFactory()
                        .optionRule()
                        .getOptionalOptions()
                        .contains(AvroFormatOptions.AVRO_ROW_NULLABLE));
    }

    private Map<String, Object> createAvroSourceConfig(String stopMode) {
        Map<String, Object> config = createBaseConfig(stopMode);
        config.put("format", "AVRO");
        config.put("schema", createNestedRowSchema());
        return config;
    }

    /**
     * Builds the schema block for a row type of {@code {id INT, payload ROW<InnerID INT>}}. The
     * nested ROW is expressed as a nested map, which is the idiomatic {@code schema.fields} form
     * for a complex type.
     */
    private Map<String, Object> createNestedRowSchema() {
        Map<String, Object> payloadFields = new LinkedHashMap<>();
        payloadFields.put("InnerID", "INT");
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("id", "INT");
        fields.put("payload", payloadFields);
        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", fields);
        return schema;
    }

    private CatalogTable createAvroCatalogTable() {
        SeaTunnelRowType innerRowType =
                new SeaTunnelRowType(
                        new String[] {"InnerID"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "payload"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, innerRowType});
        return CatalogTableUtil.getCatalogTable("", "", "", "pulsar_avro_source", rowType);
    }

    private SeaTunnelRowType getProducedRowType(PulsarSource source) {
        return source.getProducedCatalogTables().get(0).getSeaTunnelRowType();
    }

    private SeaTunnelRow rowWithoutPayload() {
        return new SeaTunnelRow(new Object[] {1, null});
    }

    private SeaTunnelRow rowWithPayload(int innerId) {
        SeaTunnelRow payload = new SeaTunnelRow(1);
        payload.setField(0, innerId);
        return new SeaTunnelRow(new Object[] {1, payload});
    }

    private Map<String, Object> createBaseConfig(String stopMode) {
        Map<String, Object> config = new HashMap<>();
        config.put("subscription.name", "seatunnel-sub");
        config.put("client.service-url", "pulsar://localhost:6650");
        config.put("admin.service-url", "http://localhost:8080");
        config.put("format", "JSON");
        config.put("cursor.startup.mode", "EARLIEST");
        config.put("cursor.stop.mode", stopMode);
        config.put("cursor.reset.mode", "EARLIEST");
        return config;
    }

    private CatalogTable createTextCatalogTable() {
        List<Column> columns = new ArrayList<>();
        columns.add(
                PhysicalColumn.builder()
                        .name("id")
                        .dataType(BasicType.INT_TYPE)
                        .nullable(true)
                        .build());
        columns.add(
                PhysicalColumn.builder()
                        .name("name")
                        .dataType(BasicType.STRING_TYPE)
                        .nullable(true)
                        .build());
        return CatalogTable.of(
                TableIdentifier.of("default", "default", "pulsar_table"),
                TableSchema.builder().columns(columns).build(),
                new HashMap<>(),
                new ArrayList<>(),
                "Pulsar text table");
    }

    @SuppressWarnings("unchecked")
    private DeserializationSchema<SeaTunnelRow> getDeserializationSchema(PulsarSource source)
            throws ReflectiveOperationException {
        Field field = PulsarSource.class.getDeclaredField("consumerMetadataMap");
        field.setAccessible(true);
        Map<TablePath, PulsarConsumerMetadata> consumerMetadataMap =
                (Map<TablePath, PulsarConsumerMetadata>) field.get(source);
        return consumerMetadataMap.values().iterator().next().getDeserializationSchema();
    }

    private Map<String, Object> createTableConfig(String tablePath, String topic) {
        Map<String, Object> config = new HashMap<>();
        config.put("table_path", tablePath);
        config.put("topic", topic);
        return config;
    }

    private Map<String, Object> createPatternTableConfig(String tablePath, String topicPattern) {
        Map<String, Object> config = new HashMap<>();
        config.put("table_path", tablePath);
        config.put("topic-pattern", topicPattern);
        return config;
    }
}
