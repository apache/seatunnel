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

package org.apache.seatunnel.connectors.seatunnel.kafka.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.format.avro.AvroDeserializationSchema;
import org.apache.seatunnel.format.avro.AvroFormatOptions;
import org.apache.seatunnel.format.avro.AvroSerializationSchema;
import org.apache.seatunnel.format.json.debezium.DebeziumJsonDeserializationSchemaDispatcher;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.DATABASE_NAME;
import static org.apache.seatunnel.api.options.ConnectorCommonOptions.SCHEMA_NAME;
import static org.apache.seatunnel.api.options.ConnectorCommonOptions.TABLE_NAME;
import static org.apache.seatunnel.api.options.table.TableIdentifierOptions.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSourceOptions.DEBEZIUM_RECORD_TABLE_FILTER;

public class KafkaSourceConfigTest {

    @Test
    void testDebeziumJsonDeserializationSchemaDispatcher() {
        Map<String, Object> schemaFields = new HashMap<>();
        schemaFields.put("id", "int");
        schemaFields.put("name", "string");
        schemaFields.put("description", "string");
        schemaFields.put("weight", "string");

        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", schemaFields);

        Map<String, Object> debeziumRecordTableFilter = new HashMap<>();
        debeziumRecordTableFilter.put(DATABASE_NAME.key(), "test");
        debeziumRecordTableFilter.put(SCHEMA_NAME.key(), "test");
        debeziumRecordTableFilter.put(TABLE_NAME.key(), "test");

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("group.id", "test");
        configMap.put("topic", "test");
        configMap.put("schema", schema);
        configMap.put("format", "debezium_json");
        configMap.put(DEBEZIUM_RECORD_TABLE_FILTER.key(), debeziumRecordTableFilter);

        KafkaSourceConfig sourceConfig = new KafkaSourceConfig(ReadonlyConfig.fromMap(configMap));

        DeserializationSchema<SeaTunnelRow> deserializationSchema =
                sourceConfig.getMapMetadata().get(TablePath.of("test")).getDeserializationSchema();

        Assertions.assertTrue(deserializationSchema instanceof KafkaEventTimeDeserializationSchema);

        DeserializationSchema<SeaTunnelRow> innerSchema =
                ((KafkaEventTimeDeserializationSchema) deserializationSchema).getDelegate();

        Assertions.assertTrue(innerSchema instanceof DebeziumJsonDeserializationSchemaDispatcher);
        Assertions.assertNotNull(
                ((DebeziumJsonDeserializationSchemaDispatcher) innerSchema)
                        .getTableDeserializationMap()
                        .get(TablePath.of("test.test.test")));
    }

    @Test
    void testKafkaHeaderFieldsExtendsOutputSchema() {
        Map<String, Object> schemaFields = new HashMap<>();
        schemaFields.put("user_id", "int");
        schemaFields.put("name", "string");

        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", schemaFields);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("topic", "test");
        configMap.put("schema", schema);
        configMap.put("format", "json");
        configMap.put("kafka_headers_fields", Arrays.asList("correlation_id", "trace_id"));

        KafkaSourceConfig sourceConfig = new KafkaSourceConfig(ReadonlyConfig.fromMap(configMap));

        CatalogTable catalogTable =
                sourceConfig.getMapMetadata().get(TablePath.of("test")).getCatalogTable();
        SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();

        // Schema should be extended with the 2 header fields
        Assertions.assertEquals(4, rowType.getTotalFields());
        List<String> fieldNames = Arrays.asList(rowType.getFieldNames());
        Assertions.assertTrue(fieldNames.contains("user_id"));
        Assertions.assertTrue(fieldNames.contains("name"));
        Assertions.assertTrue(fieldNames.contains("correlation_id"));
        Assertions.assertTrue(fieldNames.contains("trace_id"));
        // Header fields are STRING type
        Assertions.assertEquals(
                BasicType.STRING_TYPE, rowType.getFieldType(rowType.indexOf("correlation_id")));
        Assertions.assertEquals(
                BasicType.STRING_TYPE, rowType.getFieldType(rowType.indexOf("trace_id")));
    }

    @Test
    void testKafkaHeaderFieldsDeserializationSchemaWrapped() {
        Map<String, Object> schemaFields = new HashMap<>();
        schemaFields.put("user_id", "int");

        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", schemaFields);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("topic", "test");
        configMap.put("schema", schema);
        configMap.put("format", "json");
        configMap.put("kafka_headers_fields", Arrays.asList("correlation_id"));

        KafkaSourceConfig sourceConfig = new KafkaSourceConfig(ReadonlyConfig.fromMap(configMap));

        DeserializationSchema<SeaTunnelRow> deserializationSchema =
                sourceConfig.getMapMetadata().get(TablePath.of("test")).getDeserializationSchema();

        // Outer wrapper: KafkaEventTimeDeserializationSchema
        Assertions.assertInstanceOf(
                KafkaEventTimeDeserializationSchema.class, deserializationSchema);
        DeserializationSchema<SeaTunnelRow> inner =
                ((KafkaEventTimeDeserializationSchema) deserializationSchema).getDelegate();

        // Inner wrapper: KafkaHeadersDeserializationSchema
        Assertions.assertInstanceOf(KafkaHeadersDeserializationSchema.class, inner);
        SeaTunnelRowType producedType = (SeaTunnelRowType) inner.getProducedType();
        Assertions.assertEquals(2, producedType.getTotalFields());
        Assertions.assertEquals("correlation_id", producedType.getFieldName(1));
    }

    @Test
    void testKafkaHeaderFieldsExtendsSchemaForCompatibleKafkaConnectJsonFormat() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("topic", "test");
        configMap.put("format", "compatible_kafka_connect_json");
        configMap.put("kafka_headers_fields", Arrays.asList("correlation_id"));

        KafkaSourceConfig sourceConfig = new KafkaSourceConfig(ReadonlyConfig.fromMap(configMap));

        ConsumerMetadata metadata = sourceConfig.getMapMetadata().get(TablePath.of("test"));

        // Header fields stored for emitter-side injection
        Assertions.assertEquals(Arrays.asList("correlation_id"), metadata.getKafkaHeaderFields());

        // Output schema includes header field
        SeaTunnelRowType rowType = metadata.getCatalogTable().getSeaTunnelRowType();
        List<String> fieldNames = Arrays.asList(rowType.getFieldNames());
        Assertions.assertTrue(fieldNames.contains("correlation_id"));
    }

    @Test
    void testKafkaHeaderFieldsNotAppliedToNativeFormat() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("topic", "test");
        configMap.put("format", "native");
        configMap.put("kafka_headers_fields", Arrays.asList("correlation_id"));

        KafkaSourceConfig sourceConfig = new KafkaSourceConfig(ReadonlyConfig.fromMap(configMap));

        CatalogTable catalogTable =
                sourceConfig.getMapMetadata().get(TablePath.of("test")).getCatalogTable();
        SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();

        // NATIVE format already has its own fixed schema; header fields should NOT be added
        List<String> fieldNames = Arrays.asList(rowType.getFieldNames());
        Assertions.assertFalse(
                fieldNames.contains("correlation_id"),
                "NATIVE format should not add kafka_headers_fields to schema");
    }

    @Test
    void testDeserializationWithSchema() {
        Map<String, Object> schemaFields = new HashMap<>();
        schemaFields.put("id", "int");
        schemaFields.put("name", "string");
        schemaFields.put("description", "string");
        schemaFields.put("weight", "string");

        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", schemaFields);
        schema.put(TABLE.key(), "db1.table1");

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("group.id", "test");
        configMap.put("topic", "test");
        configMap.put("schema", schema);
        configMap.put("format", "text");

        KafkaSourceConfig sourceConfig = new KafkaSourceConfig(ReadonlyConfig.fromMap(configMap));

        DeserializationSchema<SeaTunnelRow> deserializationSchema =
                sourceConfig
                        .getMapMetadata()
                        .get(TablePath.of("db1.table1"))
                        .getDeserializationSchema();

        Assertions.assertNotNull(deserializationSchema);
    }

    @Test
    void testSchemaTableWithMultipleDotsCanBeUsedAsOpaqueTablePath() {
        String table = "reg.country.user_activity.activity";

        Map<String, Object> schemaFields = new HashMap<>();
        schemaFields.put("id", "int");
        schemaFields.put("name", "string");

        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", schemaFields);
        schema.put(TABLE.key(), table);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("group.id", "test");
        configMap.put("topic", "test");
        configMap.put("schema", schema);
        configMap.put("format", "json");

        KafkaSourceConfig sourceConfig = new KafkaSourceConfig(ReadonlyConfig.fromMap(configMap));

        Assertions.assertNotNull(sourceConfig.getMapMetadata().get(TablePath.of(null, table)));
    }

    // With the option enabled on the source, a message whose ROW value was written by an enabled
    // sink reads back with a null ROW field.
    @Test
    void testAvroRowNullableDeserializesNullRowField() throws IOException {
        KafkaSourceConfig sourceConfig =
                new KafkaSourceConfig(ReadonlyConfig.fromMap(avroSourceConfig(true)));
        ConsumerMetadata metadata = sourceConfig.getMapMetadata().get(TablePath.of("test"));
        SeaTunnelRowType rowType = metadata.getCatalogTable().getSeaTunnelRowType();
        Assertions.assertEquals(2, rowType.getTotalFields());

        DeserializationSchema<SeaTunnelRow> deserializationSchema =
                metadata.getDeserializationSchema();
        // The AVRO schema is always the delegate of the event time wrapper.
        Assertions.assertInstanceOf(
                KafkaEventTimeDeserializationSchema.class, deserializationSchema);
        Assertions.assertInstanceOf(
                AvroDeserializationSchema.class,
                ((KafkaEventTimeDeserializationSchema) deserializationSchema).getDelegate());

        SeaTunnelRow sourceRow = new SeaTunnelRow(rowType.getTotalFields());
        sourceRow.setField(rowType.indexOf("id"), 1);
        sourceRow.setField(rowType.indexOf("payload"), null);
        byte[] message = new AvroSerializationSchema(rowType, true).serialize(sourceRow);

        SeaTunnelRow row = deserializationSchema.deserialize(message);

        Assertions.assertEquals(1, row.getField(rowType.indexOf("id")));
        Assertions.assertNull(row.getField(rowType.indexOf("payload")));
    }

    // The option is opt-in: when it is absent the source keeps the legacy ROW layout, so messages
    // written with the default layout still read back unchanged.
    @Test
    void testAvroRowNullableAbsentKeepsLegacyLayout() throws IOException {
        assertLegacyAvroLayoutRoundTrip(null);
    }

    @Test
    void testAvroRowNullableFalseKeepsLegacyLayout() throws IOException {
        assertLegacyAvroLayoutRoundTrip(false);
    }

    // The option has to be declared by the source factory, otherwise the unknown option validator
    // rejects a job that sets avro_row_nullable.
    @Test
    void testKafkaSourceFactoryAcceptsAvroRowNullable() {
        Assertions.assertTrue(
                new KafkaSourceFactory()
                        .optionRule()
                        .getOptionalOptions()
                        .contains(AvroFormatOptions.AVRO_ROW_NULLABLE));
        Assertions.assertFalse(AvroFormatOptions.AVRO_ROW_NULLABLE.defaultValue());
    }

    /**
     * Builds an AVRO source config whose schema contains a nested ROW field, as a nested map is the
     * idiomatic way to declare a ROW in the schema block.
     *
     * @param rowNullable value for the avro_row_nullable option, or null to leave it unset
     * @return config map accepted by {@link KafkaSourceConfig}
     */
    private static Map<String, Object> avroSourceConfig(Boolean rowNullable) {
        Map<String, Object> payloadFields = new LinkedHashMap<>();
        payloadFields.put("InnerID", "int");
        Map<String, Object> schemaFields = new LinkedHashMap<>();
        schemaFields.put("id", "int");
        schemaFields.put("payload", payloadFields);
        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", schemaFields);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("group.id", "test");
        configMap.put("topic", "test");
        configMap.put("schema", schema);
        configMap.put("format", "avro");
        if (rowNullable != null) {
            configMap.put(AvroFormatOptions.AVRO_ROW_NULLABLE.key(), rowNullable);
        }
        return configMap;
    }

    /**
     * Serializes a row with a present ROW value in the legacy layout and asserts that the source
     * schema built from the given config reads it back unchanged.
     *
     * @param rowNullable value for the avro_row_nullable option, or null to leave it unset
     */
    private static void assertLegacyAvroLayoutRoundTrip(Boolean rowNullable) throws IOException {
        KafkaSourceConfig sourceConfig =
                new KafkaSourceConfig(ReadonlyConfig.fromMap(avroSourceConfig(rowNullable)));
        ConsumerMetadata metadata = sourceConfig.getMapMetadata().get(TablePath.of("test"));
        SeaTunnelRowType rowType = metadata.getCatalogTable().getSeaTunnelRowType();

        SeaTunnelRow sourceRow = new SeaTunnelRow(rowType.getTotalFields());
        sourceRow.setField(rowType.indexOf("id"), 1);
        sourceRow.setField(rowType.indexOf("payload"), new SeaTunnelRow(new Object[] {42}));
        byte[] message = new AvroSerializationSchema(rowType, false).serialize(sourceRow);

        SeaTunnelRow row = metadata.getDeserializationSchema().deserialize(message);

        Assertions.assertEquals(1, row.getField(rowType.indexOf("id")));
        Assertions.assertEquals(
                42, ((SeaTunnelRow) row.getField(rowType.indexOf("payload"))).getField(0));
    }
}
