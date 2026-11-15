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

package org.apache.seatunnel.transform.metadata;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.MetadataColumn;
import org.apache.seatunnel.api.table.catalog.MetadataSchema;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.KnowledgeSyncMetadataField;
import org.apache.seatunnel.api.table.type.MetadataUtil;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.exception.TransformException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MetadataTransformTest {

    static CatalogTable catalogTable;

    static Object[] values;

    static SeaTunnelRow inputRow;

    static Long eventTime;

    @BeforeAll
    static void setUp() {
        List<Column> metadata = new ArrayList<>();
        metadata.add(
                MetadataColumn.of(
                        CommonOptions.EVENT_TIME.getName(),
                        BasicType.LONG_TYPE,
                        (Long) null,
                        true,
                        null,
                        null));
        metadata.add(
                MetadataColumn.of(
                        CommonOptions.DELAY.getName(),
                        BasicType.LONG_TYPE,
                        (Long) null,
                        true,
                        null,
                        null));
        metadata.add(
                MetadataColumn.of(
                        CommonOptions.PARTITION.getName(),
                        ArrayType.STRING_ARRAY_TYPE,
                        (Long) null,
                        true,
                        null,
                        null));
        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", TablePath.DEFAULT),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "key1",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key2",
                                                BasicType.INT_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key3",
                                                BasicType.LONG_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key4",
                                                BasicType.DOUBLE_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key5",
                                                BasicType.FLOAT_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .build(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        "comment",
                        "test",
                        MetadataSchema.builder().columns(metadata).build());
        values = new Object[] {"value1", 1, 896657703886127105L, 3.1415916, 3.14};
        inputRow = new SeaTunnelRow(values);
        inputRow.setTableId(TablePath.DEFAULT.getFullName());
        eventTime = LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli();
        MetadataUtil.setDelay(inputRow, 150L);
        MetadataUtil.setEventTime(inputRow, eventTime);
        MetadataUtil.setPartition(inputRow, Arrays.asList("key1", "key2").toArray(new String[0]));
    }

    @Test
    void testMetadataTransform() {
        Map<String, String> metadataMapping = new LinkedHashMap<>();
        metadataMapping.put("Database", "database");
        metadataMapping.put("Table", "table");
        metadataMapping.put("Partition", "partition");
        metadataMapping.put("RowKind", "rowKind");
        metadataMapping.put("EventTime", "ts_ms");
        metadataMapping.put("Delay", "delay");
        Map<String, Object> config = new HashMap<>();
        config.put("metadata_fields", metadataMapping);
        MetadataTransform transform =
                new MetadataTransform(ReadonlyConfig.fromMap(config), catalogTable);
        transform.initRowContainerGenerator();

        Column[] columns = transform.getOutputColumns();
        Assertions.assertEquals("database", columns[0].getName());
        Assertions.assertEquals("table", columns[1].getName());
        Assertions.assertEquals("partition", columns[2].getName());
        Assertions.assertEquals("rowKind", columns[3].getName());
        Assertions.assertEquals("ts_ms", columns[4].getName());
        Assertions.assertEquals("delay", columns[5].getName());

        Assertions.assertEquals(BasicType.STRING_TYPE, columns[0].getDataType());
        Assertions.assertEquals(BasicType.STRING_TYPE, columns[1].getDataType());
        Assertions.assertEquals(ArrayType.STRING_ARRAY_TYPE, columns[2].getDataType());
        Assertions.assertEquals(BasicType.STRING_TYPE, columns[3].getDataType());
        Assertions.assertEquals(BasicType.LONG_TYPE, columns[4].getDataType());
        Assertions.assertEquals(BasicType.LONG_TYPE, columns[5].getDataType());

        Assertions.assertInstanceOf(PhysicalColumn.class, columns[0]);
        Assertions.assertInstanceOf(PhysicalColumn.class, columns[5]);

        SeaTunnelRow outputRow = transform.map(inputRow);
        Assertions.assertEquals(values.length + 6, outputRow.getArity());
        Assertions.assertEquals("default.default.default", outputRow.getTableId());
        Assertions.assertEquals(RowKind.INSERT, outputRow.getRowKind());
        Assertions.assertEquals("value1", outputRow.getField(0));
        Assertions.assertEquals(1, outputRow.getField(1));
        Assertions.assertEquals(896657703886127105L, outputRow.getField(2));
        Assertions.assertEquals(3.1415916, outputRow.getField(3));
        Assertions.assertEquals(3.14, outputRow.getField(4));
        Assertions.assertEquals("default", outputRow.getField(5));
        Assertions.assertEquals("default", outputRow.getField(6));
        Assertions.assertArrayEquals(
                new String[] {"key1", "key2"}, (String[]) outputRow.getField(7));
        Assertions.assertEquals("+I", outputRow.getField(8));
        Assertions.assertEquals(eventTime, outputRow.getField(9));
        Assertions.assertEquals(150L, outputRow.getField(10));
    }

    @Test
    void shouldProjectKnowledgeSyncMetadataFromRowOptions() {
        Map<String, String> metadataMapping = new LinkedHashMap<>();
        metadataMapping.put(
                KnowledgeSyncMetadataField.DOCUMENT_ID.getName(),
                KnowledgeSyncMetadataField.DOCUMENT_ID.getPhysicalName());
        metadataMapping.put(
                KnowledgeSyncMetadataField.CHUNK_HASH.getName(),
                KnowledgeSyncMetadataField.CHUNK_HASH.getPhysicalName());
        Map<String, Object> config = new HashMap<>();
        config.put("metadata_fields", metadataMapping);
        MetadataTransform transform =
                new MetadataTransform(
                        ReadonlyConfig.fromMap(config), knowledgeSyncCatalogTable(true));
        transform.initRowContainerGenerator();

        Column[] columns = transform.getOutputColumns();
        Assertions.assertEquals("document_id", columns[0].getName());
        Assertions.assertEquals("chunk_hash", columns[1].getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, columns[0].getDataType());
        Assertions.assertEquals(BasicType.STRING_TYPE, columns[1].getDataType());
        Assertions.assertInstanceOf(PhysicalColumn.class, columns[0]);
        Assertions.assertInstanceOf(PhysicalColumn.class, columns[1]);
        Assertions.assertFalse(columns[0].isNullable());
        Assertions.assertTrue(columns[1].isNullable());

        SeaTunnelRow input = new SeaTunnelRow(new Object[] {"chunk text"});
        input.getOptions().put(KnowledgeSyncMetadataField.DOCUMENT_ID.getName(), "doc_faq");
        input.getOptions().put(KnowledgeSyncMetadataField.CHUNK_HASH.getName(), "hash_chunk_0");

        SeaTunnelRow output = transform.map(input);
        Assertions.assertEquals(3, output.getArity());
        Assertions.assertEquals("chunk text", output.getField(0));
        Assertions.assertEquals("doc_faq", output.getField(1));
        Assertions.assertEquals("hash_chunk_0", output.getField(2));
        Assertions.assertArrayEquals(
                new String[] {"text", "document_id", "chunk_hash"},
                transform.getProducedCatalogTable().getTableSchema().getFieldNames());
    }

    @Test
    void shouldRejectKnowledgeSyncMetadataWhenSchemaDoesNotDeclareIt() {
        Map<String, String> metadataMapping = new LinkedHashMap<>();
        metadataMapping.put(
                KnowledgeSyncMetadataField.DOCUMENT_ID.getName(),
                KnowledgeSyncMetadataField.DOCUMENT_ID.getPhysicalName());
        Map<String, Object> config = new HashMap<>();
        config.put("metadata_fields", metadataMapping);
        MetadataTransform transform =
                new MetadataTransform(
                        ReadonlyConfig.fromMap(config), knowledgeSyncCatalogTable(false));

        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class, transform::initRowContainerGenerator);
        Assertions.assertTrue(
                exception.getMessage().contains(KnowledgeSyncMetadataField.DOCUMENT_ID.getName()));
    }

    @Test
    void shouldProjectMarkdownKnowledgeSyncMetadataWithAliases() {
        Map<String, String> metadataMapping = new LinkedHashMap<>();
        metadataMapping.put(KnowledgeSyncMetadataField.SOURCE_URI.getName(), "ks_source_uri");
        metadataMapping.put(KnowledgeSyncMetadataField.DOCUMENT_ID.getName(), "ks_document_id");
        metadataMapping.put(KnowledgeSyncMetadataField.DOCUMENT_HASH.getName(), "document_hash");
        metadataMapping.put(KnowledgeSyncMetadataField.CHUNK_HASH.getName(), "chunk_hash");
        Map<String, Object> config = new HashMap<>();
        config.put("metadata_fields", metadataMapping);
        MetadataTransform transform =
                new MetadataTransform(ReadonlyConfig.fromMap(config), markdownCatalogTable());
        transform.initRowContainerGenerator();
        SeaTunnelRow input = new SeaTunnelRow(new Object[13]);
        input.getOptions().put(KnowledgeSyncMetadataField.SOURCE_URI.getName(), "safe/source.md");
        input.getOptions().put(KnowledgeSyncMetadataField.DOCUMENT_ID.getName(), "doc_safe");
        input.getOptions().put(KnowledgeSyncMetadataField.DOCUMENT_HASH.getName(), "doc_hash");
        input.getOptions().put(KnowledgeSyncMetadataField.CHUNK_HASH.getName(), "chunk_hash_value");

        SeaTunnelRow output = transform.map(input);

        Assertions.assertEquals("safe/source.md", output.getField(13));
        Assertions.assertEquals("doc_safe", output.getField(14));
        Assertions.assertEquals("doc_hash", output.getField(15));
        Assertions.assertEquals("chunk_hash_value", output.getField(16));
    }

    @Test
    void shouldProjectConnectorDeclaredTypedMetadataField() {
        CatalogTable table =
                catalogTableWithCustomMetadata(
                        Collections.singletonList(
                                MetadataColumn.of(
                                        "KafkaOffset",
                                        BasicType.LONG_TYPE,
                                        19L,
                                        true,
                                        null,
                                        "Kafka record offset")));
        MetadataTransform transform =
                new MetadataTransform(
                        ReadonlyConfig.fromMap(metadataFieldsConfig("KafkaOffset", "kafka_offset")),
                        table);
        transform.initRowContainerGenerator();

        Column[] columns = transform.getOutputColumns();
        Assertions.assertEquals(1, columns.length);
        Assertions.assertEquals("kafka_offset", columns[0].getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, columns[0].getDataType());
        Assertions.assertTrue(columns[0].isNullable());
        Assertions.assertEquals(19L, columns[0].getColumnLength());
        Assertions.assertNull(columns[0].getDefaultValue());
        Assertions.assertEquals("Kafka record offset", columns[0].getComment());
        Assertions.assertInstanceOf(PhysicalColumn.class, columns[0]);

        SeaTunnelRow input = new SeaTunnelRow(new Object[] {"payload"});
        input.getOptions().put("KafkaOffset", 42L);

        SeaTunnelRow output = transform.map(input);
        Assertions.assertEquals(2, output.getArity());
        Assertions.assertEquals("payload", output.getField(0));
        Assertions.assertEquals(42L, output.getField(1));
    }

    @Test
    void shouldRejectUndeclaredConnectorMetadataKey() {
        CatalogTable table = catalogTableWithCustomMetadata(Collections.emptyList());
        Map<String, Object> config = metadataFieldsConfig("KafkaOffset", "kafka_offset");

        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class,
                        () -> new MetadataTransform(ReadonlyConfig.fromMap(config), table));
        Assertions.assertTrue(exception.getMessage().contains("KafkaOffset"));
    }

    @Test
    void shouldRejectConnectorMetadataOutputNameCollision() {
        CatalogTable table =
                catalogTableWithCustomMetadata(
                        Collections.singletonList(
                                MetadataColumn.of(
                                        "KafkaOffset",
                                        BasicType.LONG_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        "Kafka record offset")));

        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                new MetadataTransform(
                                        ReadonlyConfig.fromMap(
                                                metadataFieldsConfig("KafkaOffset", "payload")),
                                        table));
        Assertions.assertTrue(exception.getMessage().contains("KafkaOffset"));
    }

    @Test
    void shouldRejectPhysicalColumnLookalikeThatIsNotMetadata() {
        CatalogTable table =
                CatalogTable.of(
                        TableIdentifier.of("catalog", TablePath.DEFAULT),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "payload",
                                                BasicType.STRING_TYPE,
                                                (Long) null,
                                                true,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "KafkaOffset",
                                                BasicType.LONG_TYPE,
                                                (Long) null,
                                                true,
                                                null,
                                                "physical lookalike"))
                                .build(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        "comment",
                        "test",
                        MetadataSchema.builder().build());

        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                new MetadataTransform(
                                        ReadonlyConfig.fromMap(
                                                metadataFieldsConfig(
                                                        "KafkaOffset", "kafka_offset")),
                                        table));
        Assertions.assertTrue(exception.getMessage().contains("KafkaOffset"));
    }

    @Test
    void shouldProjectNullConnectorMetadataValue() {
        CatalogTable table =
                catalogTableWithCustomMetadata(
                        Collections.singletonList(
                                MetadataColumn.of(
                                        "KafkaOffset",
                                        BasicType.LONG_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        "Kafka record offset")));
        MetadataTransform transform =
                new MetadataTransform(
                        ReadonlyConfig.fromMap(metadataFieldsConfig("KafkaOffset", "kafka_offset")),
                        table);
        transform.initRowContainerGenerator();

        SeaTunnelRow input = new SeaTunnelRow(new Object[] {"payload"});
        input.getOptions().put("KafkaOffset", null);

        SeaTunnelRow output = transform.map(input);
        Assertions.assertEquals(2, output.getArity());
        Assertions.assertEquals("payload", output.getField(0));
        Assertions.assertNull(output.getField(1));
    }

    @Test
    void shouldRejectConnectorMetadataKeyCaseMismatch() {
        CatalogTable table =
                catalogTableWithCustomMetadata(
                        Collections.singletonList(
                                MetadataColumn.of(
                                        "KafkaOffset",
                                        BasicType.LONG_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        "Kafka record offset")));

        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                new MetadataTransform(
                                        ReadonlyConfig.fromMap(
                                                metadataFieldsConfig(
                                                        "kafkaoffset", "kafka_offset")),
                                        table));
        Assertions.assertTrue(exception.getMessage().contains("kafkaoffset"));
    }

    @Test
    void shouldProjectMultipleConnectorDeclaredFields() {
        List<Column> metadata = new ArrayList<>();
        metadata.add(
                MetadataColumn.of(
                        "KafkaOffset",
                        BasicType.LONG_TYPE,
                        (Long) null,
                        true,
                        null,
                        "Kafka record offset"));
        metadata.add(
                MetadataColumn.of(
                        "KafkaPartition",
                        BasicType.INT_TYPE,
                        (Long) null,
                        false,
                        0,
                        "Kafka partition id"));
        CatalogTable table = catalogTableWithCustomMetadata(metadata);

        Map<String, String> metadataMapping = new LinkedHashMap<>();
        metadataMapping.put("KafkaOffset", "kafka_offset");
        metadataMapping.put("KafkaPartition", "kafka_partition");
        Map<String, Object> config = new HashMap<>();
        config.put("metadata_fields", metadataMapping);

        MetadataTransform transform = new MetadataTransform(ReadonlyConfig.fromMap(config), table);
        transform.initRowContainerGenerator();

        Column[] columns = transform.getOutputColumns();
        Assertions.assertEquals("kafka_offset", columns[0].getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, columns[0].getDataType());
        Assertions.assertTrue(columns[0].isNullable());
        Assertions.assertEquals("Kafka record offset", columns[0].getComment());
        Assertions.assertEquals("kafka_partition", columns[1].getName());
        Assertions.assertEquals(BasicType.INT_TYPE, columns[1].getDataType());
        Assertions.assertFalse(columns[1].isNullable());
        Assertions.assertEquals(0, columns[1].getDefaultValue());
        Assertions.assertEquals("Kafka partition id", columns[1].getComment());

        SeaTunnelRow input = new SeaTunnelRow(new Object[] {"payload"});
        input.getOptions().put("KafkaOffset", 42L);
        input.getOptions().put("KafkaPartition", 3);

        SeaTunnelRow output = transform.map(input);
        Assertions.assertEquals(3, output.getArity());
        Assertions.assertEquals("payload", output.getField(0));
        Assertions.assertEquals(42L, output.getField(1));
        Assertions.assertEquals(3, output.getField(2));
    }

    @Test
    void shouldKeepComputedAndCommonMetadataKeysUnchanged() {
        Map<String, String> metadataMapping = new LinkedHashMap<>();
        metadataMapping.put("Database", "database");
        metadataMapping.put("Table", "table");
        metadataMapping.put("RowKind", "rowKind");
        metadataMapping.put("EventTime", "ts_ms");
        Map<String, Object> config = new HashMap<>();
        config.put("metadata_fields", metadataMapping);

        MetadataTransform transform =
                new MetadataTransform(ReadonlyConfig.fromMap(config), catalogTable);
        transform.initRowContainerGenerator();

        Column[] columns = transform.getOutputColumns();
        Assertions.assertEquals(BasicType.STRING_TYPE, columns[0].getDataType());
        Assertions.assertEquals(BasicType.STRING_TYPE, columns[1].getDataType());
        Assertions.assertEquals(BasicType.STRING_TYPE, columns[2].getDataType());
        Assertions.assertEquals(BasicType.LONG_TYPE, columns[3].getDataType());

        SeaTunnelRow outputRow = transform.map(inputRow);
        Assertions.assertEquals("default", outputRow.getField(5));
        Assertions.assertEquals("default", outputRow.getField(6));
        Assertions.assertEquals("+I", outputRow.getField(7));
        Assertions.assertEquals(eventTime, outputRow.getField(8));
    }

    @Test
    void shouldRejectMarkdownCanonicalPhysicalNameCollision() {
        Map<String, String> metadataMapping = new LinkedHashMap<>();
        metadataMapping.put(
                KnowledgeSyncMetadataField.DOCUMENT_ID.getName(),
                KnowledgeSyncMetadataField.DOCUMENT_ID.getPhysicalName());
        Map<String, Object> config = new HashMap<>();
        config.put("metadata_fields", metadataMapping);

        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                new MetadataTransform(
                                        ReadonlyConfig.fromMap(config), markdownCatalogTable()));

        Assertions.assertTrue(
                exception.getMessage().contains(KnowledgeSyncMetadataField.DOCUMENT_ID.getName()));
    }

    private static Map<String, Object> metadataFieldsConfig(String metadataKey, String outputName) {
        Map<String, String> metadataMapping = new LinkedHashMap<>();
        metadataMapping.put(metadataKey, outputName);
        Map<String, Object> config = new HashMap<>();
        config.put("metadata_fields", metadataMapping);
        return config;
    }

    private static CatalogTable catalogTableWithCustomMetadata(List<Column> metadataColumns) {
        return CatalogTable.of(
                TableIdentifier.of("catalog", TablePath.DEFAULT),
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "payload",
                                        BasicType.STRING_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        null))
                        .build(),
                new HashMap<>(),
                new ArrayList<>(),
                "comment",
                "test",
                MetadataSchema.builder().columns(metadataColumns).build());
    }

    private static CatalogTable knowledgeSyncCatalogTable(boolean includeKnowledgeSyncMetadata) {
        List<Column> metadata = new ArrayList<>();
        if (includeKnowledgeSyncMetadata) {
            metadata.add(KnowledgeSyncMetadataField.DOCUMENT_ID.toMetadataColumn());
            metadata.add(KnowledgeSyncMetadataField.CHUNK_HASH.toMetadataColumn());
        }
        return CatalogTable.of(
                TableIdentifier.of("catalog", TablePath.DEFAULT),
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "text",
                                        BasicType.STRING_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        null))
                        .build(),
                new HashMap<>(),
                new ArrayList<>(),
                "comment",
                "test",
                MetadataSchema.builder().columns(metadata).build());
    }

    private static CatalogTable markdownCatalogTable() {
        String[] fieldNames = {
            "element_id",
            "element_type",
            "heading_level",
            "text",
            "page_number",
            "position_index",
            "parent_id",
            "child_ids",
            "source_uri",
            "document_id",
            "chunk_id",
            "chunk_index",
            "content_hash"
        };
        org.apache.seatunnel.api.table.type.SeaTunnelDataType<?>[] fieldTypes = {
            BasicType.STRING_TYPE,
            BasicType.STRING_TYPE,
            BasicType.INT_TYPE,
            BasicType.STRING_TYPE,
            BasicType.INT_TYPE,
            BasicType.INT_TYPE,
            BasicType.STRING_TYPE,
            BasicType.STRING_TYPE,
            BasicType.STRING_TYPE,
            BasicType.STRING_TYPE,
            BasicType.STRING_TYPE,
            BasicType.INT_TYPE,
            BasicType.STRING_TYPE
        };
        TableSchema.Builder tableSchema = TableSchema.builder();
        for (int i = 0; i < fieldNames.length; i++) {
            tableSchema.column(
                    PhysicalColumn.of(fieldNames[i], fieldTypes[i], (Long) null, true, null, null));
        }
        List<Column> metadata = new ArrayList<>();
        metadata.add(KnowledgeSyncMetadataField.SOURCE_URI.toMetadataColumn());
        metadata.add(KnowledgeSyncMetadataField.DOCUMENT_ID.toMetadataColumn());
        metadata.add(KnowledgeSyncMetadataField.DOCUMENT_HASH.toMetadataColumn());
        metadata.add(KnowledgeSyncMetadataField.CHUNK_HASH.toMetadataColumn());
        return CatalogTable.of(
                TableIdentifier.of("catalog", TablePath.DEFAULT),
                tableSchema.build(),
                new HashMap<>(),
                new ArrayList<>(),
                "comment",
                "test",
                MetadataSchema.builder().columns(metadata).build());
    }
}
